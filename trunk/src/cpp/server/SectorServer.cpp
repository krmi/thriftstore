/**
 * Copyright (C) 2008-2009  Open Data ("Open Data" refers to
 * one or more of the following companies: Open Data Partners LLC,
 * Open Data Research LLC, or Open Data Capital LLC.)
 */
#include "DfsService.h"
#include "DfsService_constants.h"
#include "constant.h"
#include "index.h"
#include "fsclient.h"
#include <iostream>
#include <sstream>
#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <transport/TTransportUtils.h>
#include <vector>
#include "stringutil.h"

using std::cerr;
using std::cout;
using std::endl;
using std::stringstream;

using namespace facebook::thrift;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::server;

using boost::shared_ptr;

using namespace dfsservice;

/*
 * Default port for Thrift server. Can be over-ridden at the command line.
 */
const int DEFAULT_PORT = 9090;

/*
 * Implementation of a Thrift server to access the Sector filesystem.
 */
class DfsServiceHandler : virtual public DfsServiceIf
{
private:
    /*
     * Container to hold references to Sector clients. When a new client is
     * created it will be added to this map, and then retrieved for
     * subsequent operations.
     */
    map<int64_t, Sector*> clientHandleMap;
    /*
     * Container to hold references to Sector filehandles. Filehandles will
     * get added on file open, and then retrieved for subsequent operations
     * on the file.
     */
    map<int64_t, SectorFile*> dfsHandleMap;
    /*
     * clientHandle key.
     */
    int64_t clientId;
    /*
     * filehandle key.
     */
    int64_t dfsId;

public:
    /*
     * Initialize the ID's used as map keys.
     */
    DfsServiceHandler()
    {
        timeval t;
        gettimeofday( &t, 0 );
        clientId = (int64_t)t.tv_sec;
        dfsId = (int64_t)t.tv_sec;
    }

    /*
     * Connect to Sector.
     *
     * uri is of the form "sector://<host>:<port>" where host is the
     * hostname or IP of the server running the Sector master, and port
     * is the port number of the master.
     *
     * If initialization fails, the ClientHandle.id will be populated with
     * the return code from Sector. This value should be checked for a negative
     * value before performing further operations with the client.
     */
    void init( ClientHandle& clientHandle, const std::string& uri )
    {
        int status = 0;
        vector<string> tokens;

        Tokenize( uri, tokens, "/:" );
        string dfs = tokens[0];
        string ip = tokens[1];
        string port = tokens[2];

        Sector* client = new Sector();
        status = client->init( ip.c_str(), atoi( port.c_str() ) );
        if( status < 0 ) {
            clientHandle.id = status;
        } else {
            // Cache the client handle:
            clientHandle.id = clientId;
            clientHandleMap[clientId] = client;
        }
        
        clientId++;
    }

    /*
     * Login into Sector.
     *
     * clientHandle is the handle to the Sector client returned by init().
     *
     * Returns true on successful login, false otherwise.
     */
    bool login( const ClientHandle& clientHandle,
                const std::string& user,
                const std::string& password )
    {
        // Retrieve reference to the Sector client:
        map<int64_t, Sector*>::iterator iter =
            clientHandleMap.find( clientHandle.id );
        if( iter == clientHandleMap.end() ) {
            cerr << "SectorServer.login() - failed to find client reference"
                 << endl;
            return false;
        }
        Sector* client = iter->second;
        
        int status = client->login( user.c_str(), password.c_str() );
        if( status < 0 ) {
            cerr << "SectorServer.login() - login failed, return=" << status <<
                endl;
            return false ;
        }
        
        return true;
    }

    /*
     * Logout of Sector.
     *
     * clientHandle is the handle to the Sector client returned by init().
     *
     * Returns true on success, false otherwise.
     */
    bool logout( const ClientHandle& clientHandle )
    {
        // Retrieve reference to the Sector client:
        map<int64_t, Sector*>::iterator iter =
            clientHandleMap.find( clientHandle.id );
        if( iter == clientHandleMap.end() ) {
            cerr << "SectorServer.logout() - failed to find client reference"
                 << endl;
            return false;
        }
        Sector* client = iter->second;

        int status = client->logout();
        if( status < 0 ) {
            cerr << "SectorServer.logout() - logout failed, return=" <<
                status << endl;
            return false;
        }
        
        return true;
    }

    /*
     * Close connection to Sector and release resources. This should always be
     * called when the client is finished processing.
     *
     * clientHandle is the handle to the Sector client returned by init().
     *
     * Returns true on success, false if an error occurs.
     */
    bool closeClient( const ClientHandle& clientHandle )
    {
        // Retrieve reference to the Sector client:
        map<int64_t, Sector*>::iterator iter =
            clientHandleMap.find( clientHandle.id );
        if( iter == clientHandleMap.end() ) {
            cerr << "SectorServer.closeClient() - failed to find client reference"
                 << endl;
            return false;
        }
        Sector* client = iter->second;
        
        int status = client->close();

        free( client );
        clientHandleMap.erase( clientHandle.id );
        
        if( status < 0 ) {
            cerr << "SectorServer.closeClient() - close failed, return=" << status
                 << endl;
            return false;
        }

        return true;
    }

    /*
     * Release all resources used by this server.
     */
    bool closeDfs()
    {
        return true;
    }
    
    /*
     * List files in the path argument. This is the Sector equivalent of "ls".
     *
     * clientHandle is the handle to the Sector client returned by init().
     *
     * On return the _return vector will be populated with the FileStatus
     * objects representing the files/dirs in the path argument.
     */
    void listFiles( vector<FileStatus> & _return,
                    const ClientHandle& clientHandle, 
                    const std::string& path )
    {
        // Retrieve reference to the Sector client:
        map<int64_t, Sector*>::iterator iter =
            clientHandleMap.find( clientHandle.id );
        if( iter == clientHandleMap.end() ) {
            cerr << "SectorServer.listFiles() - failed to find client reference"
                 << endl;
            return;
        }
        Sector* client = iter->second;

        vector<SNode> filelist;
        int status = client->list( path, filelist );
        if( status < 0 ) {
            cerr << "SectorServer.listFiles() - listing of " << path <<
                " failed, return code is " << status;
            DfsServiceIOException ex;
            stringstream s;
            s << "list for " << path << " failed, return code=" << status;
            ex.message = s.str();
            throw ex;
        }

        for( vector<SNode>::size_type i = 0; i < filelist.size(); i++ ) {
            SNode snode = filelist[i];
            FileStatus stat;
            populateFileStatus( stat, snode );
            _return.push_back( stat );
        }
    }

    /*
     * Retrieve info on a file/dir in Sector.
     *
     * clientHandle is the handle to the Sector client returned by init().
     */
    void stat( FileStatus& _return,
               const ClientHandle& clientHandle, 
               const std::string& path )
    {
        // Retrieve reference to the Sector client:
        map<int64_t, Sector*>::iterator iter =
            clientHandleMap.find( clientHandle.id );
        if( iter == clientHandleMap.end() ) {
            cerr << "SectorServer.stat() - failed to find client reference"
                 << endl;
            return;
        }
        Sector* client = iter->second;

        SNode attr;
        
        int status = client->stat( path, attr );
        if( status < 0 ) {
            cerr << "SectorServer.stat() - failed to stat " << path <<
                ", return code is " << status << endl;
            DfsServiceIOException ex;
            stringstream s;
            s << "Failed to stat " << path << ", return code=" << status;
            ex.message = s.str();
            throw ex;
        }

        populateFileStatus( _return, attr );
    }

    /*
     * Convert a Sector SNode object into a FileStatus object.
     */
    void populateFileStatus( FileStatus& fileStatus, SNode attr )
    {
        fileStatus.length = attr.m_llSize;
        fileStatus.isdir = attr.m_bIsDir;
        fileStatus.modification_time = attr.m_llTimeStamp;
        fileStatus.path = attr.m_strName;
    }
    
    /*
     * Make a new directory in the Sector filesystem. This will create
     * parent/child directories if the path argument is a nested directory.
     *
     * clientHandle is the handle to the Sector client returned by init().
     */
    bool mkdir( const ClientHandle& clientHandle, const std::string& path )
    {
        map<int64_t, Sector*>::iterator iter =
            clientHandleMap.find( clientHandle.id );
        if( iter == clientHandleMap.end() ) {
            cerr << "SectorServer.mkdir() - failed to find client reference"
                 << endl;
            return false;
        }
        Sector* client = iter->second;

        int status = client->mkdir( path );
        
        if( status < 0 ) {
            cerr << "SectorServer.mkdir() - failed to create dir " << path <<
                ", return code is " << status << endl;
            DfsServiceIOException ex;
            stringstream s;
            s << "Failed to create " << path << ", return code=" << status;
            ex.message = s.str();
            throw ex;
        }

        return true;
    }

    /*
     * Move a file from "oldpath" to "newpath".
     *
     * clientHandle is the handle to the Sector client returned by init().
     */
    bool move( const ClientHandle& clientHandle,
               const std::string& oldpath,
               const std::string& newpath )
    {
        map<int64_t, Sector*>::iterator iter =
            clientHandleMap.find( clientHandle.id );
        if( iter == clientHandleMap.end() ) {
            cerr << "SectorServer.move() - failed to find client reference"
                 << endl;
            return false;
        }
        Sector* client = iter->second;

        int status = client->move( oldpath, newpath );

        if( status < 0 ) {
            cerr << "SectorServer.move() - failed to move " << oldpath <<
                " to " << newpath << ", return code " << status << endl;
            DfsServiceIOException ex;
            stringstream s;
            s << "Failed to move " << oldpath << " to " << newpath <<
                ", return code=" << status;
            ex.message = s.str();
            throw ex;
            return false;
        }

        return( true );
    }

    /*
     * Remove the file/dir in the path argument from the Sector filesystem.
     * This call will recursively remove populated directories.
     *
     * clientHandle is the handle to the Sector client returned by init().
     * The recursive argument is currently ignored.
     */
    bool remove( const ClientHandle& clientHandle,
                 const std::string& path,
                 const bool recursive )
    {
        map<int64_t, Sector*>::iterator iter =
            clientHandleMap.find( clientHandle.id );
        if( iter == clientHandleMap.end() ) {
            cerr << "SectorServer.remove(): failed to find client reference"
                 << endl;
            return false;
        }
        Sector* client = iter->second;

        int status = client->remove( path );

        if( status < 0 ) {
            cerr << "SectorServer.remove() - failed to remove " << path <<
                ", return code is " << status << endl;
            DfsServiceIOException ex;
            stringstream s;
            s << "Failed to remove " << path << ", return code=" << status;
            ex.message = s.str();
            throw ex;
            return false;
        }

        return true;
    }

    /*
     * Open a Sector file.
     *
     * Mode is READ, WRITE, or READ_WRITE.
     *
     * If the open fails, the DfsHandle.id will be populated with the return
     * code from Sector. This value should be checked for a negative value
     * before performing further operations with the client.
     */
    void open( DfsHandle& dfsHandle,
               const std::string& filename,
               const int16_t mode )
    {
        SectorFile* f = new SectorFile();
        int status = f->open( filename.c_str(), mode );
        
        if( status < 0 ) {
            cerr << "SectorServer.open() - failed to open " << filename << endl;
            dfsHandle.id = status;
        } else {
            // Cache the file handle:
            dfsHandle.id = dfsId;
            dfsHandleMap[dfsId] = f;
        }

        dfsId++;
    }

    /*
     * Close a Sector file.
     *
     * dfsHandle is the file handle returned by open().
     *
     * Returns true on success, false otherwise.
     */
    bool close( const DfsHandle& dfsHandle )
    {
        map<int64_t, SectorFile*>::iterator iter =
            dfsHandleMap.find( dfsHandle.id );
        if( iter == dfsHandleMap.end() ) {
            cerr << "SectorServer.close(): failed to find filehandle reference"
                 << endl;
            return false;
        }
        SectorFile* f = iter->second;

        int status = f->close();

        free( f );
        dfsHandleMap.erase( dfsHandle.id );

        if( status < 0 ) {
            cerr << "SectorServer.close() failed, return code is " << status <<
                endl;
            return( false );
        }

        return( true );
    }

    /*
     * Read data from a Sector file.
     *
     * offset is file offset to start reading from. Pass in -1 to use the seek
     * pointer maintained by SectorFile.
     * len is amount of data to read.
     *
     * On success, _return param will contain data read. On failure or EOF,
     * _return will be set to an empty string.
     */
    void read( std::string& _return, const DfsHandle& dfsHandle,
               const int64_t offset, const int64_t len )
    {
        map<int64_t, SectorFile*>::iterator iter =
            dfsHandleMap.find( dfsHandle.id );
        if( iter == dfsHandleMap.end() ) {
            cerr << "SectorServer.read(): failed to find filehandle reference"
                 << endl;
            _return = "\0";
            return;
        }
        SectorFile* f = iter->second;

        if( f->eof() ) {
            _return = "";
            return;
        }
        
        int status = 0;
        
        if( offset >= 0 ) {
            status = f->seekg( offset, SF_POS::BEG );
            if( status < 0 ) {
                cerr << "SectorServer.read() - seekg to offset " << offset <<
                    ", failed, return code is " << status << endl;
                DfsServiceIOException ex;
                stringstream s;
                s << "Failed to seek to offset " << offset <<
                    ", return code=" << status;
                ex.message = s.str();
                throw ex;
            }
        }

        char* buf = new char[len];        
        status = f->read( buf, len );
        if( status < 0 ) {
            cerr << "SectorServer.read() - read failed, return code is " <<
                status << endl;
            _return = "";
        } else {
            buf[status] = '\0';
            _return = string( buf );
        }

        free( buf );
    }

    /*
     * Write data in buf to a file in Sector.
     *
     * offset is file offset to start writing to.
     * len is amount of data to write.
     */
    bool write( const DfsHandle& dfsHandle, const std::string& buf,
                const int64_t offset, const int64_t len )
    {
        map<int64_t, SectorFile*>::iterator iter =
            dfsHandleMap.find( dfsHandle.id );
        if( iter == dfsHandleMap.end() ) {
            cerr << "SectorServer.write(): failed to find filehandle reference"
                 << endl;
            return( false );
        }
        SectorFile* f = iter->second;

        int status = 0;
        
        if( offset >= 0 ) {
            status = f->seekp( offset, SF_POS::BEG );
            if( status < 0 ) {
                 cerr << "SectorServer.write() - seekp to offset " << offset <<
                    ", failed, return code is " << status << endl;
                DfsServiceIOException ex;
                stringstream s;
                s << "Failed to seek to offset " << offset <<
                    ", return code=" << status;
                ex.message = s.str();
                throw ex;
            }
        }

        status = f->write( buf.c_str(), len );

        if( status < 0 ) {
            cerr << "SectorServer.write() - write failed, return code is " <<
                status << endl;
            DfsServiceIOException ex;
            stringstream s;
            s << "Write failed, return code=" << status;
            ex.message = s.str();
            throw ex;
        }

        return( true );
    }

    /*
     * Copy file from Sector to local filesystem.
     *
     * src is path to Sector file.
     * dest is path to new local file.
     *
     * Returns true on success, false otherwise.
     */
    bool copyToLocalFile( const std::string& src, const std::string& dest )
    {
        SNode attr;

        int status = Sector::stat( src, attr );
        if( status < 0 ) {
            cerr << "SectorServer.copyToLocalFile() - can't stat source file "
                 << src << ", return code=" << status << endl;
             DfsServiceIOException ex;
             stringstream s;
             s << "Failed to stat source file " << src << ", return code="
               << status;
             ex.message = s.str();
             throw ex;
         }

         SectorFile f;

         status = f.open( src.c_str(), SF_MODE::READ );
         
         if( status < 0 ) {
             cerr << "SectorServer.copyToLocalFile() - can't open source file"
                  << src << ", return=" << status << endl;
             DfsServiceIOException ex;
             stringstream s;
             s << "Failed to open source file " << src << ", return code=" <<
                 status;
             ex.message = s.str();
             throw ex;
         }

         status = f.download( dest.c_str() );
         
         if( status < 0 ) {
             cerr << "SectorServer.copyToLocalFile() - download to " << dest <<
                 " failed" << ", return=" << status << endl;
             DfsServiceIOException ex;
             stringstream s;
             s << "Failed to copy Sector file " << src << " to local file " <<
                 dest << ", return code=" << status;
             ex.message = s.str();
             throw ex;
         }

         f.close();
         
         return true;
    }

    /*
     * Copy file from local filesystem into Sector.
     *
     * src is path to local file.
     * dest is path to new Sector file.
     *
     * Returns true on success, false if error occurs.
     */
    bool copyFromLocalFile( const std::string& src, const std::string& dest )
    {
        SectorFile f;

        int status = f.open( dest.c_str(), DfsServiceConstants().WRITE );
        
        if( status < 0 ) {
            cerr << "SectorServer.copyFromLocalFile() - failed to open " << dest
                 << ", return=" << status << endl;
            DfsServiceIOException ex;
            stringstream s;
            s << "Failed to open destination file " << dest <<
                ", return code=" << status;
            ex.message = s.str();
            throw ex;
        }

        status = f.upload( src.c_str() );
        if( status < 0 ) {
            cerr << "SectorServer.copyFromLocalFile() - f.upload() failed for "
                 << src << ", return=" << status << endl;
            DfsServiceIOException ex;
            stringstream s;
            s << "Failed to upload source file " << src << ", return code=" <<
                status;
            ex.message = s.str();
            throw ex;
            return false;
        }

        f.close();
        
        return true;
    }
};

int main(int argc, char **argv)
{
    int port = DEFAULT_PORT;

    if( argc > 2 ) {
        if( 0 == strcmp( argv[1], "-p" ) ) {
            port = atoi( argv[2] );
        }
    }

    shared_ptr<DfsServiceHandler> handler(new DfsServiceHandler());
    shared_ptr<TProcessor> processor(new DfsServiceProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    shared_ptr<ThreadManager> threadManager =
        ThreadManager::newSimpleThreadManager(25);
    shared_ptr<PosixThreadFactory> threadFactory =
        shared_ptr<PosixThreadFactory>(new PosixThreadFactory());
    threadManager->threadFactory(threadFactory);
    threadManager->start();
    TThreadPoolServer server(processor,
                             serverTransport,
                             transportFactory,
                             protocolFactory,
                             threadManager);

    cout << "starting the server..." << endl;
    server.serve();
    cout << "done" << endl;
    
    return 0;
}
