#include <iostream>
#include <string>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include "../gen-cpp/DfsService.h"

using std::cerr;
using std::cout;
using std::endl;
using std::string;

using namespace facebook::thrift;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;

using namespace dfsservice;

using namespace boost;

int main(int argc, char** argv)
{
    shared_ptr<TTransport> socket(new TSocket("localhost", 9090));
    shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    DfsServiceClient client(protocol);

    bool status;
    ClientHandle cl;
    
    if( 5 != argc ) {
        cout << "usage: copyToLocalFile <Sector host> <Sector port> <src file> <dest file>"
             << endl;
        exit( 0 );
    }

    string src = string( argv[3] );
    string dest = string( argv[4] );

    string uri;
    uri.append( "sector://" ).append( string( argv[1] ) ).
        append( ":" ).append( string( argv[2] ) );
    
    try {
        transport->open();

        client.init( cl, uri );
        if( cl.id < 0 ) {
            cerr << "init failed, return= " << cl.id << endl;
            exit( -1 );
        }
        
        if( !client.login( cl, "test", "xxx" ) ) {
            cerr << "login failed" << endl;
            exit( -1 );
        }

        if( !client.copyToLocalFile( src, dest ) ) {
            cerr << "copyFromLocalFile failed" << endl;
        }
        
        client.logout( cl );
        client.closeClient( cl );
        transport->close();  
    } catch( DfsServiceIOException &ex ) {
        printf( "Caught DfsServiceIOException: %s\n", ex.message.c_str() );
    } catch (TException &tx) {
        printf("Caught TException: %s\n", tx.what());
    }
}
