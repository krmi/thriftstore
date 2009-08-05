/*
 * Copyright (C) 2008-2009  Open Data ("Open Data" refers to
 * one or more of the following companies: Open Data Partners LLC,
 * Open Data Research LLC, or Open Data Capital LLC.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 */
#include <fstream>
#include <iostream>
#include <string>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include "../gen-cpp/DfsService.h"
#include "../gen-cpp/DfsService_constants.h"

using std::cerr;
using std::cout;
using std::endl;
using std::string;
using std::ifstream;
using std::ios;

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
        cout << "usage: writeFile <Sector host> <Sector port> <src file> <dest file>"
             << endl;
        exit( 0 );
    }

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

        DfsHandle dfsHandle;
        client.open( dfsHandle, cl, argv[4], DfsServiceConstants().WRITE );
        if( dfsHandle.id < 0 ){
            cerr << "open file failed, return=" << dfsHandle.id << endl;
            exit( -1 );
        }

        ifstream in( argv[3], ios::in|ios::binary );
        if( !in.is_open() ) {
            cerr << "unable to open src file" << endl;
            exit( -1 );
        }
        
        char* buf = new char[4096];
        while( !in.eof() ) {
            in.read( buf, 4096 );
            string s( buf );
            status = client.write( cl, dfsHandle, s, -1, in.gcount() );
            if( status < 0 ) {
                cerr << "write failed, return=" << status << endl;
                exit( -1 );
            }
        }

        delete [] buf;
        
        in.close();
        client.close( cl, dfsHandle );
    } catch( DfsServiceIOException &ex ) {
        printf( "Caught DfsServiceIOException: %s\n", ex.message.c_str() );
    } catch (TException &tx) {
        printf("Caught TException: %s\n", tx.what());
    }

    client.logout( cl );
    client.closeClient( cl );
        
    transport->close();
}
