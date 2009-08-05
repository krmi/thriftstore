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

using namespace std;

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
        cout << "usage: readFile <Sector host> <Sector port> <src file> <dest file>"
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

        char* src = argv[3];
        char* dst = argv[4];

        client.open( dfsHandle, cl, src, DfsServiceConstants().READ );
        if( dfsHandle.id < 0 ){
            cerr << "open file failed, return=" << dfsHandle.id << endl;
            exit( -1 );
        }

        ofstream ofs( dst, ios::trunc );
        
        string _ret;
        do {
            client.read( _ret, cl, dfsHandle, -1, 4096 );
            ofs << _ret;
        } while( _ret.length() > 0 );
        
        client.close( cl, dfsHandle );
        ofs.close();
    } catch( DfsServiceIOException &ex ) {
        printf( "Caught DfsServiceIOException: %s\n", ex.message.c_str() );
    } catch (TException &tx) {
        printf("Caught TException: %s\n", tx.what());
    }

    client.logout( cl );
    client.closeClient( cl );
        
    transport->close();
}
