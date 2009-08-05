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
package com.opendatagroup.dfsservice.client;

// start thrift-generated files
import com.opendatagroup.dfsservice.ClientHandle;
import com.opendatagroup.dfsservice.DfsService;
import com.opendatagroup.dfsservice.DfsServiceIOException;
// end thrift-generated files

import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.transport.TSocket;
import com.facebook.thrift.transport.TTransport;

/**
 * Simple test case for cross-language.  ThriftStore exposes DFSs written in
 * Java and C++ over Thrift.  This is a simple test that uses a client written
 * in Java to access a Sector DFS.  The Sector Server code called by Thrift is
 * written in C++ since that is the language of Sector.  Generally, you are not
 * aware of the language on the other side of the IDL-- that is the point-- but
 * this is just ot demostrate that clients in any of the supported languages can
 * talk to anything on the other side of the Thrift Server.
 */
public final class MkDir
{
    // THESE HOULD BE MODIFIED FOR YOUR TEST ENVIRONMENT.

    /** IP of the Thrift Server. */
    public final static String THRIFT_SERVER = "localhost";
    /** Port the Thrift Server is listening on. */
    public final static int THRIFT_PORT = 9090;

    /** The IP of the Sector Master. */
    public final static String SECTOR_MASTER_IP = "localhost";
    /** The port the Sector Master is listening on. */
    public final static String SECTOR_MASTER_PORT = "6000";

    /**
     * This is intended to be run from the command line, so external
     * instantiation is not necessary.  All of the internal data fields are
     * public so that they appear in the javadocs for educational purposes.
     */
    private MkDir()
    {
    }

    /**
     * Attempts to reach the Sector DFS via the Thrift Server running at
     * {@link #THRIFT_SERVER} on port {@link #THIRFT_PORT}.  It then calls
     * <ul>
     *   <li>init</li>
     *   <li>login</li>
     *   <li>mkdir</li>
     * <ul>
     * In the finally block, the connection to the DFS is taken down by calls to
     * <ul>
     *   <li>logout</li>
     *   <li>closeClient</li>
     *   <li>closeDfs</li>
     * <ul>
     *
     * @see DfsService.Client
     */
    public static void main( String[] args )
    {
        try {
            TTransport transport = new TSocket( THRIFT_SERVER, THRIFT_PORT );
            TProtocol protocol = new TBinaryProtocol( transport );
            DfsService.Client client = new DfsService.Client( protocol );
            transport.open();

            ClientHandle chandle = null;
            try {
                boolean success = false;
                // INIT
                chandle = client.init( "sector://" + SECTOR_MASTER_IP
                    + ":" + SECTOR_MASTER_PORT );
                System.out.println( "init result=" + chandle.id );
                // LOGIN
                success = client.login( chandle, "test", "xxx" );
                System.out.println( "login result=" + success );
                // MKDIR
                success = client.mkdir( chandle, "testdir_fromjava" );
                System.out.println( "mkdir result= " + success );
            } catch ( DfsServiceIOException dsioe ) {
                System.out.println( "Client caught DfsServiceIOException" );
                dsioe.printStackTrace();
            } finally {
                // LOGOUT
                client.logout( chandle );
                // CLOSECLIENT
                client.closeClient( chandle );
                // CLOSEDFS
                client.closeDfs();
            }

            transport.close();

        } catch ( TException tex ) {
            tex.printStackTrace();
        }
    }
}
