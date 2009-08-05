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
import com.opendatagroup.dfsservice.Constants;
import com.opendatagroup.dfsservice.DfsHandle;
import com.opendatagroup.dfsservice.DfsService;
import com.opendatagroup.dfsservice.DfsServiceIOException;
import com.opendatagroup.dfsservice.FileStatus;
// end thrift-generated files

import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.transport.TTransport;
import com.facebook.thrift.transport.TSocket;

import java.io.File;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Tests calls to the Thrift Server and the backing DFS.  Some of these calls
 * are expected to succeed as others to fail.  Which ones are which are listed
 * in the code.  All of the calls are timed using calls to
 * <code>System.currentTimeMillis();</code>.  The status is sent to System.out.
 * <p>
 * This expects certain files and directories to be available in order to test
 * the different File System commands.  You can use any files that you like,
 * just change the public declarations in this file.
 */
public class DfsClient
{
    /**
     * Used if a port number is not passed from the command line.  This value is
     * also used if the pased in value cannot be parsed as an int.
     */ 
    public static final int DEFAULT_PORT = 9090;

    /**
     * Depending on what actual DFS is running behind the server, you may want
     * to set this value.  For example, on a Hadoop instance, if an absolute
     * path for the distributed file such as
     * <code>/user/cbennett/input_dir/input_file.dat</code> is passed in, then
     * that path is used as long as it is valid.  If the path you pass to the
     * server is <em>not an absolute path</em>, then the passed in relative path
     * is appended to whatever the hadoop master node is using as it's base
     * directory.
     * <p>
     * The value defined here is used to avoid any potential ambiguity.  In your
     * code you are free to
     * <ul>
     *   <li> ignore it</li>
     *   <li> use the base directory of your DFS</li>
     *   <li> use '.' </li>
     *   <li> use something else</li>
     * </ul>
     */
    public static final String BASE_DIR = ".";

    /**
     * Directory to use on the DFS.  This must exist if file creation does not
     * create intermediate directories.  It is eventually deleted by test of
     * remove.
     */
    public static final String DFS_WORKING_DIR = BASE_DIR + "/input";

    /**
     * In the test for copyFromLocal, this is the file that is attempted to be
     * uploaded to the DFS.  If the file exists, the call should succeed.  This
     * file is also used as the local file which is read in and whose contents
     * are written to the DFS in the write test.  The contents are writte to
     * DFS_FILE_NEW_WRITE.
     */
    public static final String LOCAL_FILE_FOR_UPLOAD_SRC =
        "/tmp/local_data/local_file.dat";

    /**
     * In the test for copyFromLocal, this is where LOCAL_FILE_FOR_UPLOAD_SRC is
     * uploaded to.  This is also used as the source file for the copy from DFS
     * to local disk test and as the source file for the DFS move.  It will have
     * <code>.moved</code> appended to the filename after that test, if it is
     * successful.
     */
    public static final String LOCAL_FILE_FOR_UPLOAD_DEST =
        DFS_WORKING_DIR + "/from_local.dat";

    /**
     * In the test for copyToLocal, this is where LOCAL_FILE_FOR_UPLOAD_DEST is
     * download to.  It is a path to a file on the local disk.
     */
    public static final String DFS_FILE_FOR_DOWNLOAD_DEST =
        "/tmp/remote_data/dfs_file.dat";

    /**
     * Destination file name in the move test for the file loaded into the DFS
     * from the copyFromLocal test.  This is used as a source file for the tests
     *
     * <ul>
     * <li>listFiles</li>
     * <li>stat</li>
     * <li>open / close for READ</li>
     * </ul>
     *
     * which do not modify it.  It is also used in the remove a file tests.  The
     * first one deletes it and then the next test tries to delete it again
     * whcih fails.
     */
    public static final String DFS_FILE_MOVED =
        LOCAL_FILE_FOR_UPLOAD_DEST + ".moved";

    /**
     * New directory to be created on the DFS in the mkdir test.  This is also
     * used to test removing an empty directory, so no files are added it.
     */
    public static final String NEW_DFS_DIR = BASE_DIR + "/newdir";

    /**
     * This is the where the contents of LOCAL_FILE_FOR_UPLOAD_SRC will be
     * written to on the DFS.  It should not exist be the call to open it.
     */
    public static final String DFS_FILE_NEW_WRITE = DFS_WORKING_DIR +
        "/write_to_dfs.dat";

    /**
     * In the Read fro mteh DFS and write to a alocal file test, this is the
     * destination for the write.  The source file on the DFS is DFS_FILE_MOVED.
     */
    public static final String DFS_READ_LOCAL_WRITE_DEST =
        "/tmp/remote_data/read_from_dfs.dat";

    // ** Constructors **

    /**
     * This is only run from the command line so external instantiation is not required.
     */
    private DfsClient()
    {
    }

    /**
     * Runs serveral tests against the backing DFS.  The command arguments are
     * inspected and there must be at least one.  The first argument is expected
     * to be the IP of the Thrift server.  The port it runs on can optionally be
     * sent in the second postion of the array.
     *
     * @param args String array containing at least the Thrift server IP in the
     * first position and optionally it's port number in the second.
     */
    public static void main( final String [] args )
    {
        String server = "";
        if ( args.length >= 1 && args[0] != null ) {
            server = args[0];
        } else {
            System.out.println( "You must pass in the Thrift Server IP" );
            System.exit( 1 );
        }

        int port = DEFAULT_PORT;
        if ( args.length >= 2 && args[1] != null ) {
            try {
                port = Integer.valueOf( args[1] );
            } catch ( NumberFormatException nfe ) {
                System.out.println( "Invalid port from runClient.sh = " +
                    args[1] + " using default value = " + DEFAULT_PORT  );
                port = DEFAULT_PORT;
            }
        }

        try {
            // IP Address for the node where the Server is run, the port is used
            // in the call:
            //   TServerTransport serverTransport = new TServerSocket( port );
            // on the server side.
            TTransport transport = new TSocket( server, port );
            TProtocol protocol = new TBinaryProtocol( transport );
            DfsService.Client client = new DfsService.Client( protocol );
            transport.open();

            ClientHandle chandle = null;

            try {

                // Values used for timing and reporting
                long start = 0L;
                long stop = 0L;
                long duration = 0L;
                boolean success = false;

                // The File System Tests

                // TEST Initialize
                System.out.println( "starting init" );
                System.out.println( "  Call to init should succeed (id > 0)" );
                // Depending on your DFS, you may have to pass in initialization
                // values.
                chandle = client.init( null );
                System.out.println( "  The result for init is " + chandle.id );
                System.out.println( "\n" );

                // TEST Login to the DFS.
                System.out.println( " starting login." );
                System.out.println( "  Call to login should succeed." );
                // Depending on your DFS, you have have to supply real
                // credentials.
                success = client.login( chandle, "luggage", "123456789" );
                System.out.println( "  The result for login is " + success );
                System.out.println( "\n" );

                // TEST Copy a local file to the DFS
                try {
                    System.out.println( "starting copyFromLocalFile" );
                    start = System.currentTimeMillis(); 
                    success = client.copyFromLocalFile(
                        LOCAL_FILE_FOR_UPLOAD_SRC, LOCAL_FILE_FOR_UPLOAD_DEST );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    
                    System.out.println(
                        "  Call to copyFromLocalFile should succeed." );
                    System.out.println( "  The result for copyFromLocal is " +
                        success );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );
                } catch ( DfsServiceIOException dsioe ) {
                    System.out.println(
                        "Client caught Exception for 'copyFromLocalFile'" );
                    dsioe.printStackTrace();
                }
                System.out.println( "\n" );

                // TEST Copy a file on the DFS to local disk
                try {
                    System.out.println( "starting copyToLocalFile" );
                    start = System.currentTimeMillis();
                    success = client.copyToLocalFile(
                        LOCAL_FILE_FOR_UPLOAD_DEST,
                        DFS_FILE_FOR_DOWNLOAD_DEST );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println(
                        "  Call to copyFromLocalFile should succeed." );
                    System.out.println( "  The result for copyToLocalFile is " +
                        success );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );
                } catch ( DfsServiceIOException dsioe ) {
                    System.out.println(
                        "Client caught Exception for 'copyToLocalFile'");
                    dsioe.printStackTrace();
                }
                System.out.println( "\n" );

                // TEST move a file on the DFS to another lcoation or name on
                // the DFS
                try {
                    System.out.println( "starting move" );
                    start = System.currentTimeMillis();
                    success = client.move( chandle, LOCAL_FILE_FOR_UPLOAD_DEST,
                        DFS_FILE_MOVED );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println( "  Call to move should succeed." );
                    System.out.println( "  The result for move is " + success );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );
                } catch ( DfsServiceIOException dsioe ) {
                    System.out.println( "Client caught Exception for 'move'" );
                    dsioe.printStackTrace();
                }
                System.out.println( "\n" );

                // TEST mkdir on the DFS
                try {
                    System.out.println( "starting mkdir" );
                    start = System.currentTimeMillis();
                    success = client.mkdir( chandle, NEW_DFS_DIR );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println( "  Call to mkdir should succeed." );
                    System.out.println( "  The result for mkdir is " + success );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );
                } catch ( DfsServiceIOException dsioe ) {
                    System.out.println( "Client caught Exception for 'mkdir'" );
                    dsioe.printStackTrace();
                }
                System.out.println( "\n" );

                // TEST listFiles on a directory
                try {
                    System.out.println( "starting listFiles on a directory" );
                    start = System.currentTimeMillis(); 
                    List<FileStatus> ls = client.listFiles( chandle,
                        DFS_WORKING_DIR );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println(
                        "  Call to listFiles for a direcotry should succeed." );
                    System.out.println( "  The result for list files is: " );
                    for ( FileStatus s : ls ) {
                        System.out.println( "  " + s );
                    }
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );
                } catch ( DfsServiceIOException dsioe ) {
                    System.out.println(
                        "Client caught Exception for 'listFiles'");
                    dsioe.printStackTrace();
                }
                System.out.println( "\n" );

                // TEST listFiles on a file that does exists.
                try {
                    System.out.println( "starting listFiles on a file" );
                    start = System.currentTimeMillis(); 
                    List<FileStatus> ls = client.listFiles( chandle,
                        DFS_FILE_MOVED );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println(
                        "  Call to listFiles for a file should succeed." );
                    System.out.println( "  The result for list files is " );
                    for ( FileStatus s : ls ) {
                        System.out.println( "  " + s );
                    }
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );
                } catch ( DfsServiceIOException dsioe ) {
                    System.out.println(
                        "Client caught Exception for 'listFiles'" );
                    dsioe.printStackTrace();
                }
                System.out.println( "\n" );

                // TEST listFiles on a file or that does not exits
                try {
                    System.out.println(
                        "starting listFiles on a file that does not exist" );
                    System.out.println(
                        "  listFiles should throw an exception or fail." );
                    start = System.currentTimeMillis(); 
                    List<FileStatus> ls = client.listFiles( chandle, BASE_DIR +
                        "/does_not_exist" );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println(
                        " Call to listFiles should fail or be empty." );
                    System.out.println( "  The result for list files is " );
                    for ( FileStatus s : ls ) {
                        System.out.println( "  " + s );
                    }
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );
                } catch ( DfsServiceIOException dsioe ) {
                    System.out.println(
                        "Client caught Exception for 'listFiles'");
                    dsioe.printStackTrace();
                }
                System.out.println( "\n" );

                // TEST stat on a directory
                try {
                    System.out.println( "starting stat on a directory" );
                    start = System.currentTimeMillis(); 
                    FileStatus st = client.stat( chandle, DFS_WORKING_DIR );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println(
                        "  Call to stat for a directory should succeed." );
                    System.out.println( "  The result for stat is " );
                    System.out.println( "  " + st );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );
                } catch ( DfsServiceIOException dsioe ) {
                    System.out.println( "Client caught Exception for 'stat'");
                    dsioe.printStackTrace();
                }
                System.out.println( "\n" );

                // TEST stat on a file that exists.
                try {
                    System.out.println( "starting stat on a file" );
                    start = System.currentTimeMillis(); 
                    FileStatus st = client.stat( chandle, DFS_FILE_MOVED );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println(
                        "  Call to stat for a file should succeed." );
                    System.out.println( "  The result for stat files is " );
                    System.out.println( "  " + st );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );
                } catch ( DfsServiceIOException dsioe ) {
                    System.out.println( "Client caught Exception for 'stat'" );
                    dsioe.printStackTrace();
                }
                System.out.println( "\n" );

                // TEST stat on a file or directory that does not exits
                try {
                    System.out.println(
                        "starting stat on a file that does not exist" );
                    System.out.println(
                        "  Call to stat should throw an exception" );
                    start = System.currentTimeMillis(); 
                    FileStatus st = client.stat( chandle,
                        DFS_FILE_MOVED + ".nothere" );

                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println(
                         "  Call to stat should fail or be empty." );
                    System.out.println( "  The result for list files is " );
                    System.out.println( "  " + st );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );
                } catch ( DfsServiceIOException dsioe ) {
                    System.out.println( "Client caught Exception for 'stat'" );
                    dsioe.printStackTrace();
                }
                System.out.println( "\n" );

                // TEST opening and closing a file with mode = READ
                try {
                    // NESTED TEST open a file for READ
                    System.out.println( "starting open" );
                    start = System.currentTimeMillis(); 
                    DfsHandle handle = client.open( chandle, DFS_FILE_MOVED,
                        new Constants().READ);

                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println(
                        "  Call to open existing file (read) should succeed.");
                    System.out.println( "  The result for open is " + handle );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );

                    // We will used the returned handle in the tests below

                    // NESTED TEST close an opend file
                    System.out.println( "starting close file" );
                    start = System.currentTimeMillis(); 
                    success = client.close( chandle, handle) ;
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println(
                        "  Call to close the just opened file should succeed.");
                    System.out.println( "  The result for close is " +
                        success );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );

                    // NESTED TEST close on a closed file.
                    System.out.println( "starting close (on a closed) file" );
                    start = System.currentTimeMillis(); 
                    success = client.close( chandle, handle );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println(
                        "  This call should fail as the file was closed above");
                    System.out.println( "  The result for close is " + success );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );

                    // NESTED TEST open a file that does not exist
                    System.out.println(
                        "starting open (non-existing) file for read" );
                    System.out.println(
                        "  Call to open should throw an exception." );
                    start = System.currentTimeMillis(); 
                    try {
                        handle = client.open( chandle,
                            DFS_FILE_MOVED + ".error", new Constants().READ);
                        stop = System.currentTimeMillis();
                        duration = ( stop - start ) / 1000;

                        System.out.println(
                            "  This call should fail as files must exist for read");
                        System.out.println( "  The result for open is " + handle );
                        System.out.println( "  started: " + start );
                        System.out.println( "  stopped: " + stop );
                        System.out.println( "  duration (sec): " + duration );
                    } catch ( DfsServiceIOException dsioe ) {
                        System.out.println( "Caught expected Exception " + 
                            dsioe.getMessage() );
                        dsioe.printStackTrace();
                    }

                    // NESTED TEST open an existing file for WRITE
                    System.out.println(
                        "starting open (existing) file for write" );
                    start = System.currentTimeMillis(); 
                    try {
                        handle = client.open( chandle, DFS_FILE_MOVED,
                            new Constants().WRITE);
                        stop = System.currentTimeMillis();
                        duration = ( stop - start ) / 1000;

                        System.out.println(
                            "  This should fail for Hadoop as writes must be to new files" );
                        System.out.println( "  The result for open is " + handle );
                        System.out.println( "  started: " + start );
                        System.out.println( "  stopped: " + stop );
                        System.out.println( "  duration (sec): " + duration );
                    } catch ( DfsServiceIOException dsioe ) {
                        System.out.println(
                            "Caught expected Exception for Hadoop." );
                        System.out.println(
                            "If using another DFS, check the its specs" );
                        System.out.println( "Exception.getMessage() = " +
                            dsioe.getMessage() );
                        dsioe.printStackTrace();
                    }

                    // open a new file for writing is tested in the write test below.

                } catch ( DfsServiceIOException dsioe ) {
                    System.out.println(
                        "Client caught Exception for 'open / close'");
                    dsioe.printStackTrace();
                }
                System.out.println( "\n" );

                // TEST read from an existing file
                try {
                    // READ <= the buffer sized used on the server
                    System.out.println(
                        "starting open for read file ( < bytes than buffer)" );
                    start = System.currentTimeMillis(); 
                    DfsHandle handle = client.open( chandle, DFS_FILE_MOVED,
                        new Constants().READ );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println( "  This call should succeed.");
                    System.out.println(
                        "  The result for open (before read) is " + handle );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );

                    System.out.println( "starting read file" );
                    start = System.currentTimeMillis(); 
                    String whatIread = client.read( chandle, handle, 10L,
                        1000L );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println( "  This call should succeed." );
                    System.out.println( "  The result for read is [" +
                        whatIread+ "]" );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );

                    System.out.println( "Closing file handle" );
                    success = client.close( chandle, handle );
                    System.out.println( "This call should succeed." );
                    System.out.println( "The result for close is " + success );
                } catch ( DfsServiceIOException dsioe ) {
                    System.out.println(
                        "Client caught Exception for read");
                    dsioe.printStackTrace();
                }
                System.out.println( "\n" );
          

                // READ a file on the DFS and write it to local disk.
                try {
                    System.out.println( "starting open for read file" );
                    start = System.currentTimeMillis(); 
                    DfsHandle handle = client.open( chandle, DFS_FILE_MOVED,
                        new Constants().READ );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println( "  This call should succeed.");
                    System.out.println(
                        "  The result for open (before read) is " + handle );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );

                    System.out.println( "starting read file" );
                    start = System.currentTimeMillis(); 

                    FileWriter fstream;
                    BufferedWriter out = null;

                    try {
                        fstream =
                            new FileWriter( DFS_READ_LOCAL_WRITE_DEST, true );
                        out = new BufferedWriter( fstream );

                        long off = 0L;
                        int bytesRead = 0;
                        String in = "";
                        long bufsize = 4096L;

                        do {
                            in = client.read( chandle, handle, off, bufsize );
                            if ( in != null ) {
                                bytesRead = in.length();
                                out.write( in );
                                off += bufsize;
                            }
                        } while ( bytesRead == bufsize
                            && in != null && in != "");

                    } catch ( IOException ioe ) {
                        System.out.println( ioe.getMessage() );
                        ioe.printStackTrace();
                    } catch ( DfsServiceIOException dsioe ) {
                        System.out.println(
                            "Client caught Exception for 'read'" );
                        dsioe.printStackTrace();
                    } finally {
                        try {
                            if ( out != null ) {
                                out.close();
                            }
                        } catch ( IOException ioe ) {
                            System.out.println(
                                "Ignoring Exception while closing file: " + 
                                    ioe.getMessage() );
                        }
                    }

                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println( "  This call should succeed." );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );

                    // Close the DFS file handle
                    System.out.println( "Closing file handle" );
                    success = client.close( chandle, handle );
                    System.out.println( "  This call should succeed." );
                    System.out.println( "  The result for close is " + success );
                } catch ( DfsServiceIOException dsioe ) {
                    System.out.println(
                        "Client caught Exception for 'open / close'");
                    dsioe.printStackTrace();
                }
                System.out.println( "\n" );
          

                // TEST write a file on the DFS
                try {
                    // Open a file for writing
                    System.out.println( "starting open for write" );
                    start = System.currentTimeMillis(); 
                    DfsHandle handle = client.open( chandle, DFS_FILE_NEW_WRITE,
                        new Constants().WRITE );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println( "  This call should succeed.");
                    System.out.println(
                        "  The result for open (before write) is " + handle );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );

                    // Read in local file and write it to the newly created file
                    // on the DFS.
                    System.out.println(
                        "starting read a local file and write it to the dfs" );
                    start = System.currentTimeMillis();
                    success = true;
                    InputStream in = null;
                    int size = 4096;
                    try {
                        in = new FileInputStream(
                            new File( LOCAL_FILE_FOR_UPLOAD_SRC ) );
                        byte[] bytes = new byte[size];
                        int offset = 0;
                        int numRead = 0;

                        while ( success &&
                            ( numRead=in.read( bytes, 0, size ) ) >= 0 ) {

                            success = client.write( chandle, handle , bytes,
                                Long.valueOf( "" + offset ), Long.valueOf( "" +
                                     bytes.length ) );
                            offset += numRead;
                        }
                    } catch ( IndexOutOfBoundsException ioobe ) {
                        System.out.println( "Caught IOOBE reading in file " +
                            ioobe.getMessage() );
                        ioobe.printStackTrace();
                        success = false;
                    } catch ( FileNotFoundException fnfe ) {
                        System.out.println( "FNFE opening file" );
                        fnfe.printStackTrace();
                        success = false;
                    } catch ( IOException ioe ) {
                        System.out.println( "IOE reading input file" );
                        ioe.printStackTrace();
                        success = false;
                    } finally {
                        if ( in != null ) {
                            try {
                                in.close();
                            } catch  ( IOException ioe ) {
                                System.out.println(
                                    "Ignoring Exception on in file close" );
                            }
                        }
                    }

                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println( "  This call should succeed." );
                    System.out.println( "  The result for write is " +
                        success );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );
                } catch ( DfsServiceIOException dsioe ) {
                    System.out.println( "Client caught Exception for write" );
                    dsioe.printStackTrace();
                }

                // TEST remove
                try {
                    // NESTED TEST Remove a file, using recursive = false
                    System.out.println(
                        "starting remove on a file with r=false" );
                    start = System.currentTimeMillis();
                    success = client.remove( chandle, DFS_FILE_MOVED, false );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println( "  This call should succeed." );
                    System.out.println( "  The result for remove is " +
                        success );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );

                    // NESTED TEST Remove a file that does not exist as it was
                    // just deleted in the test above,   r = false, but that
                    // should not matter.
                    System.out.println( "starting remove on missing file" );
                    start = System.currentTimeMillis();
                    success = client.remove( chandle, DFS_FILE_MOVED, false );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println( "  This call should fail." );
                    System.out.println( "  The result for remove is " +
                        success );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );

                    // NESTED TEST Remove on an empty directory with r=false
                    System.out.println(
                        "starting rm on an empty dir with r=false" );
                    start = System.currentTimeMillis();
                    success = client.remove( chandle, NEW_DFS_DIR, false );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println( "  This call should succeed. ");
                    System.out.println( "  The result for remove is " +
                        success );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop );
                    System.out.println( "  duration (sec): " + duration );

                    // NESTED TEST Remove on a non-empty dir with r=false
                    // returns false or throwns an exception depending on the
                    // backend
                    try {
                        System.out.println(
                            "starting rm on a non-empty dir with r=false" );
                        start = System.currentTimeMillis();
                        success = client.remove( chandle, DFS_WORKING_DIR,
                            false );
                        stop = System.currentTimeMillis();
                        duration = ( stop - start ) / 1000;

                        System.out.println( "  This call should fail." );
                        System.out.println( "  The result for remove is " +
                            success );
                        System.out.println( "  started: " + start );
                        System.out.println( "  stopped: " + stop );
                        System.out.println( "  duration (sec): " + duration );
                    } catch ( DfsServiceIOException dsioe ) {
                        System.out.println( "Caught expected exception " + 
                            dsioe.getMessage() );
                        dsioe.printStackTrace();
                    }

                    // NESTED TEST Remove a non-empty dir with r=true
                    System.out.println(
                        "starting rm on a non-empty dir with r = true" );
                    start = System.currentTimeMillis();
                    success = client.remove( chandle, DFS_WORKING_DIR, true );
                    stop = System.currentTimeMillis();
                    duration = ( stop - start ) / 1000;

                    System.out.println( "  This call should succeed." );
                    System.out.println( "  The result for remove is " +
                        success );
                    System.out.println( "  started: " + start );
                    System.out.println( "  stopped: " + stop) ;
                    System.out.println( "  duration (sec): " + duration );

                } catch ( DfsServiceIOException dsioe ) {
                    System.out.println(
                        "Client caught Exception for 'remove'");
                    dsioe.printStackTrace();
                }
                System.out.println( "\n" );

            } finally {
                client.logout( chandle );
                client.closeClient( chandle );
                // comment out if testing multiple clients
                client.closeDfs();
            }

            transport.close();
        } catch ( TException tex ) {
            tex.printStackTrace();
        }

        System.out.println( "Exiting DfsClient ...." );
    }
}
