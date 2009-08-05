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
package com.opendatagroup.dfsservice.server;

// start thrift-generated files
import com.opendatagroup.dfsservice.Constants;
import com.opendatagroup.dfsservice.ClientHandle;
import com.opendatagroup.dfsservice.DfsHandle;
import com.opendatagroup.dfsservice.DfsService;
import com.opendatagroup.dfsservice.DfsServiceIOException;
// end thrift-generated files

import com.facebook.thrift.server.TServer;
import com.facebook.thrift.server.TThreadPoolServer;
import com.facebook.thrift.transport.TServerSocket;
import com.facebook.thrift.transport.TServerTransport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

// To avoid confusion, do not import and use fully-qualified name for these
// classes:
//
//    org.apache.hadoop.fs.FileStatus
//    com.opendatagroup.dfsservice.FileStatus


/**
 * Java implementation of the the DFS Server.  This handles the interacton
 * between any of the Dfs clients using thrft to the Hadoop DFS.  This class is
 * a container that has a main method to run the contained implementation of the
 * thrift DfsService stub.
 *
 * @see com.opendata.dfsservice.client.DfsClient
 */
public final class DfsServer
{
    /**
     * Install directory for Hadoop.  This is passed in from the command line.
     */
    public static String HADOOP_DIR;

    // ** Inner Classes **

    /**
     * This is the implementation of the thrift-generated stub.
     */
    public static class DfsHandler implements DfsService.Iface
    {
        // ** Private Static Final Data **

        /** Logger. */
        public static final Log LOG =
            LogFactory.getLog( DfsHandler.class.getName() );

        /** Buffer size to use for read and write methods. */
        private static final int BUFFER_SIZE = 4096;

        // ** Private Data **

        /**
         * Used to generate a unique id for the clients that connect.  This is
         * used in caching
         */
        private AtomicLong clientId;

        /** Used for the keys in the file handle map. */
        private AtomicLong along;

        /**
         * If a non-null value is passed to {@link #init}, that is the value
         * used, otherwise this is loaded via config.
         */
        private String hdfs;

        /**
         * Handle for the clients that have connected to the server.  The values
         * for each mapped object is a map of file handles for the client.
         */
        private ConcurrentHashMap<Long,
            ConcurrentHashMap<Long, Object> > clienthandles =
                new ConcurrentHashMap<Long, ConcurrentHashMap<Long, Object> >();

        /** The Configuration resource. */
        private Configuration config;

        /** File System using {@link config}. */
        private FileSystem fs;


        // ** Constructors **

        /**
         * Atempts to adds resources to the Hadoop Configuration.
         */
        public DfsHandler()
        {
            config = new Configuration();
            // NOTE: order is import, properties declared in both are assigned
            // the value defeine in the property file loaded last.
            config.addResource( 
                new Path( HADOOP_DIR + "/conf/hadoop-default.xml" ) );
            config.addResource( 
                new Path( HADOOP_DIR + "/conf/hadoop-site.xml" ) );

            // client numbering starts at 1
            clientId = new AtomicLong( 1L );

            // file handle starts at something more random
            along = new AtomicLong( System.currentTimeMillis() );
        }


        // ** Public Methods **

        /**
         * Initializes the instance.  If a non-null value is passed in, then
         * that value is used without any validation as the basename of the file
         * system.  If <code>null</code> is passed in, then the instance of
         * {@link Configuration} set up in the constructor is used to look for
         * the value for <code>fs.default.name</code> and if a value is not
         * found, then <code>null</code> is used.
         *
         * @param uri the uri as a String to use as the hadoop file system.
         *
         * @return a not <code>null</code> ClientHandle.  The id value contained
         * in the handle is <code>1L</code> if the file is initialized without
         * an exception and <code>-1L</code> if the if cannot be loaded with
         * the Configuration set up in the constructor.
         */
        public ClientHandle init( final String uri )
        {
            LOG.debug( "DfsServer call to init with uri=" + uri );
            ClientHandle result = new ClientHandle();
            result.id = clientId.getAndIncrement();

            if ( uri != null ) {
                hdfs = config.get( "fs.default.name" );
                if ( hdfs == null ) {
                    LOG.warn( 
                        "DfsServer.init: no value for fs.default.name" );
                }
            } else {
                hdfs = uri;
            }
            LOG.info( 
                "DfsServer initialized with ( fs.default.name ) hdfs=" + hdfs );
            try {
                fs = FileSystem.get( config );
            } catch ( IOException ioe ) {
                LOG.error( 
                    "DfsServer.init: caught IOException getting the config." );
                result.id=-1L;
            }

            return result;
        }

        /**
         * Added for interoperability.  Invoking this method does not result in
         * a call to the Hadoop application.  The user name and a hash of the
         * password is logged before returning <code>true</code>.  The Id of the
         * ClientHandle is adde to the map so that it is ready for any attempts
         * to open a file.
         *
         * @param chandle the client handle.  This is ignored by the method and
         * is included for interoperability.
         * @param user the user logging in via the client
         * @param password the password for the user.
         * 
         * @return <code>true</code>.
         */
        public boolean login( final ClientHandle chandle, final String user,
            final String password )
        {
            try {
                clienthandles.put(chandle.id,
                    new ConcurrentHashMap<Long, Object>()  );
                LOG.debug( "DfsServer call to login with user=" + user
                    + " and password=" + hash( password ) );
            } catch ( NoSuchAlgorithmException nsae ) {
                LOG.error( "DfsServer.login: cannot find hashing algorithm",
                    nsae );
            }
            return true;
        }

        /**
         * Added for interoperability.  Invoking this method does not result is
         * a call to the Hadoop application.
         *
         * @param chandle the client handle.  This is ignored by the method and
         * is included for interoperability.
         *
         * @return <code>true</code>.
         */
        public boolean logout( final ClientHandle chandle )
        {
            LOG.debug( "DfsServer call to no-op logout" );
            return true;
        }

        /**
         * Closes filehandles associated with the client.  The client will no
         * longer be able to open, read or write files without first logging out
         * and then logging back in.
         *
         * <p/> Invoking this method does not result in a call to the Hadoop
         * application.
         *
         * @param chandle the client handle to close.
         *
         * @return <code>true</code> if the client is successfully shutdown,
         * <code>false</code> otherwise.
         */
        public boolean closeClient( ClientHandle chandle )
        {
            return closeClient( chandle.id );
        }

        /**
         * Closes all active filesystems and also clears any references to open
         * input and output streams after closing them.  If an exception is
         * caught during the attempt to close, it is logged and swallowed and
         * <code>false</code> is eventually returned denoting a patial (at the
         * least) failure.
         *
         * @return <code>true</code> if the file systems were closed or
         * <code>false</code> if an exception is encountered.
         */
        public boolean closeDfs()
        {
            LOG.debug( "DfsServer call to closeDfs" );
            boolean result = true;

            Set<Long> keys = clienthandles.keySet();
            for ( Long client : keys ) {
                closeClient( client );
            }

            // close file system
            try {
                fs.closeAll();
            } catch( IOException ioe ) {
                LOG.error(
                    "DfsServer.closeDfs: cannot close backing Hadoop FS" );
                result = false;
            }
            clienthandles.clear();

            return result;
        }

        /**
         * Performs a list status on the passed in path.  If the path
         * represents a directory, then the status for each of the files in the
         * directory is returned.
         *
         * @param chandle the client handle.  This is ignored by the method and
         * is included for interoperability.
         * @param path the path to a file or directory.  If this is a relative
         * path, it starts at the default / dir for the Hadoop server.  This is
         * usually <code>/user/&lt;HADOOP_USER&gt;/</code> and is not influenced
         * by the Thrift server or the user running it.
         * @return list of file statuses.  This is never <code>null</code> but
         * it may be empty.
         * @throws DfsServiceIOException if there is a problem getting the file
         * information from the File System or if the path does not exist.
         */
        public List<com.opendatagroup.dfsservice.FileStatus> listFiles(
            final ClientHandle chandle, final String path )
            throws DfsServiceIOException {

            LOG.debug( "DfsServer call to listFiles with path=" + path );
            List<com.opendatagroup.dfsservice.FileStatus> stats =
                new ArrayList<com.opendatagroup.dfsservice.FileStatus>();
            try {
                org.apache.hadoop.fs.FileStatus[] status =
                    fs.listStatus( new Path( path ) );
                if ( status != null ) {
                    for ( int i = 0; i < status.length; i++ ) {
                        stats.add( convertToThriftFileStatus( status[i] ) );
                    }
                } else {
                    LOG.warn(
                        "DfsServer.listFiles: listStatus returned null for " +
                            path );
                    throw new DfsServiceIOException(
                        "listStatus cannot access " + path );
                }
            } catch ( IOException ioe ) {
                LOG.error(
                    "DfsServer.listFiles: caught io exception with path=" +
                        path );
                throw new DfsServiceIOException( "Server-side IOException of " +
                    ioe.getMessage() );
            }
            return stats;
        }

        /**
         * Gets the file or directory status for the passed in path.
         *
         * @param chandle the client handle.  This is ignored by the method and
         * is included for interoperability.
         * @param path the path to a file or directory.  If this is a relative
         * path, it starts at the default / dir for the Hadoop server.  This is
         * usually <code>/user/&lt;HADOOP_USER&gt;/</code> and is not influenced
         * by the Thrift server or the user running it.
         *
         * @return this is never <code>null</code> and an exception is thrown if
         * any problem is encountered.
         * @throws DfsServiceIOException if there is a problem getting the file
         * information from the File System or if the path does not exist.
         */
        public com.opendatagroup.dfsservice.FileStatus stat(
            final ClientHandle chandle, final String path )
            throws DfsServiceIOException {

            LOG.debug( "DfsServer call to stat with path=" + path );
            try {
                org.apache.hadoop.fs.FileStatus status =
                    fs.getFileStatus( new Path( path ) );
                if ( status != null ) {
                        return convertToThriftFileStatus( status );
                } else {
                    LOG.warn(
                        "DfsServer.stat: getFileStatus returned null for " +
                            path );
                    throw new DfsServiceIOException( "stat cannot access " +
                        path );
                }
            } catch ( IOException ioe ) {
                LOG.error( "DfsServer.stat: caught io exception with path=" +
                    path );
                throw new DfsServiceIOException( "Server-side IOException of " +
                    ioe.getMessage() );
            }
        }

        /**
         * This is the behavior expected from running
         * <code>mkdir -p a/b/c</code> on a local file system.  The option
         * to set file permissions is not exposed and the default value will be
         * used.
         *
         * @param chandle the client handle.  This is ignored by the method and
         * is included for interoperability.
         * @param path the path to the new directory, including the name, to be
         * created.  If this is a relative path, it starts at the default / dir
         * for the Hadoop server.  This is usually
         * <code>/user/&lt;HADOOP_USER&gt;/</code> and is not influenced by the
         * Thrift server or the user running it.
         *
         * @return <code>true</code> if the directory is created and
         * <code>false</code> otherwise.
         * 
         * @throws DfsServiceIOException if an {@link IOException} is
         * encountered while trying to create the directory.
         */
        public boolean mkdir( final ClientHandle chandle, final String path )
            throws DfsServiceIOException
        {
            LOG.debug( "DfsServer call to mkdir with path=" + path );
            try {
                return fs.mkdirs( new Path( path ) );
            } catch ( IOException ioe ) {
                LOG.error( "DfsServer.mkdir: caught io exception with path=" +
                    path );
                throw new DfsServiceIOException( "Server-side IOException of " +
                    ioe.getMessage() );
            }
        }

        /**
         * Moves oldpath to newpath.  Both paths are assumed to be on the HDFS
         * and local moves are not supported.
         *
         * @param chandle the client handle.  This is ignored by the method and
         * is included for interoperability.
         * @param oldpath the path to the file or directory to be moved.  If
         * this is a relative path it starts at the default / dir for the Hadoop
         * server.  This is usually <code>/user/&lt;HADOOP_USER&gt;/</code> and
         * is not influenced by the Thrift server or the user running it.
         * @param newpath the path the file or directory is to be moved to.  If
         * this is a relative path it starts at the default / dir for the Hadoop
         * server.  This is usually <code>/user/&lt;HADOOP_USER&gt;/</code> and
         * is not influenced by the Thrift server or the user running it.
         *
         * @return <code>true</code> if the file is successfully moved and
         * otherwise <code>false</code>.
         *
         * @throws DfsServiceIOException if an {@link IOException} is
         * encountered during the move.
         */
        public boolean move( final ClientHandle chandle, final String oldpath,
            final String newpath )
            throws DfsServiceIOException
        {
            LOG.debug( "DfsServer call to move with src=" + oldpath +
                " , dest =" + newpath );
            try {
                return fs.rename( new Path( oldpath ), new Path( newpath ) );
            } catch ( IOException ioe ) {
                LOG.error( "DfsServer.move: caught io exception with src=" +
                    oldpath + " and dest=" + newpath );
                throw new DfsServiceIOException( "Server-side IOException of " +
                    ioe.getMessage() );
            }
        }

        /**
         * Removes a file or directory from the File System.  If the passed in
         * path represents a file, then the value for the recursive flag is
         * ignored.  If the path is to a non-empty directory, it will only be
         * deleted if the recursive flag is set to true.
         *
         * @param chandle the client handle.  This is ignored by the method and
         * is included for interoperability.
         * @param path the path to the resource that is to be deleted.  If this
         * is a relative path it starts at the default / dir for the Hadoop
         * server.  This is usually <code>/user/&lt;HADOOP_USER&gt;/</code> and
         * is not influenced by the Thrift server or the user running it.
         * @param recursive flag for when the passed in path is a directory.
         * A directory will only be removed if the flag is <code>true</code>
         * or if the directory is empty.
         *
         * @return <code>true</code> if the resource specified by path is
         * deleted and <code>false</code> otherwise.
         *
         * @throws DfsServiceIOException if an {@link IOException} is trying to
         * remove.  If the target is a non-empty directory and
         * <code>recursive == false</code> then an IOException is caught here
         * and rethrown with the message <pre>
         * Server-side IOException of java.io.IOException: PATH is non-empty
         * </pre>
         */
        public boolean remove( final ClientHandle chandle, final String path,
            final boolean recursive )
            throws DfsServiceIOException
        {
            LOG.debug( "DfsServer call to remove with path=" + path +
                " , recursive=" + recursive );
            try {
                return fs.delete( new Path( path ), recursive );
            } catch ( IOException ioe ) {
                LOG.error( "DfsServer.remove: caught io exception with path=" +
                    path );
                throw new DfsServiceIOException( "Server-side IOException of " +
                    ioe.getMessage() );
            }
        }

        /**
         * If the <code>mode</code> argument is READONLY, then this method opens
         * the file denoted by <code>filename</code> if it exists and throws an
         * exception if it does not.  If open is called with WRITEONLY, then the
         * file <em>must not already exist</em>.  The Hadoop File System API has
         * an option for overwriting, but we do not expose it.  If the intention
         * is to completely overwrite an exisitng file, then first delete it
         * by a call to {@link #remove} and then call open with WRITEONLY mode.
         *
         * @param chandle the ClientHandle for the client who wants to open the
         * file.  The file handle will be associated with the ClientHandle id.
         * @param filename the name of the file to be opened and any path
         * value.  If the path is relative, it starts at the default / dir for
         * the Hadoop server.  This is usually
         * <code>/user/&lt;HADOOP_USER&gt;/</code> and is not influenced by the
         * Thrift server or the user running it.
         * @param mode denotes if the file is to be opened for reads or writes.
         *
         * @return a DfsHandle to the {@link FSDataInputStream} or
         * {@link FSDataOutputStream} depending on whether READONLY or
         * WRITEONLY, respectively, was passed in as the mode.
         *
         * @throws DfsServiceIOException if the file to be opened does not exist
         * when the mode is for reading or if the file <em>does exist</em> and
         * it is to be opened for writing.  Also throws this if the mode cannot
         * be determined, <i>i.e.</i> an invalid value for <code>mode</code> is
         * passed in.
         */
        public DfsHandle open( final ClientHandle chandle,
            final String filename, final short mode )
            throws DfsServiceIOException
        {
            LOG.debug( "DfsServer call to open with ClientHandle=" + chandle.id
                + " , filename=" + filename + " , mode=" + mode );
            DfsHandle handle = null;
            if ( mode == new Constants().READ ) {
                try {
                    if ( fs.exists( new Path( filename ) ) ) {
                        FSDataInputStream in = fs.open( new Path( filename ) );

                        ConcurrentHashMap<Long, Object> filehandles =
                            clienthandles.get( chandle.id );

                        handle = new DfsHandle( along.getAndIncrement() );

                        filehandles.put(
                            Long.valueOf( handle.id ), ( Object )in );

                    } else {
                        LOG.error(
                            "DfsServer.open: could not find file to be opened, filename="
                                + filename );
                        throw new DfsServiceIOException(
                            "could not find filename=" + filename );
                    }
                } catch ( IOException ioe ) {
                    LOG.error(
                        "DfsServer.open: error checking for existence or opening file="
                            + filename, ioe );
                    throw new DfsServiceIOException(
                         "Server-side IOException of " + ioe.getMessage() );
                }
            } else if ( mode == new Constants().WRITE ) {
                try {
                    if ( fs.exists( new Path( filename ) ) ) {
                        LOG.error(
                            "DfsServer.open: cannot open existing file for write. file="
                                + filename );
                        throw new DfsServiceIOException(
                            "cannot open existing file for write. file="
                                + filename );
                    } else {
                        FSDataOutputStream out  = fs.create(
                            new Path( filename ), false, BUFFER_SIZE );

                        ConcurrentHashMap<Long, Object> filehandles =
                            clienthandles.get( chandle.id );

                        handle = new DfsHandle( along.getAndIncrement() );

                        filehandles.put(
                            Long.valueOf( handle.id ), ( Object )out );
                    }
                } catch ( IOException ioe ) {
                    LOG.error(
                        "DfsServer.open: error checking for existence or opening file="
                            + filename, ioe );
                    throw new DfsServiceIOException(
                        "Server-side IOException of " + ioe.getMessage() );
                }
            } else {
                LOG.error( "DfsServer.open: unknown mode=" + mode +
                    " passed to open filename=" + filename );
                throw new DfsServiceIOException( "unknown mode=" + mode +
                    " passed to open filename=" + filename );
            }
            return handle;
        }

        /**
         * Looks up the passed in Dfshandle and attempts to close the associated
         * file.  The DfsHandle is not associated with the ClienetHandle, the
         * close will not be allowed.
         *
         * @see #open
         *
         * @param chandle the ClientHandle for the client that opened the file
         * and now wants to close it.
         * @param handle the DfsHandle to the file that is going to be closed.
         * It must have be opened by a previous call to {@link #open}.
         *
         * @return <code>true</code> if the file associated with the passed in
         * handle is successfully closed or else <code>false</code> if the
         * handle is not be found.
         *
         * @throws DfsServiceIOException if a problem is encountered while
         * closing the file.
         */
        public boolean close( final ClientHandle chandle,
            final DfsHandle handle )
            throws DfsServiceIOException
        {
            LOG.debug( "DfsServer call to close with handle.id=" + handle.id );
            boolean result = true;


            ConcurrentHashMap<Long, Object> filehandles =
                clienthandles.get( chandle.id );
            if ( filehandles == null || filehandles.isEmpty() ) {
                LOG.error( "DfsServer.close: file handle=" + handle.id +
                    " not found in list of open files for client=" +
                    chandle.id );
                throw new DfsServiceIOException( "file handle=" + handle.id +
                    " not found in list of open files for client=" +
                    chandle.id );
            }

            if ( filehandles.containsKey( Long.valueOf( handle.id ) ) ) {
                Object fh = filehandles.remove( Long.valueOf( handle.id ) );
                try {
                    if ( fh instanceof FSDataOutputStream ) {
                        FSDataOutputStream out = ( FSDataOutputStream )fh;
                        out.close();
                    } else if ( fh instanceof FSDataInputStream ) {
                        FSDataInputStream in = ( FSDataInputStream )fh;
                        in.close();
                    } else {
                        LOG.error(
                            "DfsServer.close: unrecognized type for handle.id="
                                + handle.id );
                    }
                } catch ( IOException ioe ) {
                    LOG.error(
                        "DfsServer.close: error closing file.  handle.id=" +
                            handle.id );
                    throw new DfsServiceIOException(
                        "Server-side IOException of " + ioe.getMessage() );
                }
            } else {
                LOG.warn( "DfsServer.close: could not find file handle to close for Dfshandle.id="
                    + handle.id );
                result = false;
            }

            return result;
        }

        /**
         * Reads <code>len</code> bytes from a file starting at position
         * <code>offset</code> and returns the bytes as a String.  The read will
         * not take place if the passed in DfsHandle is not associated with the
         * passed in ClientHandle.
         *
         * @see #open
         *
         * @param chandle the ClientHandle for the client who opened the file
         * and now wants to read it.
         * @param handle the DfsHandle to the file that is going to be read.
         * It must have be opened for reading by a previous call to
         * {@link #open}.
         * @param offset the offset to begain reading at.
         * @param len the number of bytes to read for the offset.
         *
         * @return <code>len</code> bytes starting at offset of the file
         * associated with the passed in DfsHandle.  The bytes are returned as
         * a String.
         *
         * @throws DfsServiceIOException if a problem is encountered reading the
         * file or if it cannot be found or opened for reading.
         */
        public String read( final ClientHandle chandle, final DfsHandle handle,
            final long offset, final long len )
            throws DfsServiceIOException
        {
            LOG.debug( "DfsServer call to read with client.id=" + chandle.id
                + " , and handle.id=" + handle.id );

            byte[] buf = new byte[Long.valueOf( len ).intValue()];
            int bytesRead = 0;

            ConcurrentHashMap<Long, Object> filehandles =
                clienthandles.get( chandle.id );
            if ( filehandles == null || filehandles.isEmpty() ) {
                LOG.error(
                    "DfsServer.read: no filehandles associated with client id="
                        + chandle.id );

                throw new DfsServiceIOException(
                    "DfsServer.read: no filehandles associated with client id="
                        + chandle.id );
            }

            if ( filehandles.containsKey( Long.valueOf( handle.id ) ) ) {
                Object fh = filehandles.get( Long.valueOf( handle.id ) );
                try {
                    if ( fh instanceof FSDataInputStream ) {
                        FSDataInputStream in = ( FSDataInputStream )fh;
                        bytesRead = in.read( offset, buf, 0,
                            Long.valueOf( len ).intValue() );
                    } else {
                        LOG.error(
                            "DfsServer.read: error file for read.  handle.id=" +
                                handle.id );
                        throw new DfsServiceIOException(
                            "Can't open handle.id=" + handle.id +
                                " for reading." );
                    }
                } catch ( IOException ioe ) {
                    LOG.error(
                       "DfsServer.read: error file for read.  handle.id=" +
                           handle.id );
                    throw new DfsServiceIOException(
                        "Server-side IOException of " + ioe.getMessage() );
                }
            } else {
                LOG.warn(
                    "DfsServer.read: could not find file handle=" +
                        + handle.id + " for client.id=" + chandle.id );
                throw new DfsServiceIOException(
                    "Can't find handle for handle.id=" + handle.id +
                        " for client.id=" + chandle.id );
            }


            if ( bytesRead <= 0  ) {
                return "";
            }
            if ( bytesRead < len ) {
                return new String( buf ).substring( 0, bytesRead );
            }

            return new String( buf );
        }

        /**
         * Writes only to newly created empty files.  Before you can read what
         * has been written to a file, it has to be closed by a call to
         * {@link #close}.  If you open another handle to the file before
         * closing the first, the writes made with the first handle are not
         * visable.  If the passed in DfsHandle is not associated with the
         * passed in ClientHandle, the write will not take place.
         *
         * @see #close
         * @see #open
         *
         * @param chandle the ClientHandle for the client that opened the file
         * and now wants to write to it.
         * @param handle the DfsHandle to the file to be written to.  It must
         * have been opened (created) for writing by a previous call to
         * {@link #open}.
         * @param contents a byte array whose contents are to be written to the
         * file.
         * @param offset the offset to begain writing at.  The first time this
         * method is called for a file, the offset must be <code>0L</code> since
         * it is a new (empty) file.
         * @param len the number of bytes to be written.  This must be
         * <code>&gt 0</code>.
         *
         * @return <code>true</code> if the bytes are written to the file and
         * <code>false</code> if the file can not be found or opened for
         * writing.
         *
         * @throws DfsServiceIOException if there is an exception during the
         * write or if the file was not opened for writing with a previous call
         * to {@link #open} or if the offset is greater than zero which is not
         * allowed since this file is newly created.
         */
        public boolean write( final ClientHandle chandle,
            final DfsHandle handle, final byte[] contents, final long offset,
            final long len )
            throws DfsServiceIOException
        {

            LOG.debug( "DfsServer call to write for file handle.id=" +
                handle.id );

            ConcurrentHashMap<Long, Object> filehandles =
                clienthandles.get( chandle.id );
            if ( filehandles == null || filehandles.isEmpty() ) {
                LOG.error(
                    "DfsServer.write: no filehandles associated with client id="
                        + chandle.id );

                throw new DfsServiceIOException(
                    "DfsServer.write: no filehandles associated with client id="
                        + chandle.id );
            }

            boolean result = true;
            if ( filehandles.containsKey( Long.valueOf( handle.id ) ) ) {
                Object fh = filehandles.get( Long.valueOf( handle.id ) );
                try {
                    if ( fh instanceof FSDataOutputStream ) {
                        FSDataOutputStream out = ( FSDataOutputStream )fh;
                        out.write(contents, 0, Long.valueOf(len).intValue());
                    } else {
                        LOG.error(
                            "DfsServer.write: error getting file for write.  " +
                            + handle.id + " is not opened for writing." );
                        result = false;
                    }
                } catch ( IOException ioe ) {
                    LOG.error(
                        "DfsServer.write: error writing to file.  handle.id=" +
                            handle.id );
                    throw new DfsServiceIOException(
                        "Server-side IOException of " + ioe.getMessage() );
                }
            } else {
                LOG.warn(
                    "DfsServer.write: could not find handle to write " + 
                        handle.id + " for client.id=" + chandle.id );
                result = false;
            }

            return result;
        }

        /**
         * Copies the file on the HDFS file given by <code>src</code> to the
         * local file system.  The file becomes <code>localdest</code> and that
         * value may be fully qualified or relative starting from the directory
         * where Hadoop was started.  This method always returns
         * <code>true</code> if it completes and throws a
         * {@link DfsServiceIOException} if something goes wrong during the
         * copy.
         *
         * @param src the path to the dfs file to be copied to the local
         * filesystem.  If this is a relative path, it starts at the default /
         * dir for the Hadoop server.  This is usually
         * <code>/user/&lt;HADOOP_USER&gt;/</code> and is not influenced by the
         * Thrift server or the user running it.
         * @param localdest the path on the local filesystem that the file given
         * in <code>src</code> is to be copied to.  If this is a relative path,
         * it starts from the directory where the Thrift server was invoked.
         *
         * @return <code>true</code> if the copy is successful.  The value
         * <code>false</code> is never returned.
         *
         * @throws DfsServiceIOException when a {@link FileNotFound} or an
         * {@link IOException} is caught.
         */
        public boolean copyToLocalFile( final String src,
            final String localdest )
            throws DfsServiceIOException
        {
            LOG.debug( "DfsServer call to copyToLocalFile with src=" + src +
                " , dest=" + localdest );
            InputStream in = null;
            OutputStream out = null;
            try {
                in = fs.open( new Path( src ) );
                out = new BufferedOutputStream(
                    new FileOutputStream( localdest ) );
                IOUtils.copyBytes( in, out, BUFFER_SIZE, true );
            } catch ( IOException ioe ) {
                LOG.error(
                    "DfsServer.copyToLocal: caught io exception with src=" +
                        src + " and dest=" + localdest );
                throw new DfsServiceIOException( "Server-side IOException of " +
                    ioe.getMessage() );
            } finally {
                IOUtils.closeStream( in );
                IOUtils.closeStream( out );
            }
            return true;
        }

        /**
         * Copies the local file given by <code>localsrc</code> to the
         * distributed filesystem.  The file becomes <code>dest</code>
         * off of the hdfs.  This method always returns <code>true</code> if it
         * completes and throws a {@link DfsServiceIOException} if something
         * goes wrong during the copy.
         *
         * @param localsrc the path on the local filesystem to be copied to the
         * HDFS.  If this is a relative path, it starts from the directory where
         * the Thrift server was invoked.
         * @param dest the path on the HDFS that the file given in
         * <code>localsrc</code> is to be copied to.  If this is a relative
         * path, it starts at the default / dir for the Hadoop server.  This is
         * usually <code>/user/&lt;HADOOP_USER&gt;/</code> and is not influenced
         * by the Thrift server or the user running it.
         *
         * @return <code>true</code> if the copy is successful.  The value
         * <code>false</code> is never returned as an exception is thrown if any
         * error is encountered.
         *
         * @throws DfsServiceIOException when a {@link FileNotFound} or an
         * {@link IOException} is caught.
         */
        public boolean copyFromLocalFile( final String localsrc,
            final String dest )
            throws DfsServiceIOException
        {
            LOG.debug( "DfsServer call to copyFromLocalFile with src="
                + localsrc + " , dest=" + dest );
            InputStream in = null;
            OutputStream out = null;
            try {
                in = new BufferedInputStream( new FileInputStream( localsrc ) );
                out = fs.create( new Path( dest ) );
                IOUtils.copyBytes( in, out, BUFFER_SIZE, true );
            } catch ( IOException ioe ) {
                LOG.error(
                    "DfsServer.copyFromLocalFile: caught io exception with src="
                        + localsrc + " and dest=" + dest );
                throw new DfsServiceIOException( "Server-side IOException of "
                    + ioe.getMessage() );
            } finally {
                IOUtils.closeStream( in );
                IOUtils.closeStream( out );
            }
            return true;
        }


        // ** Private Methods **

        /**
         * Converts an instance of the Apache FileStatus to an instance of the
         * generated FielStatus object used with thrift.  The values are sent to
         * the fully populated constructor.
         *
         * @param org.apache.hadoop.fs.FileStatus fs the FileStatus instance to
         * be converted.
         *
         * @return an instance of com.opendatagroup.dfsservice.FileStatus
         * populated with values from the passed in
         * org.apache.hadoop.fs.FileStatus.
         */
        private static com.opendatagroup.dfsservice.FileStatus
            convertToThriftFileStatus(
                final org.apache.hadoop.fs.FileStatus fs )
        {
            return new com.opendatagroup.dfsservice.FileStatus( fs.getLen(),
                fs.isDir(),
                fs.getReplication(),
                fs.getBlockSize(),
                fs.getModificationTime(),
                fs.getPermission().toString(),
                fs.getOwner(),
                fs.getGroup(),
                fs.getPath().getName() );
        }

        /**
         * Hashes the passed in cleartext so that it can be safely logged
         * without any security concerns.  SHA is used and a
         * {@link NoSuchAlgortihmException} is thrown if that cannot be found by
         * the {@link MessageDigest}.
         *
         * @param cleartext the sensitive value to be hashed.
         *
         * @return the hashed value for the passed in plaintext.
         *
         * @throws NoSuchAlgorithmException if the intended hashing
         * algorithm cannot be found.
         */
        private static String hash( final String cleartext )
            throws NoSuchAlgorithmException {

            final char[] hexdomain = {'0','1','2','3','4','5','6','7','8','9'
                ,'a','b','c','d','e','f', };
            // NOTE: in Java, SHA and SHA-1 are synomous.
            MessageDigest md = MessageDigest.getInstance( "SHA" );
            md.update( cleartext.getBytes() );
            byte[] bytes = md.digest();

            final StringBuffer out = new StringBuffer( bytes.length * 2 );
            for ( int i = 0; i < bytes.length; i++ ) {
                final char lownibble = hexdomain[( bytes[i] & 0xF )];
                final char highnibble = hexdomain[( bytes[i] >>> 4 ) & 0xF];
                out.append( highnibble ).append( lownibble );
            }
            return out.toString();
        }


        /**
         * Closes filehandles associated with the client id.  The client will no
         * longer be able to open, read or write files without first logging
         * back in.
         *
         * <p/> Invoking this method does not result in a call to the Hadoop
         * application.
         *
         * @param chandleId the id of the client handle to close.
         *
         * @return <code>true</code> if the client is successfully shutdown,
         * <code>false</code> otherwise.
         */
        private boolean closeClient( Long chandleId )
        {

            LOG.debug( "DfsServer call to closeCient with client.id=" +
                chandleId );

            boolean result = true;

            ConcurrentHashMap<Long, Object> filehandles =
                clienthandles.get( chandleId );
            if ( filehandles == null || filehandles.isEmpty() ) {
                LOG.warn(
                    "DfsServer.closeClient: no filehandles associated with client id="
                        + chandleId );
                return result;
            }

            try {
                // close all active Streams
                Collection streams = filehandles.values();
                for ( Object obj : streams ) {
                    if ( obj instanceof FSDataInputStream ) {
                        FSDataInputStream in = ( FSDataInputStream )obj;
                        try {
                            in.close();
                        } catch ( IOException ioe ) {
                            LOG.error(
                                "DfsServer.closeDfs: failure closing file, continuing",
                                    ioe );
                            result = false;
                        }
                    } else if ( obj instanceof FSDataOutputStream ) {
                        FSDataOutputStream out = ( FSDataOutputStream )obj;
                        try {
                            out.close();
                        } catch ( IOException ioe ) {
                            LOG.error(
                                "DfsServer.closeDfs: failure closing file, continuing",
                                    ioe );
                            result = false;
                        }
                    } else {
                        obj = null;
                        LOG.error(
                            "DfsServer.closeDfs: unrecognized type, nulling the reference." );
                        result = false;
                    }
                }
            } finally {
                filehandles.clear();
                clienthandles.remove( chandleId );
            }

            return result;
        }

    }


    // ** Constructors **

    /**
     * Deny external instantiations.  Instances are only created from
     * {@link #main}.
     */
    private DfsServer()
    {
    }

    // ** Main **

    /**
     * This controls the server.  It creates a new instance of the
     * {@link DfsService.Processor} and starts a multi-threaded server that uses
     * the <code>DfsService.Processor</code>.  This is the only method of the
     * DfsServer class.  The rest in contain in the inner class.
     *
     * @param args any commandline arguments.  Currently, only the first two are
     * checked.  The first is the location of the Hadoop installation on the
     * machine running the server and the second is the optional port number the
     * server will listen on for requests from clients.
     */
    public static void main( String [] args ) {
        try {

            if ( args.length >= 1 && args[0] != null ) {
                HADOOP_DIR = args[0];
            } else {
                System.out.println(
                    "hadoop installation directory must be passed in." );
                System.exit(1);
            }

            System.out.println(" hadoop installation has been set to " +
                HADOOP_DIR );

            int port = 9090;
            if ( args.length >= 2 && args[1] != null ) {
                try {
                    port = Integer.valueOf( args[1] );
                } catch( NumberFormatException nfe ) {
                    System.out.println( "Port value = " + args[1] +
                        " is not an integer.  Using default value." );
                    port = 9090;
                }
            }

            DfsHandler handler = new DfsHandler();
            DfsService.Processor processor =
                new DfsService.Processor( handler );
            TServerTransport serverTransport = new TServerSocket( port );
            TServer server =
                new TThreadPoolServer( processor, serverTransport );
            System.out.println( "Starting server on port " + port + " ..." );
            server.serve();
        } catch ( Exception ex ) {
            ex.printStackTrace();
        }
    }

}
