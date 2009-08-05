#!/usr/local/bin/thrift --gen java --gen cpp

# Copyright (C) 2008-2009  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License. 
 

// Thrift definition for an interface to mulitple Distributed File Systems -
// e.g. Hadoop DFS, Sector, etc. This is intended to allow clients written in
// any Thrift supported language to access any DFS with a server implementation.

namespace cpp dfsservice
namespace java com.opendatagroup.dfsservice

/**
 * Structure representing a file/dir in the filesystem. Used as return from
 * stat and list commands.
 */
struct FileStatus {
  1: i64 length,            // Length of file
  2: bool isdir,            // Is this a directory?
  3: i16 block_replication, // Hadoop replication
  4: i64 blocksize,         // Hadoop blocksize
  5: i64 modification_time, // Timestamp
  6: string permission,     // File permissions
  7: string owner,          // File owner
  8: string group,          // File group ownership
  9: string path            // Full path to file
}

/**
 * This is used to reference a handle to the DFS client. This is used by the
 * Sector implementation, but not used by the Hadoop implementation.
 */
struct ClientHandle {
  1: i64 id
}

/**
 * This references a handle to a file. Returned by open() command.
 */
struct DfsHandle {
  1: i64 id
}

const i16 READ = 1
const i16 WRITE = 2
const i16 READ_WRITE = 3

exception DfsServiceIOException {
  1: string message
}

service DfsService {

// General client functions:

/**
 * Initialize the underlying filesystem.
 *
 * uri identifies underlying DFS, e.g. Sector or HDFS, as well as required
 * params for connecting.
 * For example: Sector://<IP address>:<port>
 */
ClientHandle init( 1:string uri ),

/**
 * Currently only required for Sector and is implemented as no-op for Hadoop.
 */
bool login( 1:ClientHandle clientHandle, 2:string user, 3:string password ),

/**
 * Currently only required for Sector and is implemented as no-op for Hadoop.
 */
bool logout( 1:ClientHandle clientHandle ),

/**
 * Release resources being used by a client.
 */
bool closeClient( 1:ClientHandle clientHandle ),

/**
 * Perform cleanup and release all DFS resources.
 */
bool closeDfs(),

// File system functions:

/**
 * List files in directory path.
 */
list<FileStatus> listFiles( 1:ClientHandle clientHandle, 2:string path ) throws ( 1:DfsServiceIOException ex ),

/**
 * Get info on a single file/dir.
 */
FileStatus stat( 1:ClientHandle clientHandle, 2:string path ) throws ( 1:DfsServiceIOException ex ),

/**
 * Create a directory.
 */
bool mkdir( 1:ClientHandle clientHandle, 2:string path ) throws ( 1:DfsServiceIOException ex ),

/**
 * Move file from oldpath to newpath.
 */
bool move( 1:ClientHandle clientHandle, 2:string oldpath, 3:string newpath ) throws ( 1:DfsServiceIOException ex ),

/**
 * Remove a file or directory.
 *
 * If path is a directory and recursive is true then remove directory and
 * it's contents. Otherwise if recursive is false then don't remove a
 * populated directory.
 */
bool remove( 1:ClientHandle clientHandle, 2:string path, 3:bool recursive  ) throws ( 1:DfsServiceIOException ex ),

// File functions:

/**
 * Open a file. Mode is either READ or WRITE.
 */
DfsHandle open( 1:ClientHandle clientHandle, 2:string filename, 3:i16 mode ) throws ( 1:DfsServiceIOException ex ),

/**
 * Close file.
 */
bool close( 1:ClientHandle clientHandle, 2:DfsHandle dfsHandle ) throws ( 1:DfsServiceIOException ex ),

/**
 * Read data from file.
 *
 * offset is file offset to start reading from.
 * len is length of data to read.
 */
string read( 1:ClientHandle clientHandle, 2:DfsHandle dfsHandle, 3:i64 offset, 4:i64 len ) throws ( 1:DfsServiceIOException ex ),

/**
 * Write data to a file.
 *
 * buf is data to write to file.
 * offset is file offset to begin writing to.
 * len is len of data being written.
 */
bool write( 1: ClientHandle clientHandle, 2:DfsHandle dfsHandle, 3:binary buf, 4:i64 offset, 5:i64 len ) throws ( 1:DfsServiceIOException ex ),

/**
 * Copy a file from the DFS to the local file system.
 */
bool copyToLocalFile( 1:string src, 2:string dest ) throws ( 1:DfsServiceIOException ex ),

/**
 * Copy a file from the local file system to the DFS.
 */
bool copyFromLocalFile( 1:string src, 2:string dest ) throws ( 1:DfsServiceIOException ex )
}
