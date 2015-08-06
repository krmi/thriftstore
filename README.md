We use Thrift to implement a common interface to multiple distributed file systems. Distributed file systems are storage systems designed to reliably store very large files on a multiple-node cluster. This allows the file system to be distributed across a local or
wide area network but appear as a single file system to clients. Distributed file systems provide reliability by replicating files to multiple nodes of the cluster, providing redundancy if one of the nodes becomes unavailable.

Thrift is a framework for implementing cross-language interfaces to services. Thrift uses an Interface Definition Language to define interfaces, and uses that file to generate stub code to be used in implementing RPC clients and servers that can be used across languages. Using Thrift allows us to implement a single interface that can be used with different languages to access different underlying systems.

The specific distributed file systems targeted are the Hadoop distributed file system (HDFS) and Sector. For each of these systems we implemented a custom Thrift server which utilizes the underlying systems client API.

The Hadoop implementation uses the Hadoop File System Java API to implement the server side code, and the Sector implementation uses the Sector client API, which is implemented in C++.  Clients can be written in any Thrift supported language to access the
Hadoop Java server or the Sector C++ server without requiring any code changes. The interface defines common file system commands such as

  * mkdir
  * move
  * remove
  * read
  * write
  * etc.