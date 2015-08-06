List of what we are working on

# Introduction #

`ThriftStore` is under development and we have some changes and additions in mind.  This is what we are working on.


# Details #

Client
  * implement example clients in python
  * make java and C++ clients consistent in how they handle ports and servers
Server
  * add KFS as another backend File System
  * Support more recent versions of Sector and Hadoop
Native Code
  * For comparison testing, we have created native code that performs the same calls as what is performed on the Thrift Server side, but that can be invoked directly.  This allows, for example, to isolate the overhead from Thrift when writing a large file the DFS.  We need to get these ready for release.
Documentation
  * Locality discussion and diagram