## `ThriftStore` ##

There are several open-source cloud storage systems currently in use, such as Hadoop, CloudStore, and Sector. Although all of these systems implement a distributed file system providing reliable storage of large data sets, each uses its own client interface to access this data. Additionally, using the client interfaces requires coding in the implementation language of each system.

With Thrift we're able to provide a common interface to multiple cloud storage systems which can be accessed from multiple languages.

**Version 0.6.0** has been released (2009-08-05) and on the server side, it associates all open files with the client which opened them.  In a multi-client environment, this allows you to completely shutdown some clients while others are still running.