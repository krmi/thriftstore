#!/bin/sh

# runServer.sh
# Shell script to start the Thrift server.  The value HADOOP_INSTALL is passed
# to the server class.  This value is necessary so that the appropriate Hadoop
# configuration files can be read.  Optionally, a port can be passed in which is
# the port any client must use when communicating with the server.  If a port is
# not passed in, a default value is used that is defined in the code.


# Java class whose main will be invoked.
SERVER=com.opendatagroup.dfsservice.server.DfsServer


# Update this value to be the installation directory of Hadoop that the Thrift
# server will use.
HADOOP_VER=0.18.3
HADOOP_INSTALL=/opt/hadoop


# Port the Thrift Server will listen on.  This can be left empty and the Server
# will use a default value.
PORT=$1


# Build up the classpath
LIB_DIR=../../lib
LIB_JARS=.
for J in `ls ${LIB_DIR}/*.jar`; do
    LIB_JARS=${LIB_JARS}:${J};
done;

# Add Hadoop Core Jar
# This assumes that you have the standard name for the Hadoop Installation
# eg:  hadoop-${HADOOP_VER}
LIB_JARS=${LIB_JARS}:${HADOOP_INSTALL}/hadoop-${HADOOP_VER}/hadoop-${HADOOP_VER}-core.jar


# Call to start the server
java -classpath ${LIB_JARS} ${SERVER} ${HADOOP_INSTALL}/hadoop-${HADOOP_VER} ${PORT}
