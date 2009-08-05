#!/bin/sh

# runClient.sh
# Shell script to start the Thrift client.  A server IP must be passed in that
# is the location of the Thrift server.  A port can also be passed in as the
# second argument.  If it is not, then 9090 is used as the default in DfsClient.

# Java class whose main will be invoked.
CLIENT=com.opendatagroup.dfsservice.client.DfsClient

# IP of the Thrift Server.  This is required,
SERVER_IP=$1

# Port the Thrift Server will listen on.  This can be left empty and the Server
# will use a default value.
PORT=$2


# Build up the classpath
LIB_DIR=../../lib
LIB_JARS=.
for J in `ls ${LIB_DIR}/*.jar`; do
    LIB_JARS=${LIB_JARS}:${J};
done;


# Call to run the client
java -classpath ${LIB_JARS} ${CLIENT} ${SERVER_IP} ${PORT}
