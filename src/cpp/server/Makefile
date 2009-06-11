LIB_DIR = /usr/local/lib
SECTOR_LIB_DIR=${SECTOR_HOME_DIR}/lib
CCFLAGS = -I../../../include -I$(SECTOR_HOME_DIR)/common -I$(SECTOR_HOME_DIR)/client -I$(SECTOR_HOME_DIR)/gmp -I$(SECTOR_HOME_DIR)/udt -I$(SECTOR_HOME_DIR)/security  -I$(BOOST_DIR) -I${THRIFT_INCLUDE_DIR} -I../../gen-cpp -I../include
LDFLAGS= -L${THRIFT_LIB_DIR} -L${SECTOR_LIB_DIR} -lthrift -lstdc++ -lpthread -lssl -lclient -lsecurity -lrpc -ludt -lcommon

GEN_SRC = ../../gen-cpp/DfsService.cpp ../../gen-cpp/DfsService_types.cpp ../../gen-cpp/DfsService_constants.cpp

default: server

server: SectorServer.cpp
	g++ SectorServer.cpp -o SectorServer $(CCFLAGS) $(LDFLAGS) ${GEN_SRC}

clean:
	$(RM) -r SectorServer
