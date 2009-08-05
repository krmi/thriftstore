LIB_DIR = /usr/local/lib
CCFLAGS = -I$(BOOST_DIR) -I${THRIFT_INCLUDE_DIR} -I../../gen-cpp -I../include
LDFLAGS= -L${THRIFT_LIB_DIR} -lthrift -lstdc++ -lpthread

GEN_SRC = ../../gen-cpp/DfsService.cpp ../../gen-cpp/DfsService_types.cpp ../../gen-cpp/DfsService_constants.cpp

default: mkdir copyFromLocalFile copyToLocalFile readFile writeFile stat remove list

mkdir: mkdir.cpp
	g++ mkdir.cpp -o mkdir $(CCFLAGS) $(LDFLAGS) ${GEN_SRC}

copyFromLocalFile: copyFromLocalFile.cpp
	g++ copyFromLocalFile.cpp -o copyFromLocalFile $(CCFLAGS) $(LDFLAGS) ${GEN_SRC}

copyToLocalFile: copyToLocalFile.cpp
	g++ copyToLocalFile.cpp -o copyToLocalFile $(CCFLAGS) $(LDFLAGS) ${GEN_SRC}

readFile: readFile.cpp
	g++ readFile.cpp -o readFile $(CCFLAGS) $(LDFLAGS) ${GEN_SRC}

writeFile: writeFile.cpp
	g++ writeFile.cpp -o writeFile $(CCFLAGS) $(LDFLAGS) ${GEN_SRC}

stat: stat.cpp
	g++ stat.cpp -o stat $(CCFLAGS) $(LDFLAGS) ${GEN_SRC}

remove: remove.cpp
	g++ remove.cpp -o remove $(CCFLAGS) $(LDFLAGS) ${GEN_SRC}

list: list.cpp
	g++ list.cpp -o list $(CCFLAGS) $(LDFLAGS) ${GEN_SRC}

clean:
	$(RM) -r mkdir copyFromLocalFile readFile writeFile stat
