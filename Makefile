# Makefile for PL/Container R client
#------------------------------------------------------------------------------
# 
# Copyright (c) 2016-Present Pivotal Software, Inc
#
#------------------------------------------------------------------------------


ARCH = $(shell uname -s)

ifdef R_HOME
r_libdir1x = ${R_HOME}/bin
r_libdir2x = ${R_HOME}/lib
# location of R includes
r_includespec = -I${R_HOME}/include
rhomedef = ${R_HOME}
else
R_HOME := $(shell pkg-config --variable=rhome libR)
r_libdir1x := $(shell pkg-config --variable=rlibdir libR)
r_libdir2x := $(shell pkg-config --variable=rlibdir libR)
r_includespec := $(shell pkg-config --cflags-only-I libR)
rhomedef := $(shell pkg-config --variable=rhome libR)
endif

ifeq (,${R_HOME})
#R_HOME is not defined

default all clean librcall.so:
	@echo ""; \
	 echo "*** Cannot build PL/Container R client because R_HOME cannot be found." ; \
	 echo "*** Refer to the documentation for details."; \
	 echo ""

else
#R_HOME is defined

# Global build Directories

SERVER = rserver
CXX = g++

SRC = src
INCLUDE = include

# R build flags
R_CXXFLAGS = $(r_includespec)
R_LDFLAGS = -Wl,--export-dynamic -fopenmp -Wl,-z,relro -L${r_libdir2x} -lR -Wl,-rpath,'$$ORIGIN'

ifeq ($(RELEASE), 1)
override CXXFLAGS += -std=c++11 -O3 -I$(INCLUDE)/ $(R_CXXFLAGS) -Wall -Wextra -Werror -Wno-unused-result -Wreturn-type
else
override CXXFLAGS += -std=c++11 -O0 -g3 -ggdb -I$(INCLUDE)/ $(R_CXXFLAGS) -Wall -Wextra -Werror -Wno-unused-result -Wreturn-type
endif

override LDFLAGS += $(R_LDFLAGS)

# protobuf and grpc

ifdef GRPC_LDFLAG
override GPRC_LDFLAGS = $(GPRC_LDFLAG)
else
ifeq ($(ARCH),Darwin)
override GPRC_LDFLAGS = $(shell pkg-config --libs grpc++)
else
override GPRC_LDFLAGS = -L/usr/local/lib -lgrpc++
endif
endif

ifdef PROTOBUF_LDFLAG
override PROTOBUF_LDFLAGS = $(PROTOBUF_LDFLAG)
else
ifeq ($(ARCH),Darwin)
override PROTOBUF_LDFLAGS = $(shell pkg-config --libs protobuf)
else
override PROTOBUF_LDFLAGS = -L/usr/local/lib -lprotobuf
endif
endif

override LDFLAGS += $(PROTOBUF_LDFLAGS) $(GPRC_LDFLAGS) \
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl


COMMON_OBJS = src/rserver.o src/rcall.o src/rtypeconverter.o src/rutils.o src/plcontainer.pb.o src/plcontainer.grpc.pb.o
ALL_OBJS = src/server.o $(COMMON_OBJS)

.PHONY: default
default: all



%.o: %.cc
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -fPIC -c $< -o $@

librcall.so: $(COMMON_OBJS)
	$(CXX) -shared -fpic $(LDFLAGS) -o librcall.so $(COMMON_OBJS)
	cp librcall.so bin

.PHONY: all
all:  $(ALL_OBJS) librcall.so
	$(CXX) -o $(SERVER) $^ $(LDFLAGS) $(CXXFLAGS)
	cp $(SERVER) bin




# Google TEST

ifeq ($(ENABLE_COVERAGE),yes)
	ifeq ($(ARCH),Darwin)
		LDFLAGS += -coverage
	else
		LDFLAGS += -lgcov
	endif
endif

TEST_INCLUDES = -Isrc/ -Iinclude/
TEST_OBJS = test/rserver_test.o test/rcall_test.o test/rtypeconverter_test.o

TEST_SRC = $(TEST_OBJS:.o=.cc)
TEST_APP = rserver_gtest
gtest_filter ?= *

DEP_FILES := $(patsubst %.o,%.d,$(TEST_OBJS))
TEST_SRC = $(TEST_OBJS:.o=.cc)
-include $(DEP_FILES)

GMOCK_DIR = googletest/googlemock
GTEST_DIR = googletest/googletest

$(TEST_OBJS) gtest_main.o gtest-all.o gmock-all.o: TEST_INCLUDES += -I$(GTEST_DIR)/ -I$(GMOCK_DIR)/ -I$(GTEST_DIR)/src -I$(GMOCK_DIR)/src -I$(GTEST_DIR)/include -I$(GMOCK_DIR)/include

gmock-all.o :
	$(CXX) $(TEST_INCLUDES) $(CXXFLAGS) -c $(GMOCK_DIR)/src/gmock-all.cc

gtest-all.o :
	$(CXX) $(TEST_INCLUDES) $(CXXFLAGS) -c $(GTEST_DIR)/src/gtest-all.cc

gtest_main.o :
	$(CXX) $(TEST_INCLUDES) $(CXXFLAGS) -c $(GTEST_DIR)/src/gtest_main.cc

gtest_main.a : gtest_main.o gtest-all.o gmock-all.o
	$(AR) -rv $@ $^

buildtest: $(TEST_APP)

# Keep gtest_main.a at end, otherwise linker will report undefined symbol error.
%_test.o: %_test.cc
	$(CXX) $(CXXFLAGS) $(TEST_INCLUDES) -MMD -MP -c $< -o $@

$(TEST_APP): $(TEST_OBJS) gtest_main.a
	$(CXX) $^ librcall.so -o $(TEST_APP) $(LDFLAGS) $(CXXFLAGS) $(TEST_INCLUDES)

test: $(TEST_APP)
	@-rm -f *.gcda test/*.gcda # workaround for XCode/Clang
	./$(TEST_APP) --gtest_filter=$(gtest_filter)

coverage: test
	@gcov $(TEST_SRC) | grep -A 1 "src/.*.cc"	

clone-gtest:
	@git clone  --branch release-1.10.0 https://github.com/google/googletest.git --depth 1


.PHONY: clean
clean:
	rm -f librcall.so
	rm -f bin/librcall.so
	rm -f src/*.o test/*.o
	rm -f $(SERVER)
	rm -f bin/$(SERVER)
	rm -f test/*.d test/*.a test/*.gcov test/*.gcda test/*.gcno $(TEST_APP)
	rm -f *.o *.d *.a *.gcov *.gcda *.gcno $(TEST_APP)

.PHONY: buildtest test coverage clean clone-gtest


endif # R_HOME check
