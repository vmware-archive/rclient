# Makefile for PL/Container R client
#------------------------------------------------------------------------------
# 
# Copyright (c) 2016-Present Pivotal Software, Inc
#
#------------------------------------------------------------------------------

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

PLCONTAINER_DIR = ..

# R build flags
#CLIENT_CFLAGS = $(shell pkg-config --cflags libR)
#CLIENT_LDFLAGS = $(shell pkg-config --libs libR)
CLIENT_CFLAGS = -I${R_HOME}/include
CLIENT_LDFLAGS = -Wl,--export-dynamic -fopenmp -Wl,-z,relro -L${R_HOME}/lib -lR -Wl,-rpath,'$$ORIGIN'

override CFLAGS += $(CLIENT_CFLAGS) -I$(PLCONTAINER_DIR)/ -DPLC_CLIENT -Wall -Wextra -Werror
override LDFLAGS += $(CLIENT_LDFLAGS)

CLIENT = rclient
common_src = $(shell find $(PLCONTAINER_DIR)/common -name "*.c")
common_objs = $(foreach src,$(common_src),$(subst .c,.$(CLIENT).o,$(src)))
shared_src = rcall.c rconversions.c rlogging.c

.PHONY: default
default: all

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $^

$(common_objs): %.$(CLIENT).o: %.c
	$(CC) $(CFLAGS) -c -o $@ $^

librcall.so: $(shared_src)
	$(CC) $(CFLAGS) -fpic -c $(shared_src) $^
	$(CC) -shared $(LDFLAGS) -o librcall.so rcall.o rconversions.o rlogging.o
	cp librcall.so bin

.PHONY: all
all: client.o librcall.so $(common_objs)
	$(CC) -o $(CLIENT) $^ $(LDFLAGS)
	cp $(CLIENT) bin

.PHONY: clean
clean:
	rm -f $(common_objs)
	rm -f librcall.so
	rm -f bin/librcall.so
	rm -f *.o
	rm -f $(CLIENT)
	rm -f bin/$(CLIENT)

endif # R_HOME check
