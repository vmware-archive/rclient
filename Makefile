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
shared_objs = $(foreach src,$(shared_src),$(subst .c,.o,$(src)))

.PHONY: default
default: all

common_dep = $(foreach src,$(common_src),$(subst .c,.$(CLIENT).d,$(src)))
$(common_dep): $(common_src)
	@set -e; rm -f $@; $(CC) -M $(CFLAGS) $< > $@.$$$$; sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; rm -f $@.$$$$
$(common_objs): %.$(CLIENT).o: %.c $(common_dep)
	$(CC)  $(CFLAGS) -c -o $@ $<

DEPDIR := .d
$(shell mkdir -p $(DEPDIR) >/dev/null)
DEPFLAGS = -MT $@ -MMD -MP -MF $(DEPDIR)/$*.Td
POSTCOMPILE = @mv -f $(DEPDIR)/$*.Td $(DEPDIR)/$*.d && touch $@
$(shared_objs): %.o: %.c $(DEPDIR)/%.d
	$(CC) $(DEPFLAGS) $(CFLAGS) -fpic -c -o $@ $<
	$(POSTCOMPILE)

librcall.so: $(shared_objs)
	$(CC) -shared $(LDFLAGS) -o librcall.so $(shared_objs)
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
	rm -f $(common_dep)
	rm -rf $(DEPDIR)

$(DEPDIR)/%.d: ;
.PRECIOUS: $(DEPDIR)/%.d

include $(wildcard $(patsubst %,$(DEPDIR)/%.d,$(basename $(shared_src))))
endif # R_HOME check
