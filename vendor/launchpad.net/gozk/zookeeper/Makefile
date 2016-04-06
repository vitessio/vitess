include $(GOROOT)/src/Make.inc

all: package

TARG=launchpad.net/gozk/zookeeper

GOFILES=\
	server.go\
	runserver.go\
	
CGOFILES=\
	zk.go\

CGO_OFILES=\
	helpers.o\

ifdef ZKROOT
LIBDIR=$(ZKROOT)/src/c/.libs
LD_LIBRARY_PATH:=$(LIBDIR):$(LD_LIBRARY_PATH)
CGO_CFLAGS+=-I$(ZKROOT)/src/c/include -I$(ZKROOT)/src/c/generated
CGO_LDFLAGS+=-L$(LIBDIR)
else
LIBDIR=/usr/lib
endif

# For static compilation, will have to take LDFLAGS out of gozk.go too.
ifdef STATIC
CGO_LDFLAGS+=-lm -lpthread
# XXX This has ordering issues with current Make.pkg:
#CGO_OFILES+=$(wildcard _lib/*.o)
#
#_lib:
#	@mkdir -p _lib
#	cd _lib && ar x $(LIBDIR)/libzookeeper_mt.a
#
#_cgo_defun.c: _lib
CGO_OFILES+=\
	_lib/hashtable_itr.o\
	_lib/libzkmt_la-zk_hashtable.o\
	_lib/hashtable.o\
	_lib/libzkmt_la-zk_log.o\
	_lib/libzkmt_la-mt_adaptor.o\
	_lib/libzkmt_la-zookeeper.jute.o\
	_lib/libzkmt_la-recordio.o\
	_lib/libzkmt_la-zookeeper.o\

_lib/%.o:
	@mkdir -p _lib
	cd _lib && ar x $(LIBDIR)/libzookeeper_mt.a

endif

CLEANFILES+=_lib

GOFMT=gofmt
BADFMT:=$(shell $(GOFMT) -l $(GOFILES) $(CGOFILES) $(wildcard *_test.go))

gofmt: $(BADFMT)
	@for F in $(BADFMT); do $(GOFMT) -w $$F && echo $$F; done

ifneq ($(BADFMT),)
ifneq ($(MAKECMDGOALS),gofmt)
$(warning WARNING: make gofmt: $(BADFMT))
endif
endif

include $(GOROOT)/src/Make.pkg
