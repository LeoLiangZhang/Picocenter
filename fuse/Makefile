include makefile.defs

SHARED=
LDLIBS=-lboost_filesystem -lboost_system -lboost_iostreams -lcurl -lssl -lxml2 -lcrypto -lpthread 
# (un)comment this to (show)hide trace log
# CXXFLAGS+= -DPICO_TRACE

all: main test

test: test_picostore test_tar test_utils

clean_extra: clean_webstor
	rm -rf main

main.o : main.c
fops.o : fops.cpp

wsutils.o        : wsutils.cpp
picoutils.o	     : picoutils.cpp
picostore.o      : picostore.cpp
test_picostore.o : test_picostore.cpp
test_tar.o       : test_tar.cpp
test_utils.o     : test_utils.cpp

webstor:
	$(MAKE) -C ext/webstor

ext/webstor/webstor.a: webstor

.PHONY: webstor clean_webstor

clean_webstor:
	$(MAKE) -C ext/webstor clean

test_picostore: test_picostore.o picostore.o picoutils.o wsutils.o ext/webstor/webstor.a
	$(CXX) -o $@ $^ $(CXXFLAGS) $(LDLIBS)

test_tar: test_tar.o
	$(CXX) -o $@ $^ $(CXXFLAGS) -lboost_filesystem -lboost_system -lboost_iostreams

test_utils: test_utils.o
	$(CXX) -o $@ $^ $(CXXFLAGS) -lboost_iostreams

main: main.o fops.o picostore.o picoutils.o wsutils.o ext/webstor/webstor.a
	$(CXX) -o $@ $^ $(CXXFLAGS) $(LDLIBS)

main_quick: main.o fops.o picostore.o picoutils.o wsutils.o
	$(CXX) -o $@ $^ ext/webstor/webstor.a $(CXXFLAGS) $(LDLIBS)

