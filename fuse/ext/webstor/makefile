# Copyright (c) 2011-2013, OblakSoft LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
#
# Author: Artem Livshits <artem.livshits@gmail.com>


# Defines that can be used to customize build flavor.

DEFINES=

#DEFINES+=-DDEBUG  # use this to enable debug-only functionality
#DEFINES+=-DPERF   # use this to enable perf testing / tracing

# Include paths.  The webstor library depends on the following
# libraries:
#
# * curl
# * libxml2
# * openssl
#
# If you have custom installations of those, modify your include
# paths correspondingly.

INCLUDES=-I/usr/include/libxml2 

# Library search paths.  Similar to include paths.

LIBRARIES=

### RULES ###

CXXFLAGS+=$(DEFINES) $(INCLUDES) $(LIBRARIES) -Wno-enum-compare -g
LOADLIBES+=-lcurl -lssl -lxml2 -lcrypto -lpthread 

.PHONY: all
all: wsdbg wsperf wscmd

.PHONY: clean
clean:
	rm -f wsdbg wsperf wscmd webstor.a

wsdbg: webstor.a

wsperf: webstor.a

wscmd: webstor.a

webstor.a: webstor.a(asyncurl.o wsconn.o sysutils.o)
