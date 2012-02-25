# Copyright 2012, Google Inc.
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:

#     * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following disclaimer
# in the documentation and/or other materials provided with the
# distribution.
#     * Neither the name of Google Inc. nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

MAKEFLAGS = -r

# GNU Make syntax:
ifndef GOROOT
export GOROOT=$(HOME)/dev/go
endif

ifndef GOBIN
export GOBIN=$(GOROOT)/bin
endif

ifndef GOARCH
export GOARCH=386
endif

ifndef GOOS
export GOOS=linux
endif

include $(GOROOT)/src/Make.inc

CLEANFILES+=*.sgo.go

%.clean_all:
	find $(GOTOP) -name '*.o' -o  -name '*.[ao68qv]' -o -name '*.out' | xargs -n 10 rm
	find $(GOTOP) -name '_obj' -o -name '_test' | xargs -n 10 rm -rf

%.make:
	make -C $*

%.clean:
	make -C $* clean

%.nuke:
	make -C $* nuke_all || make -C $* nuke

%.fmt:
	$(GOBIN)/gofmt -w -tabindent=false -spaces -tabwidth=2 $(GOFILES)
