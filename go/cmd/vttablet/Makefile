# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s

all:
	cd $(GOTOP)/vt/sqlparser; $(MAKE)
	go build

clean:
	go clean
	cd $(GOTOP)/vt/sqlparser; $(MAKE) clean
