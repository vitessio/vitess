# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s

all:
	cd vt/sqlparser; $(MAKE)
	cd cmd/normalizer; go build
	cd cmd/vtocc; go build
	cd cmd/vttablet; go build


clean:
	cd vt/sqlparser; $(MAKE) clean
	cd cmd/normalizer; go clean
	cd cmd/vtocc; go clean
	cd cmd/vttablet; go clean
