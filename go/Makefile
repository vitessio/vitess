# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s

all:
	cd cmd/normalizer; $(MAKE)
	cd cmd/vtocc; $(MAKE)

clean:
	cd cmd/normalizer; $(MAKE) clean
	cd cmd/vtocc; $(MAKE) clean
