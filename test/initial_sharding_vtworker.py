#!/usr/bin/env python
#
# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import utils
import initial_sharding

# this test is the same as initial_sharding.py, but it uses vtworker to
# do the clone.
if __name__ == '__main__':
  initial_sharding.use_clone_worker = True
  utils.main(initial_sharding)
