#!/usr/bin/env python
#
# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import utils
import initial_sharding

from vtdb import keyrange_constants

# this test is the same as initial_sharding_bytes.py, but it uses vtworker to
# do the clone.
if __name__ == '__main__':
  initial_sharding.use_clone_worker = True
  initial_sharding.keyspace_id_type = keyrange_constants.KIT_BYTES
  utils.main(initial_sharding)
