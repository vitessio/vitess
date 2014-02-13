#!/usr/bin/python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import initial_sharding
import utils

from vtdb import keyrange

# this test is just re-running an entire initial_sharding.py with a
# varbinary keyspace_id
if __name__ == '__main__':
  initial_sharding.keyspace_id_type = keyrange.KIT_BYTES
  utils.main(initial_sharding)
