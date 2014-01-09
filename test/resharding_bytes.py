#!/usr/bin/python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import sys

import resharding
import utils

# this test is just re-running an entire resharding.py with a
# varbinary keyspace_id
if __name__ == '__main__':
  resharding.keyspace_id_type = "bytes"
  utils.main(sys.modules['resharding'])
