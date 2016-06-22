#!/usr/bin/env python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""Re-runs resharding.py with a varbinary keyspace_id."""

from vtdb import keyrange_constants

import base_sharding
import resharding
import utils

# this test is just re-running an entire resharding.py with a
# varbinary keyspace_id
if __name__ == '__main__':
  base_sharding.keyspace_id_type = keyrange_constants.KIT_BYTES
  utils.main(resharding)
