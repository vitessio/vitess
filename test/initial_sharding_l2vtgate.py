#!/usr/bin/env python
#
# Copyright 2016, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""Re-runs initial_sharding.py with a l2vtgate process."""

import initial_sharding
import utils

if __name__ == '__main__':
  initial_sharding.use_l2vtgate = True
  utils.main(initial_sharding)
