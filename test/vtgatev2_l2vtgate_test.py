#!/usr/bin/env python
#
# Copyright 2016, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""Re-runs vtgatev2_test.py with a l2vtgate process."""

import vtgatev2_test
import utils

# this test is just re-running an entire vtgatev2_test.py with a
# l2vtgate process in the middle
if __name__ == '__main__':
  vtgatev2_test.use_l2vtgate = True
  utils.main(vtgatev2_test)
