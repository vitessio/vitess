#!/usr/bin/env python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""Re-runs resharding.py with RBR on."""

import base_sharding
import vertical_split
import utils

if __name__ == '__main__':
  base_sharding.use_rbr = True
  utils.main(vertical_split)
