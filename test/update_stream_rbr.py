#!/usr/bin/env python
#
# Copyright 2017, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""Re-runs update_stream.py with RBR."""

import update_stream
import utils

if __name__ == '__main__':
  update_stream.use_rbr = True
  utils.main(update_stream)
