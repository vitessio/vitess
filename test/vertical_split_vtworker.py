#!/usr/bin/env python
#
# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import utils
import vertical_split

# this test is the same as vertical_split.py, but it uses vtworker to
# do the clone.
if __name__ == '__main__':
  vertical_split.use_clone_worker = True
  utils.main(vertical_split)
