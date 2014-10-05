#!/usr/bin/env python
#
# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import utils
import resharding

# this test is the same as resharding.py, but it uses vtworker to
# do the clone.
if __name__ == '__main__':
  resharding.use_clone_worker = True
  utils.main(resharding)
