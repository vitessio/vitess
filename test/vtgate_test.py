#!/usr/bin/python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import vtdb_test
import utils

# this test is just re-running an entire vtdb_test.py with a
# client type VTGate
if __name__ == '__main__':
  vtdb_test.client_type = vtdb_test.CLIENT_TYPE_VTGATE
  utils.main(vtdb_test)
