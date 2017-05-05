#!/usr/bin/env python
#
# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Re-runs vtgatev2_test.py with a l2vtgate process."""

import vtgatev2_test
import utils

# this test is just re-running an entire vtgatev2_test.py with a
# l2vtgate process in the middle
if __name__ == '__main__':
  vtgatev2_test.use_l2vtgate = True
  utils.main(vtgatev2_test)
