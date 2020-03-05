#!/usr/bin/env python
#
# Copyright 2019 The Vitess Authors.
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

"""Re-runs initial_sharding.py using multiple-split-diff."""

from vtdb import keyrange_constants

import base_sharding
import initial_sharding
import utils

# this test is just re-running an entire initial_sharding.py with a
# varbinary keyspace_id
if __name__ == '__main__':
  base_sharding.use_multi_split_diff = True
  utils.main(initial_sharding)
