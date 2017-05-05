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

"""Re-runs merge_sharding.py with a varbinary keyspace_id."""

from vtdb import keyrange_constants

import base_sharding
import merge_sharding
import utils


if __name__ == '__main__':
  base_sharding.keyspace_id_type = keyrange_constants.KIT_BYTES
  utils.main(merge_sharding)
