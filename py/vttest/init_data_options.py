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

"""Stores options used for initializing the database with randomized data.

The options stored correspond to command line flags. See run_local_database.py
for more details on each option.
"""


class InitDataOptions(object):
  valid_attrs = set([
      'rng_seed',
      'min_table_shard_size',
      'max_table_shard_size',
      'null_probability',
      ])

  def __setattr__(self, name, value):
    if name not in self.valid_attrs:
      raise Exception(
          'InitDataOptions: unsupported attribute: %s' % name)
    self.__dict__[name] = value
