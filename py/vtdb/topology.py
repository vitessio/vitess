"""Deprecated module that holds keyspace / sharding methods."""

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

# DEPRECATED module, just one hardcoded function left, so vtrouting.py
# is not changed yet. Will be cleaned up soon.

from vtdb import keyrange_constants


def get_sharding_col(keyspace_name):
  _ = keyspace_name
  return 'keyspace_id', keyrange_constants.KIT_UINT64
