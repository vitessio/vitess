#!/usr/bin/env python

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


"""Sharding utils."""


def get_shard_index(shard_name):
  """Returns tuple of shard index, num_shards based on a shard name."""
  if shard_name in ['0', '-']:
    return 0, 1

  shard_begin, shard_end = shard_name.split('-')
  num_bytes_used = max(len(shard_begin), len(shard_end)) / 2
  if shard_begin:
    shard_begin = int(shard_begin, 16)
  else:
    shard_begin = 0
  if shard_end:
    shard_end = int(shard_end, 16)
  else:
    shard_end = 1 << num_bytes_used * 8
  shard_width = shard_end - shard_begin
  num_shards = (1 << num_bytes_used * 8) / (shard_width)
  shard_num = shard_begin / shard_width
  return shard_num, num_shards


def get_shard_name(shard, num_shards):
  """Returns an appropriate shard name, as a string.

  A single shard name is simply 0; otherwise it will attempt to split up 0x100
  into multiple shards.  For example, in a two sharded keyspace, shard 0 is
  -80, shard 1 is 80-.  This function currently only applies to sharding setups
  where the shard count is 256 or less, and all shards are equal width.

  Args:
    shard: The integer shard index (zero based)
    num_shards: Total number of shards (int)

  Returns:
    The shard name as a string.
  """

  if num_shards == 1:
    return '0'

  shard_width = int(0x100 / num_shards)

  if shard == 0:
    return '-%02x' % shard_width
  elif shard == num_shards - 1:
    return '%02x-' % (shard * shard_width)
  else:
    return '%02x-%02x' % (shard * shard_width, (shard + 1) * shard_width)


def get_shard_names(num_shards):
  """Create a generator of shard names.

  Args:
    num_shards: Total number of shards (int)

  Returns:
    The shard name generator.
  """
  return (get_shard_name(x, num_shards) for x in range(num_shards))
