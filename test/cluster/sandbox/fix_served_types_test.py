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

"""Tests for fix_served_types."""

import collections
import unittest
from vtproto import topodata_pb2
from . import fix_served_types

_ALL_TYPES = dict(
    served_types=[
        dict(tablet_type=topodata_pb2.REPLICA),
        dict(tablet_type=topodata_pb2.RDONLY),
        dict(tablet_type=topodata_pb2.MASTER),
    ])

_NO_MASTER = dict(
    served_types=[
        dict(tablet_type=topodata_pb2.REPLICA),
        dict(tablet_type=topodata_pb2.RDONLY),
    ])

_JUST_MASTER = dict(
    served_types=[
        dict(tablet_type=topodata_pb2.MASTER),
    ])


class FixServedTypesTest(unittest.TestCase):

  def test_partial_serving_overlap_shard_1(self):
    """Source shard and destination shard 1 are serving all types."""
    shards = collections.OrderedDict(
        [('-80', _ALL_TYPES), ('80-', {}), ('0', _ALL_TYPES)])
    expected_result = [
        ['SetShardServedTypes', '--remove', 'foo/-80', 'replica'],
        ['SetShardServedTypes', '--remove', 'foo/-80', 'rdonly'],
        ['SetShardServedTypes', '--remove', 'foo/-80', 'master'],
        ['SetShardServedTypes', 'foo/0', 'replica'],
        ['SetShardServedTypes', 'foo/0', 'rdonly'],
        ['SetShardServedTypes', 'foo/0', 'master']]
    self.assertEqual(
        fix_served_types.get_vtctl_commands('foo', shards), expected_result)

  def test_destination_shards_serving(self):
    """Destination shards are serving all types."""
    shards = collections.OrderedDict(
        [('-80', _ALL_TYPES), ('80-', _ALL_TYPES), ('0', {})])
    expected_result = [
        ['SetShardServedTypes', '--remove', 'foo/-80', 'replica'],
        ['SetShardServedTypes', '--remove', 'foo/-80', 'rdonly'],
        ['SetShardServedTypes', '--remove', 'foo/-80', 'master'],
        ['SetShardServedTypes', '--remove', 'foo/80-', 'replica'],
        ['SetShardServedTypes', '--remove', 'foo/80-', 'rdonly'],
        ['SetShardServedTypes', '--remove', 'foo/80-', 'master'],
        ['SetShardServedTypes', 'foo/0', 'replica'],
        ['SetShardServedTypes', 'foo/0', 'rdonly'],
        ['SetShardServedTypes', 'foo/0', 'master']]
    self.assertEqual(
        fix_served_types.get_vtctl_commands('foo', shards), expected_result)

  def test_source_shards_serving(self):
    """Nominal case where the source shard is serving all types."""
    shards = collections.OrderedDict(
        [('-80', {}), ('80-', {}), ('0', _ALL_TYPES)])
    expected_result = [
        ['SetShardServedTypes', 'foo/0', 'replica'],
        ['SetShardServedTypes', 'foo/0', 'rdonly'],
        ['SetShardServedTypes', 'foo/0', 'master']]
    self.assertEqual(
        fix_served_types.get_vtctl_commands('foo', shards), expected_result)

  def test_partial_serving_overlap_shard_2(self):
    """Source shard and destination shard 2 are serving all types."""
    shards = collections.OrderedDict(
        [('-80', {}), ('80-', _ALL_TYPES), ('0', _ALL_TYPES)])
    expected_result = [
        ['SetShardServedTypes', '--remove', 'foo/80-', 'replica'],
        ['SetShardServedTypes', '--remove', 'foo/80-', 'rdonly'],
        ['SetShardServedTypes', '--remove', 'foo/80-', 'master'],
        ['SetShardServedTypes', 'foo/0', 'replica'],
        ['SetShardServedTypes', 'foo/0', 'rdonly'],
        ['SetShardServedTypes', 'foo/0', 'master']]
    self.assertEqual(
        fix_served_types.get_vtctl_commands('foo', shards), expected_result)

  def test_mixed_serving_types(self):
    """Source shard serves some types, destination shards serve other types."""
    shards = collections.OrderedDict(
        [('-80', _NO_MASTER), ('80-', _NO_MASTER), ('0', _JUST_MASTER)])
    expected_result = [
        ['SetShardServedTypes', '--remove', 'foo/-80', 'replica'],
        ['SetShardServedTypes', '--remove', 'foo/-80', 'rdonly'],
        ['SetShardServedTypes', '--remove', 'foo/80-', 'replica'],
        ['SetShardServedTypes', '--remove', 'foo/80-', 'rdonly'],
        ['SetShardServedTypes', 'foo/0', 'replica'],
        ['SetShardServedTypes', 'foo/0', 'rdonly'],
        ['SetShardServedTypes', 'foo/0', 'master']]
    self.assertEqual(
        fix_served_types.get_vtctl_commands('foo', shards), expected_result)


if __name__ == '__main__':
  unittest.main()

