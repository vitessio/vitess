"""Tests for fix_served_types."""

import collections
import unittest
from vtproto import topodata_pb2
import fix_served_types

_all_types = dict(
    served_types=[
        dict(tablet_type=topodata_pb2.REPLICA),
        dict(tablet_type=topodata_pb2.RDONLY),
        dict(tablet_type=topodata_pb2.MASTER),
    ])

_no_master = dict(
    served_types=[
        dict(tablet_type=topodata_pb2.REPLICA),
        dict(tablet_type=topodata_pb2.RDONLY),
    ])


class FixServedTypesTest(unittest.TestCase):

  def test_get_vtctl_commands(self):
    tests = [collections.OrderedDict(x) for x in [
        [('-80', _all_types), ('80-', {}), ('0', _all_types)],
        [('-80', _all_types), ('80-', _all_types), ('0', {})],
        [('-80', {}), ('80-', {}), ('0', _all_types)],
        [('-80', {}), ('80-', _all_types), ('0', _all_types)],
        [('-80', {}), ('80-', _no_master), ('0', _all_types)]
    ]]

    expected_results = [
        [['SetShardServedTypes', '--remove', 'foo/-80', 'replica'],
         ['SetShardServedTypes', '--remove', 'foo/-80', 'rdonly'],
         ['SetShardServedTypes', '--remove', 'foo/-80', 'master'],
         ['SetShardServedTypes', 'foo/0', 'replica'],
         ['SetShardServedTypes', 'foo/0', 'rdonly'],
         ['SetShardServedTypes', 'foo/0', 'master']],

        [['SetShardServedTypes', '--remove', 'foo/-80', 'replica'],
         ['SetShardServedTypes', '--remove', 'foo/-80', 'rdonly'],
         ['SetShardServedTypes', '--remove', 'foo/-80', 'master'],
         ['SetShardServedTypes', '--remove', 'foo/80-', 'replica'],
         ['SetShardServedTypes', '--remove', 'foo/80-', 'rdonly'],
         ['SetShardServedTypes', '--remove', 'foo/80-', 'master'],
         ['SetShardServedTypes', 'foo/0', 'replica'],
         ['SetShardServedTypes', 'foo/0', 'rdonly'],
         ['SetShardServedTypes', 'foo/0', 'master']],

        [['SetShardServedTypes', 'foo/0', 'replica'],
         ['SetShardServedTypes', 'foo/0', 'rdonly'],
         ['SetShardServedTypes', 'foo/0', 'master']],

        [['SetShardServedTypes', '--remove', 'foo/80-', 'replica'],
         ['SetShardServedTypes', '--remove', 'foo/80-', 'rdonly'],
         ['SetShardServedTypes', '--remove', 'foo/80-', 'master'],
         ['SetShardServedTypes', 'foo/0', 'replica'],
         ['SetShardServedTypes', 'foo/0', 'rdonly'],
         ['SetShardServedTypes', 'foo/0', 'master']],

        [['SetShardServedTypes', '--remove', 'foo/80-', 'replica'],
         ['SetShardServedTypes', '--remove', 'foo/80-', 'rdonly'],
         ['SetShardServedTypes', 'foo/0', 'replica'],
         ['SetShardServedTypes', 'foo/0', 'rdonly'],
         ['SetShardServedTypes', 'foo/0', 'master']],
    ]

    for test, expected_result in zip(tests, expected_results):
      self.assertEquals(
          fix_served_types.get_vtctl_commands('foo', test), expected_result)


if __name__ == '__main__':
  unittest.main()
