#!/usr/bin/env python
#
# Copyright 2015 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""This test uses vtgateclienttest to test the vtdb python vtgate client.
"""

import struct
import unittest

import environment
from protocols_flavor import protocols_flavor
import utils

from vtdb import dbexceptions
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtgate_client


vtgateclienttest_process = None
vtgateclienttest_port = None
vtgateclienttest_grpc_port = None


def setUpModule():
  global vtgateclienttest_process
  global vtgateclienttest_port
  global vtgateclienttest_grpc_port

  environment.topo_server().setup()

  vtgateclienttest_port = environment.reserve_ports(1)
  print 'log_dir=%s' % (environment.vtlogroot,)
  args = environment.binary_args('vtgateclienttest') + [
      '-log_dir', environment.vtlogroot,
      '-port', str(vtgateclienttest_port),
      ]

  if protocols_flavor().vtgate_python_protocol() == 'grpc':
    vtgateclienttest_grpc_port = environment.reserve_ports(1)
    args.extend(['-grpc_port', str(vtgateclienttest_grpc_port)])
  if protocols_flavor().service_map():
    args.extend(['-service_map', ','.join(protocols_flavor().service_map())])

  vtgateclienttest_process = utils.run_bg(args)
  utils.wait_for_vars('vtgateclienttest', vtgateclienttest_port)


def tearDownModule():
  utils.kill_sub_process(vtgateclienttest_process, soft=True)
  vtgateclienttest_process.wait()

  environment.topo_server().teardown()


class TestPythonClient(unittest.TestCase):
  CONNECT_TIMEOUT = 10.0

  def setUp(self):
    addr = 'localhost:%d' % vtgateclienttest_port
    protocol = protocols_flavor().vtgate_python_protocol()
    self.conn = vtgate_client.connect(protocol, addr, 30.0)

  def tearDown(self):
    self.conn.close()

  def test_success_get_srv_keyspace(self):
    """Test we get the right results from get_srv_keyspace.

    We only test the successful cases.
    """

    # big has one big shard
    big = self.conn.get_srv_keyspace('big')
    self.assertEquals(big.name, 'big')
    self.assertEquals(big.sharding_col_name, 'sharding_column_name')
    self.assertEquals(big.sharding_col_type, keyrange_constants.KIT_UINT64)
    self.assertEquals(big.served_from, {'master': 'other_keyspace'})
    self.assertEquals(big.get_shards('replica'),
                      [{'Name': 'shard0',
                        'KeyRange': {
                            'Start': '\x40\x00\x00\x00\x00\x00\x00\x00',
                            'End': '\x80\x00\x00\x00\x00\x00\x00\x00',
                            }}])
    self.assertEquals(big.get_shard_count('replica'), 1)
    self.assertEquals(big.get_shard_count('rdonly'), 0)
    self.assertEquals(big.get_shard_names('replica'), ['shard0'])
    self.assertEquals(big.keyspace_id_to_shard_name_for_db_type(
        0x6000000000000000, 'replica'), 'shard0')
    with self.assertRaises(ValueError):
      big.keyspace_id_to_shard_name_for_db_type(0x2000000000000000, 'replica')

    # small has no shards
    small = self.conn.get_srv_keyspace('small')
    self.assertEquals(small.name, 'small')
    self.assertEquals(small.sharding_col_name, '')
    self.assertEquals(small.sharding_col_type, keyrange_constants.KIT_UNSET)
    self.assertEquals(small.served_from, {})
    self.assertEquals(small.get_shards('replica'), [])
    self.assertEquals(small.get_shard_count('replica'), 0)
    with self.assertRaises(ValueError):
      small.keyspace_id_to_shard_name_for_db_type(0x6000000000000000, 'replica')

  def test_integrity_error(self):
    """Test we correctly raise dbexceptions.IntegrityError.
    """

    # FIXME(alainjobart) add test for Execute once factory supports it

    # FIXME(alainjobart) add test for ExecuteShards once factory supports it

    # ExecuteKeyspaceIds test
    cursor = self.conn.cursor('keyspace', 'master',
                              keyspace_ids=[struct.Struct('!Q').pack(0x80)])
    with self.assertRaises(dbexceptions.IntegrityError):
      cursor.execute('return integrity error', {})
    cursor.close()

    # ExecuteKeyRanges test
    kr = keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)
    cursor = self.conn.cursor('keyspace', 'master',
                              keyranges=[kr])
    with self.assertRaises(dbexceptions.IntegrityError):
      cursor.execute('return integrity error', {})
    cursor.close()

    # ExecuteEntityIds test
    cursor = self.conn.cursor('keyspace', 'master')
    with self.assertRaises(dbexceptions.IntegrityError):
      cursor.execute_entity_ids(
          'return integrity error', {},
          entity_keyspace_id_map={
              1: struct.Struct('!Q').pack(1761124146422844620)},
          entity_column_name='user_id')
    cursor.close()


    # FIXME(alainjobart) add test for ExecuteBatchShard

    # FIXME(alainjobart) add test for ExecuteBatchKeyspaceIds

  def test_error(self):
    """Test a regular server error raises the right exception.
    """

    # GetSrvKeyspace test
    with self.assertRaises(dbexceptions.DatabaseError):
      self.conn.get_srv_keyspace('error')

  def test_effective_caller_id(self):
    effective_caller_id = 'abcde'
    cursor = self.conn.cursor('keyspace', 'master',
                              keyspace_ids=[struct.Struct('!Q').pack(0x80)])
    effective_caller_id = {
        'Principal': 'pr', 'Component': 'co', 'Subcomponent': 'su'}
    cursor.execute(
        'callerid://{"principal":"pr", "component":"co", "subcomponent":"su"}',
        {},
        effective_caller_id=effective_caller_id)
    cursor.close()
    cursor = self.conn.cursor('keyspace', 'master')
    cursor.execute_entity_ids(
        'callerid://{"principal":"pr", "component":"co", "subcomponent":"su"}',
        {},
        entity_keyspace_id_map={
            1: struct.Struct('!Q').pack(1761124146422844620)},
        entity_column_name='user_id',
        effective_caller_id=effective_caller_id)
    cursor.close()

if __name__ == '__main__':
  utils.main()
