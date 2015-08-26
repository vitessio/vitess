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
from vtdb import vtgate_cursor


vtgateclienttest_process = None
vtgateclienttest_port = None
vtgateclienttest_grpc_port = None


def setUpModule():
  global vtgateclienttest_process
  global vtgateclienttest_port
  global vtgateclienttest_grpc_port

  environment.topo_server().setup()

  vtgateclienttest_port = environment.reserve_ports(1)
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

  # A packed keyspace_id from the middle of the full keyrange.
  KEYSPACE_ID_0X80 = struct.Struct('!Q').pack(0x80 << 56)

  def _open_keyspace_ids_cursor(self):
    return self.conn.cursor(
        'keyspace', 'master', keyspace_ids=[self.KEYSPACE_ID_0X80])

  def _open_keyranges_cursor(self):
    kr = keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)
    return self.conn.cursor('keyspace', 'master', keyranges=[kr])

  def _open_batch_cursor(self):
    return self.conn.cursor(
        tablet_type='master', cursorclass=vtgate_cursor.BatchVTGateCursor)

  def _open_stream_keyranges_cursor(self):
    kr = keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)
    return self.conn.cursor(
        'keyspace', 'master', keyranges=[kr],
        cursorclass=vtgate_cursor.StreamVTGateCursor)

  def _open_stream_keyspace_ids_cursor(self):
    return self.conn.cursor(
        'keyspace', 'master', keyspace_ids=[self.KEYSPACE_ID_0X80],
        cursorclass=vtgate_cursor.StreamVTGateCursor)

  def test_integrity_error(self):
    """Test we correctly raise dbexceptions.IntegrityError.
    """

    # Special query that makes vtgateclienttest raise an IntegrityError.
    integrity_error_test_query = 'return integrity error'

    # FIXME(alainjobart) add test for Execute once factory supports it

    # FIXME(alainjobart) add test for ExecuteShards once factory supports it

    # ExecuteKeyspaceIds test
    cursor = self._open_keyspace_ids_cursor()
    with self.assertRaises(dbexceptions.IntegrityError):
      cursor.execute(integrity_error_test_query, {})
    cursor.close()

    # ExecuteKeyRanges test
    cursor = self._open_keyranges_cursor()
    with self.assertRaises(dbexceptions.IntegrityError):
      cursor.execute(integrity_error_test_query, {})
    cursor.close()

    # ExecuteEntityIds test
    cursor = self.conn.cursor('keyspace', 'master')
    with self.assertRaises(dbexceptions.IntegrityError):
      cursor.execute_entity_ids(
          integrity_error_test_query, {},
          entity_keyspace_id_map={1: self.KEYSPACE_ID_0X80},
          entity_column_name='user_id')
    cursor.close()

    # ExecuteBatchKeyspaceIds test
    cursor = self._open_batch_cursor()
    cursor.execute(
        sql=integrity_error_test_query, bind_variables={},
        keyspace='keyspace',
        keyspace_ids=[self.KEYSPACE_ID_0X80])
    with self.assertRaises(dbexceptions.IntegrityError):
      cursor.flush()
    cursor.close()

    # VTGate.StreamExecuteKeyspaceIds, VTGate.StreamExecuteKeyRanges:
    # not handled in vtgateclienttest/services/errors.go.

  def test_error(self):
    """Test a regular server error raises the right exception.
    """

    # GetSrvKeyspace test
    with self.assertRaises(dbexceptions.DatabaseError):
      self.conn.get_srv_keyspace('error')

  def test_effective_caller_id(self):
    """Test that the passed in effective_caller_id is parsed correctly.

    Pass a special sql query that sends the expected
    effective_caller_id through different vtgate interfaces. Make sure
    the good_effective_caller_id works, and the
    bad_effective_caller_id raises a DatabaseError.

    """

    # Special query that makes vtgateclienttest match effective_caller_id.
    effective_caller_id_test_query = (
        'callerid://{"principal":"pr", "component":"co", "subcomponent":"su"}')
    good_effective_caller_id = {
        'Principal': 'pr', 'Component': 'co', 'Subcomponent': 'su'}
    bad_effective_caller_id = {
        'Principal': 'pr_wrong',
        'Component': 'co_wrong', 'Subcomponent': 'su_wrong'}

    def check_good_and_bad_effective_caller_ids(cursor, cursor_execute_method):
      cursor_execute_method(cursor, good_effective_caller_id)
      with self.assertRaises(dbexceptions.DatabaseError):
        cursor_execute_method(cursor, bad_effective_caller_id)
      cursor.close()

    def cursor_execute_keyspace_ids_method(cursor, effective_caller_id):
      cursor.execute(
          effective_caller_id_test_query, {},
          effective_caller_id=effective_caller_id)

    check_good_and_bad_effective_caller_ids(
        self._open_keyspace_ids_cursor(), cursor_execute_keyspace_ids_method)

    def cursor_execute_key_ranges_method(cursor, effective_caller_id):
      cursor.execute(
          effective_caller_id_test_query, {},
          effective_caller_id=effective_caller_id)

    check_good_and_bad_effective_caller_ids(
        self._open_keyranges_cursor(), cursor_execute_key_ranges_method)

    def cursor_execute_entity_ids_method(cursor, effective_caller_id):
      cursor.execute_entity_ids(
          effective_caller_id_test_query, {},
          entity_keyspace_id_map={1: self.KEYSPACE_ID_0X80},
          entity_column_name='user_id',
          effective_caller_id=effective_caller_id)

    check_good_and_bad_effective_caller_ids(
        self.conn.cursor('keyspace', 'master'),
        cursor_execute_entity_ids_method)

    def cursor_execute_batch_keyspace_ids_method(cursor, effective_caller_id):
      cursor.execute(
          sql=effective_caller_id_test_query, bind_variables={},
          keyspace='keyspace',
          keyspace_ids=[self.KEYSPACE_ID_0X80])
      cursor.flush(effective_caller_id=effective_caller_id)

    check_good_and_bad_effective_caller_ids(
        self._open_batch_cursor(),
        cursor_execute_batch_keyspace_ids_method)

    def cursor_stream_execute_keyspace_ids_method(cursor, effective_caller_id):
      cursor.execute(
          sql=effective_caller_id_test_query, bind_variables={},
          effective_caller_id=effective_caller_id)

    check_good_and_bad_effective_caller_ids(
        self._open_stream_keyspace_ids_cursor(),
        cursor_stream_execute_keyspace_ids_method)

    def cursor_stream_execute_keyranges_method(cursor, effective_caller_id):
      cursor.execute(
          sql=effective_caller_id_test_query, bind_variables={},
          effective_caller_id=effective_caller_id)

    check_good_and_bad_effective_caller_ids(
        self._open_stream_keyranges_cursor(),
        cursor_stream_execute_keyranges_method)

if __name__ == '__main__':
  utils.main()
