#!/usr/bin/env python
#
# Copyright 2015 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""This test uses vtgateclienttest to test the vtdb python vtgate client.
"""

import logging
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

  try:
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
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.kill_sub_process(vtgateclienttest_process, soft=True)
  if vtgateclienttest_process:
    vtgateclienttest_process.wait()

  environment.topo_server().teardown()


class TestPythonClientBase(unittest.TestCase):
  """Base class for Python client tests."""
  CONNECT_TIMEOUT = 10.0

  # A packed keyspace_id from the middle of the full keyrange.
  KEYSPACE_ID_0X80 = struct.Struct('!Q').pack(0x80 << 56)

  def setUp(self):
    super(TestPythonClientBase, self).setUp()
    addr = 'localhost:%d' % vtgateclienttest_port
    protocol = protocols_flavor().vtgate_python_protocol()
    self.conn = vtgate_client.connect(protocol, addr, 30.0)
    logging.info(
        'Start: %s, protocol %s.',
        '.'.join(self.id().split('.')[-2:]), protocol)

  def tearDown(self):
    self.conn.close()

  def _open_keyspace_ids_cursor(self):
    return self.conn.cursor(
        'keyspace', 'master', keyspace_ids=[self.KEYSPACE_ID_0X80])

  def _open_keyranges_cursor(self):
    kr = keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)
    return self.conn.cursor('keyspace', 'master', keyranges=[kr])

  def _open_batch_cursor(self):
    return self.conn.cursor(keyspace=None, tablet_type='master')

  def _open_stream_keyranges_cursor(self):
    kr = keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)
    return self.conn.cursor(
        'keyspace', 'master', keyranges=[kr],
        cursorclass=vtgate_cursor.StreamVTGateCursor)

  def _open_stream_keyspace_ids_cursor(self):
    return self.conn.cursor(
        'keyspace', 'master', keyspace_ids=[self.KEYSPACE_ID_0X80],
        cursorclass=vtgate_cursor.StreamVTGateCursor)


class TestPythonClientErrors(TestPythonClientBase):
  """Test cases to verify that the Python client can handle errors correctly."""

  def test_execute_integrity_errors(self):
    """Test we raise dbexceptions.IntegrityError for Execute calls."""
    # Special query that makes vtgateclienttest return an IntegrityError.
    self._verify_exception_for_execute(
        'error://integrity error',
        dbexceptions.IntegrityError)

  def test_partial_integrity_errors(self):
    """Raise an IntegrityError when Execute returns a partial error."""
    # Special query that makes vtgateclienttest return a partial error.
    self._verify_exception_for_execute(
        'partialerror://integrity error',
        dbexceptions.IntegrityError)

  def _verify_exception_for_execute(self, query, exception):
    """Verify that we raise a specific exception for all Execute calls.

    Args:
      query: query string to use for execute calls.
      exception: exception class that we expect the execute call to raise.
    """
    # FIXME(alainjobart) add test for Execute once factory supports it

    # FIXME(alainjobart) add test for ExecuteShards once factory supports it

    # ExecuteKeyspaceIds test
    cursor = self._open_keyspace_ids_cursor()
    with self.assertRaises(exception):
      cursor.execute(query, {})
    cursor.close()

    # ExecuteKeyRanges test
    cursor = self._open_keyranges_cursor()
    with self.assertRaises(exception):
      cursor.execute(query, {})
    cursor.close()

    # ExecuteEntityIds test
    cursor = self.conn.cursor('keyspace', 'master')
    with self.assertRaises(exception):
      cursor.execute(
          query, {},
          entity_keyspace_id_map={1: self.KEYSPACE_ID_0X80},
          entity_column_name='user_id')
    cursor.close()

    # ExecuteBatchKeyspaceIds test
    cursor = self._open_batch_cursor()
    with self.assertRaises(exception):
      cursor.executemany(
          sql=None,
          params_list=[
              dict(
                  sql=query,
                  bind_variables={},
                  keyspace='keyspace',
                  keyspace_ids=[self.KEYSPACE_ID_0X80])])
    cursor.close()

    # ExecuteBatchShard test
    cursor = self._open_batch_cursor()
    with self.assertRaises(exception):
      cursor.executemany(
          sql=None,
          params_list=[
              dict(
                  sql=query,
                  bind_variables={},
                  keyspace='keyspace',
                  shards=[keyrange_constants.SHARD_ZERO])])
    cursor.close()

  def _verify_exception_for_stream_execute(self, query, exception):
    """Verify that we raise a specific exception for all StreamExecute calls.

    Args:
      query: query string to use for StreamExecute calls.
      exception: exception class that we expect StreamExecute to raise.
    """
    # StreamExecuteKeyspaceIds test
    cursor = self._open_stream_keyspace_ids_cursor()
    with self.assertRaises(exception):
      cursor.execute(query, {})
    cursor.close()

    # StreamExecuteKeyRanges test
    cursor = self._open_stream_keyranges_cursor()
    with self.assertRaises(exception):
      cursor.execute(query, {})
    cursor.close()

  def test_streaming_integrity_error(self):
    """Test we raise dbexceptions.IntegrityError for StreamExecute calls."""
    self._verify_exception_for_stream_execute(
        'error://integrity error',
        dbexceptions.IntegrityError)

  def test_transient_error(self):
    """Test we raise dbexceptions.TransientError for Execute calls."""
    # Special query that makes vtgateclienttest return a TransientError.
    self._verify_exception_for_execute(
        'error://transient error',
        dbexceptions.TransientError)

  def test_streaming_transient_error(self):
    """Test we raise dbexceptions.IntegrityError for StreamExecute calls."""
    self._verify_exception_for_stream_execute(
        'error://transient error',
        dbexceptions.TransientError)

  def test_error(self):
    """Test a regular server error raises the right exception."""
    error_request = 'error://unknown error'
    error_caller_id = vtgate_client.CallerID(principal=error_request)

    # Begin test
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, 'forced error'):
      self.conn.begin(error_caller_id)

    # Commit test
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, 'forced error'):
      self.conn.begin(error_caller_id)

    # Rollback test
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, 'forced error'):
      self.conn.begin(error_caller_id)

    # GetSrvKeyspace test
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, 'forced error'):
      self.conn.get_srv_keyspace(error_request)


class TestPythonClient(TestPythonClientBase):
  """Non-error test cases for the Python client."""

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
    good_effective_caller_id = vtgate_client.CallerID(
        principal='pr', component='co', subcomponent='su')
    bad_effective_caller_id = vtgate_client.CallerID(
        principal='pr_wrong', component='co_wrong', subcomponent='su_wrong')

    def check_good_and_bad_effective_caller_ids(cursor, cursor_execute_method):
      cursor.set_effective_caller_id(good_effective_caller_id)
      with self.assertRaises(dbexceptions.DatabaseError) as cm:
        cursor_execute_method(cursor)
      self.assertIn('SUCCESS:', str(cm.exception))

      cursor.set_effective_caller_id(bad_effective_caller_id)
      with self.assertRaises(dbexceptions.DatabaseError) as cm:
        cursor_execute_method(cursor)
      self.assertNotIn('SUCCESS:', str(cm.exception))

    def cursor_execute_keyspace_ids_method(cursor):
      cursor.execute(effective_caller_id_test_query, {})

    check_good_and_bad_effective_caller_ids(
        self._open_keyspace_ids_cursor(), cursor_execute_keyspace_ids_method)

    def cursor_execute_key_ranges_method(cursor):
      cursor.execute(effective_caller_id_test_query, {})

    check_good_and_bad_effective_caller_ids(
        self._open_keyranges_cursor(), cursor_execute_key_ranges_method)

    def cursor_execute_entity_ids_method(cursor):
      cursor.execute(
          effective_caller_id_test_query, {},
          entity_keyspace_id_map={1: self.KEYSPACE_ID_0X80},
          entity_column_name='user_id')

    check_good_and_bad_effective_caller_ids(
        self.conn.cursor('keyspace', 'master'),
        cursor_execute_entity_ids_method)

    def cursor_execute_batch_keyspace_ids_method(cursor):
      cursor.executemany(
          sql=None,
          params_list=[dict(
              sql=effective_caller_id_test_query, bind_variables={},
              keyspace='keyspace',
              keyspace_ids=[self.KEYSPACE_ID_0X80])])

    check_good_and_bad_effective_caller_ids(
        self._open_batch_cursor(), cursor_execute_batch_keyspace_ids_method)

    def cursor_execute_batch_shard_method(cursor):
      cursor.executemany(
          sql=None,
          params_list=[dict(
              sql=effective_caller_id_test_query, bind_variables={},
              keyspace='keyspace',
              shards=[keyrange_constants.SHARD_ZERO])])

    check_good_and_bad_effective_caller_ids(
        self._open_batch_cursor(), cursor_execute_batch_shard_method)

    def cursor_stream_execute_keyspace_ids_method(cursor):
      cursor.execute(sql=effective_caller_id_test_query, bind_variables={})

    check_good_and_bad_effective_caller_ids(
        self._open_stream_keyspace_ids_cursor(),
        cursor_stream_execute_keyspace_ids_method)

    def cursor_stream_execute_keyranges_method(cursor):
      cursor.execute(sql=effective_caller_id_test_query, bind_variables={})

    check_good_and_bad_effective_caller_ids(
        self._open_stream_keyranges_cursor(),
        cursor_stream_execute_keyranges_method)


if __name__ == '__main__':
  utils.main()
