#!/usr/bin/env python
#
# Copyright 2015 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""This is the test suite for a vtgate_client implementation.
"""

import struct

from google.protobuf import text_format

from vtdb import dbexceptions
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtgate_client
from vtdb import vtgate_cursor
from vtproto import query_pb2
from vtproto import topodata_pb2


class TestPythonClientBase(object):
  """Base class for Python client tests."""

  # conn must be opened as a vtgate_client connection.
  # This is not done by this class.
  conn = None

  # A packed keyspace_id from the middle of the full keyrange.
  KEYSPACE_ID_0X80 = struct.Struct('!Q').pack(0x80 << 56)

  def _open_v3_cursor(self):
    return self.conn.cursor(keyspace=None, tablet_type='master')

  def _open_shards_cursor(self):
    return self.conn.cursor(
        tablet_type='master', keyspace='keyspace', shards=['-80'])

  def _open_keyspace_ids_cursor(self):
    return self.conn.cursor(
        tablet_type='master', keyspace='keyspace',
        keyspace_ids=[self.KEYSPACE_ID_0X80])

  def _open_keyranges_cursor(self):
    kr = keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)
    return self.conn.cursor(
        tablet_type='master', keyspace='keyspace', keyranges=[kr])

  def _open_batch_cursor(self):
    return self.conn.cursor(tablet_type='master', keyspace=None)

  def _open_stream_v3_cursor(self):
    return self.conn.cursor(
        tablet_type='master', keyspace=None,
        cursorclass=vtgate_cursor.StreamVTGateCursor)

  def _open_stream_shards_cursor(self):
    return self.conn.cursor(
        tablet_type='master', keyspace='keyspace', shards=['-80'],
        cursorclass=vtgate_cursor.StreamVTGateCursor)

  def _open_stream_keyspace_ids_cursor(self):
    return self.conn.cursor(
        tablet_type='master', keyspace='keyspace',
        keyspace_ids=[self.KEYSPACE_ID_0X80],
        cursorclass=vtgate_cursor.StreamVTGateCursor)

  def _open_stream_keyranges_cursor(self):
    kr = keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)
    return self.conn.cursor(
        tablet_type='master', keyspace='keyspace', keyranges=[kr],
        cursorclass=vtgate_cursor.StreamVTGateCursor)


class TestErrors(TestPythonClientBase):
  """Test cases to verify that the Python client can handle errors correctly."""

  def _verify_exception_for_execute(self, query, exception):
    """Verify that we raise a specific exception for all Execute calls.

    Args:
      query: query string to use for execute calls.
      exception: exception class that we expect the execute call to raise.
    """

    # Execute test
    cursor = self._open_v3_cursor()
    with self.assertRaises(exception):
      cursor.execute(query, {})
    cursor.close()

    # ExecuteShards test
    cursor = self._open_shards_cursor()
    with self.assertRaises(exception):
      cursor.execute(query, {})
    cursor.close()

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
    cursor = self.conn.cursor(tablet_type='master', keyspace='keyspace')
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

    # ExecuteBatchShards test
    cursor = self._open_batch_cursor()
    with self.assertRaises(exception):
      cursor.executemany(
          sql=None,
          params_list=[
              dict(
                  sql=query,
                  bind_variables={},
                  keyspace='keyspace',
                  shards=['0'])])
    cursor.close()

  def _verify_exception_for_stream_execute(self, query, exception):
    """Verify that we raise a specific exception for all StreamExecute calls.

    Args:
      query: query string to use for StreamExecute calls.
      exception: exception class that we expect StreamExecute to raise.
    """
    # StreamExecute test
    cursor = self._open_stream_v3_cursor()
    with self.assertRaises(exception):
      cursor.execute(query, {})
    cursor.close()

    # StreamExecuteShards test
    cursor = self._open_stream_shards_cursor()
    with self.assertRaises(exception):
      cursor.execute(query, {})
    cursor.close()

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

    # UpdateStream test
    with self.assertRaises(exception):
      for _, _ in self.conn.update_stream(
          'test_keyspace', topodata_pb2.MASTER,
          shard=query):
        pass

  def test_partial_integrity_errors(self):
    """Raise an IntegrityError when Execute returns a partial error."""
    # Special query that makes vtgateclienttest return a partial error.
    self._verify_exception_for_execute(
        'partialerror://integrity error',
        dbexceptions.IntegrityError)

  def _verify_exception_for_all_execute_methods(self, query, exception):
    self._verify_exception_for_execute(query, exception)
    self._verify_exception_for_stream_execute(query, exception)

  def test_integrity_error(self):
    """Test we raise dbexceptions.IntegrityError."""
    self._verify_exception_for_all_execute_methods(
        'error://integrity error',
        dbexceptions.IntegrityError)

  def test_transient_error(self):
    """Test we raise dbexceptions.TransientError for Execute calls."""
    # Special query that makes vtgateclienttest return a TransientError.
    self._verify_exception_for_all_execute_methods(
        'error://transient error',
        dbexceptions.TransientError)

  def test_throttled_error(self):
    """Test we raise dbexceptions.ThrottledError."""
    # Special query that makes vtgateclienttest return a ThrottledError.
    self._verify_exception_for_all_execute_methods(
        'error://throttled error',
        dbexceptions.ThrottledError)

  def test_query_not_served_error(self):
    """Test we raise dbexceptions.QueryNotServed."""
    # Special query that makes vtgateclienttest return QueryNotServed.
    self._verify_exception_for_all_execute_methods(
        'error://query not served',
        dbexceptions.QueryNotServed)

  def test_programming_error(self):
    """Test we raise dbexceptions.ProgrammingError."""
    # Special query that makes vtgateclienttest return a ProgrammingError.
    self._verify_exception_for_all_execute_methods(
        'error://bad input',
        dbexceptions.ProgrammingError)

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


class TestTransactionFlags(TestPythonClientBase):
  """Test transaction flags."""

  def test_begin(self):
    """Test begin transaction flags."""
    self.conn.begin()
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, 'single db'):
      self.conn.begin(single_db=True)

    self.conn.commit()
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, 'twopc'):
      self.conn.commit(twopc=True)


class TestSuccess(TestPythonClientBase):
  """Success test cases for the Python client."""

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


class TestCallerId(TestPythonClientBase):
  """Caller ID test cases for the Python client."""

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

    # test Execute
    def cursor_execute_method(cursor):
      cursor.execute(effective_caller_id_test_query, {})

    check_good_and_bad_effective_caller_ids(
        self._open_v3_cursor(), cursor_execute_method)

    # test ExecuteShards
    def cursor_execute_shards_method(cursor):
      cursor.execute(effective_caller_id_test_query, {})

    check_good_and_bad_effective_caller_ids(
        self._open_shards_cursor(), cursor_execute_shards_method)

    # test ExecuteKeyspaceIds
    def cursor_execute_keyspace_ids_method(cursor):
      cursor.execute(effective_caller_id_test_query, {})

    check_good_and_bad_effective_caller_ids(
        self._open_keyspace_ids_cursor(), cursor_execute_keyspace_ids_method)

    # test ExecuteKeyRanges
    def cursor_execute_key_ranges_method(cursor):
      cursor.execute(effective_caller_id_test_query, {})

    check_good_and_bad_effective_caller_ids(
        self._open_keyranges_cursor(), cursor_execute_key_ranges_method)

    # test ExecuteEntityIds
    def cursor_execute_entity_ids_method(cursor):
      cursor.execute(
          effective_caller_id_test_query, {},
          entity_keyspace_id_map={1: self.KEYSPACE_ID_0X80},
          entity_column_name='user_id')

    check_good_and_bad_effective_caller_ids(
        self.conn.cursor(tablet_type='master', keyspace='keyspace'),
        cursor_execute_entity_ids_method)

    # test ExecuteBatchKeyspaceIds
    def cursor_execute_batch_keyspace_ids_method(cursor):
      cursor.executemany(
          sql=None,
          params_list=[dict(
              sql=effective_caller_id_test_query, bind_variables={},
              keyspace='keyspace',
              keyspace_ids=[self.KEYSPACE_ID_0X80])])

    check_good_and_bad_effective_caller_ids(
        self._open_batch_cursor(), cursor_execute_batch_keyspace_ids_method)

    # test ExecuteBatchShards
    def cursor_execute_batch_shard_method(cursor):
      cursor.executemany(
          sql=None,
          params_list=[dict(
              sql=effective_caller_id_test_query, bind_variables={},
              keyspace='keyspace',
              shards=['0'])])

    check_good_and_bad_effective_caller_ids(
        self._open_batch_cursor(), cursor_execute_batch_shard_method)

    # test StreamExecute
    def cursor_stream_execute_v3_method(cursor):
      cursor.execute(sql=effective_caller_id_test_query, bind_variables={})

    check_good_and_bad_effective_caller_ids(
        self._open_stream_v3_cursor(),
        cursor_stream_execute_v3_method)

    # test StreamExecuteShards
    def cursor_stream_execute_shards_method(cursor):
      cursor.execute(sql=effective_caller_id_test_query, bind_variables={})

    check_good_and_bad_effective_caller_ids(
        self._open_stream_shards_cursor(),
        cursor_stream_execute_shards_method)

    # test StreamExecuteKeyspaceIds
    def cursor_stream_execute_keyspace_ids_method(cursor):
      cursor.execute(sql=effective_caller_id_test_query, bind_variables={})

    check_good_and_bad_effective_caller_ids(
        self._open_stream_keyspace_ids_cursor(),
        cursor_stream_execute_keyspace_ids_method)

    # test StreamExecuteKeyRanges
    def cursor_stream_execute_keyranges_method(cursor):
      cursor.execute(sql=effective_caller_id_test_query, bind_variables={})

    check_good_and_bad_effective_caller_ids(
        self._open_stream_keyranges_cursor(),
        cursor_stream_execute_keyranges_method)


class TestEcho(TestPythonClientBase):
  """Send queries to the server, check the returned result matches."""

  echo_prefix = 'echo://'

  query = (
      u'test query with bind variables: :int :float :bytes, unicode: '
      u'\u6211\u80fd\u541e\u4e0b\u73bb\u7483\u800c\u4e0d\u50b7\u8eab\u9ad4'
      ).encode('utf-8')
  query_echo = (
      u'test query with bind variables: :int :float :bytes, unicode: '
      u'\u6211\u80fd\u541e\u4e0b\u73bb\u7483\u800c\u4e0d\u50b7\u8eab\u9ad4'
      ).encode('utf-8')
  keyspace = 'test_keyspace'

  shards = ['-80', '80-']
  shards_echo = '[-80 80-]'

  keyspace_ids = ['\x01\x02\x03\x04', '\x05\x06\x07\x08']
  keyspace_ids_echo = '[[1 2 3 4] [5 6 7 8]]'

  # FIXME(alainjobart) using a map for the entities makes it impossible to
  # guarantee the order of the entities in the query. It is really an API
  # problem here? For this test, however, I'll just use a single value for now
  entity_keyspace_ids = {
      123: '\x01\x02\x03',
      #      2.0: '\x04\x05\x06',
      #      '\x01\x02\x03': '\x07\x08\x09',
  }
#  entity_keyspace_ids_echo = ('[type:INT64 value:"123" '
#                              'keyspace_id:"\\001\\002\\003"  '
#                              'type:FLOAT64 value:"2" '
#                              'keyspace_id:"\\004\\005\\006"  '
#                              'type:VARBINARY value:"\\001\\002\\003" '
#                              'keyspace_id:"\\007\\010\\t" ]')
  entity_keyspace_ids_echo = ('[type:INT64 value:"123" '
                              'keyspace_id:"\\001\\002\\003" ]')

  key_ranges = [keyrange.KeyRange('01020304-05060708')]
  key_ranges_echo = '[start:"\\001\\002\\003\\004" end:"\\005\\006\\007\\010" ]'

  tablet_type = 'replica'
  tablet_type_echo = 'REPLICA'

  bind_variables = {
      'int': 123,
      'float': 2.1,
      'bytes': '\x01\x02\x03',
      'bool': True,
  }
  bind_variables_echo = ('map[bool:type:INT64 value:"1"  '
                         'bytes:type:VARBINARY value:"\\001\\002\\003"  '
                         'float:type:FLOAT64 value:"2.1"  '
                         'int:type:INT64 value:"123" ]')

  caller_id = vtgate_client.CallerID(
      principal='test_principal',
      component='test_component',
      subcomponent='test_subcomponent')
  caller_id_echo = ('principal:"test_principal" component:"test_component"'
                    ' subcomponent:"test_subcomponent" ')

  event_token = query_pb2.EventToken(timestamp=123,
                                     shard=shards[0],
                                     position='test_pos')
  options_echo = ('include_event_token:true compare_event_token:'
                  '<timestamp:123 shard:"-80" position:"test_pos" > ')

  def test_echo_execute(self):
    """This test calls the echo method."""

    # Execute
    cursor = self.conn.cursor(tablet_type=self.tablet_type, keyspace=None)
    cursor.set_effective_caller_id(self.caller_id)
    cursor.execute(self.echo_prefix+self.query, self.bind_variables,
                   include_event_token=True,
                   compare_event_token=self.event_token)
    self._check_echo(cursor, {
        'callerId': self.caller_id_echo,
        # FIXME(alainjobart) change this to query_echo once v3 understand binds
        'query': self.echo_prefix+self.query,
        'bindVars': self.bind_variables_echo,
        'tabletType': self.tablet_type_echo,
        'options': self.options_echo,
        'fresher': True,
        'eventToken': self.event_token,
    })
    cursor.close()

    # ExecuteShards
    cursor = self.conn.cursor(
        tablet_type=self.tablet_type, keyspace=self.keyspace,
        shards=self.shards)
    cursor.set_effective_caller_id(self.caller_id)
    cursor.execute(self.echo_prefix+self.query, self.bind_variables,
                   include_event_token=True,
                   compare_event_token=self.event_token)
    self._check_echo(cursor, {
        'callerId': self.caller_id_echo,
        'query': self.echo_prefix+self.query_echo,
        'keyspace': self.keyspace,
        'shards': self.shards_echo,
        'bindVars': self.bind_variables_echo,
        'tabletType': self.tablet_type_echo,
        'options': self.options_echo,
        'fresher': True,
        'eventToken': self.event_token,
    })
    cursor.close()

    # ExecuteKeyspaceIds
    cursor = self.conn.cursor(
        tablet_type=self.tablet_type, keyspace=self.keyspace,
        keyspace_ids=self.keyspace_ids)
    cursor.set_effective_caller_id(self.caller_id)
    cursor.execute(self.echo_prefix+self.query, self.bind_variables,
                   include_event_token=True,
                   compare_event_token=self.event_token)
    self._check_echo(cursor, {
        'callerId': self.caller_id_echo,
        'query': self.echo_prefix+self.query_echo,
        'keyspace': self.keyspace,
        'keyspaceIds': self.keyspace_ids_echo,
        'bindVars': self.bind_variables_echo,
        'tabletType': self.tablet_type_echo,
        'options': self.options_echo,
        'fresher': True,
        'eventToken': self.event_token,
    })
    cursor.close()

    # ExecuteKeyRanges
    cursor = self.conn.cursor(
        tablet_type=self.tablet_type, keyspace=self.keyspace,
        keyranges=self.key_ranges)
    cursor.set_effective_caller_id(self.caller_id)
    cursor.execute(self.echo_prefix+self.query, self.bind_variables,
                   include_event_token=True,
                   compare_event_token=self.event_token)
    self._check_echo(cursor, {
        'callerId': self.caller_id_echo,
        'query': self.echo_prefix+self.query_echo,
        'keyspace': self.keyspace,
        'keyRanges': self.key_ranges_echo,
        'bindVars': self.bind_variables_echo,
        'tabletType': self.tablet_type_echo,
    })
    cursor.close()

    # ExecuteEntityIds
    cursor = self.conn.cursor(
        tablet_type=self.tablet_type, keyspace=self.keyspace)
    cursor.set_effective_caller_id(self.caller_id)
    cursor.execute(self.echo_prefix+self.query, self.bind_variables,
                   entity_keyspace_id_map=self.entity_keyspace_ids,
                   entity_column_name='column1',
                   include_event_token=True,
                   compare_event_token=self.event_token)
    self._check_echo(cursor, {
        'callerId': self.caller_id_echo,
        'query': self.echo_prefix+self.query_echo,
        'keyspace': self.keyspace,
        'entityColumnName': 'column1',
        'entityIds': self.entity_keyspace_ids_echo,
        'bindVars': self.bind_variables_echo,
        'tabletType': self.tablet_type_echo,
        'options': self.options_echo,
        'fresher': True,
        'eventToken': self.event_token,
    })
    cursor.close()

    # ExecuteBatchShards
    cursor = self.conn.cursor(
        tablet_type=self.tablet_type, keyspace=None,
        as_transaction=True)
    cursor.set_effective_caller_id(self.caller_id)
    cursor.executemany(sql=None,
                       params_list=[
                           dict(
                               sql=self.echo_prefix+self.query,
                               bind_variables=self.bind_variables,
                               keyspace=self.keyspace,
                               shards=self.shards)])
    self._check_echo(cursor, {
        'callerId': self.caller_id_echo,
        'query': self.echo_prefix+self.query_echo,
        'keyspace': self.keyspace,
        'shards': self.shards_echo,
        'bindVars': self.bind_variables_echo,
        'tabletType': self.tablet_type_echo,
        'asTransaction': 'true',
    })
    cursor.close()

    # ExecuteBatchKeyspaceIds
    cursor = self.conn.cursor(
        tablet_type=self.tablet_type, keyspace=None,
        as_transaction=True)
    cursor.set_effective_caller_id(self.caller_id)
    cursor.executemany(sql=None,
                       params_list=[
                           dict(
                               sql=self.echo_prefix+self.query,
                               bind_variables=self.bind_variables,
                               keyspace=self.keyspace,
                               keyspace_ids=self.keyspace_ids)])
    self._check_echo(cursor, {
        'callerId': self.caller_id_echo,
        'query': self.echo_prefix+self.query_echo,
        'keyspace': self.keyspace,
        'keyspaceIds': self.keyspace_ids_echo,
        'bindVars': self.bind_variables_echo,
        'tabletType': self.tablet_type_echo,
        'asTransaction': 'true',
    })
    cursor.close()

  def _get_echo(self, cursor):
    result = {}
    data = cursor.fetchall()
    for i, (n, _) in enumerate(cursor.description):
      result[n] = data[0][i]
    return result

  def _check_echo(self, cursor, values):
    """_check_echo makes sure the echo result is correct."""
    got = self._get_echo(cursor)
    for k, v in values.iteritems():
      if k == 'fresher':
        self.assertTrue(self.conn.fresher)
      elif k == 'eventToken':
        self.assertEqual(text_format.MessageToString(self.conn.event_token),
                         text_format.MessageToString(v))
      else:
        self.assertEqual(got[k], v, 'item %s is different in result: got %s'
                         ' expected %s' % (k, got[k], v))

    # Check NULL and empty string.
    self.assertEqual(got['null'], None)
    self.assertEqual(got['emptyString'], '')
