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

"""This test provides a blue print for a service using a caching layer.

We use an in-memory cache to simplify the implementation, but using a
distributed memcache pool for instance would work the same way. Note
we always use CAS (compare and swap) on the cache values, and ignore
the failures. That way multiple processes trying to affect the cache
won't be an issue.

It starts an invalidation thread, that mimics what a cache
invalidation process would do.

It also has an application layer that can also write entries to the
cache if necessary.
"""

import logging
import threading
import time
import unittest

import environment
import tablet
import utils
from vtdb import dbexceptions
from vtdb import event_token
from vtdb import proto3_encoding
from vtdb import vtgate_client
from vtproto import topodata_pb2
from vtproto import query_pb2


master_tablet = tablet.Tablet()
replica_tablet = tablet.Tablet()

_create_vt_a = '''create table if not exists vt_a (
id bigint,
name varchar(128),
primary key(id)
) Engine=InnoDB'''

_create_vt_b = '''create table if not exists vt_b (
id bigint,
address varchar(128),
primary key(id)
) Engine=InnoDB'''


def setUpModule():
  try:
    environment.topo_server().setup()
    setup_procs = [master_tablet.init_mysql(),
                   replica_tablet.init_mysql()]
    utils.wait_procs(setup_procs)

    # start a vtctld so the vtctl insert commands are just RPCs, not forks.
    utils.Vtctld().start()

    # Start up a master mysql and vttablet
    logging.debug('Setting up tablets')
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])
    master_tablet.init_tablet('replica', 'test_keyspace', '0', tablet_index=0)
    replica_tablet.init_tablet('replica', 'test_keyspace', '0', tablet_index=1)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)
    master_tablet.create_db('vt_test_keyspace')
    replica_tablet.create_db('vt_test_keyspace')

    master_tablet.start_vttablet(wait_for_state=None)
    replica_tablet.start_vttablet(wait_for_state=None)
    master_tablet.wait_for_vttablet_state('NOT_SERVING')
    replica_tablet.wait_for_vttablet_state('NOT_SERVING')
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     master_tablet.tablet_alias], auto_log=True)

    utils.wait_for_tablet_type(replica_tablet.tablet_alias, 'replica')
    master_tablet.wait_for_vttablet_state('SERVING')
    replica_tablet.wait_for_vttablet_state('SERVING')

    master_tablet.mquery('vt_test_keyspace', _create_vt_a)
    master_tablet.mquery('vt_test_keyspace', _create_vt_b)

    utils.run_vtctl(['ReloadSchemaShard', 'test_keyspace/0'])
    utils.run_vtctl(['RebuildVSchemaGraph'])

    utils.VtGate().start(tablets=[master_tablet, replica_tablet])
    utils.vtgate.wait_for_endpoints('test_keyspace.0.master', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.0.replica', 1)

  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return
  logging.debug('Tearing down the servers and setup')
  tablet.kill_tablets([master_tablet, replica_tablet])
  teardown_procs = [master_tablet.teardown_mysql(),
                    replica_tablet.teardown_mysql()]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()
  master_tablet.remove_tree()
  replica_tablet.remove_tree()


class Cache(object):
  """Cache is the in-memory cache for objects.

  Its backing store is a dict, indexed by key '<table name>-<id>'.
  Each value is a dict, with three values:
  - 'version': a version number.
  - 'token': an EventToken.
  - 'value': an optional value. We use an empty tuple if the row is
  not in the database.
  """

  def __init__(self):
    self.values = {}
    self.stats = {
        'cache_miss': 0,
        'cache_hit': 0,
        'add': 0,
        'cas': 0,
        'noop': 0,
    }

  def gets(self, table_name, row_id):
    """Returns a cache entry if it exists.

    Args:
      table_name: the name of the table.
      row_id: the row id.

    Returns:
      version: entry version, or None if there is no entry.
      token: the EventToken for that row.
      value: an optional value, or None if not set.
    """
    key = '%s-%d' % (table_name, row_id)
    if key not in self.values:
      self.stats['cache_miss'] += 1
      return None, None, None
    self.stats['cache_hit'] += 1
    entry = self.values[key]
    return entry['version'], entry['token'], entry['value']

  def add(self, table_name, row_id, token, value):
    """Add a value to the cache, only if it doesn't exist.

    Args:
      table_name: the name of the table.
      row_id: the row id.
      token: the EventToken associated with the read / invalidation.
      value: the actual value.

    Raises:
      KeyError: if the entry already exists.
    """
    key = '%s-%d' % (table_name, row_id)
    if key in self.values:
      raise KeyError('add failed: key %s already in cache' % key)
    self.stats['add'] += 1
    self.values[key] = {
        'version': 1,
        'token': token,
        'value': value,
    }

  def cas(self, table_name, row_id, version, token, value):
    """Update an entry in the cache.

    Args:
      table_name: the name of the table.
      row_id: the row id.
      version: the existing version to update.
      token: the EventToken associated with the read / invalidation.
      value: the actual value.

    Raises:
      KeyError: if the entry doesn't exist.
      Exception: if the version is wrong.
    """
    key = '%s-%d' % (table_name, row_id)
    if key not in self.values:
      raise KeyError('cas failed: key %s not in cache' % key)
    if self.values[key]['version'] != version:
      raise Exception(
          'cas failed: invalid version %d, have version %d in cache' %
          (version, self.values[key]['version']))
    self.stats['cas'] += 1
    self.values[key] = {
        'version': version+1,
        'token': token,
        'value': value,
    }

  def noop(self):
    """Increments the noop counter."""
    self.stats['noop'] += 1

  def stats_copy(self):
    """Returns a copy of the cache stats."""
    return self.stats.copy()

  def stats_diff(self, before, **kwargs):
    """Returns true iff the before stats differ exactly with provided args."""
    for name, value in kwargs.iteritems():
      if self.stats[name] != before[name] + value:
        return False
    return True


class InvalidatorThread(threading.Thread):

  def __init__(self, cache):
    threading.Thread.__init__(self)
    self.cache = cache
    self.done = False

    protocol, addr = utils.vtgate.rpc_endpoint(python=True)
    self.conn = vtgate_client.connect(protocol, addr, 30.0)
    self.timestamp = long(time.time())

    self.start()

  def run(self):
    while True:
      try:
        for event, resume_timestamp in self.conn.update_stream(
            'test_keyspace', topodata_pb2.REPLICA,
            timestamp=self.timestamp,
            shard='0'):

          if self.done:
            return

          # Save the timestamp we get, so we can resume from it in case of
          # restart.
          self.timestamp = resume_timestamp
          for statement in event.statements:
            if statement.category == 1:  # query_pb2.StreamEvent.DML
              _, rows = proto3_encoding.convert_stream_event_statement(
                  statement)
              for row in rows:
                row_id = row[0]
                self.invalidate(statement.table_name, row_id,
                                event.event_token)

      except dbexceptions.DatabaseError:
        logging.exception(
            'InvalidatorThread got exception, continuing from timestamp %d',
            self.timestamp)

  def invalidate(self, table_name, row_id, token):
    logging.debug('Invalidating %s(%d) - %s:', table_name, row_id, token)
    version, cache_event_token, _ = self.cache.gets(table_name, row_id)
    if version is None:
      logging.debug('  no entry in cache, saving event_token')
      self.cache.add(table_name, row_id, token, None)
      return

    if event_token.fresher(cache_event_token, token) >= 0:
      # For invalidation, a couple things to consider:
      # 1. If we can't compare the EventTokens, we want to store the
      # invalidation, so it's safer.
      # 2. If we have exactly the same EventToken, we do not want to
      # store the ivalidation. We have either an invalidation or a
      # value, in both cases we're fine keeping it.
      logging.debug('  invalidation event is older or equal than cache value,'
                    ' ignoring')
      return

    logging.debug('  updating entry in the cache')
    self.cache.cas(table_name, row_id, version, token, None)

  def kill(self):
    """Kill stops the invalidator. We force an event so we can exit the loop."""
    logging.info('Stopping invalidator')
    self.done = True
    replica_tablet.mquery('vt_test_keyspace', 'flush logs')
    self.join()
    self.conn.close()


class TestCacheInvalidation(unittest.TestCase):

  def setUp(self):
    self.cache = Cache()
    self.invalidator = InvalidatorThread(self.cache)

    # Sleep a bit to be sure all binlog reading threads are going, and we
    # eat up all previous invalidation messages.
    time.sleep(1)

  def tearDown(self):
    self.invalidator.kill()

  def _vtgate_connection(self):
    protocol, addr = utils.vtgate.rpc_endpoint(python=True)
    return vtgate_client.connect(protocol, addr, 30.0)

  def _insert_value_a(self, row_id, name):
    logging.debug('Inserting value %d into vt_a', row_id)
    conn = self._vtgate_connection()
    cursor = conn.cursor(tablet_type='master', keyspace='test_keyspace',
                         writable=True)
    cursor.begin()
    insert = 'insert into vt_a (id, name) values (:id, :name)'
    bind_variables = {
        'id': row_id,
        'name': name,
    }
    cursor.execute(insert, bind_variables)
    cursor.commit()
    conn.close()

  def _get_value(self, table_name, row_id):
    """Returns the value for a row, as an array.

    Args:
      table_name: the name of the table.
      row_id: the row id.

    Returns:
      The value for the row as a tuple, or an empty tuple if the row
        doesn't exist.
    """
    logging.debug('Getting value %d from %s:', row_id, table_name)

    # First look in the cache.
    version, cache_event_token, value = self.cache.gets(table_name, row_id)
    if value is not None:
      logging.debug('  got value from cache: %s', value)
      return value

    # It's not in the cache, get it from the database.
    conn = self._vtgate_connection()
    cursor = conn.cursor(tablet_type='replica', keyspace='test_keyspace')
    cursor.execute('select * from %s where id=:id' % table_name,
                   {'id': row_id}, include_event_token=True,
                   compare_event_token=cache_event_token)
    result = cursor.fetchall()
    if not result:
      # Not in the database. Use an empty array.
      logging.debug('  not in the database')
      value = ()
    else:
      value = result[0]
    conn.close()

    # If there was no cached version, cache what we got,
    # along with the event token.
    if version is None:
      logging.debug('  adding value to cache: %s', value)
      self.cache.add(table_name, row_id, conn.event_token, value)
      return value

    # If there was a cached version, and the version we got is older,
    # we can't update the cache.
    if cache_event_token and not conn.fresher:
      logging.debug('  database value is not fresher: %s', value)
      self.cache.noop()
      return value

    # Save in the cache.
    logging.debug('  setting value in cache: %s', value)
    self.cache.cas(table_name, row_id, version, conn.event_token, value)
    return value

  def wait_for_cache_stats(self, stats, **kwargs):
    timeout = 10
    while True:
      if self.cache.stats_diff(stats, **kwargs):
        return
      timeout = utils.wait_step('cache stats update %s' % str(kwargs), timeout)

  def test_cache_invalidation(self):
    """Main test case in this suite."""

    # Insert.
    stats = self.cache.stats_copy()
    self._insert_value_a(1, 'test_cache_invalidation object')

    # Sleep a bit to be sure the value was propagated.
    self.wait_for_cache_stats(stats, cache_miss=1, add=1)

    # Then get the value.  We cannot populate the cache here, as the
    # timestamp of the latest replication EventToken is the same as
    # the invalidation token (only one transaction in the binlogs so
    # far).
    stats = self.cache.stats_copy()
    result = self._get_value('vt_a', 1)
    self.assertEqual(result, (1, 'test_cache_invalidation object'))
    self.assertTrue(self.cache.stats_diff(stats, cache_hit=1, noop=1))

    # Insert a second value with a greater timestamp to move the
    # current replica EventToken.
    time.sleep(1)
    stats = self.cache.stats_copy()
    self._insert_value_a(2, 'second object')

    # Sleep a bit to be sure the value was propagated.
    self.wait_for_cache_stats(stats, cache_miss=1, add=1)

    # This time, when we get the value, the current replica
    # EventToken is ahead of the invalidation timestamp of the first
    # object, so we should save it in the cache.
    stats = self.cache.stats_copy()
    result = self._get_value('vt_a', 1)
    self.assertEqual(result, (1, 'test_cache_invalidation object'))
    self.assertTrue(self.cache.stats_diff(stats, cache_hit=1, cas=1))

    # Ask again, should be from cache now.
    stats = self.cache.stats_copy()
    result = self._get_value('vt_a', 1)
    self.assertEqual(result, (1, 'test_cache_invalidation object'))
    self.assertTrue(self.cache.stats_diff(stats, cache_hit=1,
                                          add=0, cas=0, noop=0))

  def test_empty_cache_value(self):
    """Tests a non-existing value is cached properly."""

    # Try to read a non-existing value, should get a miss.
    stats = self.cache.stats_copy()
    result = self._get_value('vt_a', 3)
    self.assertEqual(result, ())
    self.assertTrue(self.cache.stats_diff(stats, cache_miss=1, add=1))

    # Try to read again, should get a hit to an empty value.
    stats = self.cache.stats_copy()
    result = self._get_value('vt_a', 3)
    self.assertEqual(result, ())
    self.assertTrue(self.cache.stats_diff(stats, cache_hit=1,
                                          add=0, cas=0, noop=0))

    # Now create the value.
    stats = self.cache.stats_copy()
    self._insert_value_a(3, 'empty cache test object')

    # Wait a bit for cache to get event, make sure we invalidated.
    self.wait_for_cache_stats(stats, cache_hit=1, cas=1)

    # Get the value, make sure we got it from DB, with no possible cache update.
    stats = self.cache.stats_copy()
    result = self._get_value('vt_a', 3)
    self.assertEqual(result, (3, 'empty cache test object'))
    self.assertTrue(self.cache.stats_diff(stats, cache_hit=1, noop=1))

    # Insert a second value with a greater timestamp to move the
    # current replica EventToken.
    time.sleep(1)
    stats = self.cache.stats_copy()
    self._insert_value_a(4, 'second object')

    # Sleep a bit to be sure the value was propagated.
    self.wait_for_cache_stats(stats, cache_miss=1, add=1)

    # This time, when we get the value, the current replica
    # EventToken is ahead of the invalidation timestamp of the first
    # object, so we should save it in the cache.
    stats = self.cache.stats_copy()
    result = self._get_value('vt_a', 3)
    self.assertEqual(result, (3, 'empty cache test object'))
    self.assertTrue(self.cache.stats_diff(stats, cache_hit=1, cas=1))

    # Ask again, should be from cache now.
    stats = self.cache.stats_copy()
    result = self._get_value('vt_a', 3)
    self.assertEqual(result, (3, 'empty cache test object'))
    self.assertTrue(self.cache.stats_diff(stats, cache_hit=1,
                                          add=0, cas=0, noop=0))

  def test_event_token_fresher(self):
    """event_token.fresher test suite."""
    test_cases = [
        {
            'ev1': None,
            'ev2': None,
            'expected': -1,
        }, {
            'ev1': query_pb2.EventToken(
                timestamp=123,
            ),
            'ev2': None,
            'expected': -1,
        }, {
            'ev1': None,
            'ev2': query_pb2.EventToken(
                timestamp=123,
            ),
            'expected': -1,
        }, {
            'ev1': query_pb2.EventToken(
                timestamp=123,
            ),
            'ev2': query_pb2.EventToken(
                timestamp=123,
            ),
            'expected': -1,
        }, {
            'ev1': query_pb2.EventToken(
                timestamp=200,
            ),
            'ev2': query_pb2.EventToken(
                timestamp=100,
            ),
            'expected': 100,
        }, {
            'ev1': query_pb2.EventToken(
                timestamp=100,
            ),
            'ev2': query_pb2.EventToken(
                timestamp=200,
            ),
            'expected': -100,
        }, {
            # Test cases with not enough information to compare.
            'ev1': query_pb2.EventToken(
                timestamp=100,
            ),
            'ev2': query_pb2.EventToken(
                timestamp=100,
            ),
            'expected': -1,
        }, {
            'ev1': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
            ),
            'ev2': query_pb2.EventToken(
                timestamp=100,
                shard='s2',
            ),
            'expected': -1,
        }, {
            'ev1': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
            ),
            'ev2': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
            ),
            'expected': -1,
        }, {
            'ev1': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='pos1',
            ),
            'ev2': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
            ),
            'expected': -1,
        }, {
            'ev1': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
            ),
            'ev2': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='pos2',
            ),
            'expected': -1,
        }, {
            'ev1': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='pos1',  # invalid on purpose
            ),
            'ev2': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='pos2',  # invalid on purpose
            ),
            'expected': -1,
        }, {
            'ev1': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='MariaDB/0-1-123',  # valid but different
            ),
            'ev2': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='MySQL56/33333333-3333-3333-3333-333333333333:456-789',
            ),
            'expected': -1,
        }, {
            # MariaDB test cases.
            'ev1': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='MariaDB/0-1-200',
            ),
            'ev2': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='MariaDB/0-1-100',
            ),
            'expected': 100,
        }, {
            'ev1': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='MariaDB/0-1-100',
            ),
            'ev2': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='MariaDB/0-1-200',
            ),
            'expected': -100,
        }, {
            'ev1': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='MariaDB/0-1-100',
            ),
            'ev2': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='MariaDB/0-1-100',
            ),
            'expected': 0,
        }, {
            # MySQL56 test cases, not supported yet.
            'ev1': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='MySQL56/33333333-3333-3333-3333-333333333333:1-200',
            ),
            'ev2': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='MySQL56/33333333-3333-3333-3333-333333333333:1-100',
            ),
            'expected': -1,  # Should be: 1,
        }, {
            'ev1': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='MySQL56/33333333-3333-3333-3333-333333333333:1-100',
            ),
            'ev2': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='MySQL56/33333333-3333-3333-3333-333333333333:1-200',
            ),
            'expected': -1,
        }, {
            'ev1': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='MySQL56/33333333-3333-3333-3333-333333333333:1-100',
            ),
            'ev2': query_pb2.EventToken(
                timestamp=100,
                shard='s1',
                position='MySQL56/33333333-3333-3333-3333-333333333333:1-100',
            ),
            'expected': -1,  # Should be: 0,
        }
    ]

    for tcase in test_cases:
      got = event_token.fresher(tcase['ev1'], tcase['ev2'])
      self.assertEqual(got, tcase['expected'],
                       'got %d but expected %d for Fresher(%s, %s)' %
                       (got, tcase['expected'], tcase['ev1'], tcase['ev2']))


if __name__ == '__main__':
  utils.main()
