#!/usr/bin/env python

import warnings

import json
import time
import urllib2

import logging
import unittest

import environment
import tablet
import utils

# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter('ignore')

master_tablet = tablet.Tablet()
replica_tablet = tablet.Tablet()
# Second replica to provide semi-sync ACKs while testing
# scenarios when the first replica is down.
replica2_tablet = tablet.Tablet()

all_tablets = [master_tablet, replica_tablet, replica2_tablet]

create_vt_insert_test = '''create table vt_insert_test (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''


def setUpModule():
  try:
    environment.topo_server().setup()

    # start mysql instance external to the test
    utils.wait_procs([t.init_mysql() for t in all_tablets])

    # start a vtctld so the vtctl insert commands are just RPCs, not forks
    utils.Vtctld().start()

    # Start up a master mysql and vttablet
    logging.debug('Setting up tablets')
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])
    master_tablet.init_tablet('master', 'test_keyspace', '0')
    replica_tablet.init_tablet('replica', 'test_keyspace', '0')
    replica2_tablet.init_tablet('replica', 'test_keyspace', '0')
    utils.validate_topology()

    for t in all_tablets:
      t.populate('vt_test_keyspace', create_vt_insert_test)

    for t in all_tablets:
      t.start_vttablet(memcache=True, wait_for_state=None)
    for t in all_tablets:
      t.wait_for_vttablet_state('SERVING')

    utils.run_vtctl(['InitShardMaster', 'test_keyspace/0',
                     master_tablet.tablet_alias], auto_log=True)
    utils.validate_topology()

    # restart the replica tablet so the stats are reset
    replica_tablet.kill_vttablet()
    replica_tablet.start_vttablet(memcache=True)
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return
  logging.debug('Tearing down the servers and setup')
  tablet.kill_tablets(all_tablets)
  utils.wait_procs([t.teardown_mysql() for t in all_tablets],
                   raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()
  for t in all_tablets:
    t.remove_tree()


class MultiDict(dict):

  def __getattr__(self, name):
    v = self[name]
    if type(v) == dict:
      v = MultiDict(v)
    return v

  def mget(self, mkey, default=None):
    keys = mkey.split('.')
    try:
      v = self
      for key in keys:
        v = v[key]
    except KeyError:
      v = default
    if type(v) == dict:
      v = MultiDict(v)
    return v


class RowCacheInvalidator(unittest.TestCase):

  def setUp(self):
    self.perform_insert(400)

  def tearDown(self):
    self.perform_delete()

  def replica_stats(self):
    url = 'http://localhost:%d/debug/table_stats' % replica_tablet.port
    return MultiDict(json.load(urllib2.urlopen(url)))

  def replica_vars(self):
    url = 'http://localhost:%d/debug/vars' % replica_tablet.port
    return MultiDict(json.load(urllib2.urlopen(url)))

  def perform_insert(self, count):
    for i in xrange(count):
      self._exec_vt_txn(
          "insert into vt_insert_test (msg) values ('test %s')" % i)

  def perform_delete(self):
    self._exec_vt_txn('delete from vt_insert_test')

  def _wait_for_replica(self):
    master_position = utils.mysql_query(master_tablet.tablet_uid,
                                        'vt_test_keyspace',
                                        'show master status')
    replica_tablet.mquery('vt_test_keyspace',
                          "select MASTER_POS_WAIT('%s', %d)" %
                          (master_position[0][0], master_position[0][1]), 5)

  def test_cache_invalidation(self):
    self._wait_for_replica()
    invalidations = self.replica_stats()['Totals']['Invalidations']
    invalidator_stats = self.replica_vars()
    logging.debug(
        'Invalidations %d InvalidatorStats %s',
        invalidations, invalidator_stats['RowcacheInvalidatorPosition'])
    self.assertTrue(
        invalidations > 0, 'Invalidations are not flowing through.')

    res = replica_tablet.mquery('vt_test_keyspace',
                                'select min(id) from vt_insert_test')
    self.assertNotEqual(res[0][0], None,
                        'Cannot proceed, no rows in vt_insert_test')
    mid = int(res[0][0])
    stats_dict = self.replica_stats()['vt_insert_test']
    logging.debug('vt_insert_test stats %s', stats_dict)
    misses = stats_dict['Misses']
    hits = stats_dict['Hits']
    replica_tablet.execute('select * from vt_insert_test where id=:id',
                           bindvars={'id': mid})
    stats_dict = self.replica_stats()['vt_insert_test']
    self.assertEqual(stats_dict['Misses'] - misses, 1,
                     "This shouldn't have hit the cache")

    replica_tablet.execute('select * from vt_insert_test where id=:id',
                           bindvars={'id': mid})
    stats_dict = self.replica_stats()['vt_insert_test']
    self.assertEqual(stats_dict['Hits'] - hits, 1,
                     'This should have hit the cache')

  def _wait_for_value(self, expected_result):
    timeout = 10
    while True:
      result = self._exec_replica_query(
          'select * from vt_insert_test where id = 1000000')
      if result == expected_result:
        return
      timeout = utils.wait_step(
          'replica rowcache updated, got %s expected %s' %
          (str(result), str(expected_result)), timeout,
          sleep_time=0.1)

  def test_outofband_statements(self):
    start = self.replica_vars()['InternalErrors'].get('Invalidation', 0)

    # Test update statement
    self._exec_vt_txn(
        "insert into vt_insert_test (id, msg) values (1000000, 'start')")
    self._wait_for_replica()
    self._wait_for_value([[1000000, 'start']])
    utils.mysql_write_query(
        master_tablet.tablet_uid,
        'vt_test_keyspace',
        "update vt_insert_test set msg = 'foo' where id = 1000000")
    self._wait_for_replica()
    self._wait_for_value([[1000000, 'foo']])
    end1 = self.replica_vars()['InternalErrors'].get('Invalidation', 0)
    self.assertEqual(start, end1)

    # Test delete statement
    utils.mysql_write_query(master_tablet.tablet_uid,
                            'vt_test_keyspace',
                            'delete from vt_insert_test where id = 1000000')
    self._wait_for_replica()
    self._wait_for_value([])
    end2 = self.replica_vars()['InternalErrors'].get('Invalidation', 0)
    self.assertEqual(end1, end2)

    # Test insert statement
    utils.mysql_write_query(
        master_tablet.tablet_uid,
        'vt_test_keyspace',
        "insert into vt_insert_test (id, msg) values(1000000, 'bar')")
    self._wait_for_replica()
    self._wait_for_value([[1000000, 'bar']])
    end3 = self.replica_vars()['InternalErrors'].get('Invalidation', 0)
    self.assertEqual(end2, end3)

    # Test unrecognized statement
    utils.mysql_query(master_tablet.tablet_uid,
                      'vt_test_keyspace',
                      'truncate table vt_insert_test')
    self._wait_for_replica()
    timeout = 10
    while True:
      end4 = self.replica_vars()['InternalErrors'].get('Invalidation', 0)
      if end4 == end3+1:
        break
      timeout = utils.wait_step('invalidation errors, got %d expecting %d' %
                                (end4, end3+1), timeout, sleep_time=0.1)
    self.assertEqual(end4, end3+1)

  def test_stop_replication(self):
    # wait for replication to catch up.
    self._wait_for_replica()

    # restart the replica tablet so the stats are reset
    replica_tablet.kill_vttablet()
    replica_tablet.start_vttablet(memcache=True)

    # insert 100 values, should cause 100 invalidations
    self.perform_insert(100)
    self._wait_for_replica()

    # wait until the slave processed all data
    timeout = 30
    while True:
      inv_count1 = self.replica_stats()['Totals']['Invalidations']
      if inv_count1 == 100:
        break
      timeout = utils.wait_step('invalidation count, got %d expecting %d' %
                                (inv_count1, 100), timeout, sleep_time=0.1)

    # stop replication insert more data, restart replication
    replica_tablet.mquery('vt_test_keyspace', 'stop slave')
    self.perform_insert(100)
    time.sleep(2)
    replica_tablet.mquery('vt_test_keyspace', 'start slave')
    self._wait_for_replica()

    # wait until the slave processed all data
    timeout = 30
    while True:
      inv_count2 = self.replica_stats()['Totals']['Invalidations']
      if inv_count2 == 200:
        break
      timeout = utils.wait_step('invalidation count, got %d expecting %d' %
                                (inv_count2, 200), timeout, sleep_time=0.1)

    # check and display some stats
    invalidator_stats = self.replica_vars()
    logging.debug('invalidator_stats %s',
                  invalidator_stats['RowcacheInvalidatorPosition'])
    self.assertEqual(invalidator_stats['RowcacheInvalidatorState'], 'Running',
                     'Row-cache invalidator should be enabled')

  def test_cache_hit(self):
    res = replica_tablet.mquery('vt_test_keyspace',
                                'select min(id) from vt_insert_test')
    self.assertNotEqual(res[0][0], None,
                        'Cannot proceed, no rows in vt_insert_test')
    mid = int(res[0][0])
    stats_dict = self.replica_stats()['vt_insert_test']
    misses = stats_dict['Misses']
    hits = stats_dict['Hits']
    replica_tablet.execute('select * from vt_insert_test where id=:id',
                           bindvars={'id': mid})
    stats_dict = self.replica_stats()['vt_insert_test']
    self.assertEqual(stats_dict['Misses'] - misses, 1,
                     "This shouldn't have hit the cache")

    replica_tablet.execute('select * from vt_insert_test where id=:id',
                           bindvars={'id': mid})
    hits2 = self.replica_stats()['vt_insert_test']['Hits']
    self.assertEqual(hits2 - hits, 1, 'This should have hit the cache')

  def _exec_vt_txn(self, query):
    master_tablet.execute(query, auto_log=False)

  def _exec_replica_query(self, query):
    result = replica_tablet.execute(query, auto_log=False)
    return result['rows']


if __name__ == '__main__':
  utils.main()
