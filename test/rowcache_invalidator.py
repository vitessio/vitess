#!/usr/bin/python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import json
import logging
import time
import unittest
import urllib2

from zk import zkocc
from vtdb import vtclient

import environment
import framework
import tablet
import utils

master_tablet = tablet.Tablet()
replica_tablet = tablet.Tablet()

vtgate_server = None
vtgate_port = None
vtgate_secure_port = None

create_vt_insert_test = '''create table vt_insert_test (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''


def setUpModule():
  global vtgate_server
  global vtgate_port
  global vtgate_secure_port

  try:
    environment.topo_server_setup()

    # start mysql instance external to the test
    setup_procs = [master_tablet.init_mysql(),
                   replica_tablet.init_mysql()]
    utils.wait_procs(setup_procs)

    # Start up a master mysql and vttablet
    logging.debug("Setting up tablets")
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])
    master_tablet.init_tablet('master', 'test_keyspace', '0')
    replica_tablet.init_tablet('replica', 'test_keyspace', '0')
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'])
    utils.validate_topology()

    master_tablet.populate('vt_test_keyspace', create_vt_insert_test)
    replica_tablet.populate('vt_test_keyspace', create_vt_insert_test)

    vtgate_server, vtgate_port, vtgate_secure_port = utils.vtgate_start()

    master_tablet.start_vttablet(memcache=True, wait_for_state=None)
    replica_tablet.start_vttablet(memcache=True, wait_for_state=None)
    master_tablet.wait_for_vttablet_state('SERVING')
    replica_tablet.wait_for_vttablet_state('SERVING')

    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/0',
                     master_tablet.tablet_alias], auto_log=True)
    utils.validate_topology()

    # restart the replica tablet so the stats are reset
    replica_tablet.kill_vttablet()
    replica_tablet.start_vttablet(memcache=True)
  except:
    tearDownModule()
    raise

def tearDownModule():
  if utils.options.skip_teardown:
    return
  logging.debug("Tearing down the servers and setup")
  tablet.kill_tablets([master_tablet, replica_tablet])
  teardown_procs = [master_tablet.teardown_mysql(),
                    replica_tablet.teardown_mysql()]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server_teardown()
  utils.vtgate_kill(vtgate_server)
  utils.kill_sub_processes()
  utils.remove_tmp_files()
  master_tablet.remove_tree()
  replica_tablet.remove_tree()

class RowCacheInvalidator(unittest.TestCase):
  def setUp(self):
    self.vtgate_client = zkocc.ZkOccConnection("localhost:%u" % vtgate_port,
                                               "test_nj", 30.0)
    self.perform_insert(400)

  def tearDown(self):
    self.perform_delete()

  def replica_stats(self):
    url = "http://localhost:%u/debug/table_stats" % replica_tablet.port
    return framework.MultiDict(json.load(urllib2.urlopen(url)))

  def replica_vars(self):
    url = "http://localhost:%u/debug/vars" % replica_tablet.port
    return framework.MultiDict(json.load(urllib2.urlopen(url)))

  def perform_insert(self, count):
    for i in xrange(count):
      self._exec_vt_txn(["insert into vt_insert_test (msg) values ('test %s')" % i])

  def perform_delete(self):
    self._exec_vt_txn(['delete from vt_insert_test',])

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
    invalidatorStats = self.replica_vars()
    logging.debug("Invalidations %d InvalidatorStats %s" %
                  (invalidations,
                   invalidatorStats['RowcacheInvalidatorPosition']))
    self.assertTrue(invalidations > 0, "Invalidations are not flowing through.")

    res = replica_tablet.mquery('vt_test_keyspace',
                                "select min(id) from vt_insert_test")
    self.assertNotEqual(res[0][0], None,
                        "Cannot proceed, no rows in vt_insert_test")
    id = int(res[0][0])
    stats_dict = self.replica_stats()['vt_insert_test']
    logging.debug("vt_insert_test stats %s" % stats_dict)
    misses = stats_dict['Misses']
    hits = stats_dict["Hits"]
    replica_tablet.vquery("select * from vt_insert_test where id=%d" % (id),
                          path='test_keyspace/0')
    stats_dict = self.replica_stats()['vt_insert_test']
    self.assertEqual(stats_dict['Misses'] - misses, 1,
                     "This shouldn't have hit the cache")

    replica_tablet.vquery("select * from vt_insert_test where id=%d" % (id),
                          path='test_keyspace/0')
    stats_dict = self.replica_stats()['vt_insert_test']
    self.assertEqual(stats_dict['Hits'] - hits, 1,
                     "This should have hit the cache")

  def test_invalidation_failure(self):
    start = self.replica_vars()['InternalErrors'].get('Invalidation', 0)
    self.perform_insert(10)
    utils.mysql_write_query(master_tablet.tablet_uid,
                            'vt_test_keyspace',
                            "update vt_insert_test set msg = 'foo' where id = 1")
    self._wait_for_replica()
    time.sleep(1.0)
    end1 = self.replica_vars()['InternalErrors'].get('Invalidation', 0)
    self.assertEqual(start+1, end1)
    utils.mysql_query(master_tablet.tablet_uid,
                      'vt_test_keyspace',
                       "truncate table vt_insert_test")
    self._wait_for_replica()
    time.sleep(1.0)
    end2 = self.replica_vars()['InternalErrors'].get('Invalidation', 0)
    self.assertEqual(end1+1, end2)

  def test_stop_replication(self):
    # restart the replica tablet so the stats are reset
    replica_tablet.kill_vttablet()
    replica_tablet.start_vttablet(memcache=True)

    # insert 100 values, should cause 100 invalidations
    self.perform_insert(100)
    master_position = utils.mysql_query(master_tablet.tablet_uid,
                                        'vt_test_keyspace',
                                        'show master status')
    replica_tablet.mquery('vt_test_keyspace',
                          "select MASTER_POS_WAIT('%s', %d)" %
                          (master_position[0][0], master_position[0][1]), 5)

    # wait until the slave processed all data
    for timeout in xrange(300):
      time.sleep(0.1)
      inv_count1 = self.replica_stats()['Totals']['Invalidations']
      logging.debug("Got %u invalidations" % inv_count1)
      if inv_count1 == 100:
        break
    inv_count1 = self.replica_stats()['Totals']['Invalidations']
    self.assertEqual(inv_count1, 100,
                     "Unexpected number of invalidations: %u" % inv_count1)

    # stop replication insert more data, restart replication
    replica_tablet.mquery('vt_test_keyspace', "stop slave")
    self.perform_insert(100)
    time.sleep(2)
    replica_tablet.mquery('vt_test_keyspace', "start slave")
    master_position = utils.mysql_query(master_tablet.tablet_uid,
                                        'vt_test_keyspace',
                                        'show master status')
    replica_tablet.mquery('vt_test_keyspace',
                          "select MASTER_POS_WAIT('%s', %d)" %
                          (master_position[0][0], master_position[0][1]), 5)

    # wait until the slave processed all data
    for timeout in xrange(300):
      time.sleep(0.1)
      inv_count2 = self.replica_stats()['Totals']['Invalidations']
      logging.debug("Got %u invalidations" % inv_count2)
      if inv_count2 == 200:
        break
    inv_count2 = self.replica_stats()['Totals']['Invalidations']
    self.assertEqual(inv_count2, 200, "Unexpected number of invalidations: %u" %
                     inv_count2)

    # check and display some stats
    invalidatorStats = self.replica_vars()
    logging.debug("invalidatorStats %s" %
                  invalidatorStats['RowcacheInvalidatorPosition'])
    self.assertEqual(invalidatorStats["RowcacheInvalidatorState"], "Running",
                     "Row-cache invalidator should be enabled")

  def test_cache_hit(self):
    res = replica_tablet.mquery('vt_test_keyspace',
                                "select min(id) from vt_insert_test")
    self.assertNotEqual(res[0][0], None,
                        "Cannot proceed, no rows in vt_insert_test")
    id = int(res[0][0])
    stats_dict = self.replica_stats()['vt_insert_test']
    misses = stats_dict['Misses']
    hits = stats_dict["Hits"]
    replica_tablet.vquery("select * from vt_insert_test where id=%d" % (id),
                          path='test_keyspace/0')
    stats_dict = self.replica_stats()['vt_insert_test']
    self.assertEqual(stats_dict['Misses'] - misses, 1,
                     "This shouldn't have hit the cache")

    replica_tablet.vquery("select * from vt_insert_test where id=%d" % (id),
                          path='test_keyspace/0')
    hits2 = self.replica_stats()['vt_insert_test']['Hits']
    self.assertEqual(hits2 - hits, 1, "This should have hit the cache")

  def test_service_disabled(self):
    # perform some inserts, then change state to stop the invalidator
    self.perform_insert(500)
    inv_before = self.replica_stats()['Totals']['Invalidations']
    invStats_before = self.replica_vars()
    utils.run_vtctl(['ChangeSlaveType', replica_tablet.tablet_alias, 'spare'])

    # wait until it's stopped
    for timeout in xrange(300):
      time.sleep(0.1)
      invStats_after = self.replica_vars()
      logging.debug("Got state %s" %
                    invStats_after["RowcacheInvalidatorState"])
      if invStats_after["RowcacheInvalidatorState"] == "Stopped":
        break

    # check all data is right
    inv_after = self.replica_stats()['Totals']['Invalidations']
    invStats_after = self.replica_vars()
    logging.debug("Tablet Replica->Spare\n\tBefore: Invalidations: %d InvalidatorStats %s\n\tAfter: Invalidations: %d InvalidatorStats %s" % (inv_before, invStats_before['RowcacheInvalidatorPosition'], inv_after, invStats_after['RowcacheInvalidatorPosition']))
    self.assertEqual(inv_after, 0,
                     "Row-cache invalid. should be disabled, no invalidations")
    self.assertEqual(invStats_after["RowcacheInvalidatorState"], "Stopped",
                     "Row-cache invalidator should be disabled")

    # and restore the type
    utils.run_vtctl(['ChangeSlaveType', replica_tablet.tablet_alias, 'replica'])

  def _vtdb_conn(self):
    conn = vtclient.VtOCCConnection(self.vtgate_client, 'test_keyspace', '0',
                                    'master', 30)
    conn.connect()
    return conn

  def _exec_vt_txn(self, query_list=None):
    if query_list is None:
      return
    vtdb_conn = self._vtdb_conn()
    vtdb_cursor = vtdb_conn.cursor()
    vtdb_conn.begin()
    for q in query_list:
      vtdb_cursor.execute(q, {})
    vtdb_conn.commit()

if __name__ == '__main__':
  utils.main()
