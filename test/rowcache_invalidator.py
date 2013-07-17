#!/usr/bin/python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import json
from optparse import OptionParser
import os
import sys
import time
import traceback
import threading
import urllib2

import MySQLdb

import framework
import tablet
import utils
import unittest

from vtdb import update_stream_service
from vtdb import vt_occ2


master_tablet = tablet.Tablet(62344)
replica_tablet = tablet.Tablet(62345)

master_host = "localhost:%u" % master_tablet.port
replica_host = "localhost:%u" % replica_tablet.port


create_vt_insert_test = '''create table vt_insert_test (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''


def _get_master_current_position():
  res = utils.mysql_query(62344, 'vt_test_keyspace', 'show master status')
  start_position = update_stream_service.BinlogPosition(res[0][0], res[0][1])
  return start_position.__dict__


def _get_repl_current_position():
  conn = MySQLdb.Connect(user='vt_dba',
                         unix_socket='%s/vt_%010d/mysql.sock' % (utils.vtdataroot, 62345),
                         db='vt_test_keyspace')
  cursor = MySQLdb.cursors.DictCursor(conn)
  cursor.execute('show master status')
  res = cursor.fetchall()
  slave_dict = res[0]
  master_log = slave_dict['File']
  master_pos = slave_dict['Position']
  start_position = update_stream_service.BinlogPosition(master_log, master_pos)
  return start_position.__dict__


def setup():
  utils.zk_setup()

  # start mysql instance external to the test
  setup_procs = [master_tablet.init_mysql(),
                 replica_tablet.init_mysql()
                ]
  utils.wait_procs(setup_procs)
  setup_tablets()

def teardown():
  if utils.options.skip_teardown:
    return
  utils.debug("Tearing down the servers and setup")
  teardown_procs = [master_tablet.teardown_mysql(),
                    replica_tablet.teardown_mysql()]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  utils.zk_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()
  master_tablet.kill_vttablet()
  replica_tablet.kill_vttablet()
  master_tablet.remove_tree()
  replica_tablet.remove_tree()

def setup_tablets():
  # Start up a master mysql and vttablet
  utils.debug("Setting up tablets")
  utils.run_vtctl('CreateKeyspace test_keyspace')
  master_tablet.init_tablet('master', 'test_keyspace', '0')
  utils.run_vtctl('RebuildShardGraph test_keyspace/0')
  utils.run_vtctl('RebuildKeyspaceGraph test_keyspace')
  utils.validate_topology()

  setup_schema()
  replica_tablet.create_db('vt_test_keyspace')
  master_tablet.start_vttablet(memcache=True)

  replica_tablet.init_tablet('idle', 'test_keyspace', start=True, memcache=True)
  snapshot_dir = os.path.join(utils.vtdataroot, 'snapshot')
  utils.run("mkdir -p " + snapshot_dir)
  utils.run("chmod +w " + snapshot_dir)
  utils.run_vtctl('Clone -force %s %s' %
                  (master_tablet.tablet_alias, replica_tablet.tablet_alias))

  utils.run_vtctl('Ping test_nj-0000062344')
  utils.run_vtctl('SetReadWrite ' + master_tablet.tablet_alias)
  utils.check_db_read_write(62344)

  utils.validate_topology()
  utils.run_vtctl('Ping test_nj-0000062345')
  utils.run_vtctl('ChangeSlaveType test_nj-0000062345 replica')

def setup_schema():
  master_tablet.create_db('vt_test_keyspace')
  master_tablet.mquery('vt_test_keyspace', create_vt_insert_test)

def perform_insert(count):
  for i in xrange(count):
    _exec_vt_txn(master_host, ["insert into vt_insert_test (msg) values ('test %s')" % i])

def perform_delete():
  _exec_vt_txn(master_host, ['delete from vt_insert_test',])


class RowCacheInvalidator(unittest.TestCase):
  def setUp(self):
    perform_insert(100)

  def tearDown(self):
    perform_delete()

  def test_cache_invalidation(self):
    utils.debug("===========test_cache_invalidation=========")
    master_position = utils.mysql_query(62344, 'vt_test_keyspace', 'show master status')
    #The sleep is needed here, so the invalidator can catch up and the number can be tested.
    replica_tablet.mquery('vt_test_keyspace', "select MASTER_POS_WAIT('%s', %d)" % (master_position[0][0], master_position[0][1]), 5)
    time.sleep(5)
    invalidations = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['Totals']['Invalidations']
    invalidatorStats = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/vars" % replica_host)))['CacheInvalidationProcessor']
    utils.debug("Invalidations %d InvalidatorStats %s" % (invalidations, invalidatorStats))
    self.assertTrue(invalidations > 0, "Invalidations are flowing through.")

    res = replica_tablet.mquery('vt_test_keyspace', "select min(id) from vt_insert_test")
    self.assertNotEqual(res[0][0], None, "Cannot proceed, no rows in vt_insert_test")
    id = int(res[0][0])
    stats_dict = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']
    utils.debug("vt_insert_test stats %s" % stats_dict)
    misses = stats_dict['Misses']
    hits = stats_dict["Hits"]
    replica_tablet.vquery("select * from vt_insert_test where id=%d" % (id), path='test_keyspace/0')
    stats_dict = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']
    self.assertEqual(stats_dict['Misses'] - misses, 1, "This shouldn't have hit the cache")

    replica_tablet.vquery("select * from vt_insert_test where id=%d" % (id), path='test_keyspace/0')
    stats_dict = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']
    self.assertEqual(stats_dict['Hits'] - hits, 1, "This should have hit the cache")


  def test_purge_cache(self):
    utils.debug("===========test_purge_cache=========")
    cache_counters = framework.MultiDict(utils.get_vars(replica_tablet.port))['CacheCounters']
    utils.debug("cache counters %s" % cache_counters)
    try:
      purge_cache_counter = cache_counters['PurgeCache']
    except KeyError, e:
      purge_cache_counter = 0

    perform_insert(100)

    utils.run_vtctl('ChangeSlaveType test_nj-0000062345 spare')
    # reset master to make checkpoint invalid.
    replica_tablet.mquery('vt_test_keyspace', "reset master")
    time.sleep(30)

    utils.run_vtctl('ChangeSlaveType test_nj-0000062345 replica')
    time.sleep(30)

    cache_counters = framework.MultiDict(utils.get_vars(replica_tablet.port))['CacheCounters']
    try:
      purge_cache_counter2 = cache_counters['PurgeCache']
    except KeyError, e:
      purge_cache_counter2 = 0
    utils.debug("cache counters %s" % cache_counters)
    self.assertEqual(purge_cache_counter2 - purge_cache_counter, 1, "Check that the cache has been purged")

  def test_tablet_restart(self):
    utils.debug("===========test_tablet_restart=========")
    utils.run_vtctl('ChangeSlaveType test_nj-0000062345 replica')
    time.sleep(5)
    perform_insert(100)
    time.sleep(5)
    invalidatorStats = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/vars" % replica_host)))['CacheInvalidationProcessor']
    checkpoint1 = invalidatorStats['Checkpoint']
    utils.debug("invalidatorStats %s checkpoint1 %s" % (invalidatorStats, checkpoint1))

    cache_counters = framework.MultiDict(utils.get_vars(replica_tablet.port))['CacheCounters']
    utils.debug("cache counters %s" % cache_counters)
    try:
      purge_cache_counter = cache_counters['PurgeCache']
    except KeyError, e:
      purge_cache_counter = 0

    utils.run_vtctl('ChangeSlaveType test_nj-0000062345 spare')
    time.sleep(5)

    utils.run_vtctl('ChangeSlaveType test_nj-0000062345 replica')
    #The sleep is needed here, so the invalidator can catch and the number can be tested.
    time.sleep(5)
    perform_insert(100)
    time.sleep(5)
    invalidatorStats = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/vars" % replica_host)))['CacheInvalidationProcessor']
    checkpoint3 = invalidatorStats['Checkpoint']
    utils.debug("invalidatorStats %s checkpoint3 %s" % (invalidatorStats, checkpoint3))

    cache_counters = framework.MultiDict(utils.get_vars(replica_tablet.port))['CacheCounters']
    try:
      purge_cache_counter2 = cache_counters['PurgeCache']
    except KeyError, e:
      purge_cache_counter2 = 0
    self.assertEqual(purge_cache_counter2 - purge_cache_counter, 0, "Cache should not have been purged")


  def test_stop_replication(self):
    utils.debug("===========test_stop_replication=========")
    utils.run_vtctl('ChangeSlaveType test_nj-0000062345 replica')
    time.sleep(10)
    perform_insert(100)
    master_position = utils.mysql_query(62344, 'vt_test_keyspace', 'show master status')
    #The sleep is needed here, so the invalidator can catch up and the number can be tested.
    replica_tablet.mquery('vt_test_keyspace', "select MASTER_POS_WAIT('%s', %d)" % (master_position[0][0], master_position[0][1]), 5)
    time.sleep(5)
    inv_count1 = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['Totals']['Invalidations']
    replica_tablet.mquery('vt_test_keyspace', "stop slave")
    perform_insert(100)
    # EOF is returned after 30s, sleeping a bit more to ensure we catch the EOF
    # and can test replication stop effectively.
    time.sleep(35)
    replica_tablet.mquery('vt_test_keyspace', "start slave")
    master_position = utils.mysql_query(62344, 'vt_test_keyspace', 'show master status')
    #The sleep is needed here, so the invalidator can catch up and the number can be tested.
    replica_tablet.mquery('vt_test_keyspace', "select MASTER_POS_WAIT('%s', %d)" % (master_position[0][0], master_position[0][1]), 5)
    time.sleep(10)
    invalidatorStats = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/vars" % replica_host)))['CacheInvalidationProcessor']
    utils.debug("invalidatorStats %s" % invalidatorStats)
    inv_count2 = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['Totals']['Invalidations']
    utils.debug("invalidator count1 %d count2 %d" % (inv_count1, inv_count2))
    self.assertEqual(invalidatorStats["States"]["Current"], "Enabled", "Row-cache invalidator should be enabled")
    self.assertTrue(inv_count2 - inv_count1 > 0, "invalidator was able to restart after a small pause in replication")


  def test_cache_hit(self):
    utils.debug("===========test_cache_hit=========")
    res = replica_tablet.mquery('vt_test_keyspace', "select min(id) from vt_insert_test")
    self.assertNotEqual(res[0][0], None, "Cannot proceed, no rows in vt_insert_test")
    id = int(res[0][0])
    stats_dict = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']
    misses = stats_dict['Misses']
    hits = stats_dict["Hits"]
    replica_tablet.vquery("select * from vt_insert_test where id=%d" % (id), path='test_keyspace/0')
    stats_dict = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']
    self.assertEqual(stats_dict['Misses'] - misses, 1, "This shouldn't have hit the cache")

    replica_tablet.vquery("select * from vt_insert_test where id=%d" % (id), path='test_keyspace/0')
    hits2 = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']['Hits']
    self.assertEqual(hits2 - hits, 1, "This should have hit the cache")

  def test_service_disabled(self):
    utils.debug("===========test_service_disabled=========")
    perform_insert(500)
    inv_before = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['Totals']['Invalidations']
    invStats_before = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/vars" % replica_host)))['CacheInvalidationProcessor']
    utils.run_vtctl('ChangeSlaveType test_nj-0000062345 spare')
    time.sleep(5)
    inv_after = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['Totals']['Invalidations']
    invStats_after = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/vars" % replica_host)))['CacheInvalidationProcessor']
    utils.debug("Tablet Replica->Spare\n\tBefore: Invalidations: %d InvalidatorStats %s\n\tAfter: Invalidations: %d InvalidatorStats %s" % (inv_before, invStats_before, inv_after, invStats_after))
    self.assertEqual(inv_after, 0, "Row-cache invalidator should be disabled, no invalidations")
    self.assertEqual(invStats_after["States"]["Current"], "Disabled", "Row-cache invalidator should be disabled")


def _vtdb_conn(host):
  return vt_occ2.connect(host, 'test_keyspace', '0', 2)

def _exec_vt_txn(host, query_list=None):
  if query_list is None:
    return
  vtdb_conn = _vtdb_conn(host)
  vtdb_cursor = vtdb_conn.cursor()
  vtdb_conn.begin()
  for q in query_list:
    vtdb_cursor.execute(q, {})
  vtdb_conn.commit()

def main():
  print "Note: This is a slow test, has a couple of sleeps in it to simulate proper state changes"
  args = utils.get_args()
  vt_mysqlbinlog =  os.environ.get('VT_MYSQL_ROOT') + '/bin/vt_mysqlbinlog'
  if not os.path.isfile(vt_mysqlbinlog):
    sys.exit("%s is not present, please install it and then re-run the test" % vt_mysqlbinlog)

  try:
    suite = unittest.TestSuite()
    if args[0] == 'run_all':
      setup()
      suite.addTests(unittest.TestLoader().loadTestsFromTestCase(RowCacheInvalidator))
    else:
      if args[0] != 'teardown':
        setup()
        if args[0] != 'setup':
          for arg in args:
            if hasattr(RowCacheInvalidator,arg):
              suite.addTest(RowCacheInvalidator(arg))
    if suite.countTestCases() > 0:
      unittest.TextTestRunner(verbosity=utils.options.verbose).run(suite)
  except KeyboardInterrupt:
    pass
  except utils.Break:
    utils.options.skip_teardown = True
  finally:
    teardown()


if __name__ == '__main__':
  main()
