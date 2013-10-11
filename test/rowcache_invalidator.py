#!/usr/bin/python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import json
import logging
import os
import sys
import time
import unittest
import urllib2

import MySQLdb

from vtdb import update_stream_service
from vtdb import vtclient

import framework
import tablet
import utils
from zk import zkocc

master_tablet = tablet.Tablet(62344)
replica_tablet = tablet.Tablet(62345)

master_host = "localhost:%u" % master_tablet.port
replica_host = "localhost:%u" % replica_tablet.port

zkocc_client = zkocc.ZkOccConnection("localhost:%u" % utils.zkocc_port_base,
                                     "test_nj", 30.0)

create_vt_insert_test = '''create table vt_insert_test (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''


def _get_master_current_position():
  res = utils.mysql_query(62344, 'vt_test_keyspace', 'show master status')
  start_position = update_stream_service.Coord(res[0][0], res[0][1])
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
  start_position = update_stream_service.Coord(master_log, master_pos)
  return start_position.__dict__


def setUpModule():
  utils.zk_setup()

  # start mysql instance external to the test
  setup_procs = [master_tablet.init_mysql(),
                 replica_tablet.init_mysql()
                ]
  utils.wait_procs(setup_procs)
  setup_tablets()

def tearDownModule():
  if utils.options.skip_teardown:
    return
  logging.debug("Tearing down the servers and setup")
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
  logging.debug("Setting up tablets")
  utils.run_vtctl('CreateKeyspace test_keyspace')
  master_tablet.init_tablet('master', 'test_keyspace', '0')
  replica_tablet.init_tablet('replica', 'test_keyspace', '0')
  utils.run_vtctl('RebuildShardGraph test_keyspace/0')
  utils.validate_topology()
  master_tablet.create_db('vt_test_keyspace')
  replica_tablet.create_db('vt_test_keyspace')

  utils.run_vtctl('RebuildKeyspaceGraph test_keyspace')

  zkocc_server = utils.zkocc_start()

  master_tablet.start_vttablet(memcache=True)
  replica_tablet.start_vttablet(memcache=True)

  utils.run_vtctl('SetReadWrite ' + master_tablet.tablet_alias)
  utils.check_db_read_write(62344)

  for t in [master_tablet, replica_tablet]:
    t.reset_replication()
  utils.run_vtctl('ReparentShard -force test_keyspace/0 ' + master_tablet.tablet_alias, auto_log=True)

  utils.validate_topology()
  # reset counter so tests don't assert
  tablet.Tablet.tablets_running = 0
  setup_schema()
  replica_tablet.kill_vttablet()
  replica_tablet.start_vttablet(memcache=True)
  master_tablet.vquery("set vt_schema_reload_time=86400", path="test_keyspace/0")
  replica_tablet.vquery("set vt_schema_reload_time=86400", path="test_keyspace/0")

def setup_schema():
  master_tablet.mquery('vt_test_keyspace', create_vt_insert_test)

def perform_insert(count):
  for i in xrange(count):
    _exec_vt_txn(master_host, ["insert into vt_insert_test (msg) values ('test %s')" % i])

def perform_delete():
  _exec_vt_txn(master_host, ['delete from vt_insert_test',])


class RowCacheInvalidator(unittest.TestCase):
  def setUp(self):
    perform_insert(400)

  def tearDown(self):
    perform_delete()

  def test_cache_invalidation(self):
    logging.debug("===========test_cache_invalidation=========")
    master_position = utils.mysql_query(62344, 'vt_test_keyspace', 'show master status')
    replica_tablet.mquery('vt_test_keyspace', "select MASTER_POS_WAIT('%s', %d)" % (master_position[0][0], master_position[0][1]), 5)
    invalidations = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['Totals']['Invalidations']
    invalidatorStats = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/vars" % replica_host)))
    logging.debug("Invalidations %d InvalidatorStats %s" % (invalidations, invalidatorStats['RowcacheInvalidationCheckPoint']))
    self.assertTrue(invalidations > 0, "Invalidations are flowing through.")

    res = replica_tablet.mquery('vt_test_keyspace', "select min(id) from vt_insert_test")
    self.assertNotEqual(res[0][0], None, "Cannot proceed, no rows in vt_insert_test")
    id = int(res[0][0])
    stats_dict = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']
    logging.debug("vt_insert_test stats %s" % stats_dict)
    misses = stats_dict['Misses']
    hits = stats_dict["Hits"]
    replica_tablet.vquery("select * from vt_insert_test where id=%d" % (id), path='test_keyspace/0')
    stats_dict = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']
    self.assertEqual(stats_dict['Misses'] - misses, 1, "This shouldn't have hit the cache")

    replica_tablet.vquery("select * from vt_insert_test where id=%d" % (id), path='test_keyspace/0')
    stats_dict = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']
    self.assertEqual(stats_dict['Hits'] - hits, 1, "This should have hit the cache")


  def test_stop_replication(self):
    logging.debug("===========test_stop_replication=========")
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
    invalidatorStats = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/vars" % replica_host)))
    logging.debug("invalidatorStats %s" % invalidatorStats['RowcacheInvalidationCheckPoint'])
    inv_count2 = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['Totals']['Invalidations']
    logging.debug("invalidator count1 %d count2 %d" % (inv_count1, inv_count2))
    self.assertEqual(invalidatorStats["RowcacheInvalidationState"]["Current"], "Enabled", "Row-cache invalidator should be enabled")
    self.assertTrue(inv_count2 - inv_count1 > 0, "invalidator was able to restart after a small pause in replication")


  def test_cache_hit(self):
    logging.debug("===========test_cache_hit=========")
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
    logging.debug("===========test_service_disabled=========")
    perform_insert(500)
    inv_before = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['Totals']['Invalidations']
    invStats_before = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/vars" % replica_host)))
    utils.run_vtctl('ChangeSlaveType test_nj-0000062345 spare')
    time.sleep(5)
    inv_after = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['Totals']['Invalidations']
    invStats_after = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/vars" % replica_host)))
    logging.debug("Tablet Replica->Spare\n\tBefore: Invalidations: %d InvalidatorStats %s\n\tAfter: Invalidations: %d InvalidatorStats %s" % (inv_before, invStats_before['RowcacheInvalidationCheckPoint'], inv_after, invStats_after['RowcacheInvalidationCheckPoint']))
    self.assertEqual(inv_after, 0, "Row-cache invalidator should be disabled, no invalidations")
    self.assertEqual(invStats_after["RowcacheInvalidationState"]["Current"], "Disabled", "Row-cache invalidator should be disabled")


def _vtdb_conn(host):
  conn = vtclient.VtOCCConnection(zkocc_client, 'test_keyspace', '0', "master", 30)
  conn.connect()
  return conn

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
  vt_mysqlbinlog =  os.environ.get('VT_MYSQL_ROOT') + '/bin/vt_mysqlbinlog'
  if not os.path.isfile(vt_mysqlbinlog):
    sys.exit("%s is not present, please install it and then re-run the test" % vt_mysqlbinlog)

  print "Note: This is a slow test, has a couple of sleeps in it to simulate proper state changes"

  utils.main()

if __name__ == '__main__':
  main()
