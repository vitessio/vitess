#!/usr/bin/python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import json
from optparse import OptionParser
import os
import shlex
import shutil
import signal
import socket
from subprocess import check_call, Popen, CalledProcessError, PIPE
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


class Position(object):
  RelayFilename = ""
  RelayPosition = 0
  MasterFilename = ""
  MasterPosition = 0

  def __init__(self, **kargs):
    for k, v in kargs.iteritems():
      self.__dict__[k] = v

  def encode_json(self):
    return json.dumps(self.__dict__)

  def decode_json(self, position):
    self.__dict__ = json.loads(position)
    return self


def _get_master_current_position():
  res = utils.mysql_query(62344, 'vt_test_keyspace', 'show master status')
  start_position = Position(MasterFilename=res[0][0], MasterPosition=res[0][1]).encode_json()
  return start_position


def _get_repl_current_position():
  conn = MySQLdb.Connect(user='vt_dba',
                         unix_socket='%s/vt_%010d/mysql.sock' % (utils.vtdataroot, 62345),
                         db='vt_test_keyspace')
  cursor = MySQLdb.cursors.DictCursor(conn)
  cursor.execute('show slave status')
  res = cursor.fetchall()
  slave_dict = res[0]
  master_log = slave_dict['Relay_Master_Log_File']
  master_pos = slave_dict['Exec_Master_Log_Pos']
  relay_log = slave_dict['Relay_Log_File']
  relay_pos = slave_dict['Relay_Log_Pos']
  start_position = Position(MasterFilename=master_log, MasterPosition=master_pos, RelayFilename=relay_log, RelayPosition=relay_pos).encode_json()
  return start_position


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
  utils.run_vtctl('CreateKeyspace -force /zk/global/vt/keyspaces/test_keyspace')
  master_tablet.init_tablet('master', 'test_keyspace', '0')
  utils.run_vtctl('RebuildShardGraph /zk/global/vt/keyspaces/test_keyspace/shards/0')
  utils.run_vtctl('RebuildKeyspaceGraph /zk/global/vt/keyspaces/test_keyspace')
  utils.run_vtctl('Validate /zk/global/vt/keyspaces')

  setup_schema()
  replica_tablet.create_db('vt_test_keyspace')
  master_tablet.start_vttablet(memcache=True)

  replica_tablet.init_tablet('idle', 'test_keyspace', start=True)
  replica_tablet.start_memcache()
  utils.run("mkdir -p %s/snapshot" % utils.vtdataroot)
  utils.run("chmod +w %s/snapshot" % utils.vtdataroot)
  utils.run_vtctl('Clone -force %s %s' %
                  (master_tablet.zk_tablet_path, replica_tablet.zk_tablet_path))

  utils.run_vtctl('Ping /zk/test_nj/vt/tablets/0000062344')
  utils.run_vtctl('SetReadWrite ' + master_tablet.zk_tablet_path)
  utils.check_db_read_write(62344)

  utils.run_vtctl('Validate /zk/global/vt/keyspaces')
  utils.run_vtctl('Ping /zk/test_nj/vt/tablets/0000062345')
  utils.run_vtctl('ChangeSlaveType /zk/test_nj/vt/tablets/0000062345 replica')

def setup_schema():
  master_tablet.create_db('vt_test_keyspace')
  master_tablet.mquery('vt_test_keyspace', create_vt_insert_test)

def perform_insert(count):
  for i in xrange(count):
    _exec_vt_txn(master_host, 'vt_test_keyspace', ["insert into vt_insert_test (msg) values ('test %s')" % i])

def perform_delete():
  _exec_vt_txn(master_host, 'vt_test_keyspace', ['delete from vt_insert_test',])


class RowCacheInvalidator(unittest.TestCase):
  def setUp(self):
    perform_insert(100)

  def tearDown(self):
    perform_delete()

  def test_cache_invalidation(self):
    utils.debug("===========test_cache_invalidation=========")
    perform_insert(500)
    master_position = utils.mysql_query(62344, 'vt_test_keyspace', 'show master status')
    #The sleep is needed here, so the invalidator can catch up and the number can be tested.
    replica_tablet.mquery('vt_test_keyspace', "select MASTER_POS_WAIT('%s', %d)" % (master_position[0][0], master_position[0][1]), 5)
    time.sleep(5)
    invalidations = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['Totals']['Invalidations']
    utils.debug("test_cache_invalidation invalidations %d" % invalidations)
    self.assertTrue(invalidations > 0, "invalidator code is working")

  def test_purge_cache(self):
    utils.debug("===========test_purge_cache=========")
    res = replica_tablet.mquery('vt_test_keyspace', "select min(id) from vt_insert_test")
    self.assertNotEqual(res[0][0], None, "Cannot proceed, no rows in vt_insert_test")
    id = int(res[0][0])
    stats_dict = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']
    misses = stats_dict['Misses']
    hits = stats_dict["Hits"]
    replica_tablet.vquery("select * from vt_insert_test where id=%d" % (id), dbname='vt_test_keyspace')
    stats_dict = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']
    self.assertEqual(stats_dict['Misses'] - misses, 1, "This shouldn't have hit the cache")

    replica_tablet.vquery("select * from vt_insert_test where id=%d" % (id), dbname='vt_test_keyspace')
    stats_dict = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']
    self.assertEqual(stats_dict['Hits'] - hits, 1, "This should have hit the cache")

    purge_cache_counter = framework.MultiDict(utils.get_vars(replica_tablet.port))['CacheCounters']['PurgeCache']
    utils.run_vtctl('ChangeSlaveType /zk/test_nj/vt/tablets/0000062345 spare')
    #Flush logs will make sure that the InvalidationPosition saved in the cache will become invalid.
    #which should cause purge cache.
    replica_tablet.mquery('vt_test_keyspace', "flush logs")
    replica_tablet.mquery('vt_test_keyspace', "flush logs")
    replica_tablet.mquery('vt_test_keyspace', "flush logs")

    utils.run_vtctl('ChangeSlaveType /zk/test_nj/vt/tablets/0000062345 replica')
    #The sleep is needed here, so the invalidator can catch and the number can be tested.
    time.sleep(5)

    cache_counters = framework.MultiDict(utils.get_vars(replica_tablet.port))['CacheCounters']
    self.assertEqual(cache_counters['PurgeCache'] - purge_cache_counter, 1, "Check that the cache has been purged")

    misses = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']['Misses']
    replica_tablet.vquery("select * from vt_insert_test where id=%d" % (id), dbname='vt_test_keyspace')
    stats_dict = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']
    self.assertEqual(stats_dict['Misses'] - misses, 1, "This shouldn't have hit the cache")

  def test_cache_hit(self):
    utils.debug("===========test_cache_hit=========")
    res = replica_tablet.mquery('vt_test_keyspace', "select min(id) from vt_insert_test")
    self.assertNotEqual(res[0][0], None, "Cannot proceed, no rows in vt_insert_test")
    id = int(res[0][0])
    stats_dict = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']
    misses = stats_dict['Misses']
    hits = stats_dict["Hits"]
    replica_tablet.vquery("select * from vt_insert_test where id=%d" % (id), dbname='vt_test_keyspace')
    stats_dict = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']
    self.assertEqual(stats_dict['Misses'] - misses, 1, "This shouldn't have hit the cache")

    replica_tablet.vquery("select * from vt_insert_test where id=%d" % (id), dbname='vt_test_keyspace')
    hits2 = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['vt_insert_test']['Hits']
    self.assertEqual(hits2 - hits, 1, "This should have hit the cache")


  def test_service_disabled(self):
    utils.debug("===========test_service_disabled=========")
    utils.run_vtctl('ChangeSlaveType /zk/test_nj/vt/tablets/0000062345 spare')
    invalidations = framework.MultiDict(json.load(urllib2.urlopen("http://%s/debug/table_stats" % replica_host)))['Totals']['Invalidations']
    utils.debug("test_service_disabled invalidations %d" % invalidations)
    self.assertEqual(invalidations, 0, "Row-cache invalidator should be disabled, no invalidations")


def _vtdb_conn(host, dbname):
  return vt_occ2.connect(host, 2, dbname=dbname)

def _exec_vt_txn(host, dbname, query_list=None):
  if query_list is None:
    return
  vtdb_conn = _vtdb_conn(host, dbname)
  vtdb_cursor = vtdb_conn.cursor()
  vtdb_cursor.execute('begin', {})
  for q in query_list:
    vtdb_cursor.execute(q, {})
  vtdb_cursor.execute('commit', {})

def main():
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
