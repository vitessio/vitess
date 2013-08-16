#!/usr/bin/env python
# coding: utf-8

import logging
import optparse
import os
import sys
import time
import traceback
import unittest

import tablet
import utils

from zk import zkocc
from net import gorpc
from net import bsonrpc
from vtdb import cursor
from vtdb import tablet3
from vtdb import vt_occ2
from vtdb import topology
from vtdb import dbexceptions

devnull = open('/dev/null', 'w')

shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()

shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()

TEST_KEYSPACE = "test_keyspace"

create_vt_insert_test = '''create table vt_insert_test (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

drop_vt_insert_test = '''drop table vt_insert_test'''

populate_vt_insert_test = [
    "insert into vt_insert_test (msg) values ('test %s')" % x
    for x in xrange(4)]

create_vt_a = '''create table vt_a (
eid bigint,
id int,
primary key(eid, id)
) Engine=InnoDB'''

drop_vt_a = '''drop table vt_a'''

def populate_vt_a(count):
  return ["insert into vt_a (eid, id) values (%d, %d)" % (x, x)
    for x in xrange(count+1) if x >0]

create_vt_b = '''create table vt_b (
eid bigint,
name varchar(128),
foo varbinary(128),
primary key(eid, name)
) Engine=InnoDB'''

def populate_vt_b(count):
  return ["insert into vt_b (eid, name, foo) values (%d, 'name %s', 'foo %s')" % (x, x, x)
    for x in xrange(count)]


def setUpModule():
  logging.debug("in setUpModule")
  try:
    utils.zk_setup()

    # start mysql instance external to the test
    setup_procs = [shard_0_master.init_mysql(),
                   shard_0_replica.init_mysql(),
                   shard_1_master.init_mysql(),
                   shard_1_replica.init_mysql()
                  ]
    utils.wait_procs(setup_procs)
    setup_tablets()
  except:
    tearDownModule()
    raise

def tearDownModule():
  logging.debug("in tearDownModule")
  if utils.options.skip_teardown:
    return
  logging.debug("Tearing down the servers and setup")
  teardown_procs = [shard_0_master.teardown_mysql(),
                    shard_0_replica.teardown_mysql(),
                    shard_1_master.teardown_mysql(),
                    shard_1_replica.teardown_mysql(),
                   ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  utils.zk_teardown()
  shard_0_master.kill_vttablet()
  shard_0_replica.kill_vttablet()
  shard_1_master.kill_vttablet()
  shard_1_replica.kill_vttablet()

  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica.remove_tree()
  shard_1_master.remove_tree()
  shard_1_replica.remove_tree()

def setup_tablets():
  # Start up a master mysql and vttablet
  logging.debug("Setting up tablets")
  utils.run_vtctl('CreateKeyspace %s' % TEST_KEYSPACE)
  shard_0_master.init_tablet('master', keyspace=TEST_KEYSPACE, shard='0')
  shard_0_replica.init_tablet('replica', keyspace=TEST_KEYSPACE, shard='0')
  shard_1_master.init_tablet('master', keyspace=TEST_KEYSPACE, shard='1')
  shard_1_replica.init_tablet('replica', keyspace=TEST_KEYSPACE, shard='1')

  utils.run_vtctl('RebuildShardGraph %s/0' % TEST_KEYSPACE, auto_log=True)
  utils.run_vtctl('RebuildShardGraph %s/1' % TEST_KEYSPACE, auto_log=True)
  utils.validate_topology()
  shard_0_master.create_db(shard_0_master.dbname)
  shard_0_replica.create_db(shard_0_master.dbname)
  shard_1_master.create_db(shard_0_master.dbname)
  shard_1_replica.create_db(shard_0_master.dbname)
  setup_schema()

  utils.run_vtctl('RebuildKeyspaceGraph %s' % TEST_KEYSPACE, auto_log=True)

  zkocc_server = utils.zkocc_start()

  shard_0_master.start_vttablet()
  shard_0_replica.start_vttablet()
  shard_1_master.start_vttablet()
  shard_1_replica.start_vttablet()

  utils.run_vtctl('SetReadWrite ' + shard_0_master.tablet_alias)
  utils.run_vtctl('SetReadWrite ' + shard_1_master.tablet_alias)
  utils.check_db_read_write(62344)


  for t in [shard_0_master, shard_0_replica]:
    t.reset_replication()
  utils.run_vtctl('ReparentShard -force test_keyspace/0 ' + shard_0_master.tablet_alias, auto_log=True)

  for t in [shard_1_master, shard_1_replica]:
    t.reset_replication()
  utils.run_vtctl('ReparentShard -force test_keyspace/1 ' + shard_1_master.tablet_alias, auto_log=True)


  # then get the topology and check it
  zkocc_client = zkocc.ZkOccConnection("localhost:%u" % utils.zkocc_port_base,
                                       "test_nj", 30.0)
  topology.read_keyspaces(zkocc_client)
  shard_0_master_addrs = topology.get_host_port_by_name(zkocc_client, "test_keyspace.0.master:_vtocc")
  logging.debug(shard_0_master_addrs)


def setup_schema():
  shard_0_master.mquery(shard_0_master.dbname, create_vt_insert_test)
  shard_0_master.mquery(shard_0_master.dbname, create_vt_a)
  shard_1_master.mquery(shard_0_master.dbname, create_vt_insert_test)
  shard_1_master.mquery(shard_0_master.dbname, create_vt_a)
  shard_0_replica.mquery(shard_0_master.dbname, create_vt_insert_test)
  shard_0_replica.mquery(shard_0_master.dbname, create_vt_a)
  shard_1_replica.mquery(shard_0_master.dbname, create_vt_insert_test)
  shard_1_replica.mquery(shard_0_master.dbname, create_vt_a)


class DBParams(object):
  addr = None
  keyspace = None
  shard = None
  timeout = None
  user = None
  password = None
  encrypted = False
  keyfile = None
  certfile = None
  
def get_vt_connection_params(db_key, port="_vtocc"):
  zkocc_client = zkocc.ZkOccConnection("localhost:%u" % utils.zkocc_port_base,
                                       "test_nj", 30.0)
  keyspace, shard, db_type = db_key.split('.')
  topo = topology.read_keyspaces(zkocc_client)
  addr = topology.get_host_port_by_name(zkocc_client, "%s:%s" % (db_key, port))
  vt_addr = "%s:%d" % (addr[0][0], addr[0][1])
  vt_params = DBParams()
  vt_params.addr = vt_addr
  vt_params.keyspace = keyspace
  vt_params.shard = shard
  vt_params.timeout = 10.0
  return vt_params.__dict__

def get_master_connection(shard=0):
  db_key = "%s.%d.master" % (TEST_KEYSPACE, shard)
  master_db_params = get_vt_connection_params(db_key)
  logging.debug("connecting to master with params %s" % master_db_params)
  master_conn = vt_occ2.connect(**master_db_params)
  return master_conn

def get_replica_connection(shard=0):
  db_key = "%s.%d.replica" % (TEST_KEYSPACE, shard)
  replica_db_params = get_vt_connection_params(db_key)
  logging.debug("connecting to replica with params %s" % replica_db_params)
  replica_conn = vt_occ2.connect(**replica_db_params)
  return replica_conn

def do_write(count):
  master_conn = get_master_connection()
  master_conn.begin()
  master_conn._execute("delete from vt_insert_test", {})
  for x in xrange(count):
    master_conn._execute("insert into vt_insert_test (msg) values (%(msg)s)", {'msg': 'test %s' % x})
  master_conn.commit()


class TestTabletFunctions(unittest.TestCase):
  def test_connect(self):
    try:
      master_conn = get_master_connection()
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    self.assertNotEqual(master_conn, None)
    self.assertIsInstance(master_conn, tablet3.TabletConnection, "Invalid master connection")
    try:
      replica_conn = get_replica_connection()
    except Exception, e:
      self.fail("Connection to shard0 replica failed with error %s" % str(e))
    self.assertNotEqual(replica_conn, None)
    self.assertIsInstance(replica_conn, tablet3.TabletConnection, "Invalid replica connection")

  def test_writes(self):
    try:
      master_conn = get_master_connection()
      count = 10
      master_conn.begin()
      master_conn._execute("delete from vt_insert_test", {})
      for x in xrange(count):
        master_conn._execute("insert into vt_insert_test (msg) values (%(msg)s)", {'msg': 'test %s' % x})
      master_conn.commit()
      results, rowcount, _, _ = master_conn._execute("select * from vt_insert_test", {})
      self.assertEqual(rowcount, count, "master fetch works")
    except Exception, e:
      self.fail("Write failed with error %s" % str(e))

  def test_batch_read(self):
    try:
      master_conn = get_master_connection()
      count = 10
      master_conn.begin()
      master_conn._execute("delete from vt_insert_test", {})
      for x in xrange(count):
        master_conn._execute("insert into vt_insert_test (msg) values (%(msg)s)", {'msg': 'test %s' % x})
      master_conn.commit()
      master_conn.begin()
      master_conn._execute("delete from vt_a", {})
      for x in xrange(count):
        master_conn._execute("insert into vt_a (eid, id) values (%(eid)s, %(id)s)", {'eid': x, 'id': x})
      master_conn.commit()
      rowsets = master_conn._execute_batch(["select * from vt_insert_test", "select * from vt_a"], [{}, {}])
      self.assertEqual(rowsets[0][1], count)
      self.assertEqual(rowsets[1][1], count)
    except Exception, e:
      self.fail("Write failed with error %s %s" % (str(e), traceback.print_exc()))

  def test_batch_write(self):
    try:
      master_conn = get_master_connection()
      count = 10
      query_list = []
      bind_vars_list = []
      query_list.append("delete from vt_insert_test")
      bind_vars_list.append({})
      for x in xrange(count):
        query_list.append("insert into vt_insert_test (msg) values (%(msg)s)")
        bind_vars_list.append({'msg': 'test %s' % x})
      query_list.append("delete from vt_a")
      bind_vars_list.append({})
      for x in xrange(count):
        query_list.append("insert into vt_a (eid, id) values (%(eid)s, %(id)s)")
        bind_vars_list.append({'eid': x, 'id': x})
      master_conn.begin()
      master_conn._execute_batch(query_list, bind_vars_list)
      master_conn.commit()
      results, rowcount, _, _ = master_conn._execute("select * from vt_insert_test", {})
      self.assertEqual(rowcount, count)
      results, rowcount, _, _ = master_conn._execute("select * from vt_a", {})
      self.assertEqual(rowcount, count)
    except Exception, e:
      self.fail("Write failed with error %s" % str(e))

  def test_streaming_fetchsubset(self):
    try:
      count = 100
      do_write(count)
      # Fetch a subset of the total size.
      master_conn = get_master_connection()
      stream_cursor = cursor.StreamCursor(master_conn) 
      stream_cursor.execute("select * from vt_insert_test", {})
      fetch_size = 10
      rows = stream_cursor.fetchmany(size=fetch_size)
      rowcount = 0
      for r in rows:
        rowcount +=1
      self.assertEqual(rowcount, fetch_size)
      stream_cursor.close()
    except Exception, e:
      self.fail("Failed with error %s %s" % (str(e), traceback.print_exc()))

  def test_streaming_fetchall(self):
    try:
      count = 100
      do_write(count)
      # Fetch all.
      master_conn = get_master_connection()
      stream_cursor = cursor.StreamCursor(master_conn) 
      stream_cursor.execute("select * from vt_insert_test", {})
      rows = stream_cursor.fetchall()
      rowcount = 0
      for r in rows:
        rowcount +=1
      self.assertEqual(rowcount, count)
      stream_cursor.close()
    except Exception, e:
      self.fail("Failed with error %s %s" % (str(e), traceback.print_exc()))

  def test_streaming_fetchone(self):
    try:
      count = 100
      do_write(count)
      # Fetch one.
      master_conn = get_master_connection()
      stream_cursor = cursor.StreamCursor(master_conn) 
      stream_cursor.execute("select * from vt_insert_test", {})
      rows = stream_cursor.fetchone()
      self.assertTrue(type(rows) == tuple, "Received a valid row")
      stream_cursor.close()
    except Exception, e:
      self.fail("Failed with error %s %s" % (str(e), traceback.print_exc()))

  def test_streaming_zero_results(self):
    try:
      master_conn = get_master_connection()
      master_conn.begin()
      master_conn._execute("delete from vt_insert_test", {})
      master_conn.commit()
      # After deletion, should result zero.
      stream_cursor = cursor.StreamCursor(master_conn) 
      stream_cursor.execute("select * from vt_insert_test", {})
      rows = stream_cursor.fetchall()
      rowcount = 0
      for r in rows:
        rowcount +=1
      self.assertEqual(rowcount, 0)
    except Exception, e:
      self.fail("Failed with error %s %s" % (str(e), traceback.print_exc()))


class TestFailures(unittest.TestCase):
  def test_tablet_restart_read(self):
    try:
      replica_conn = get_replica_connection()
    except Exception, e:
      self.fail("Connection to shard0 replica failed with error %s" % str(e))
    shard_0_replica.kill_vttablet()
    with self.assertRaises(dbexceptions.OperationalError):
      replica_conn._execute("select 1 from vt_insert_test", {})
    proc = shard_0_replica.start_vttablet()
    try:
      replica_conn = get_replica_connection()
      results = replica_conn._execute("select 1 from vt_insert_test", {})
    except Exception, e:
      self.fail("Communication with shard0 replica failed with error %s" % str(e))

  def test_tablet_restart_begin(self):
    try:
      master_conn = get_master_connection()
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    shard_0_master.kill_vttablet()
    with self.assertRaises(dbexceptions.OperationalError):
      master_conn.begin()
    proc = shard_0_master.start_vttablet()
    try:
      master_conn = get_master_connection()
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    try:
      master_conn.begin()
      master_conn._execute("delete from vt_insert_test", {})
      master_conn.commit()
    except Exception, e:
      self.fail("Failure in executing txn after restart, '%s'" % str(e))

  def test_tablet_restart_write(self):
    try:
      master_conn = get_master_connection()
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.OperationalError):
      master_conn.begin()
      shard_0_master.kill_vttablet()
      master_conn._execute("delete from vt_insert_test", {})
      master_conn.commit()
    proc = shard_0_master.start_vttablet()
    try:
      master_conn = get_master_connection()
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    try:
      master_conn.begin()
      master_conn._execute("delete from vt_insert_test", {})
      master_conn.commit()
    except Exception, e:
      self.fail("Failure in executing txn after restart, '%s'" % str(e))


  def test_query_timeout(self):
    try:
      replica_conn = get_replica_connection()
    except Exception, e:
      self.fail("Connection to shard0 replica failed with error %s" % str(e))
    with self.assertRaises(tablet3.TimeoutError):
      replica_conn._execute("select sleep(12) from dual", {})

    try:
      master_conn = get_master_connection()
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    with self.assertRaises(tablet3.TimeoutError):
      master_conn._execute("select sleep(12) from dual", {})

  def test_mysql_failure(self):
    try:
      replica_conn = get_replica_connection()
    except Exception, e:
      self.fail("Connection to shard0 replica failed with error %s" % str(e))
    utils.wait_procs([shard_0_replica.shutdown_mysql(),])
    with self.assertRaises(tablet3.FatalError):
      replica_conn._execute("select 1 from vt_insert_test", {})
    utils.wait_procs([shard_0_replica.start_mysql(),])
    shard_0_replica.kill_vttablet()
    shard_0_replica.start_vttablet()


if __name__ == '__main__':
  utils.main()
