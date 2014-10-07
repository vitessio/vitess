#!/usr/bin/env python
# coding: utf-8

import hmac
import json
import logging
import os
import struct
import threading
import time
import traceback
import unittest
import urllib

import environment
import tablet
import utils

from net import gorpc
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import keyspace
from vtdb import topology
from vtdb import dbexceptions
from vtdb import vtgatev2
from vtdb import vtgate_cursor
from zk import zkocc


conn_class = vtgatev2

shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()

shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()

vtgate_server = None
vtgate_port = None

KEYSPACE_NAME = 'test_keyspace'
shard_names = ['-80', '80-']
shard_kid_map = {'-80': [527875958493693904, 626750931627689502,
                         345387386794260318, 332484755310826578,
                         1842642426274125671, 1326307661227634652,
                         1761124146422844620, 1661669973250483744,
                         3361397649937244239, 2444880764308344533],
                 '80-': [9767889778372766922, 9742070682920810358,
                         10296850775085416642, 9537430901666854108,
                         10440455099304929791, 11454183276974683945,
                         11185910247776122031, 10460396697869122981,
                         13379616110062597001, 12826553979133932576],
                 }

create_vt_insert_test = '''create table vt_insert_test (
id bigint auto_increment,
msg varchar(64),
keyspace_id bigint(20) unsigned NOT NULL,
primary key (id)
) Engine=InnoDB'''

create_vt_a = '''create table vt_a (
eid bigint,
id int,
keyspace_id bigint(20) unsigned NOT NULL,
primary key(eid, id)
) Engine=InnoDB'''

create_tables = [create_vt_insert_test, create_vt_a]
pack_kid = struct.Struct('!Q').pack


def setUpModule():
  logging.debug("in setUpModule")
  try:
    environment.topo_server_setup()

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
  global vtgate_server
  logging.debug("in tearDownModule")
  if utils.options.skip_teardown:
    return
  logging.debug("Tearing down the servers and setup")
  utils.vtgate_kill(vtgate_server)
  tablet.kill_tablets([shard_0_master, shard_0_replica, shard_1_master,
                       shard_1_replica])
  teardown_procs = [shard_0_master.teardown_mysql(),
                    shard_0_replica.teardown_mysql(),
                    shard_1_master.teardown_mysql(),
                    shard_1_replica.teardown_mysql(),
                   ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server_teardown()

  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica.remove_tree()
  shard_1_master.remove_tree()
  shard_1_replica.remove_tree()

def setup_tablets():
  global vtgate_server
  global vtgate_port

  # Start up a master mysql and vttablet
  logging.debug("Setting up tablets")
  utils.run_vtctl(['CreateKeyspace', KEYSPACE_NAME])
  utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', KEYSPACE_NAME,
                   'keyspace_id', 'uint64'])
  shard_0_master.init_tablet('master', keyspace=KEYSPACE_NAME, shard='-80')
  shard_0_replica.init_tablet('replica', keyspace=KEYSPACE_NAME, shard='-80')
  shard_1_master.init_tablet('master', keyspace=KEYSPACE_NAME, shard='80-')
  shard_1_replica.init_tablet('replica', keyspace=KEYSPACE_NAME, shard='80-')

  utils.run_vtctl(['RebuildKeyspaceGraph', KEYSPACE_NAME], auto_log=True)

  for t in [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica]:
    t.create_db('vt_test_keyspace')
    for create_table in create_tables:
      t.mquery(shard_0_master.dbname, create_table)
    t.start_vttablet(wait_for_state=None)

  for t in [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica]:
    t.wait_for_vttablet_state('SERVING')

  utils.run_vtctl(['ReparentShard', '-force', KEYSPACE_NAME+'/-80',
                   shard_0_master.tablet_alias], auto_log=True)
  utils.run_vtctl(['ReparentShard', '-force', KEYSPACE_NAME+'/80-',
                   shard_1_master.tablet_alias], auto_log=True)

  utils.run_vtctl(['RebuildKeyspaceGraph', KEYSPACE_NAME],
                   auto_log=True)

  utils.check_srv_keyspace('test_nj', KEYSPACE_NAME,
                           'Partitions(master): -80 80-\n' +
                           'Partitions(replica): -80 80-\n' +
                           'TabletTypes: master,replica')

  vtgate_server, vtgate_port = utils.vtgate_start()
  vtgate_client = zkocc.ZkOccConnection("localhost:%u" % vtgate_port,
                                        "test_nj", 30.0)
  topology.read_topology(vtgate_client)


def get_connection(user=None, password=None):
  global vtgate_port
  timeout = 10.0
  conn = None
  vtgate_addrs = {"_vt": ["localhost:%s" % (vtgate_port),]}
  conn = conn_class.connect(vtgate_addrs, timeout,
                          user=user, password=password)
  return conn

def get_keyrange(shard_name):
  return topology.get_keyrange_from_shard_name(KEYSPACE_NAME, shard_name, "master")


def do_write(count, shard_index):
  kid_list = shard_kid_map[shard_names[shard_index]]
  vtgate_conn = get_connection()
  vtgate_conn.begin()
  vtgate_conn._execute(
      "delete from vt_insert_test", {},
      KEYSPACE_NAME, 'master',
      keyranges=[get_keyrange(shard_names[shard_index])])
  for x in xrange(count):
    keyspace_id = kid_list[x%len(kid_list)]
    vtgate_conn._execute(
        "insert into vt_insert_test (msg, keyspace_id) values (%(msg)s, %(keyspace_id)s)",
        {'msg': 'test %s' % x, 'keyspace_id': keyspace_id},
        KEYSPACE_NAME, 'master', keyspace_ids=[pack_kid(keyspace_id)])
  vtgate_conn.commit()


def restart_vtgate(extra_args={}):
  global vtgate_server, vtgate_port
  utils.vtgate_kill(vtgate_server)
  vtgate_server, vtgate_port = utils.vtgate_start(vtgate_port, extra_args=extra_args)


class TestVTGateFunctions(unittest.TestCase):
  def setUp(self):
    self.shard_index = 0
    self.master_tablet = shard_0_master
    self.replica_tablet = shard_0_replica

  def test_status(self):
    self.assertIn('</html>', utils.get_status(vtgate_port))

  def test_connect(self):
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    self.assertNotEqual(vtgate_conn, None)

  def test_writes(self):
    try:
      vtgate_conn = get_connection()
      count = 10
      vtgate_conn.begin()
      vtgate_conn._execute(
          "delete from vt_insert_test", {},
          KEYSPACE_NAME, 'master',
          keyranges=[keyrange.KeyRange(shard_names[self.shard_index])])
      kid_list = shard_kid_map[shard_names[self.shard_index]]
      for x in xrange(count):
        keyspace_id = kid_list[count%len(kid_list)]
        vtgate_conn._execute(
            "insert into vt_insert_test (msg, keyspace_id) values (%(msg)s, %(keyspace_id)s)",
            {'msg': 'test %s' % x, 'keyspace_id': keyspace_id},
            KEYSPACE_NAME, 'master', keyspace_ids=[str(keyspace_id)])
      vtgate_conn.commit()
      results, rowcount = vtgate_conn._execute(
          "select * from vt_insert_test", {},
          KEYSPACE_NAME, 'master',
          keyranges=[keyrange.KeyRange(shard_names[self.shard_index])])[:2]
      self.assertEqual(rowcount, count, "master fetch works")
    except Exception, e:
      logging.debug("Write failed with error %s" % str(e))
      raise

  def test_query_routing(self):
    """Test VtGate routes queries to the right tablets"""
    try:
      row_counts = [50, 75]
      for shard_index in [0,1]:
        do_write(row_counts[shard_index], shard_index)
      master_conn = get_connection()
      for shard_index in [0,1]:
        # Fetch all rows in each shard
        results, rowcount = master_conn._execute("select * from vt_insert_test", {},
              KEYSPACE_NAME, 'master', keyranges=[get_keyrange(shard_names[shard_index])])[:2]
        # Verify row count
        self.assertEqual(rowcount, row_counts[shard_index])
        # Verify keyspace id
        for result in results:
          kid = result[2]
          self.assertTrue(kid in shard_kid_map[shard_names[shard_index]])
      # Do a cross shard range query and assert all rows are fetched
      results, rowcount = master_conn._execute("select * from vt_insert_test", {},
            KEYSPACE_NAME, 'master', keyranges=[get_keyrange('75-95')])[:2]
      self.assertEqual(rowcount, row_counts[0] + row_counts[1])
    except Exception, e:
      logging.debug("failed with error %s, %s" % (str(e), traceback.print_exc()))
      raise


  def test_rollback(self):
    try:
      vtgate_conn = get_connection()
      count = 10
      vtgate_conn.begin()
      vtgate_conn._execute(
          "delete from vt_insert_test", {},
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
      kid_list = shard_kid_map[shard_names[self.shard_index]]
      for x in xrange(count):
        keyspace_id = kid_list[count%len(kid_list)]
        vtgate_conn._execute(
            "insert into vt_insert_test (msg, keyspace_id) values (%(msg)s, %(keyspace_id)s)",
            {'msg': 'test %s' % x, 'keyspace_id': keyspace_id},
            KEYSPACE_NAME, 'master', keyspace_ids=[pack_kid(keyspace_id)])
      vtgate_conn.commit()
      vtgate_conn.begin()
      vtgate_conn._execute(
          "delete from vt_insert_test", {},
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
      vtgate_conn.rollback()
      results, rowcount = vtgate_conn._execute(
          "select * from vt_insert_test", {},
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])[:2]
      logging.debug("ROLLBACK TEST rowcount %d count %d" % (rowcount, count))
      self.assertEqual(rowcount, count, "Fetched rows(%d) != inserted rows(%d), rollback didn't work" % (rowcount, count))
    except Exception, e:
      logging.debug("Write failed with error %s" % str(e))
      raise


  def test_execute_entity_ids(self):
    try:
      vtgate_conn = get_connection()
      count = 10
      vtgate_conn.begin()
      vtgate_conn._execute(
          "delete from vt_a", {},
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
      eid_map = {}
      kid_list = shard_kid_map[shard_names[self.shard_index]]
      for x in xrange(count):
        keyspace_id = kid_list[x%len(kid_list)]
        eid_map[x] = str(keyspace_id)
        vtgate_conn._execute(
            "insert into vt_a (eid, id, keyspace_id) \
             values (%(eid)s, %(id)s, %(keyspace_id)s)",
            {'eid': x, 'id': x, 'keyspace_id': keyspace_id},
            KEYSPACE_NAME, 'master', keyspace_ids=[pack_kid(keyspace_id)])
      vtgate_conn.commit()
      results, rowcount, _, _ = vtgate_conn._execute_entity_ids(
          "select * from vt_a", {},
          KEYSPACE_NAME, 'master', eid_map, 'id')
      self.assertEqual(rowcount, count, "entity_ids works")
    except Exception, e:
      self.fail("Execute entity ids failed with error %s %s" %
                (str(e),
                 traceback.print_exc()))

  def test_batch_read(self):
    try:
      vtgate_conn = get_connection()
      count = 10
      vtgate_conn.begin()
      vtgate_conn._execute(
          "delete from vt_insert_test", {},
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
      kid_list = shard_kid_map[shard_names[self.shard_index]]
      for x in xrange(count):
        keyspace_id = kid_list[x%len(kid_list)]
        vtgate_conn._execute(
            "insert into vt_insert_test (msg, keyspace_id) values (%(msg)s, %(keyspace_id)s)",
            {'msg': 'test %s' % x, 'keyspace_id': keyspace_id},
            KEYSPACE_NAME, 'master', keyspace_ids=[pack_kid(keyspace_id)])
      vtgate_conn.commit()
      vtgate_conn.begin()
      vtgate_conn._execute(
          "delete from vt_a", {},
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
      for x in xrange(count):
        keyspace_id = kid_list[x%len(kid_list)]
        vtgate_conn._execute(
            "insert into vt_a (eid, id, keyspace_id) \
             values (%(eid)s, %(id)s, %(keyspace_id)s)",
            {'eid': x, 'id': x, 'keyspace_id': keyspace_id},
            KEYSPACE_NAME, 'master', keyspace_ids=[pack_kid(keyspace_id)])
      vtgate_conn.commit()
      rowsets = vtgate_conn._execute_batch(
          ["select * from vt_insert_test",
           "select * from vt_a"], [{}, {}],
          KEYSPACE_NAME, 'master', keyspace_ids=[str(kid) for kid in kid_list])
      self.assertEqual(rowsets[0][1], count)
      self.assertEqual(rowsets[1][1], count)
    except Exception, e:
      self.fail("Write failed with error %s %s" % (str(e),
                                                   traceback.print_exc()))

  def test_batch_write(self):
    try:
      vtgate_conn = get_connection()
      count = 10
      query_list = []
      bind_vars_list = []
      query_list.append("delete from vt_insert_test")
      bind_vars_list.append({})
      kid_list = shard_kid_map[shard_names[self.shard_index]]
      for x in xrange(count):
        keyspace_id = kid_list[x%len(kid_list)]
        query_list.append("insert into vt_insert_test (msg, keyspace_id) values (%(msg)s, %(keyspace_id)s)")
        bind_vars_list.append({'msg': 'test %s' % x, 'keyspace_id': keyspace_id})
      query_list.append("delete from vt_a")
      bind_vars_list.append({})
      for x in xrange(count):
        keyspace_id = kid_list[x%len(kid_list)]
        query_list.append("insert into vt_a (eid, id, keyspace_id) values (%(eid)s, %(id)s, %(keyspace_id)s)")
        bind_vars_list.append({'eid': x, 'id': x, 'keyspace_id': keyspace_id})
      vtgate_conn.begin()
      vtgate_conn._execute_batch(
          query_list, bind_vars_list,
          KEYSPACE_NAME, 'master', keyspace_ids=[str(kid) for kid in kid_list])
      vtgate_conn.commit()
      results, rowcount, _, _ = vtgate_conn._execute(
          "select * from vt_insert_test", {},
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
      self.assertEqual(rowcount, count)
      results, rowcount, _, _ = vtgate_conn._execute(
          "select * from vt_a", {},
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
      self.assertEqual(rowcount, count)
    except Exception, e:
      self.fail("Write failed with error %s" % str(e))

  def test_streaming_fetchsubset(self):
    try:
      count = 100
      do_write(count, self.shard_index)
      # Fetch a subset of the total size.
      vtgate_conn = get_connection()
      stream_cursor = vtgate_cursor.StreamVTGateCursor(
          vtgate_conn,
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
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
      do_write(count, self.shard_index)
      # Fetch all.
      vtgate_conn = get_connection()
      stream_cursor = vtgate_cursor.StreamVTGateCursor(
          vtgate_conn,
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
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
      do_write(count, self.shard_index)
      # Fetch one.
      vtgate_conn = get_connection()
      stream_cursor = vtgate_cursor.StreamVTGateCursor(
          vtgate_conn,
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
      stream_cursor.execute("select * from vt_insert_test", {})
      rows = stream_cursor.fetchone()
      self.assertTrue(type(rows) == tuple, "Received a valid row")
      stream_cursor.close()
    except Exception, e:
      self.fail("Failed with error %s %s" % (str(e), traceback.print_exc()))

  def test_streaming_zero_results(self):
    try:
      vtgate_conn = get_connection()
      vtgate_conn.begin()
      vtgate_conn._execute("delete from vt_insert_test", {},
                           KEYSPACE_NAME, 'master',
                           keyranges=[get_keyrange(shard_names[self.shard_index])])
      vtgate_conn.commit()
      # After deletion, should result zero.
      stream_cursor = vtgate_cursor.StreamVTGateCursor(
          vtgate_conn,
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
      stream_cursor.execute("select * from vt_insert_test", {})
      rows = stream_cursor.fetchall()
      rowcount = 0
      for r in rows:
        rowcount +=1
      self.assertEqual(rowcount, 0)
    except Exception, e:
      self.fail("Failed with error %s %s" % (str(e), traceback.print_exc()))

  def test_interleaving(self):
    tablet_type = "master"
    try:
      vtgate_conn = get_connection()
      vtgate_conn.begin()
      vtgate_conn._execute(
          "delete from vt_insert_test", {},
          KEYSPACE_NAME, tablet_type,
          keyranges=[keyrange.KeyRange(shard_names[self.shard_index])])
      kid_list = shard_kid_map[shard_names[self.shard_index]]
      count = len(kid_list)
      for x in xrange(count):
        keyspace_id = kid_list[x]
        vtgate_conn._execute(
            "insert into vt_insert_test (msg, keyspace_id) values (%(msg)s, %(keyspace_id)s)",
            {'msg': 'test %s' % x, 'keyspace_id': keyspace_id},
            KEYSPACE_NAME, tablet_type, keyspace_ids=[str(keyspace_id)])
      vtgate_conn.commit()
      vtgate_conn2 = get_connection()
      query = "select keyspace_id from vt_insert_test where keyspace_id = %(kid)s"
      thd = threading.Thread(target=self._query_lots, args=(
          vtgate_conn2,
          query,
          {'kid': kid_list[0]},
          KEYSPACE_NAME,
          tablet_type,
          [str(kid_list[0])]))
      thd.start()
      for i in xrange(count):
        (result, _, _, _) = vtgate_conn._execute(query,
            {'kid':kid_list[i]},
            KEYSPACE_NAME, tablet_type,
            keyspace_ids=[str(kid_list[i])])
        self.assertEqual(result, [(kid_list[i],)])
        if i % 10 == 0:
          vtgate_conn._stream_execute(query, {'kid':kid_list[i]}, KEYSPACE_NAME,
                                      tablet_type,
                                      keyspace_ids=[str(kid_list[i])])
          while 1:
            result = vtgate_conn._stream_next()
            if not result:
              break
            self.assertEqual(result, (kid_list[i],))
      thd.join()
    except Exception, e:
      self.fail("Failed with error %s %s" % (str(e), traceback.print_exc()))

  def _query_lots(self,
                  conn,
                  query,
                  bind_vars,
                  keyspace_name,
                  tablet_type,
                  keyspace_ids):
    for i in xrange(500):
      (result, _, _, _) = conn._execute(query, bind_vars,
                                        keyspace_name,
                                        tablet_type,
                                        keyspace_ids=keyspace_ids)
      self.assertEqual(result, [tuple(bind_vars.values())])


class TestFailures(unittest.TestCase):
  def setUp(self):
    self.shard_index = 0
    self.master_tablet = shard_0_master
    self.replica_tablet = shard_0_replica

  def test_tablet_restart_read(self):
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % (str(e)))
    self.replica_tablet.kill_vttablet()
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn._execute(
          "select 1 from vt_insert_test", {},
          KEYSPACE_NAME, 'replica',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
    proc = self.replica_tablet.start_vttablet()
    try:
      results = vtgate_conn._execute(
          "select 1 from vt_insert_test", {},
          KEYSPACE_NAME, 'replica',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
    except Exception, e:
      self.fail("Communication with shard %s replica failed with error %s" %
                (shard_names[self.shard_index], str(e)))

  def test_vtgate_restart_read(self):
    global vtgate_server, vtgate_port
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % (str(e)))
    utils.vtgate_kill(vtgate_server)
    with self.assertRaises(dbexceptions.OperationalError):
      vtgate_conn._execute(
          "select 1 from vt_insert_test", {},
          KEYSPACE_NAME, 'replica',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
    vtgate_server, vtgate_port = utils.vtgate_start(vtgate_port)
    vtgate_conn = get_connection()
    try:
      results = vtgate_conn._execute(
          "select 1 from vt_insert_test", {},
          KEYSPACE_NAME, 'replica',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
    except Exception, e:
      self.fail("Communication with shard %s replica failed with error %s" %
                (shard_names[self.shard_index], str(e)))

  def test_tablet_restart_stream_execute(self):
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % (str(e)))
    stream_cursor = vtgate_cursor.StreamVTGateCursor(
        vtgate_conn, KEYSPACE_NAME, 'replica',
        keyranges=[get_keyrange(shard_names[self.shard_index])])
    self.replica_tablet.kill_vttablet()
    with self.assertRaises(dbexceptions.DatabaseError):
      stream_cursor.execute("select * from vt_insert_test", {})
    proc = self.replica_tablet.start_vttablet()
    self.replica_tablet.wait_for_vttablet_state('SERVING')
    try:
      stream_cursor.execute("select * from vt_insert_test", {})
    except Exception, e:
      self.fail("Communication with shard0 replica failed with error %s" %
                str(e))

  def test_vtgate_restart_stream_execute(self):
    global vtgate_server, vtgate_port
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % (str(e)))
    stream_cursor = vtgate_conn.cursor(
        vtgate_cursor.StreamVTGateCursor, vtgate_conn,
        KEYSPACE_NAME, 'replica',
        keyranges=[get_keyrange(shard_names[self.shard_index])])
    utils.vtgate_kill(vtgate_server)
    with self.assertRaises(dbexceptions.OperationalError):
      stream_cursor.execute("select * from vt_insert_test", {})
    vtgate_server, vtgate_port = utils.vtgate_start(vtgate_port)
    vtgate_conn = get_connection()
    stream_cursor = vtgate_conn.cursor(
        vtgate_cursor.StreamVTGateCursor, vtgate_conn,
        KEYSPACE_NAME, 'replica',
        keyranges=[get_keyrange(shard_names[self.shard_index])])
    try:
      stream_cursor.execute("select * from vt_insert_test", {})
    except Exception, e:
      self.fail("Communication with shard0 replica failed with error %s" %
                str(e))

  # vtgate begin doesn't make any back-end connections to
  # vttablet so the kill and restart shouldn't have any effect.
  def test_tablet_restart_begin(self):
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    self.master_tablet.kill_vttablet()
    vtgate_conn.begin()
    proc = self.master_tablet.start_vttablet()
    vtgate_conn.begin()
    # this succeeds only if retry_count > 0
    vtgate_conn._execute(
        "delete from vt_insert_test", {},
        KEYSPACE_NAME, 'master',
        keyranges=[get_keyrange(shard_names[self.shard_index])])
    vtgate_conn.commit()

  def test_vtgate_restart_begin(self):
    global vtgate_server, vtgate_port
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    utils.vtgate_kill(vtgate_server)
    with self.assertRaises(dbexceptions.OperationalError):
      vtgate_conn.begin()
    vtgate_server, vtgate_port = utils.vtgate_start(vtgate_port)
    vtgate_conn = get_connection()
    vtgate_conn.begin()

  def test_tablet_fail_write(self):
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn.begin()
      self.master_tablet.kill_vttablet()
      vtgate_conn._execute(
          "delete from vt_insert_test", {},
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
      vtgate_conn.commit()
    proc = self.master_tablet.start_vttablet()
    vtgate_conn.begin()
    vtgate_conn._execute(
        "delete from vt_insert_test", {},
        KEYSPACE_NAME, 'master',
        keyranges=[get_keyrange(shard_names[self.shard_index])])
    vtgate_conn.commit()

  def test_error_on_dml(self):
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    vtgate_conn.begin()
    keyspace_id = shard_kid_map[shard_names[self.shard_index+1]][0]
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn._execute(
          "insert into vt_insert_test values(:msg, :keyspace_id)",
          {"msg": "test4", "keyspace_id": keyspace_id}, KEYSPACE_NAME, 'master',
          keyranges=[keyrange.KeyRange(shard_names[self.shard_index])])
    if conn_class == vtgatev2:
      transaction_id = vtgate_conn.session["ShardSessions"][0]["TransactionId"]
    else:
      transaction_id = vtgate_conn.session.shard_sessions[0].transaction_id
    self.assertTrue(transaction_id != 0)
    vtgate_conn.commit()

  def test_vtgate_fail_write(self):
    global vtgate_server, vtgate_port
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.OperationalError):
      vtgate_conn.begin()
      utils.vtgate_kill(vtgate_server)
      vtgate_conn._execute(
          "delete from vt_insert_test", {},
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
      vtgate_conn.commit()
    vtgate_server, vtgate_port = utils.vtgate_start(vtgate_port)
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    vtgate_conn.begin()
    vtgate_conn._execute(
        "delete from vt_insert_test", {},
        KEYSPACE_NAME, 'master',
        keyranges=[get_keyrange(shard_names[self.shard_index])])
    vtgate_conn.commit()

  # test timeout between py client and vtgate
  # the default timeout is 10 seconds
  def test_vtgate_timeout(self):
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.TimeoutError):
      vtgate_conn._execute(
          "select sleep(12) from dual", {},
          KEYSPACE_NAME, 'replica',
          keyranges=[get_keyrange(shard_names[self.shard_index])])

    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.TimeoutError):
      vtgate_conn._execute(
          "select sleep(12) from dual", {},
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
     # Currently this is causing vttablet to become unreachable at
     # the timeout boundary and kill any query being executed
     # at the time. Prevent flakiness in other tests by sleeping
     # until the query times out.
     # TODO fix b/17733518
    time.sleep(3)

  # test timeout between vtgate and vttablet
  # the default timeout is 5 seconds
  def test_tablet_timeout(self):
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn.begin()
      vtgate_conn._execute(
          "select sleep(7) from dual", {},
          KEYSPACE_NAME, 'replica',
          keyranges=[get_keyrange(shard_names[self.shard_index])])

    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn.begin()
      vtgate_conn._execute(
          "select sleep(7) from dual", {},
          KEYSPACE_NAME, 'master',
          keyranges=[get_keyrange(shard_names[self.shard_index])])

  def test_restart_mysql_failure(self):
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    utils.wait_procs([self.replica_tablet.shutdown_mysql(),])
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn._execute(
          "select 1 from vt_insert_test", {},
          KEYSPACE_NAME, 'replica',
          keyranges=[get_keyrange(shard_names[self.shard_index])])
    utils.wait_procs([self.replica_tablet.start_mysql(),])
    self.replica_tablet.kill_vttablet()
    self.replica_tablet.start_vttablet()
    self.replica_tablet.wait_for_vttablet_state('SERVING')
    vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[get_keyrange(shard_names[self.shard_index])])

  # FIXME(shrutip): this test is basically just testing that
  # txn pool full error doesn't get thrown anymore with vtgate.
  # vtgate retries for this condition. Not a very high value
  # test at this point, could be removed if there is coverage at vtgate level.
  def test_retry_txn_pool_full(self):
    vtgate_conn = get_connection()
    vtgate_conn._execute(
        "set vt_transaction_cap=1", {},
        KEYSPACE_NAME, 'master',
        keyranges=[get_keyrange(shard_names[self.shard_index])])
    vtgate_conn.begin()
    vtgate_conn2 = get_connection()
    vtgate_conn2.begin()
    vtgate_conn.commit()
    vtgate_conn._execute(
        "set vt_transaction_cap=20", {},
        KEYSPACE_NAME, 'master',
        keyranges=[get_keyrange(shard_names[self.shard_index])])
    vtgate_conn.begin()
    vtgate_conn._execute(
        "delete from vt_insert_test", {},
        KEYSPACE_NAME, 'master',
        keyranges=[get_keyrange(shard_names[self.shard_index])])
    vtgate_conn.commit()


class TestAuthentication(unittest.TestCase):

  def setUp(self):
    global vtgate_server, vtgate_port
    self.shard_index = 0
    self.replica_tablet = shard_0_replica
    self.replica_tablet.kill_vttablet()
    self.replica_tablet.start_vttablet(auth=True)
    utils.vtgate_kill(vtgate_server)
    vtgate_server, vtgate_port = utils.vtgate_start(auth=True)
    credentials_file_name = os.path.join(environment.vttop, 'test', 'test_data',
                                         'authcredentials_test.json')
    credentials_file = open(credentials_file_name, 'r')
    credentials = json.load(credentials_file)
    self.user = str(credentials.keys()[0])
    self.password = str(credentials[self.user][0])
    self.secondary_password = str(credentials[self.user][1])

  def test_correct_credentials(self):
    try:
      vtgate_conn = get_connection(user=self.user,
                                    password=self.password)
    finally:
      vtgate_conn.close()

  def test_secondary_credentials(self):
    try:
      vtgate_conn = get_connection(user=self.user,
                                    password=self.secondary_password)
    finally:
      vtgate_conn.close()

  def test_incorrect_user(self):
    with self.assertRaises(dbexceptions.OperationalError):
      vtgate_conn = get_connection(user="romek", password="ma raka")

  def test_incorrect_credentials(self):
    with self.assertRaises(dbexceptions.OperationalError):
      vtgate_conn = get_connection(user=self.user, password="ma raka")

  def test_challenge_is_used(self):
    vtgate_conn = get_connection(user=self.user, password=self.password)
    challenge = ""
    proof =  "%s %s" %(self.user, hmac.HMAC(self.password,
                                            challenge).hexdigest())
    self.assertRaises(gorpc.AppError, vtgate_conn.client.call,
                      'AuthenticatorCRAMMD5.Authenticate', {"Proof": proof})

  def test_only_few_requests_are_allowed(self):
    vtgate_conn = get_connection(user=self.user,
                                  password=self.password)
    for i in range(4):
      try:
        vtgate_conn.client.call('AuthenticatorCRAMMD5.GetNewChallenge',
                                 "")
      except gorpc.GoRpcError:
        break
    else:
      self.fail("Too many requests were allowed (%s)." % (i + 1))


if __name__ == '__main__':
  utils.main()
