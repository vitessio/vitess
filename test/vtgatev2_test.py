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

from multiprocessing.pool import ThreadPool

import environment
import tablet
import utils

from net import gorpc
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import keyspace
from vtdb import dbexceptions
from vtdb import vtdb_logger
from vtdb import vtgatev2
from vtdb import vtgate_cursor
from zk import zkocc

conn_class = vtgatev2

shard_0_master = tablet.Tablet()
shard_0_replica1 = tablet.Tablet()
shard_0_replica2 = tablet.Tablet()

shard_1_master = tablet.Tablet()
shard_1_replica1 = tablet.Tablet()
shard_1_replica2 = tablet.Tablet()

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
    environment.topo_server().setup()

    # start mysql instance external to the test
    setup_procs = [shard_0_master.init_mysql(),
                   shard_0_replica1.init_mysql(),
                   shard_0_replica2.init_mysql(),
                   shard_1_master.init_mysql(),
                   shard_1_replica1.init_mysql(),
                   shard_1_replica2.init_mysql()
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
  tablet.kill_tablets([shard_0_master,
                       shard_0_replica1, shard_0_replica2,
                       shard_1_master,
                       shard_1_replica1, shard_1_replica2])
  teardown_procs = [shard_0_master.teardown_mysql(),
                    shard_0_replica1.teardown_mysql(),
                    shard_0_replica2.teardown_mysql(),
                    shard_1_master.teardown_mysql(),
                    shard_1_replica1.teardown_mysql(),
                    shard_1_replica2.teardown_mysql(),
                   ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()

  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica1.remove_tree()
  shard_0_replica2.remove_tree()
  shard_1_master.remove_tree()
  shard_1_replica1.remove_tree()
  shard_1_replica2.remove_tree()

def setup_tablets():
  global vtgate_server
  global vtgate_port

  # Start up a master mysql and vttablet
  logging.debug("Setting up tablets")
  utils.run_vtctl(['CreateKeyspace', KEYSPACE_NAME])
  utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', KEYSPACE_NAME,
                   'keyspace_id', 'uint64'])
  shard_0_master.init_tablet('master', keyspace=KEYSPACE_NAME, shard='-80')
  shard_0_replica1.init_tablet('replica', keyspace=KEYSPACE_NAME, shard='-80')
  shard_0_replica2.init_tablet('replica', keyspace=KEYSPACE_NAME, shard='-80')
  shard_1_master.init_tablet('master', keyspace=KEYSPACE_NAME, shard='80-')
  shard_1_replica1.init_tablet('replica', keyspace=KEYSPACE_NAME, shard='80-')
  shard_1_replica2.init_tablet('replica', keyspace=KEYSPACE_NAME, shard='80-')

  utils.run_vtctl(['RebuildKeyspaceGraph', KEYSPACE_NAME], auto_log=True)

  for t in [shard_0_master, shard_0_replica1, shard_0_replica2,
            shard_1_master, shard_1_replica1, shard_1_replica2]:
    t.create_db('vt_test_keyspace')
    for create_table in create_tables:
      t.mquery(shard_0_master.dbname, create_table)
    t.start_vttablet(wait_for_state=None)

  for t in [shard_0_master, shard_0_replica1, shard_0_replica2,
            shard_1_master, shard_1_replica1, shard_1_replica2]:
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


def get_connection(user=None, password=None, timeout=10.0):
  global vtgate_port
  conn = None
  vtgate_addrs = {"vt": ["localhost:%s" % (vtgate_port),]}
  conn = conn_class.connect(vtgate_addrs, timeout,
                            user=user, password=password)
  return conn

def get_keyrange(shard_name):
  kr = None
  if shard_name == keyrange_constants.SHARD_ZERO:
    kr = keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)
  else:
    kr = keyrange.KeyRange(shard_name)
  return kr


def _delete_all(shard_index, table_name):
  vtgate_conn = get_connection()
  # This write is to set up the test with fresh insert
  # and hence performing it directly on the connection.
  vtgate_conn.begin()
  vtgate_conn._execute("delete from %s" % table_name, {},
                       KEYSPACE_NAME, 'master',
                       keyranges=[get_keyrange(shard_names[shard_index])])
  vtgate_conn.commit()


def do_write(count, shard_index):
  kid_list = shard_kid_map[shard_names[shard_index]]
  _delete_all(shard_index, 'vt_insert_test')
  vtgate_conn = get_connection()

  for x in xrange(count):
    keyspace_id = kid_list[x%len(kid_list)]
    cursor = vtgate_conn.cursor(KEYSPACE_NAME, 'master',
                                keyspace_ids=[pack_kid(keyspace_id)],
                                writable=True)
    cursor.begin()
    cursor.execute(
        "insert into vt_insert_test (msg, keyspace_id) values (%(msg)s, %(keyspace_id)s)",
        {'msg': 'test %s' % x, 'keyspace_id': keyspace_id})
    cursor.commit()


def restart_vtgate(extra_args={}):
  global vtgate_server, vtgate_port
  utils.vtgate_kill(vtgate_server)
  vtgate_server, vtgate_port = utils.vtgate_start(vtgate_port, extra_args=extra_args)


class TestVTGateFunctions(unittest.TestCase):
  def setUp(self):
    self.shard_index = 1
    self.keyrange = get_keyrange(shard_names[self.shard_index])
    self.master_tablet = shard_1_master
    self.replica_tablet = shard_1_replica1

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
      _delete_all(self.shard_index, 'vt_insert_test')
      count = 10
      kid_list = shard_kid_map[shard_names[self.shard_index]]
      for x in xrange(count):
        keyspace_id = kid_list[count%len(kid_list)]
        cursor = vtgate_conn.cursor(KEYSPACE_NAME, 'master',
                                    keyspace_ids=[pack_kid(keyspace_id)],
                                    writable=True)
        cursor.begin()
        cursor.execute(
            "insert into vt_insert_test (msg, keyspace_id) values (%(msg)s, %(keyspace_id)s)",
            {'msg': 'test %s' % x, 'keyspace_id': keyspace_id})
        cursor.commit()
      cursor = vtgate_conn.cursor(KEYSPACE_NAME, 'master',
                                  keyranges=[self.keyrange])
      rowcount = cursor.execute("select * from vt_insert_test", {})
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
      vtgate_conn = get_connection()
      for shard_index in [0,1]:
        # Fetch all rows in each shard
        cursor = vtgate_conn.cursor(KEYSPACE_NAME, 'master',
                                    keyranges=[get_keyrange(shard_names[shard_index])])
        rowcount = cursor.execute("select * from vt_insert_test", {})
        # Verify row count
        self.assertEqual(rowcount, row_counts[shard_index])
        # Verify keyspace id
        for result in cursor.results:
          kid = result[2]
          self.assertTrue(kid in shard_kid_map[shard_names[shard_index]])
      # Do a cross shard range query and assert all rows are fetched
      cursor = vtgate_conn.cursor(KEYSPACE_NAME, 'master',
                                  keyranges=[get_keyrange('75-95')])
      rowcount = cursor.execute("select * from vt_insert_test", {})
      self.assertEqual(rowcount, row_counts[0] + row_counts[1])
    except Exception, e:
      logging.debug("failed with error %s, %s" % (str(e), traceback.print_exc()))
      raise

  def test_rollback(self):
    try:
      vtgate_conn = get_connection()
      count = 10
      _delete_all(self.shard_index, 'vt_insert_test')
      kid_list = shard_kid_map[shard_names[self.shard_index]]
      for x in xrange(count):
        keyspace_id = kid_list[x%len(kid_list)]
        cursor = vtgate_conn.cursor(KEYSPACE_NAME, 'master',
                                    keyspace_ids=[pack_kid(keyspace_id)],
                                    writable=True)
        cursor.begin()
        cursor.execute(
            "insert into vt_insert_test (msg, keyspace_id) values (%(msg)s, %(keyspace_id)s)",
            {'msg': 'test %s' % x, 'keyspace_id': keyspace_id})
        cursor.commit()

      vtgate_conn.begin()
      vtgate_conn._execute(
          "delete from vt_insert_test", {},
          KEYSPACE_NAME, 'master',
          keyranges=[self.keyrange])
      vtgate_conn.rollback()
      cursor = vtgate_conn.cursor(KEYSPACE_NAME, 'master',
                                  keyranges=[self.keyrange])
      rowcount = cursor.execute("select * from vt_insert_test", {})
      logging.debug("ROLLBACK TEST rowcount %d count %d" % (rowcount, count))
      self.assertEqual(rowcount, count, "Fetched rows(%d) != inserted rows(%d), rollback didn't work" % (rowcount, count))
      do_write(10, self.shard_index)
    except Exception, e:
      self.fail("Write failed with error %s %s" % (str(e), traceback.print_exc()))

  def test_execute_entity_ids(self):
    try:
      vtgate_conn = get_connection()
      count = 10
      _delete_all(self.shard_index, 'vt_a')
      eid_map = {}
      kid_list = shard_kid_map[shard_names[self.shard_index]]
      for x in xrange(count):
        keyspace_id = kid_list[x%len(kid_list)]
        eid_map[x] = pack_kid(keyspace_id)
        cursor = vtgate_conn.cursor(KEYSPACE_NAME, 'master',
                                    keyspace_ids=[pack_kid(keyspace_id)],
                                    writable=True)
        cursor.begin()
        cursor.execute(
            "insert into vt_a (eid, id, keyspace_id) \
             values (%(eid)s, %(id)s, %(keyspace_id)s)",
            {'eid': x, 'id': x, 'keyspace_id': keyspace_id})
        cursor.commit()
      cursor = vtgate_conn.cursor(KEYSPACE_NAME, 'master',
                                  keyspace_ids=eid_map.values())
      rowcount = cursor.execute_entity_ids("select * from vt_a", {}, eid_map,
                                           'id')
      self.assertEqual(rowcount, count, "entity_ids works")
    except Exception, e:
      self.fail("Execute entity ids failed with error %s %s" %
                (str(e),
                 traceback.print_exc()))

  def test_batch_read(self):
    try:
      vtgate_conn = get_connection()
      count = 10
      _delete_all(self.shard_index, 'vt_insert_test')
      kid_list = shard_kid_map[shard_names[self.shard_index]]
      for x in xrange(count):
        keyspace_id = kid_list[x%len(kid_list)]
        cursor = vtgate_conn.cursor(KEYSPACE_NAME, 'master',
                                    keyspace_ids=[pack_kid(keyspace_id)],
                                    writable=True)
        cursor.begin()
        cursor.execute(
            "insert into vt_insert_test (msg, keyspace_id) values (%(msg)s, %(keyspace_id)s)",
            {'msg': 'test %s' % x, 'keyspace_id': keyspace_id})
        cursor.commit()
      _delete_all(self.shard_index, 'vt_a')
      for x in xrange(count):
        keyspace_id = kid_list[x%len(kid_list)]
        cursor = vtgate_conn.cursor(KEYSPACE_NAME, 'master',
                                    keyspace_ids=[pack_kid(keyspace_id)],
                                    writable=True)
        cursor.begin()
        cursor.execute(
            "insert into vt_a (eid, id, keyspace_id) \
             values (%(eid)s, %(id)s, %(keyspace_id)s)",
            {'eid': x, 'id': x, 'keyspace_id': keyspace_id})
        cursor.commit()
      kid_list = [pack_kid(kid) for kid in kid_list]
      cursor = vtgate_conn.cursor(KEYSPACE_NAME, 'master',
                                  keyspace_ids=kid_list,
                                  cursorclass=vtgate_cursor.BatchVTGateCursor)
      cursor.execute("select * from vt_insert_test", {})
      cursor.execute("select * from vt_a", {})
      cursor.flush()
      self.assertEqual(cursor.rowsets[0][1], count)
      self.assertEqual(cursor.rowsets[1][1], count)
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
          KEYSPACE_NAME, 'master', keyspace_ids=[pack_kid(kid) for kid in kid_list])
      vtgate_conn.commit()
      results, rowcount, _, _ = vtgate_conn._execute(
          "select * from vt_insert_test", {},
          KEYSPACE_NAME, 'master',
          keyranges=[self.keyrange])
      self.assertEqual(rowcount, count)
      results, rowcount, _, _ = vtgate_conn._execute(
          "select * from vt_a", {},
          KEYSPACE_NAME, 'master',
          keyranges=[self.keyrange])
      self.assertEqual(rowcount, count)
    except Exception, e:
      self.fail("Write failed with error %s" % str(e))

  def test_streaming_fetchsubset(self):
    try:
      count = 100
      do_write(count, self.shard_index)
      # Fetch a subset of the total size.
      vtgate_conn = get_connection()
      stream_cursor = vtgate_conn.cursor(
        KEYSPACE_NAME, 'master',
        keyranges=[self.keyrange],
        cursorclass=vtgate_cursor.StreamVTGateCursor)
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
      stream_cursor = vtgate_conn.cursor(KEYSPACE_NAME, 'master',
                                   keyranges=[self.keyrange],
                                   cursorclass=vtgate_cursor.StreamVTGateCursor)
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
      stream_cursor = vtgate_conn.cursor(KEYSPACE_NAME, 'master',
                                   keyranges=[self.keyrange],
                                   cursorclass=vtgate_cursor.StreamVTGateCursor)
      stream_cursor.execute("select * from vt_insert_test", {})
      rows = stream_cursor.fetchone()
      self.assertTrue(type(rows) == tuple, "Received a valid row")
      stream_cursor.close()
    except Exception, e:
      self.fail("Failed with error %s %s" % (str(e), traceback.print_exc()))

  def test_streaming_multishards(self):
    try:
      count = 100
      do_write(count, 0)
      do_write(count, 1)
      vtgate_conn = get_connection()
      stream_cursor = vtgate_conn.cursor(
        KEYSPACE_NAME, 'master',
        keyranges=[keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)],
        cursorclass=vtgate_cursor.StreamVTGateCursor)
      stream_cursor.execute("select * from vt_insert_test", {})
      rows = stream_cursor.fetchall()
      rowcount = 0
      for row in rows:
        rowcount += 1
      self.assertEqual(rowcount, count*2)
      stream_cursor.close()
    except Exception, e:
      self.fail("Failed with error %s %s" % (str(e), traceback.print_exc()))

  def test_streaming_zero_results(self):
    try:
      vtgate_conn = get_connection()
      vtgate_conn.begin()
      vtgate_conn._execute("delete from vt_insert_test", {},
                           KEYSPACE_NAME, 'master',
                           keyranges=[self.keyrange])
      vtgate_conn.commit()
      # After deletion, should result zero.
      stream_cursor = vtgate_conn.cursor(KEYSPACE_NAME, 'master',
                                   keyranges=[self.keyrange],
                                   cursorclass=vtgate_cursor.StreamVTGateCursor)
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
          keyranges=[self.keyrange])
      kid_list = shard_kid_map[shard_names[self.shard_index]]
      count = len(kid_list)
      for x in xrange(count):
        keyspace_id = kid_list[x]
        vtgate_conn._execute(
            "insert into vt_insert_test (msg, keyspace_id) values (%(msg)s, %(keyspace_id)s)",
            {'msg': 'test %s' % x, 'keyspace_id': keyspace_id},
            KEYSPACE_NAME, tablet_type, keyspace_ids=[pack_kid(keyspace_id)])
      vtgate_conn.commit()
      vtgate_conn2 = get_connection()
      query = "select keyspace_id from vt_insert_test where keyspace_id = %(kid)s"
      thd = threading.Thread(target=self._query_lots, args=(
          vtgate_conn2,
          query,
          {'kid': kid_list[0]},
          KEYSPACE_NAME,
          tablet_type,
          [pack_kid(kid_list[0])]))
      thd.start()
      for i in xrange(count):
        (result, _, _, _) = vtgate_conn._execute(query,
            {'kid':kid_list[i]},
            KEYSPACE_NAME, tablet_type,
            keyspace_ids=[pack_kid(kid_list[i])])
        self.assertEqual(result, [(kid_list[i],)])
        if i % 10 == 0:
          vtgate_conn._stream_execute(query, {'kid':kid_list[i]}, KEYSPACE_NAME,
                                      tablet_type,
                                      keyspace_ids=[pack_kid(kid_list[i])])
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
    global vtgate_server, vtgate_port
    self.shard_index = 1
    self.keyrange = get_keyrange(shard_names[self.shard_index])
    self.master_tablet = shard_1_master
    self.master_tablet.kill_vttablet()
    self.tablet_start(self.master_tablet)
    self.replica_tablet = shard_1_replica1
    self.replica_tablet.kill_vttablet()
    self.tablet_start(self.replica_tablet)
    self.replica_tablet2 = shard_1_replica2
    self.replica_tablet2.kill_vttablet()
    #self.tablet_start(self.replica_tablet2)
    utils.vtgate_kill(vtgate_server)
    vtgate_server, vtgate_port = utils.vtgate_start(vtgate_port)

  def tablet_start(self, tablet):
    return tablet.start_vttablet(lameduck_period='1s')

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
          keyranges=[self.keyrange])
    proc = self.tablet_start(self.replica_tablet)
    try:
      results = vtgate_conn._execute(
          "select 1 from vt_insert_test", {},
          KEYSPACE_NAME, 'replica',
          keyranges=[self.keyrange])
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
          keyranges=[self.keyrange])
    vtgate_server, vtgate_port = utils.vtgate_start(vtgate_port)
    vtgate_conn = get_connection()
    try:
      results = vtgate_conn._execute(
          "select 1 from vt_insert_test", {},
          KEYSPACE_NAME, 'replica',
          keyranges=[self.keyrange])
    except Exception, e:
      self.fail("Communication with shard %s replica failed with error %s" %
                (shard_names[self.shard_index], str(e)))

  def test_tablet_restart_stream_execute(self):
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % (str(e)))
    stream_cursor = vtgate_conn.cursor(
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange],
        cursorclass=vtgate_cursor.StreamVTGateCursor)
    self.replica_tablet.kill_vttablet()
    with self.assertRaises(dbexceptions.DatabaseError):
      stream_cursor.execute("select * from vt_insert_test", {})
    proc = self.tablet_start(self.replica_tablet)
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
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange],
        cursorclass=vtgate_cursor.StreamVTGateCursor)
    utils.vtgate_kill(vtgate_server)
    with self.assertRaises(dbexceptions.OperationalError):
      stream_cursor.execute("select * from vt_insert_test", {})
    vtgate_server, vtgate_port = utils.vtgate_start(vtgate_port)
    vtgate_conn = get_connection()
    stream_cursor = vtgate_conn.cursor(
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange],
        cursorclass=vtgate_cursor.StreamVTGateCursor)
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
    proc = self.tablet_start(self.master_tablet)
    vtgate_conn.begin()
    # this succeeds only if retry_count > 0
    vtgate_conn._execute(
        "delete from vt_insert_test", {},
        KEYSPACE_NAME, 'master',
        keyranges=[self.keyrange])
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
          keyranges=[self.keyrange])
      vtgate_conn.commit()
    proc = self.tablet_start(self.master_tablet)
    vtgate_conn.begin()
    vtgate_conn._execute(
        "delete from vt_insert_test", {},
        KEYSPACE_NAME, 'master',
        keyranges=[self.keyrange])
    vtgate_conn.commit()

  def test_error_on_dml(self):
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    vtgate_conn.begin()
    keyspace_id = shard_kid_map[shard_names[
        (self.shard_index+1)%len(shard_names)
        ]][0]
    try:
      vtgate_conn._execute(
          "insert into vt_insert_test values(:msg, :keyspace_id)",
          {"msg": "test4", "keyspace_id": keyspace_id}, KEYSPACE_NAME, 'master',
          keyranges=[self.keyrange])
      vtgate_conn.commit()
      self.fail("Failed to raise DatabaseError exception")
    except dbexceptions.DatabaseError:
      if conn_class == vtgatev2:
        logging.info("SHARD SESSIONS: %s", vtgate_conn.session["ShardSessions"])
        transaction_id = vtgate_conn.session["ShardSessions"][0]["TransactionId"]
      else:
        transaction_id = vtgate_conn.session.shard_sessions[0].transaction_id
      self.assertTrue(transaction_id != 0)
    except Exception, e:
      self.fail("Expected DatabaseError as exception, got %s" % str(e))
    finally:
      vtgate_conn.rollback()


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
          keyranges=[self.keyrange])
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
        keyranges=[self.keyrange])
    vtgate_conn.commit()

  # test timeout between py client and vtgate
  def test_vtgate_timeout(self):
    try:
      vtgate_conn = get_connection(timeout=3.0)
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.TimeoutError):
      vtgate_conn._execute(
          "select sleep(4) from dual", {},
          KEYSPACE_NAME, 'replica',
          keyranges=[self.keyrange])

    try:
      vtgate_conn = get_connection(timeout=3.0)
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.TimeoutError):
      vtgate_conn._execute(
          "select sleep(4) from dual", {},
          KEYSPACE_NAME, 'master',
          keyranges=[self.keyrange])
     # Currently this is causing vttablet to become unreachable at
     # the timeout boundary and kill any query being executed
     # at the time. Prevent flakiness in other tests by sleeping
     # until the query times out.
     # TODO fix b/17733518
    time.sleep(3)

  # test timeout between vtgate and vttablet
  # the timeout is set to 5 seconds
  def test_tablet_timeout(self):
    # this test only makes sense if there is a shorter/protective timeout
    # set for vtgate-vttablet connection.
    # TODO(liguo): evaluate if we want such a timeout
    return
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn.begin()
      vtgate_conn._execute(
          "select sleep(7) from dual", {},
          KEYSPACE_NAME, 'replica',
          keyranges=[self.keyrange])

    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn.begin()
      vtgate_conn._execute(
          "select sleep(7) from dual", {},
          KEYSPACE_NAME, 'master',
          keyranges=[self.keyrange])

  # Test the case that no query sent during tablet shuts down (single tablet)
  def test_restart_mysql_tablet_idle(self):
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    utils.wait_procs([self.replica_tablet.shutdown_mysql(),])
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])
    utils.wait_procs([self.replica_tablet.start_mysql(),])
    # force health check so tablet can become serving
    utils.run_vtctl(['RunHealthCheck', self.replica_tablet.tablet_alias, 'replica'],
                    auto_log=True)
    self.replica_tablet.wait_for_vttablet_state('SERVING')
    vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])
    self.replica_tablet.kill_vttablet()
    self.tablet_start(self.replica_tablet)
    self.replica_tablet.wait_for_vttablet_state('SERVING')
    # TODO: expect to fail until we can detect vttablet proper shuts down vs crashes
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])

  # Test the case that there are queries sent during vttablet shuts down,
  # and all querys fail because there is only one vttablet.
  def test_restart_mysql_tablet_queries(self):
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    utils.wait_procs([self.replica_tablet.shutdown_mysql(),])
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])
    utils.wait_procs([self.replica_tablet.start_mysql(),])
    # force health check so tablet can become serving
    utils.run_vtctl(['RunHealthCheck', self.replica_tablet.tablet_alias, 'replica'],
                    auto_log=True)
    self.replica_tablet.wait_for_vttablet_state('SERVING')
    self.replica_tablet.kill_vttablet(wait=False)
    # send query while vttablet is in lameduck, should fail as no vttablet
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])
    # send another query, should also fail
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])
    # sleep over the lameduck period
    time.sleep(1)
    self.tablet_start(self.replica_tablet)
    self.replica_tablet.wait_for_vttablet_state('SERVING')
    # as the cached vtgate-tablet conn was marked down, it should succeed
    vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])

  # Test the case that there are queries sent during one vttablet shuts down,
  # and all querys succeed because there is another vttablet.
  def test_restart_mysql_tablet_queries_multi_tablets(self):
    self.tablet_start(self.replica_tablet2)
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    utils.wait_procs([self.replica_tablet.shutdown_mysql(),])
    # should retry on tablet2 and succeed
    vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])
    utils.wait_procs([self.replica_tablet.start_mysql(),])
    # force health check so tablet can become serving
    utils.run_vtctl(['RunHealthCheck', self.replica_tablet.tablet_alias, 'replica'],
                    auto_log=True)
    self.replica_tablet.wait_for_vttablet_state('SERVING')
    vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])
    self.replica_tablet2.kill_vttablet(wait=False)
    # send query while tablet2 is in lameduck, should retry on tablet1
    vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])
    # sleep over the lameduck period
    time.sleep(1)
    # send another query, should also succeed on tablet1
    vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])
    self.tablet_start(self.replica_tablet2)
    self.replica_tablet2.wait_for_vttablet_state('SERVING')
    # it should succeed on tablet1
    vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])
    self.replica_tablet2.kill_vttablet()

  # Test the case that there are queries sent during one vttablet is killed,
  # and all querys succeed because there is another vttablet.
  def test_kill_mysql_tablet_queries_multi_tablets(self):
    self.tablet_start(self.replica_tablet2)
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    utils.wait_procs([self.replica_tablet.shutdown_mysql(),])
    # should retry on tablet2 and succeed
    vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])
    utils.wait_procs([self.replica_tablet.start_mysql(),])
    # force health check so tablet can become serving
    utils.run_vtctl(['RunHealthCheck', self.replica_tablet.tablet_alias, 'replica'],
                    auto_log=True)
    self.replica_tablet.wait_for_vttablet_state('SERVING')
    vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])
    self.replica_tablet2.hard_kill_vttablet()
    # send query after tablet2 is killed, should not retry on the cached conn
    with self.assertRaises(dbexceptions.DatabaseError):
      vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])
    # send another query, should succeed on tablet1
    vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])
    self.tablet_start(self.replica_tablet2)
    self.replica_tablet2.wait_for_vttablet_state('SERVING')
    # it should succeed on tablet1
    vtgate_conn._execute(
        "select 1 from vt_insert_test", {},
        KEYSPACE_NAME, 'replica',
        keyranges=[self.keyrange])
    self.replica_tablet2.kill_vttablet()


  # FIXME(shrutip): this test is basically just testing that
  # txn pool full error doesn't get thrown anymore with vtgate.
  # vtgate retries for this condition. Not a very high value
  # test at this point, could be removed if there is coverage at vtgate level.
  def test_retry_txn_pool_full(self):
    vtgate_conn = get_connection()
    vtgate_conn._execute(
        "set vt_transaction_cap=1", {},
        KEYSPACE_NAME, 'master',
        keyranges=[self.keyrange])
    vtgate_conn.begin()
    vtgate_conn2 = get_connection()
    vtgate_conn2.begin()
    vtgate_conn.commit()
    vtgate_conn._execute(
        "set vt_transaction_cap=20", {},
        KEYSPACE_NAME, 'master',
        keyranges=[self.keyrange])
    vtgate_conn.begin()
    vtgate_conn._execute(
        "delete from vt_insert_test", {},
        KEYSPACE_NAME, 'master',
        keyranges=[self.keyrange])
    vtgate_conn.commit()

  def test_bind_vars_in_exception_message(self):
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))
    keyspace_id = None

    count = 1
    vtgate_conn.begin()
    vtgate_conn._execute(
        "delete from vt_a", {},
        KEYSPACE_NAME, 'master',
        keyranges=[get_keyrange(shard_names[self.shard_index])])
    vtgate_conn.commit()
    eid_map = {}
    # start transaction
    vtgate_conn.begin()
    kid_list = shard_kid_map[shard_names[self.shard_index]]

    #kill vttablet
    self.master_tablet.kill_vttablet()

    try:
      # perform write, this should fail
      for x in xrange(count):
        keyspace_id = kid_list[x%len(kid_list)]
        eid_map[x] = str(keyspace_id)
        vtgate_conn._execute(
            "insert into vt_a (eid, id, keyspace_id) \
             values (%(eid)s, %(id)s, %(keyspace_id)s)",
            {'eid': x, 'id': x, 'keyspace_id': keyspace_id},
            KEYSPACE_NAME, 'master', keyspace_ids=[pack_kid(keyspace_id)])
      vtgate_conn.commit()
    except Exception, e:
      # check that bind var value is not present in exception message.
      if str(keyspace_id) in str(e):
        self.fail("bind_vars present in the exception message")
    finally:
      vtgate_conn.rollback()
    # Start master tablet again
    self.tablet_start(self.master_tablet)

  def test_fail_fast_when_no_serving_tablets(self):
    """Verify VtGate requests fail-fast when tablets are unavailable.

    When there are no SERVING tablets available to serve a request, VtGate should
    fail-fast (returning an appropriate error) without waiting around till the
    request deadline expires.
    """
    try:
      tablet_type = 'replica'
      keyranges = [get_keyrange(shard_names[self.shard_index])]
      query = 'select * from vt_insert_test'
      # Execute a query to warm VtGate's caches for connections and endpoints
      get_rtt(KEYSPACE_NAME, query, tablet_type, keyranges)

      # Shutdown mysql and ensure tablet is in NOT_SERVING state
      utils.wait_procs([self.replica_tablet.shutdown_mysql()])
      try:
        get_rtt(KEYSPACE_NAME, query, tablet_type, keyranges)
        self.replica_tablet.wait_for_vttablet_state('NOT_SERVING')
      except Exception, e:
        self.fail('unable to set tablet to NOT_SERVING state')

      # Fire off a few requests in parallel
      num_requests = 10
      pool = ThreadPool(processes=num_requests)
      async_results = []
      for i in range(num_requests):
        async_result = pool.apply_async(get_rtt, (KEYSPACE_NAME, query, tablet_type, keyranges,))
        async_results.append(async_result)

      # Fetch all round trip times and verify max
      rt_times = []
      for async_result in async_results:
        rt_times.append(async_result.get())
      # The true upper limit is 2 seconds (1s * 2 retries as in utils.py). To account for
      # network latencies and other variances, we keep an upper bound of 3 here.
      self.assertTrue(max(rt_times) < 3, 'at least one request did not fail-fast; round trip times: %s' % rt_times)

      # Restart tablet and put it back to SERVING state
      utils.wait_procs([self.replica_tablet.start_mysql(),])
      self.replica_tablet.kill_vttablet()
      self.tablet_start(self.replica_tablet)
      self.replica_tablet.wait_for_vttablet_state('SERVING')
    except Exception, e:
      logging.debug("failed with error %s, %s" % (str(e), traceback.print_exc()))
      raise


# Return round trip time for a VtGate query, ignore any errors
def get_rtt(keyspace, query, tablet_type, keyranges):
  vtgate_conn = get_connection()
  cursor = vtgate_conn.cursor(keyspace, tablet_type, keyranges=keyranges)
  start = time.time()
  try:
    cursor.execute(query, {})
  except Exception, e:
    pass
  duration = time.time() - start
  return duration


class VTGateTestLogger(vtdb_logger.VtdbLogger):

  def __init__(self):
    self._integrity_error_count = 0

  def integrity_error(self, e):
    self._integrity_error_count += 1

  def get_integrity_error_count(self):
    return self._integrity_error_count


DML_KEYWORDS = ['insert', 'update', 'delete']


class TestExceptionLogging(unittest.TestCase):
  def setUp(self):
    self.shard_index = 1
    self.keyrange = get_keyrange(shard_names[self.shard_index])
    self.master_tablet = shard_1_master
    self.replica_tablet = shard_1_replica1
    vtdb_logger.register_vtdb_logger(VTGateTestLogger())
    self.logger = vtdb_logger.get_logger()

  def test_integrity_error_logging(self):
    try:
      vtgate_conn = get_connection()
    except Exception, e:
      self.fail("Connection to vtgate failed with error %s" % str(e))

    vtgate_conn.begin()
    vtgate_conn._execute(
          "delete from vt_a", {},
          KEYSPACE_NAME, 'master',
          keyranges=[self.keyrange])
    vtgate_conn.commit()

    keyspace_id = shard_kid_map[shard_names[self.shard_index]][0]

    old_error_count = self.logger.get_integrity_error_count()
    try:
      vtgate_conn.begin()
      vtgate_conn._execute(
        "insert into vt_a (eid, id, keyspace_id) values (%(eid)s, %(id)s, %(keyspace_id)s)",
        {'eid': 1, 'id': 1, 'keyspace_id':keyspace_id}, KEYSPACE_NAME, 'master',
        keyspace_ids=[pack_kid(keyspace_id)])
      vtgate_conn._execute(
        "insert into vt_a (eid, id, keyspace_id) values (%(eid)s, %(id)s, %(keyspace_id)s)",
        {'eid': 1, 'id': 1, 'keyspace_id':keyspace_id}, KEYSPACE_NAME, 'master',
        keyspace_ids=[pack_kid(keyspace_id)])
      vtgate_conn.commit()
    except dbexceptions.IntegrityError as e:
      parts = str(e).split(',')
      exc_msg = parts[0]
      for kw in DML_KEYWORDS:
        if kw in exc_msg:
          self.fail("IntegrityError shouldn't contain the query %s" % exc_msg)
    except Exception as e:
      self.fail("Expected IntegrityError to be raised, raised %s" % str(e))
    finally:
      vtgate_conn.rollback()
    # The underlying execute is expected to catch and log the integrity error.
    self.assertEqual(self.logger.get_integrity_error_count(), old_error_count+1)


class TestAuthentication(unittest.TestCase):

  def setUp(self):
    global vtgate_server, vtgate_port
    self.shard_index = 1
    self.replica_tablet = shard_1_replica1
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
