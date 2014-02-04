#!/usr/bin/env python
# coding: utf-8

import hmac
import json
import logging
import os
import traceback
import unittest

import environment
import tablet
import utils

from net import gorpc
from vtdb import tablet as tablet3
from vtdb import cursor
from vtdb import vtclient
from vtdb import vtgate
from vtdb import dbexceptions
from zk import zkocc

VTGATE_PROTOCOL_TABLET = 'v0'
VTGATE_PROTOCOL_V1BSON = 'v1bson'
vtgate_protocol = VTGATE_PROTOCOL_TABLET

shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()

shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()

vtgate_server = None
vtgate_port = None

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
  utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])
  utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', 'test_keyspace',
                   'keyspace_id', 'uint64'])
  shard_0_master.init_tablet('master', keyspace='test_keyspace', shard='-80')
  shard_0_replica.init_tablet('replica', keyspace='test_keyspace', shard='-80')
  shard_1_master.init_tablet('master', keyspace='test_keyspace', shard='80-')
  shard_1_replica.init_tablet('replica', keyspace='test_keyspace', shard='80-')

  utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

  for t in [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica]:
    t.create_db('vt_test_keyspace')
    t.mquery(shard_0_master.dbname, create_vt_insert_test)
    t.mquery(shard_0_master.dbname, create_vt_a)
    t.start_vttablet(wait_for_state=None)

  for t in [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica]:
    t.wait_for_vttablet_state('SERVING')

  utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/-80',
                   shard_0_master.tablet_alias], auto_log=True)
  utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/80-',
                   shard_1_master.tablet_alias], auto_log=True)

  utils.run_vtctl(['RebuildKeyspaceGraph', '-use-served-types', 'test_keyspace'],
                   auto_log=True)

  utils.check_srv_keyspace('test_nj', 'test_keyspace',
                           'Partitions(master): -80 80-\n' +
                           'Partitions(replica): -80 80-\n' +
                           'TabletTypes: master,replica')

  vtgate_server, vtgate_port = utils.vtgate_start()


def get_connection(db_type='master', shard_index=0, user=None, password=None):
  global vtgate_protocol
  global vtgate_port
  timeout = 10.0
  conn = None
  shard = shard_names[shard_index]
  if vtgate_protocol == VTGATE_PROTOCOL_TABLET:
    vtgate_addrs = []
  elif vtgate_protocol == VTGATE_PROTOCOL_V1BSON:
    vtgate_addrs = ["localhost:%s"%(vtgate_port),]
  else:
    raise Exception("Unknown vtgate_protocol %s", vtgate_protocol)

  vtgate_client = zkocc.ZkOccConnection("localhost:%u" % vtgate_port,
                                        "test_nj", 30.0)
  conn = vtclient.VtOCCConnection(vtgate_client, 'test_keyspace', shard,
                                  db_type, timeout,
                                  user=user, password=password,
                                  vtgate_protocol=vtgate_protocol,
                                  vtgate_addrs=vtgate_addrs)
  conn.connect()
  return conn

def do_write(count):
  master_conn = get_connection(db_type='master')
  master_conn.begin()
  master_conn._execute("delete from vt_insert_test", {})
  kid_list = shard_kid_map[master_conn.shard]
  for x in xrange(count):
    keyspace_id = kid_list[count%len(kid_list)]
    master_conn._execute("insert into vt_insert_test (msg, keyspace_id) values (%(msg)s, %(keyspace_id)s)",
                         {'msg': 'test %s' % x, 'keyspace_id': keyspace_id})
  master_conn.commit()


class TestTabletFunctions(unittest.TestCase):
  def setUp(self):
    self.shard_index = 0
    self.master_tablet = shard_0_master
    self.replica_tablet = shard_0_replica

  def test_connect(self):
    global vtgate_protocol
    try:
      master_conn = get_connection(db_type='master', shard_index=self.shard_index)
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    self.assertNotEqual(master_conn, None)
    try:
      replica_conn = get_connection(db_type='replica', shard_index=self.shard_index)
    except Exception, e:
      logging.debug("Connection to %s replica failed with error %s" %
                    (shard_names[self.shard_index], str(e)))
      raise
    self.assertNotEqual(replica_conn, None)
    if vtgate_protocol == VTGATE_PROTOCOL_TABLET:
      self.assertIsInstance(master_conn.conn, tablet3.TabletConnection,
                            "Invalid master connection")
      self.assertIsInstance(replica_conn.conn, tablet3.TabletConnection,
                            "Invalid replica connection")
    elif vtgate_protocol == VTGATE_PROTOCOL_V1BSON:
      self.assertIsInstance(master_conn.conn, vtgate.VtgateConnection,
                            "Invalid master connection")
      self.assertIsInstance(replica_conn.conn, vtgate.VtgateConnection,
                            "Invalid replica connection")

  def test_writes(self):
    try:
      master_conn = get_connection(db_type='master', shard_index=self.shard_index)
      count = 10
      master_conn.begin()
      master_conn._execute("delete from vt_insert_test", {})
      kid_list = shard_kid_map[master_conn.shard]
      for x in xrange(count):
        keyspace_id = kid_list[count%len(kid_list)]
        master_conn._execute("insert into vt_insert_test (msg, keyspace_id) values (%(msg)s, %(keyspace_id)s)",
                             {'msg': 'test %s' % x, 'keyspace_id': keyspace_id})
      master_conn.commit()
      results, rowcount = master_conn._execute("select * from vt_insert_test",
                                               {})[:2]
      self.assertEqual(rowcount, count, "master fetch works")
    except Exception, e:
      logging.debug("Write failed with error %s" % str(e))
      raise

  def test_batch_read(self):
    try:
      master_conn = get_connection(db_type='master', shard_index=self.shard_index)
      count = 10
      master_conn.begin()
      master_conn._execute("delete from vt_insert_test", {})
      kid_list = shard_kid_map[master_conn.shard]
      for x in xrange(count):
        keyspace_id = kid_list[count%len(kid_list)]
        master_conn._execute("insert into vt_insert_test (msg, keyspace_id) values (%(msg)s, %(keyspace_id)s)",
                             {'msg': 'test %s' % x, 'keyspace_id': keyspace_id})
      master_conn.commit()
      master_conn.begin()
      master_conn._execute("delete from vt_a", {})
      for x in xrange(count):
        keyspace_id = kid_list[count%len(kid_list)]
        master_conn._execute("insert into vt_a (eid, id, keyspace_id) \
                              values (%(eid)s, %(id)s, %(keyspace_id)s)",
                              {'eid': x, 'id': x, 'keyspace_id': keyspace_id})
      master_conn.commit()
      rowsets = master_conn._execute_batch(["select * from vt_insert_test",
                                            "select * from vt_a"], [{}, {}])
      self.assertEqual(rowsets[0][1], count)
      self.assertEqual(rowsets[1][1], count)
    except Exception, e:
      self.fail("Write failed with error %s %s" % (str(e),
                                                   traceback.print_exc()))

  def test_batch_write(self):
    try:
      master_conn = get_connection(db_type='master', shard_index=self.shard_index)
      count = 10
      query_list = []
      bind_vars_list = []
      query_list.append("delete from vt_insert_test")
      bind_vars_list.append({})
      kid_list = shard_kid_map[master_conn.shard]
      for x in xrange(count):
        keyspace_id = kid_list[count%len(kid_list)]
        query_list.append("insert into vt_insert_test (msg, keyspace_id) values (%(msg)s, %(keyspace_id)s)")
        bind_vars_list.append({'msg': 'test %s' % x, 'keyspace_id': keyspace_id})
      query_list.append("delete from vt_a")
      bind_vars_list.append({})
      for x in xrange(count):
        keyspace_id = kid_list[count%len(kid_list)]
        query_list.append("insert into vt_a (eid, id, keyspace_id) values (%(eid)s, %(id)s, %(keyspace_id)s)")
        bind_vars_list.append({'eid': x, 'id': x, 'keyspace_id': keyspace_id})
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
      master_conn = get_connection(db_type='master', shard_index=self.shard_index)
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
      master_conn = get_connection(db_type='master', shard_index=self.shard_index)
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
      master_conn = get_connection(db_type='master', shard_index=self.shard_index)
      stream_cursor = cursor.StreamCursor(master_conn)
      stream_cursor.execute("select * from vt_insert_test", {})
      rows = stream_cursor.fetchone()
      self.assertTrue(type(rows) == tuple, "Received a valid row")
      stream_cursor.close()
    except Exception, e:
      self.fail("Failed with error %s %s" % (str(e), traceback.print_exc()))

  def test_streaming_zero_results(self):
    try:
      master_conn = get_connection(db_type='master', shard_index=self.shard_index)
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
  def setUp(self):
    self.shard_index = 0
    self.master_tablet = shard_0_master
    self.replica_tablet = shard_0_replica

  def test_tablet_restart_read(self):
    try:
      replica_conn = get_connection(db_type='replica', shard_index=self.shard_index)
    except Exception, e:
      self.fail("Connection to shard %s replica failed with error %s" % (shard_names[self.shard_index], str(e)))
    self.replica_tablet.kill_vttablet()
    with self.assertRaises(dbexceptions.OperationalError):
      replica_conn._execute("select 1 from vt_insert_test", {})
    proc = self.replica_tablet.start_vttablet()
    try:
      results = replica_conn._execute("select 1 from vt_insert_test", {})
    except Exception, e:
      self.fail("Communication with shard %s replica failed with error %s" % (shard_names[self.shard_index], str(e)))

  def test_tablet_restart_stream_execute(self):
    try:
      replica_conn = get_connection(db_type='replica', shard_index=self.shard_index)
    except Exception, e:
      self.fail("Connection to %s replica failed with error %s" % (shard_names[self.shard_index], str(e)))
    stream_cursor = cursor.StreamCursor(replica_conn)
    self.replica_tablet.kill_vttablet()
    with self.assertRaises(dbexceptions.OperationalError):
      stream_cursor.execute("select * from vt_insert_test", {})
    proc = self.replica_tablet.start_vttablet()
    self.replica_tablet.wait_for_vttablet_state('SERVING')
    try:
      stream_cursor.execute("select * from vt_insert_test", {})
    except Exception, e:
      self.fail("Communication with shard0 replica failed with error %s" %
                str(e))

  def test_tablet_restart_begin(self):
    try:
      master_conn = get_connection(db_type='master')
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    self.master_tablet.kill_vttablet()
    with self.assertRaises(dbexceptions.OperationalError):
      master_conn.begin()
    proc = self.master_tablet.start_vttablet()
    master_conn.begin()

  def test_tablet_fail_write(self):
    try:
      master_conn = get_connection(db_type='master')
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.OperationalError):
      master_conn.begin()
      self.master_tablet.kill_vttablet()
      master_conn._execute("delete from vt_insert_test", {})
      master_conn.commit()
    proc = self.master_tablet.start_vttablet()
    try:
      master_conn = get_connection(db_type='master')
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    master_conn.begin()
    master_conn._execute("delete from vt_insert_test", {})
    master_conn.commit()

  def test_query_timeout(self):
    try:
      replica_conn = get_connection(db_type='replica', shard_index=self.shard_index)
    except Exception, e:
      self.fail("Connection to shard0 replica failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.TimeoutError):
      replica_conn._execute("select sleep(12) from dual", {})

    try:
      master_conn = get_connection(db_type='master')
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.TimeoutError):
      master_conn._execute("select sleep(12) from dual", {})

  def test_restart_mysql_failure(self):
    try:
      replica_conn = get_connection(db_type='replica', shard_index=self.shard_index)
    except Exception, e:
      self.fail("Connection to shard0 replica failed with error %s" % str(e))
    utils.wait_procs([self.replica_tablet.shutdown_mysql(),])
    with self.assertRaises(dbexceptions.DatabaseError):
      replica_conn._execute("select 1 from vt_insert_test", {})
    utils.wait_procs([self.replica_tablet.start_mysql(),])
    self.replica_tablet.kill_vttablet()
    self.replica_tablet.start_vttablet()
    replica_conn._execute("select 1 from vt_insert_test", {})

  def test_retry_txn_pool_full(self):
    master_conn = get_connection(db_type='master')
    master_conn._execute("set vt_transaction_cap=1", {})
    master_conn.begin()
    with self.assertRaises(dbexceptions.OperationalError):
      master_conn2 = get_connection(db_type='master')
      master_conn2.begin()
    master_conn.commit()
    master_conn._execute("set vt_transaction_cap=20", {})
    master_conn.begin()
    master_conn._execute("delete from vt_insert_test", {})
    master_conn.commit()

class TestAuthentication(unittest.TestCase):

  def setUp(self):
    self.shard_index = 0
    self.replica_tablet = shard_0_replica
    self.replica_tablet.kill_vttablet()
    self.replica_tablet.start_vttablet(auth=True)
    credentials_file_name = os.path.join(environment.vttop, 'test', 'test_data',
                                         'authcredentials_test.json')
    credentials_file = open(credentials_file_name, 'r')
    credentials = json.load(credentials_file)
    self.user = str(credentials.keys()[0])
    self.password = str(credentials[self.user][0])
    self.secondary_password = str(credentials[self.user][1])

  def test_correct_credentials(self):
    try:
      replica_conn = get_connection(db_type='replica', shard_index = self.shard_index, user=self.user,
                                    password=self.password)
      replica_conn.connect()
    finally:
      replica_conn.close()

  def test_secondary_credentials(self):
    try:
      replica_conn = get_connection(db_type='replica', shard_index = self.shard_index, user=self.user,
                                            password=self.secondary_password)
      replica_conn.connect()
    finally:
      replica_conn.close()

  def test_incorrect_user(self):
    with self.assertRaises(dbexceptions.OperationalError):
      replica_conn = get_connection(db_type='replica', shard_index = self.shard_index, user="romek", password="ma raka")
      replica_conn.connect()

  def test_incorrect_credentials(self):
    with self.assertRaises(dbexceptions.OperationalError):
      replica_conn = get_connection(db_type='replica', shard_index = self.shard_index, user=self.user, password="ma raka")
      replica_conn.connect()

  def test_challenge_is_used(self):
    replica_conn = get_connection(db_type='replica', shard_index = self.shard_index, user=self.user,
                                          password=self.password)
    replica_conn.connect()
    challenge = ""
    proof =  "%s %s" %(self.user, hmac.HMAC(self.password,
                                            challenge).hexdigest())
    self.assertRaises(gorpc.AppError, replica_conn.conn.client.call,
                      'AuthenticatorCRAMMD5.Authenticate', {"Proof": proof})

  def test_only_few_requests_are_allowed(self):
    replica_conn = get_connection(db_type='replica', shard_index = self.shard_index, user=self.user,
                                          password=self.password)
    replica_conn.connect()
    for i in range(4):
      try:
        replica_conn.conn.client.call('AuthenticatorCRAMMD5.GetNewChallenge',
                                      "")
      except gorpc.GoRpcError:
        break
    else:
      self.fail("Too many requests were allowed (%s)." % (i + 1))


if __name__ == '__main__':
  utils.main()
