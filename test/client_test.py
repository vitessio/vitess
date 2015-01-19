"""Test environment for client library tests.

This module has functions for creating keyspaces, tablets for the client
library test.
"""
#!/usr/bin/env python
# coding: utf-8

import hashlib
import logging
import struct
import threading
import time
import traceback
import unittest

import environment
import tablet
import utils
from clientlib_tests import topo_schema
from clientlib_tests import db_class_unsharded
from clientlib_tests import db_class_sharded
from clientlib_tests import db_class_lookup

from vtdb import database_context
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import keyspace
from vtdb import dbexceptions
from vtdb import shard_constants
from vtdb import topology
from vtdb import vtdb_logger
from vtdb import vtgatev2
from vtdb import vtgate_cursor
from zk import zkocc

conn_class = vtgatev2
__tablets = None

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

pack_kid = struct.Struct('!Q').pack

def setUpModule():
  global vtgate_server, vtgate_port
  logging.debug("in setUpModule")
  try:
    environment.topo_server().setup()
    setup_topology()

    # start mysql instance external to the test
    global __tablets
    setup_procs = []
    for tablet in __tablets:
      setup_procs.append(tablet.init_mysql())
    utils.wait_procs(setup_procs)
    create_db()
    start_tablets()
    vtgate_server, vtgate_port = utils.vtgate_start()
    # FIXME(shrutip): this should be removed once vtgate_cursor's
    # dependency on topology goes away.
    vtgate_client = zkocc.ZkOccConnection("localhost:%u" % vtgate_port,
                                          "test_nj", 30.0)
    topology.read_topology(vtgate_client)
  except:
    tearDownModule()
    raise

def tearDownModule():
  global vtgate_server
  global __tablets
  logging.debug("in tearDownModule")
  if utils.options.skip_teardown:
    return
  logging.debug("Tearing down the servers and setup")
  utils.vtgate_kill(vtgate_server)
  if __tablets is not None:
    tablet.kill_tablets(__tablets)
    teardown_procs = []
    for t in __tablets:
      teardown_procs.append(t.teardown_mysql())
    utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()

  utils.kill_sub_processes()
  utils.remove_tmp_files()

  if __tablets is not None:
    for t in __tablets:
      t.remove_tree()

def setup_topology():
  global __tablets
  if __tablets is None:
    __tablets = []

  keyspaces = topo_schema.keyspaces
  for ks in keyspaces:
    ks_name = ks[0]
    ks_type = ks[1]
    utils.run_vtctl(['CreateKeyspace', ks_name])
    if ks_type == shard_constants.UNSHARDED:
      shard_master = tablet.Tablet()
      shard_replica = tablet.Tablet()
      shard_master.init_tablet('master', keyspace=ks_name, shard='0')
      __tablets.append(shard_master)
      shard_replica.init_tablet('replica', keyspace=ks_name, shard='0')
      __tablets.append(shard_replica)
    elif ks_type == shard_constants.RANGE_SHARDED:
      utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', ks_name,
                       'keyspace_id', 'uint64'])
      for shard_name in shard_names:
        shard_master = tablet.Tablet()
        shard_replica = tablet.Tablet()
        shard_master.init_tablet('master', keyspace=ks_name, shard=shard_name)
        __tablets.append(shard_master)
        shard_replica.init_tablet('replica', keyspace=ks_name, shard=shard_name)
        __tablets.append(shard_replica)
    utils.run_vtctl(['RebuildKeyspaceGraph', ks_name], auto_log=True)


def create_db():
  global __tablets
  for t in __tablets:
    t.create_db(t.dbname)
    ks_name = t.keyspace
    for table_tuple in topo_schema.keyspace_table_map[ks_name]:
      t.mquery(t.dbname, table_tuple[1])

def start_tablets():
  global __tablets
  # start tablets
  for t in __tablets:
    t.start_vttablet(wait_for_state=None)

  # wait for them to come in serving state
  for t in __tablets:
    t.wait_for_vttablet_state('SERVING')

  # ReparentShard for master tablets
  for t in __tablets:
    if t.tablet_type == 'master':
      utils.run_vtctl(['ReparentShard', '-force', t.keyspace+'/'+t.shard,
                       t.tablet_alias], auto_log=True)

  for ks in topo_schema.keyspaces:
    ks_name = ks[0]
    ks_type = ks[1]
    utils.run_vtctl(['RebuildKeyspaceGraph', ks_name],
                     auto_log=True)
    if ks_type == shard_constants.RANGE_SHARDED:
      utils.check_srv_keyspace('test_nj', ks_name,
                               'Partitions(master): -80 80-\n' +
                               'Partitions(replica): -80 80-\n' +
                               'TabletTypes: master,replica')


def get_connection(user=None, password=None):
  global vtgate_port
  timeout = 10.0
  conn = None
  vtgate_addrs = {"_vt": ["localhost:%s" % (vtgate_port),]}
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


def _delete_all(keyspace, shard_name, table_name):
  vtgate_conn = get_connection()
  # This write is to set up the test with fresh insert
  # and hence performing it directly on the connection.
  vtgate_conn.begin()
  vtgate_conn._execute("delete from %s" % table_name, {},
                       keyspace, 'master',
                       keyranges=[get_keyrange(shard_name)])
  vtgate_conn.commit()


def restart_vtgate(extra_args={}):
  global vtgate_server, vtgate_port
  utils.vtgate_kill(vtgate_server)
  vtgate_server, vtgate_port = utils.vtgate_start(vtgate_port, extra_args=extra_args)

def populate_table():
  keyspace = "KS_UNSHARDED"
  _delete_all(keyspace, keyrange_constants.SHARD_ZERO, 'vt_unsharded')
  vtgate_conn = get_connection()
  cursor = vtgate_conn.cursor(keyspace, 'master', keyranges=[get_keyrange(keyrange_constants.SHARD_ZERO),],writable=True)
  cursor.begin()
  for x in xrange(10):
    cursor.execute('insert into vt_unsharded (id, msg) values (%s, %s)' % (str(x), 'msg'), {})
  cursor.commit()

class TestUnshardedTable(unittest.TestCase):

  def setUp(self):
    self.vtgate_addrs = {"_vt": ["localhost:%s" % (vtgate_port),]}
    self.dc = database_context.DatabaseContext(self.vtgate_addrs)
    with database_context.WriteTransaction(self.dc) as context:
      for x in xrange(10):
        db_class_unsharded.VtUnsharded.insert(context.get_cursor(),
                                              id=x, msg=str(x))

  def tearDown(self):
    _delete_all("KS_UNSHARDED", "0", 'vt_unsharded')

  def test_read(self):
    with database_context.ReadFromMaster(self.dc) as context:
      rows = db_class_unsharded.VtUnsharded.select_by_id(
          context.get_cursor(), 2)
      self.assertEqual(len(rows), 1, "wrong number of rows fetched")
      self.assertEqual(rows[0].id, 2, "wrong row fetched")

  def test_update_and_read(self):
    where_column_value_pairs = [('id', 2)]
    with database_context.WriteTransaction(self.dc) as context:
      db_class_unsharded.VtUnsharded.update_columns(context.get_cursor(),
                                                    where_column_value_pairs,
                                                    msg="test update")

    with database_context.ReadFromMaster(self.dc) as context:
      rows = db_class_unsharded.VtUnsharded.select_by_id(context.get_cursor(), 2)
      self.assertEqual(len(rows), 1, "wrong number of rows fetched")
      self.assertEqual(rows[0].msg, "test update", "wrong row fetched")

  def test_delete_and_read(self):
    where_column_value_pairs = [('id', 2)]
    with database_context.WriteTransaction(self.dc) as context:
      db_class_unsharded.VtUnsharded.delete_by_columns(context.get_cursor(),
                                                    where_column_value_pairs)

    with database_context.ReadFromMaster(self.dc) as context:
      rows = db_class_unsharded.VtUnsharded.select_by_id(context.get_cursor(), 2)
      self.assertEqual(len(rows), 0, "wrong number of rows fetched")


class TestRangeSharded(unittest.TestCase):
  def populate_tables(self):
    # vt_user
    user_id_list = []
    # This should create the lookup entries and sharding key.
    with database_context.WriteTransaction(self.dc) as context:
      for x in xrange(20):
        # vt_user - EntityRangeSharded
        user_id = db_class_sharded.VtUser.insert(context.get_cursor(),
                                       username="user%s" % x, msg=str(x))
        user_id_list.append(user_id)

        # vt_user_email - RangeSharded
        email = 'user%s@google.com' % x
        m = hashlib.md5()
        m.update(email)
        email_hash = m.digest()
        entity_id_map={'user_id':user_id}
        db_class_sharded.VtUserEmail.insert(
            context.get_cursor(entity_id_map=entity_id_map),
            user_id=user_id, email=email,
            email_hash=email_hash)
    # vt_song
    # vt_song_detail
    return user_id_list

  def setUp(self):
    self.vtgate_addrs = {"_vt": ["localhost:%s" % (vtgate_port),]}
    self.dc = database_context.DatabaseContext(self.vtgate_addrs)
    self.user_id_list = self.populate_tables()

  def tearDown(self):
    with database_context.WriteTransaction(self.dc) as context:
      for uid in self.user_id_list:
        try:
          db_class_sharded.VtUser.delete_by_columns(context.get_cursor(entity_id_map={'id':uid}),
                                                    [('id', uid),])
          db_class_sharded.VtUserEmail.delete_by_columns(context.get_cursor(entity_id_map={'user_id':uid}),
                                                         [('user_id', uid),])
        except dbexceptions.DatabaseError as e:
          if str(e) == "DB Row not found":
            pass

  def test_sharding_key_read(self):
    with database_context.ReadFromMaster(self.dc) as context:
      where_column_value_pairs = [('id', self.user_id_list[0]),]
      rows = db_class_sharded.VtUser.select_by_columns(
          context.get_cursor(entity_id_map={'id':self.user_id_list[0]}),
          where_column_value_pairs)
      self.assertEqual(len(rows), 1, "wrong number of rows fetched")

      where_column_value_pairs = [('user_id', self.user_id_list[0]),]
      rows = db_class_sharded.VtUserEmail.select_by_columns(
          context.get_cursor(entity_id_map={'user_id':self.user_id_list[0]}),
          where_column_value_pairs)
      self.assertEqual(len(rows), 1, "wrong number of rows fetched")

  def test_entity_id_read(self):
    with database_context.ReadFromMaster(self.dc) as context:
      entity_id_map = {'username': 'user0'}
      rows = db_class_sharded.VtUser.select_by_columns(
          context.get_cursor(entity_id_map=entity_id_map),
          [('id', self.user_id_list[0]),])
      self.assertEqual(len(rows), 1, "wrong number of rows fetched")

  def test_in_clause_read(self):
    with database_context.ReadFromMaster(self.dc) as context:
      user_id_list = [self.user_id_list[0], self.user_id_list[1]]

      where_column_value_pairs = (('id', user_id_list),)
      entity_id_map = dict(where_column_value_pairs)
      rows = db_class_sharded.VtUser.select_by_ids(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
      self.assertEqual(len(rows), 2, "wrong number of rows fetched")
      self.assertEqual(user_id_list, [row.id for row in rows], "wrong rows fetched")

      username_list = [row.username for row in rows]
      where_column_value_pairs = (('username', username_list),)
      entity_id_map = dict(where_column_value_pairs)
      rows = db_class_sharded.VtUser.select_by_ids(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
      self.assertEqual(len(rows), 2, "wrong number of rows fetched")
      self.assertEqual(username_list, [row.username for row in rows], "wrong rows fetched")

      where_column_value_pairs = (('user_id', user_id_list),)
      entity_id_map = dict(where_column_value_pairs)
      rows = db_class_sharded.VtUserEmail.select_by_ids(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
      self.assertEqual(len(rows), 2, "wrong number of rows fetched")
      self.assertEqual(user_id_list, [row.user_id for row in rows], "wrong rows fetched")

  def test_keyrange_read(self):
    where_column_value_pairs = []
    with database_context.ReadFromMaster(self.dc) as context:
      rows1 = db_class_sharded.VtUser.select_by_columns(
          context.get_cursor(keyrange='-80'), where_column_value_pairs)
      rows2 = db_class_sharded.VtUser.select_by_columns(
          context.get_cursor(keyrange='80-'), where_column_value_pairs)
      self.assertEqual(len(rows1) + len(rows2), len(self.user_id_list), "wrong number of rows fetched")

  def update_columns(self):
    with database_context.WriteTransaction(self.dc) as context:
      user_id = self.user_id_list[1]
      where_column_value_pairs = [('id', user_id),]
      entity_id_map = {'id': user_id}
      new_username = 'new_user%s' % user_id
      db_class_sharded.VtUser.update_columns(context.get_cursor(entity_id_map=entity_id_map),
                                             where_column_value_pairs,
                                             username=new_username)
      # verify the updated value.
      where_column_value_pairs = [('id', user_id),]
      rows = db_class_sharded.VtUser.select_by_columns(
          context.get_cursor(entity_id_map={'id': user_id}),
          where_column_value_pairs)
      self.assertEqual(len(rows), 1, "wrong number of rows fetched")
      self.assertEqual(new_username, rows[0].username)

      where_column_value_pairs = [('user_id', user_id),]
      entity_id_map = {'user_id': user_id}
      new_email = 'new_user%s@google.com' % user_id
      m = hashlib.md5()
      m.update(new_email)
      email_hash = m.digest()
      db_class_sharded.VtUserEmail.update_columns(context.get_cursor(entity_id_map={'user_id':user_id}),
                                                  where_column_value_pairs,
                                                  email=new_email,
                                                  email_hash=email_hash)

    # verify the updated value.
    with database_context.ReadFromMaster(self.dc) as context:
      where_column_value_pairs = [('user_id', user_id),]
      entity_id_map = dict(where_column_value_pairs)
      rows = db_class_sharded.VtUserEmail.select_by_ids(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
      self.assertEqual(len(rows), 1, "wrong number of rows fetched")
      self.assertEqual(new_email, rows[0].email)

  def delete_columns(self):
    user_id = self.user_id_list[-1]
    with database_context.WriteTransaction(self.dc) as context:
      where_column_value_pairs = [('id', user_id),]
      entity_id_map = {'id': user_id}
      db_class_sharded.VtUser.delete_by_columns(context.get_cursor(entity_id_map=entity_id_map),
                                             where_column_value_pairs)

      where_column_value_pairs = [('user_id', user_id),]
      entity_id_map = {'user_id': user_id}
      db_class_sharded.VtUserEmail.delete_by_columns(context.get_cursor(entity_id_map=entity_id_map),
                                                  where_column_value_pairs)

    with database_context.ReadFromMaster(self.dc) as context:
      rows = db_class_sharded.VtUser.select_by_columns(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
      self.assertEqual(len(rows), 0, "wrong number of rows fetched")

      rows = db_class_sharded.VtUserEmail.select_by_ids(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
      self.assertEqual(len(rows), 0, "wrong number of rows fetched")
    self.user_id_list = self.user_id_list[:-1]


if __name__ == '__main__':
  utils.main()
