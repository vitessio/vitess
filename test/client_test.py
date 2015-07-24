#!/usr/bin/env python
# coding: utf-8
"""Test environment for client library tests.

This module has functions for creating keyspaces, tablets for the client
library test.
"""

import hashlib
import random
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
from vtdb import db_object
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import keyspace
from vtdb import dbexceptions
from vtdb import shard_constants
from vtdb import vtdb_logger
from vtdb import vtgatev2
from vtdb import vtgate_cursor
from zk import zkocc

conn_class = vtgatev2
__tablets = None

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
    utils.VtGate().start()
  except:
    tearDownModule()
    raise

def tearDownModule():
  global __tablets
  if utils.options.skip_teardown:
    return
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

  # InitShardMaster for master tablets
  for t in __tablets:
    if t.tablet_type == 'master':
      utils.run_vtctl(['InitShardMaster', t.keyspace+'/'+t.shard,
                       t.tablet_alias], auto_log=True)

  for ks in topo_schema.keyspaces:
    ks_name = ks[0]
    ks_type = ks[1]
    utils.run_vtctl(['RebuildKeyspaceGraph', ks_name],
                     auto_log=True)
    if ks_type == shard_constants.RANGE_SHARDED:
      utils.check_srv_keyspace('test_nj', ks_name,
                               'Partitions(master): -80 80-\n' +
                               'Partitions(rdonly): -80 80-\n' +
                               'Partitions(replica): -80 80-\n')


def get_connection(user=None, password=None):
  timeout = 10.0
  conn = None
  vtgate_addrs = {"vt": [utils.vtgate.addr(),]}
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
  port = utils.vtgate.port
  utils.vtgate.kill()
  utils.VtGate(port=port).start(extra_args=extra_args)

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
    self.vtgate_addrs = {"vt": [utils.vtgate.addr(),]}
    self.dc = database_context.DatabaseContext(self.vtgate_addrs)
    self.all_ids = []
    with database_context.WriteTransaction(self.dc) as context:
      for x in xrange(20):
        ret_id = db_class_unsharded.VtUnsharded.insert(context.get_cursor(),
                                                       msg="test message")
        self.all_ids.append(ret_id)

  def tearDown(self):
    _delete_all("KS_UNSHARDED", "0", 'vt_unsharded')

  def test_read(self):
    id_val = self.all_ids[0]
    with database_context.ReadFromMaster(self.dc) as context:
      rows = db_class_unsharded.VtUnsharded.select_by_id(
          context.get_cursor(), id_val)
      expected = 1
      self.assertEqual(len(rows), expected, "wrong number of rows fetched %d, expected %d" % (len(rows), expected))
      self.assertEqual(rows[0].id, id_val, "wrong row fetched")

  def test_update_and_read(self):
    id_val = self.all_ids[0]
    where_column_value_pairs = [('id', id_val)]
    with database_context.WriteTransaction(self.dc) as context:
      update_cols = [('msg', "test update"),]
      db_class_unsharded.VtUnsharded.update_columns(context.get_cursor(),
                                                    where_column_value_pairs,
                                                    update_column_value_pairs=update_cols)

    with database_context.ReadFromMaster(self.dc) as context:
      rows = db_class_unsharded.VtUnsharded.select_by_id(context.get_cursor(), id_val)
      self.assertEqual(len(rows), 1, "wrong number of rows fetched")
      self.assertEqual(rows[0].msg, "test update", "wrong row fetched")

  def test_delete_and_read(self):
    id_val = self.all_ids[-1]
    where_column_value_pairs = [('id', id_val)]
    with database_context.WriteTransaction(self.dc) as context:
      db_class_unsharded.VtUnsharded.delete_by_columns(context.get_cursor(),
                                                    where_column_value_pairs)

    with database_context.ReadFromMaster(self.dc) as context:
      rows = db_class_unsharded.VtUnsharded.select_by_id(context.get_cursor(), id_val)
      self.assertEqual(len(rows), 0, "wrong number of rows fetched")
    self.all_ids = self.all_ids[:-1]

  def test_count(self):
    with database_context.ReadFromMaster(self.dc) as context:
      count = db_class_unsharded.VtUnsharded.get_count(
          context.get_cursor(), msg="test message")
      expected = len(self.all_ids)
      self.assertEqual(count, expected, "wrong count fetched; expected %d got %d" % (expected, count))

  def test_min_id(self):
    with database_context.ReadFromMaster(self.dc) as context:
      min_id = db_class_unsharded.VtUnsharded.get_min(
          context.get_cursor())
      expected = min(self.all_ids)
      self.assertEqual(min_id, expected, "wrong min value fetched; expected %d got %d" % (expected, min_id))

  def test_max_id(self):
    with database_context.ReadFromMaster(self.dc) as context:
      max_id = db_class_unsharded.VtUnsharded.get_max(
          context.get_cursor())
      self.all_ids.sort()
      expected = max(self.all_ids)
      self.assertEqual(max_id, expected, "wrong max value fetched; expected %d got %d" % (expected, max_id))


class TestRangeSharded(unittest.TestCase):
  def populate_tables(self):
    self.user_id_list = []
    self.song_id_list = []
    self.user_song_map = {}
    r = random.Random()
    # This should create the lookup entries and sharding key.
    with database_context.WriteTransaction(self.dc) as context:
      for x in xrange(20):
        # vt_user - EntityRangeSharded; creates username:user_id lookup
        user_id = db_class_sharded.VtUser.insert(context.get_cursor(),
                                       username="user%s" % x, msg="test message")
        self.user_id_list.append(user_id)

        # vt_user_email - RangeSharded; references user_id:keyspace_id hash
        email = 'user%s@google.com' % x
        m = hashlib.md5()
        m.update(email)
        email_hash = m.digest()
        entity_id_map={'user_id':user_id}
        db_class_sharded.VtUserEmail.insert(
            context.get_cursor(entity_id_map=entity_id_map),
            user_id=user_id, email=email,
            email_hash=email_hash)

        # vt_song - EntityRangeSharded; creates song_id:user_id lookup
        num_songs_for_user = r.randint(1, 5)
        for i in xrange(num_songs_for_user):
          song_id = db_class_sharded.VtSong.insert(context.get_cursor(),
                                                   user_id=user_id, title="Test Song")
          self.song_id_list.append(song_id)
          self.user_song_map.setdefault(user_id, []).append(song_id)

          # vt_song_detail - RangeSharded; references song_id:user_id lookup
          entity_id_map = {'song_id':song_id}
          db_class_sharded.VtSongDetail.insert(context.get_cursor(entity_id_map=entity_id_map),
                                               song_id=song_id, album_name="Test album",
                                               artist="Test artist")



  def setUp(self):
    self.vtgate_addrs = {"vt": [utils.vtgate.addr(),]}
    self.dc = database_context.DatabaseContext(self.vtgate_addrs)
    self.populate_tables()

  def tearDown(self):
    with database_context.WriteTransaction(self.dc) as context:
      for uid in self.user_id_list:
        try:
          db_class_sharded.VtUser.delete_by_columns(context.get_cursor(entity_id_map={'id':uid}),
                                                    [('id', uid),])
          db_class_sharded.VtUserEmail.delete_by_columns(context.get_cursor(entity_id_map={'user_id':uid}),
                                                         [('user_id', uid),])
          db_class_sharded.VtSong.delete_by_columns(context.get_cursor(entity_id_map={'user_id':uid}),
                                                         [('user_id', uid),])
          song_id_list = self.user_song_map[uid]
          for sid in song_id_list:
            db_class_sharded.VtSongDetail.delete_by_columns(context.get_cursor(entity_id_map={'song_id':sid}),
                                                            [('song_id', sid),])
        except dbexceptions.DatabaseError as e:
          if str(e) == "DB Row not found":
            pass

  def test_sharding_key_read(self):
    user_id = self.user_id_list[0]
    with database_context.ReadFromMaster(self.dc) as context:
      where_column_value_pairs = [('id', user_id),]
      entity_id_map = dict(where_column_value_pairs)
      rows = db_class_sharded.VtUser.select_by_columns(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
      self.assertEqual(len(rows), 1, "wrong number of rows fetched")

      where_column_value_pairs = [('user_id', user_id),]
      entity_id_map = dict(where_column_value_pairs)
      rows = db_class_sharded.VtUserEmail.select_by_columns(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
      self.assertEqual(len(rows), 1, "wrong number of rows fetched")

      where_column_value_pairs = [('user_id', user_id),]
      entity_id_map = dict(where_column_value_pairs)
      rows = db_class_sharded.VtSong.select_by_columns(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
      self.assertEqual(len(rows), len(self.user_song_map[user_id]), "wrong number of rows fetched")

  def test_entity_id_read(self):
    user_id = self.user_id_list[0]
    with database_context.ReadFromMaster(self.dc) as context:
      entity_id_map = {'username': 'user0'}
      rows = db_class_sharded.VtUser.select_by_columns(
          context.get_cursor(entity_id_map=entity_id_map),
          [('id', user_id),])
      self.assertEqual(len(rows), 1, "wrong number of rows fetched")

      where_column_value_pairs = [('id', self.user_song_map[user_id][0]),]
      entity_id_map = dict(where_column_value_pairs)
      rows = db_class_sharded.VtSong.select_by_columns(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
      self.assertEqual(len(rows), 1, "wrong number of rows fetched")

      where_column_value_pairs = [('song_id', self.user_song_map[user_id][0]),]
      entity_id_map = dict(where_column_value_pairs)
      rows = db_class_sharded.VtSongDetail.select_by_columns(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
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
      got = [row.id for row in rows]
      got.sort()
      self.assertEqual(user_id_list, got, "wrong rows fetched; expected %s got %s" % (user_id_list, got))

      username_list = [row.username for row in rows]
      username_list.sort()
      where_column_value_pairs = (('username', username_list),)
      entity_id_map = dict(where_column_value_pairs)
      rows = db_class_sharded.VtUser.select_by_ids(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
      self.assertEqual(len(rows), 2, "wrong number of rows fetched")
      got = [row.username for row in rows]
      got.sort()
      self.assertEqual(username_list, got, "wrong rows fetched; expected %s got %s" % (username_list, got))

      where_column_value_pairs = (('user_id', user_id_list),)
      entity_id_map = dict(where_column_value_pairs)
      rows = db_class_sharded.VtUserEmail.select_by_ids(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
      self.assertEqual(len(rows), 2, "wrong number of rows fetched")
      got = [row.user_id for row in rows]
      got.sort()
      self.assertEqual(user_id_list, got, "wrong rows fetched; expected %s got %s" % (user_id_list, got))

      song_id_list = []
      for user_id in user_id_list:
        song_id_list.extend(self.user_song_map[user_id])
      song_id_list.sort()
      where_column_value_pairs = [('id', song_id_list),]
      entity_id_map = dict(where_column_value_pairs)
      rows = db_class_sharded.VtSong.select_by_columns(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
      got = [row.id for row in rows]
      got.sort()
      self.assertEqual(song_id_list, got, "wrong rows fetched %s got %s" % (song_id_list, got))

      where_column_value_pairs = [('song_id', song_id_list),]
      entity_id_map = dict(where_column_value_pairs)
      rows = db_class_sharded.VtSongDetail.select_by_columns(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
      got = [row.song_id for row in rows]
      got.sort()
      self.assertEqual(song_id_list, got, "wrong rows fetched %s got %s" % (song_id_list, got))


  def test_keyrange_read(self):
    where_column_value_pairs = []
    with database_context.ReadFromMaster(self.dc) as context:
      rows1 = db_class_sharded.VtUser.select_by_columns(
          context.get_cursor(keyrange='-80'), where_column_value_pairs)
      rows2 = db_class_sharded.VtUser.select_by_columns(
          context.get_cursor(keyrange='80-'), where_column_value_pairs)
      fetched_rows = len(rows1) + len(rows2)
      expected = len(self.user_id_list)
      self.assertEqual(fetched_rows, expected, "wrong number of rows fetched expected:%d got:%d" % (expected, fetched_rows))

  def test_scatter_read(self):
    where_column_value_pairs = []
    with database_context.ReadFromMaster(self.dc) as context:
      rows = db_class_sharded.VtUser.select_by_columns(
          context.get_cursor(keyrange=keyrange_constants.NON_PARTIAL_KEYRANGE),
          where_column_value_pairs)
      self.assertEqual(len(rows), len(self.user_id_list), "wrong number of rows fetched, expecting %d got %d" % (len(self.user_id_list), len(rows)))

  def test_streaming_read(self):
    where_column_value_pairs = []
    with database_context.ReadFromMaster(self.dc) as context:
      rows = db_class_sharded.VtUser.select_by_columns_streaming(
          context.get_cursor(keyrange=keyrange_constants.NON_PARTIAL_KEYRANGE),
          where_column_value_pairs)
      got_user_id_list = []
      for r in rows:
        got_user_id_list.append(r.id)
      self.assertEqual(len(got_user_id_list), len(self.user_id_list), "wrong number of rows fetched")

  def update_columns(self):
    with database_context.WriteTransaction(self.dc) as context:
      user_id = self.user_id_list[1]
      where_column_value_pairs = [('id', user_id),]
      entity_id_map = {'id': user_id}
      new_username = 'new_user%s' % user_id
      update_cols = [('username', new_username),]
      db_class_sharded.VtUser.update_columns(context.get_cursor(entity_id_map=entity_id_map),
                                             where_column_value_pairs,
                                             update_column_value_pairs=update_cols)
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
      update_cols = [('email', new_email), ('email_hash', email_hash)]
      db_class_sharded.VtUserEmail.update_columns(context.get_cursor(entity_id_map={'user_id':user_id}),
                                                  where_column_value_pairs,
                                                  update_column_value_pairs=update_cols)

    # verify the updated value.
    with database_context.ReadFromMaster(self.dc) as context:
      where_column_value_pairs = [('user_id', user_id),]
      entity_id_map = dict(where_column_value_pairs)
      rows = db_class_sharded.VtUserEmail.select_by_ids(
          context.get_cursor(entity_id_map=entity_id_map),
          where_column_value_pairs)
      self.assertEqual(len(rows), 1, "wrong number of rows fetched")
      self.assertEqual(new_email, rows[0].email)
    self.user_id_list.sort()

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
    self.user_id_list.sort()

  def test_count(self):
    with database_context.ReadFromMaster(self.dc) as context:
      count = db_class_sharded.VtUser.get_count(
          context.get_cursor(keyrange=keyrange_constants.NON_PARTIAL_KEYRANGE),
          msg="test message")
      expected = len(self.user_id_list)
      self.assertEqual(count, expected, "wrong count fetched; expected %d got %d" % (expected, count))

  def test_min_id(self):
    with database_context.ReadFromMaster(self.dc) as context:
      min_id = db_class_sharded.VtUser.get_min(
          context.get_cursor(keyrange=keyrange_constants.NON_PARTIAL_KEYRANGE))
      self.user_id_list.sort()
      expected = min(self.user_id_list)
      rows1 = db_class_sharded.VtUser.select_by_columns(
          context.get_cursor(keyrange=keyrange_constants.NON_PARTIAL_KEYRANGE), [])
      id_list = [row.id for row in rows1]
      self.assertEqual(min_id, expected, "wrong min value fetched; expected %d got %d" % (expected, min_id))

  def test_max_id(self):
    with database_context.ReadFromMaster(self.dc) as context:
      max_id = db_class_sharded.VtUser.get_max(
          context.get_cursor(keyrange=keyrange_constants.NON_PARTIAL_KEYRANGE))
      expected = max(self.user_id_list)
      self.assertEqual(max_id, expected, "wrong max value fetched; expected %d got %d" % (expected, max_id))



if __name__ == '__main__':
  utils.main()
