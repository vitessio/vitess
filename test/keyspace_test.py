#!/usr/bin/python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import base64
import logging
import threading
import struct
import time
import unittest

from vtdb import keyrange_constants
from vtdb import keyspace

import environment
import utils
import tablet

from zk import zkocc

SHARDED_KEYSPACE = "TEST_KEYSPACE_SHARDED"
UNSHARDED_KEYSPACE = "TEST_KEYSPACE_UNSHARDED"

# shards for SHARDED_KEYSPACE
# range "" - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
shard_0_rdonly = tablet.Tablet()
# range 80 - ""
shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()
shard_1_rdonly = tablet.Tablet()

# shard for UNSHARDED_KEYSPACE
unsharded_master = tablet.Tablet()
unsharded_replica = tablet.Tablet()
unsharded_rdonly = tablet.Tablet()

vtgate_server = None
vtgate_port = None
vtgate_secure_port = None

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


def setUpModule():
  try:
    environment.topo_server_setup()

    setup_procs = [
        shard_0_master.init_mysql(),
        shard_0_replica.init_mysql(),
        shard_0_rdonly.init_mysql(),
        shard_1_master.init_mysql(),
        shard_1_replica.init_mysql(),
        shard_1_rdonly.init_mysql(),
        unsharded_master.init_mysql(),
        unsharded_replica.init_mysql(),
        unsharded_rdonly.init_mysql(),
        ]
    utils.wait_procs(setup_procs)
    setup_tablets()
  except:
    tearDownModule()
    raise


def tearDownModule():
  if utils.options.skip_teardown:
    return

  global vtgate_server
  utils.vtgate_kill(vtgate_server)
  tablet.kill_tablets([shard_0_master, shard_0_replica, shard_0_rdonly,
                      shard_1_master, shard_1_replica, shard_1_rdonly])
  teardown_procs = [
      shard_0_master.teardown_mysql(),
      shard_0_replica.teardown_mysql(),
      shard_0_rdonly.teardown_mysql(),
      shard_1_master.teardown_mysql(),
      shard_1_replica.teardown_mysql(),
      shard_1_rdonly.teardown_mysql(),
      unsharded_master.teardown_mysql(),
      unsharded_replica.teardown_mysql(),
      unsharded_rdonly.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica.remove_tree()
  shard_0_rdonly.remove_tree()
  shard_1_master.remove_tree()
  shard_1_replica.remove_tree()
  shard_1_rdonly.remove_tree()
  shard_1_rdonly.remove_tree()
  shard_1_rdonly.remove_tree()
  unsharded_master.remove_tree()
  unsharded_replica.remove_tree()
  unsharded_rdonly.remove_tree()

def setup_tablets():
  global vtgate_server
  global vtgate_port
  global vtgate_secure_port

  setup_sharded_keyspace()
  setup_unsharded_keyspace()
  vtgate_server, vtgate_port, vtgate_secure_port = utils.vtgate_start()


def setup_sharded_keyspace():
  utils.run_vtctl(['CreateKeyspace', SHARDED_KEYSPACE])
  utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', SHARDED_KEYSPACE,
                   'keyspace_id', 'uint64'])
  shard_0_master.init_tablet('master', keyspace=SHARDED_KEYSPACE, shard='-80')
  shard_0_replica.init_tablet('replica', keyspace=SHARDED_KEYSPACE, shard='-80')
  shard_0_rdonly.init_tablet('rdonly', keyspace=SHARDED_KEYSPACE, shard='-80')
  shard_1_master.init_tablet('master', keyspace=SHARDED_KEYSPACE, shard='80-')
  shard_1_replica.init_tablet('replica', keyspace=SHARDED_KEYSPACE,  shard='80-')
  shard_1_rdonly.init_tablet('rdonly', keyspace=SHARDED_KEYSPACE,  shard='80-')

  utils.run_vtctl(['RebuildKeyspaceGraph', SHARDED_KEYSPACE,], auto_log=True)

  for t in [shard_0_master, shard_0_replica, shard_0_rdonly, shard_1_master, shard_1_replica, shard_1_rdonly]:
    t.create_db('vt_test_keyspace_sharded')
    t.mquery(shard_0_master.dbname, create_vt_insert_test)
    t.start_vttablet(wait_for_state=None)

  for t in [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica]:
    t.wait_for_vttablet_state('SERVING')

  utils.run_vtctl(['ReparentShard', '-force', '%s/-80' % SHARDED_KEYSPACE,
                   shard_0_master.tablet_alias], auto_log=True)
  utils.run_vtctl(['ReparentShard', '-force', '%s/80-' % SHARDED_KEYSPACE,
                   shard_1_master.tablet_alias], auto_log=True)

  utils.run_vtctl(['RebuildKeyspaceGraph', SHARDED_KEYSPACE],
                   auto_log=True)

  utils.check_srv_keyspace('test_nj', SHARDED_KEYSPACE,
                           'Partitions(master): -80 80-\n' +
                           'Partitions(rdonly): -80 80-\n' +
                           'Partitions(replica): -80 80-\n' +
                           'TabletTypes: master,rdonly,replica')


def setup_unsharded_keyspace():
  utils.run_vtctl(['CreateKeyspace', UNSHARDED_KEYSPACE])
  utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', UNSHARDED_KEYSPACE,
                   'keyspace_id', 'uint64'])
  unsharded_master.init_tablet('master', keyspace=UNSHARDED_KEYSPACE, shard='0')
  unsharded_replica.init_tablet('replica', keyspace=UNSHARDED_KEYSPACE, shard='0')
  unsharded_rdonly.init_tablet('rdonly', keyspace=UNSHARDED_KEYSPACE, shard='0')

  utils.run_vtctl(['RebuildKeyspaceGraph', UNSHARDED_KEYSPACE,], auto_log=True)

  for t in [unsharded_master, unsharded_replica, unsharded_rdonly]:
    t.create_db('vt_test_keyspace_unsharded')
    t.mquery(unsharded_master.dbname, create_vt_insert_test)
    t.start_vttablet(wait_for_state=None)

  for t in [unsharded_master, unsharded_replica, unsharded_rdonly]:
    t.wait_for_vttablet_state('SERVING')

  utils.run_vtctl(['ReparentShard', '-force', '%s/0' % UNSHARDED_KEYSPACE,
                   unsharded_master.tablet_alias], auto_log=True)

  utils.run_vtctl(['RebuildKeyspaceGraph', UNSHARDED_KEYSPACE],
                   auto_log=True)

  utils.check_srv_keyspace('test_nj', UNSHARDED_KEYSPACE,
                           'Partitions(master): -\n' +
                           'Partitions(rdonly): -\n' +
                           'Partitions(replica): -\n' +
                           'TabletTypes: master,rdonly,replica')


ALL_DB_TYPES = ['master', 'replica', 'rdonly']

class TestKeyspace(unittest.TestCase):
  def _read_keyspace(self, keyspace_name):
    global vtgate_port
    vtgate_client = zkocc.ZkOccConnection("localhost:%u" % vtgate_port,
                                        "test_nj", 30.0)
    return keyspace.read_keyspace(vtgate_client, keyspace_name)

  def test_shard_count(self):
    sharded_ks = self._read_keyspace(SHARDED_KEYSPACE)
    self.assertEqual(sharded_ks.shard_count, 2)
    for db_type in ALL_DB_TYPES:
      self.assertEqual(sharded_ks.get_shard_count(db_type), 2)
    unsharded_ks = self._read_keyspace(UNSHARDED_KEYSPACE)
    self.assertEqual(unsharded_ks.shard_count, 1)
    for db_type in ALL_DB_TYPES:
      self.assertEqual(unsharded_ks.get_shard_count(db_type), 1)

  def test_shard_names(self):
    sharded_ks = self._read_keyspace(SHARDED_KEYSPACE)
    self.assertEqual(sharded_ks.shard_names, ['-80', '80-'])
    for db_type in ALL_DB_TYPES:
      self.assertEqual(sharded_ks.get_shard_names(db_type), ['-80', '80-'])
    unsharded_ks = self._read_keyspace(UNSHARDED_KEYSPACE)
    self.assertEqual(unsharded_ks.shard_names, ['0'])
    for db_type in ALL_DB_TYPES:
      self.assertEqual(unsharded_ks.get_shard_names(db_type), ['0'])

  def test_shard_max_keys(self):
    sharded_ks = self._read_keyspace(SHARDED_KEYSPACE)
    want = ['80', '']
    for i, smk in enumerate(sharded_ks.shard_max_keys):
      self.assertEqual(smk.encode('hex').upper(), want[i])
    for db_type in ALL_DB_TYPES:
      for i, smk in enumerate(sharded_ks.get_shard_max_keys(db_type)):
        self.assertEqual(smk.encode('hex').upper(), want[i])
    unsharded_ks = self._read_keyspace(UNSHARDED_KEYSPACE)
    self.assertEqual(unsharded_ks.shard_max_keys, None)
    for db_type in ALL_DB_TYPES:
      self.assertEqual(unsharded_ks.get_shard_max_keys(db_type), [''])

  def test_db_types(self):
    sharded_ks = self._read_keyspace(SHARDED_KEYSPACE)
    self.assertEqual(set(sharded_ks.db_types), set(ALL_DB_TYPES))
    unsharded_ks = self._read_keyspace(UNSHARDED_KEYSPACE)
    self.assertEqual(set(unsharded_ks.db_types), set(ALL_DB_TYPES))


  def test_keyspace_id_to_shard_index(self):
    sharded_ks = self._read_keyspace(SHARDED_KEYSPACE)
    for i, sn in enumerate(shard_names):
      for keyspace_id in shard_kid_map[sn]:
        self.assertEqual(sharded_ks.keyspace_id_to_shard_index(keyspace_id), i)
        self.assertEqual(sharded_ks.keyspace_id_to_shard_index_for_db_type(keyspace_id, 'master'), i)

if __name__ == '__main__':
  utils.main()
