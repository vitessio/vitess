#!/usr/bin/env python
#
# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import json

from vtdb import vtgate_client

import environment
import tablet
import utils

SHARDED_KEYSPACE = 'test_keyspace_sharded'
UNSHARDED_KEYSPACE = 'test_keyspace_unsharded'

# shards for SHARDED_KEYSPACE
# range '' - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
# range 80 - ''
shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()

# shard for UNSHARDED_KEYSPACE
unsharded_master = tablet.Tablet()
unsharded_replica = tablet.Tablet()

shard_names = ['-80', '80-']
shard_kid_map = {
    '-80': [527875958493693904, 626750931627689502,
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
    environment.topo_server().setup()

    setup_procs = [
        shard_0_master.init_mysql(),
        shard_0_replica.init_mysql(),
        shard_1_master.init_mysql(),
        shard_1_replica.init_mysql(),
        unsharded_master.init_mysql(),
        unsharded_replica.init_mysql(),
        ]
    utils.wait_procs(setup_procs)
    setup_tablets()
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  tablet.kill_tablets([shard_0_master, shard_0_replica,
                       shard_1_master, shard_1_replica])
  teardown_procs = [
      shard_0_master.teardown_mysql(),
      shard_0_replica.teardown_mysql(),
      shard_1_master.teardown_mysql(),
      shard_1_replica.teardown_mysql(),
      unsharded_master.teardown_mysql(),
      unsharded_replica.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica.remove_tree()
  shard_1_master.remove_tree()
  shard_1_replica.remove_tree()
  unsharded_master.remove_tree()
  unsharded_replica.remove_tree()


def setup_tablets():
  setup_sharded_keyspace()
  setup_unsharded_keyspace()
  utils.VtGate().start(tablets=[
      shard_0_master, shard_0_replica,
      shard_1_master, shard_1_replica,
      unsharded_master, unsharded_replica,
      ])
  utils.vtgate.wait_for_endpoints(
      '%s.%s.master' % (SHARDED_KEYSPACE, '80-'),
      1)
  utils.vtgate.wait_for_endpoints(
      '%s.%s.replica' % (SHARDED_KEYSPACE, '80-'),
      1)
  utils.vtgate.wait_for_endpoints(
      '%s.%s.master' % (SHARDED_KEYSPACE, '-80'),
      1)
  utils.vtgate.wait_for_endpoints(
      '%s.%s.replica' % (SHARDED_KEYSPACE, '-80'),
      1)
  utils.vtgate.wait_for_endpoints(
      '%s.%s.master' % (UNSHARDED_KEYSPACE, '0'),
      1)
  utils.vtgate.wait_for_endpoints(
      '%s.%s.replica' % (UNSHARDED_KEYSPACE, '0'),
      1)


def setup_sharded_keyspace():
  utils.run_vtctl(['CreateKeyspace', SHARDED_KEYSPACE])
  utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', SHARDED_KEYSPACE,
                   'keyspace_id', 'uint64'])

  shard_0_master.init_tablet(
      'replica',
      keyspace=SHARDED_KEYSPACE,
      shard='-80',
      tablet_index=0)
  shard_0_replica.init_tablet(
      'replica',
      keyspace=SHARDED_KEYSPACE,
      shard='-80',
      tablet_index=1)
  shard_1_master.init_tablet(
      'replica',
      keyspace=SHARDED_KEYSPACE,
      shard='80-',
      tablet_index=0)
  shard_1_replica.init_tablet(
      'replica',
      keyspace=SHARDED_KEYSPACE,
      shard='80-',
      tablet_index=1)

  for t in [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica]:
    t.create_db('vt_test_keyspace_sharded')
    t.mquery(shard_0_master.dbname, create_vt_insert_test)
    t.start_vttablet(wait_for_state=None)

  for t in [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica]:
    t.wait_for_vttablet_state('NOT_SERVING')

  utils.run_vtctl(['InitShardMaster', '-force', '%s/-80' % SHARDED_KEYSPACE,
                   shard_0_master.tablet_alias], auto_log=True)
  utils.run_vtctl(['InitShardMaster', '-force', '%s/80-' % SHARDED_KEYSPACE,
                   shard_1_master.tablet_alias], auto_log=True)

  for t in [shard_0_replica, shard_1_replica]:
    utils.wait_for_tablet_type(t.tablet_alias, 'replica')
  for t in [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica]:
    t.wait_for_vttablet_state('SERVING')

  # rebuild to be sure we have the latest data
  utils.run_vtctl(
      ['RebuildKeyspaceGraph', SHARDED_KEYSPACE], auto_log=True)
  utils.check_srv_keyspace('test_nj', SHARDED_KEYSPACE,
                           'Partitions(master): -80 80-\n'
                           'Partitions(rdonly): -80 80-\n'
                           'Partitions(replica): -80 80-\n')


def setup_unsharded_keyspace():
  utils.run_vtctl(['CreateKeyspace', UNSHARDED_KEYSPACE])
  utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', UNSHARDED_KEYSPACE,
                   'keyspace_id', 'uint64'])

  unsharded_master.init_tablet(
      'replica',
      keyspace=UNSHARDED_KEYSPACE,
      shard='0',
      tablet_index=0)
  unsharded_replica.init_tablet(
      'replica',
      keyspace=UNSHARDED_KEYSPACE,
      shard='0',
      tablet_index=1)

  for t in [unsharded_master, unsharded_replica]:
    t.create_db('vt_test_keyspace_unsharded')
    t.mquery(unsharded_master.dbname, create_vt_insert_test)
    t.start_vttablet(wait_for_state=None)

  for t in [unsharded_master, unsharded_replica]:
    t.wait_for_vttablet_state('NOT_SERVING')

  utils.run_vtctl(['InitShardMaster', '-force', '%s/0' % UNSHARDED_KEYSPACE,
                   unsharded_master.tablet_alias], auto_log=True)

  for t in [unsharded_replica]:
    utils.wait_for_tablet_type(t.tablet_alias, 'replica')
  for t in [unsharded_master, unsharded_replica]:
    t.wait_for_vttablet_state('SERVING')

  # rebuild to be sure we have the right version
  utils.run_vtctl(['RebuildKeyspaceGraph', UNSHARDED_KEYSPACE], auto_log=True)
  utils.check_srv_keyspace('test_nj', UNSHARDED_KEYSPACE,
                           'Partitions(master): -\n'
                           'Partitions(rdonly): -\n'
                           'Partitions(replica): -\n')


ALL_DB_TYPES = ['master', 'rdonly', 'replica']


class TestKeyspace(unittest.TestCase):

  def _read_srv_keyspace(self, keyspace_name):
    protocol, addr = utils.vtgate.rpc_endpoint(python=True)
    conn = vtgate_client.connect(protocol, addr, 30.0)
    result = conn.get_srv_keyspace(keyspace_name)
    conn.close()
    return result

  def test_get_keyspace(self):
    ki = utils.run_vtctl_json(['GetKeyspace', UNSHARDED_KEYSPACE])
    self.assertEqual('keyspace_id', ki['sharding_column_name'])
    self.assertEqual(1, ki['sharding_column_type'])

  def test_delete_keyspace(self):
    utils.run_vtctl(['CreateKeyspace', 'test_delete_keyspace'])
    utils.run_vtctl(['CreateShard', 'test_delete_keyspace/0'])
    utils.run_vtctl(
        ['InitTablet', '-keyspace=test_delete_keyspace', '-shard=0',
         'test_nj-0000000100', 'master'])

    # Can't delete keyspace if there are shards present.
    utils.run_vtctl(
        ['DeleteKeyspace', 'test_delete_keyspace'], expect_fail=True)
    # Can't delete shard if there are tablets present.
    utils.run_vtctl(['DeleteShard', '-even_if_serving',
                     'test_delete_keyspace/0'], expect_fail=True)

    # Use recursive DeleteShard to remove tablets.
    utils.run_vtctl(['DeleteShard', '-even_if_serving', '-recursive',
                     'test_delete_keyspace/0'])
    # Now non-recursive DeleteKeyspace should work.
    utils.run_vtctl(['DeleteKeyspace', 'test_delete_keyspace'])

    # Start over and this time use recursive DeleteKeyspace to do everything.
    utils.run_vtctl(['CreateKeyspace', 'test_delete_keyspace'])
    utils.run_vtctl(['CreateShard', 'test_delete_keyspace/0'])
    utils.run_vtctl(
        ['InitTablet', '-port=1234', '-keyspace=test_delete_keyspace',
         '-shard=0', 'test_nj-0000000100', 'master'])

    # Create the serving/replication entries and check that they exist,
    # so we can later check they're deleted.
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_delete_keyspace'])
    utils.run_vtctl(
        ['GetShardReplication', 'test_nj', 'test_delete_keyspace/0'])
    utils.run_vtctl(['GetSrvKeyspace', 'test_nj', 'test_delete_keyspace'])

    # Recursive DeleteKeyspace
    utils.run_vtctl(['DeleteKeyspace', '-recursive', 'test_delete_keyspace'])

    # Check that everything is gone.
    utils.run_vtctl(['GetKeyspace', 'test_delete_keyspace'], expect_fail=True)
    utils.run_vtctl(['GetShard', 'test_delete_keyspace/0'], expect_fail=True)
    utils.run_vtctl(['GetTablet', 'test_nj-0000000100'], expect_fail=True)
    utils.run_vtctl(
        ['GetShardReplication', 'test_nj', 'test_delete_keyspace/0'],
        expect_fail=True)
    utils.run_vtctl(
        ['GetSrvKeyspace', 'test_nj', 'test_delete_keyspace'],
        expect_fail=True)

  def test_remove_keyspace_cell(self):
    utils.run_vtctl(['CreateKeyspace', 'test_delete_keyspace'])
    utils.run_vtctl(['CreateShard', 'test_delete_keyspace/0'])
    utils.run_vtctl(['CreateShard', 'test_delete_keyspace/1'])
    utils.run_vtctl(
        ['InitTablet', '-port=1234', '-keyspace=test_delete_keyspace',
         '-shard=0', 'test_ca-0000000100', 'master'])
    utils.run_vtctl(
        ['InitTablet', '-port=1234', '-keyspace=test_delete_keyspace',
         '-shard=1', 'test_ca-0000000101', 'master'])
    utils.run_vtctl(
        ['InitTablet', '-port=1234', '-keyspace=test_delete_keyspace',
         '-shard=0', 'test_nj-0000000100', 'replica'])
    utils.run_vtctl(
        ['InitTablet', '-port=1234', '-keyspace=test_delete_keyspace',
         '-shard=1', 'test_nj-0000000101', 'replica'])

    # Create the serving/replication entries and check that they exist,
    # so we can later check they're deleted.
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_delete_keyspace'])
    utils.run_vtctl(
        ['GetShardReplication', 'test_nj', 'test_delete_keyspace/0'])
    utils.run_vtctl(
        ['GetShardReplication', 'test_nj', 'test_delete_keyspace/1'])
    utils.run_vtctl(['GetSrvKeyspace', 'test_nj', 'test_delete_keyspace'])
    utils.run_vtctl(['GetSrvKeyspace', 'test_ca', 'test_delete_keyspace'])

    # Just remove the shard from one cell (including tablets),
    # but leaving the global records and other cells/shards alone.
    utils.run_vtctl(
        ['RemoveShardCell', '-recursive', 'test_delete_keyspace/0', 'test_nj'])
    # Check that the shard is gone from test_nj.
    srv_keyspace = utils.run_vtctl_json(['GetSrvKeyspace', 'test_nj', 'test_delete_keyspace'])
    for partition in srv_keyspace['partitions']:
      self.assertEqual(len(partition['shard_references']), 1,
          'RemoveShardCell should have removed one shard from the target cell: ' +
          json.dumps(srv_keyspace))
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_delete_keyspace'])

    utils.run_vtctl(['GetKeyspace', 'test_delete_keyspace'])
    utils.run_vtctl(['GetShard', 'test_delete_keyspace/0'])
    utils.run_vtctl(['GetTablet', 'test_ca-0000000100'])
    utils.run_vtctl(['GetTablet', 'test_nj-0000000100'], expect_fail=True)
    utils.run_vtctl(['GetTablet', 'test_nj-0000000101'])
    utils.run_vtctl(
        ['GetShardReplication', 'test_ca', 'test_delete_keyspace/0'])
    utils.run_vtctl(
        ['GetShardReplication', 'test_nj', 'test_delete_keyspace/0'],
        expect_fail=True)
    utils.run_vtctl(
        ['GetShardReplication', 'test_nj', 'test_delete_keyspace/1'])
    utils.run_vtctl(['GetSrvKeyspace', 'test_nj', 'test_delete_keyspace'])

    # Add it back to do another test.
    utils.run_vtctl(
        ['InitTablet', '-port=1234', '-keyspace=test_delete_keyspace',
         '-shard=0', 'test_nj-0000000100', 'replica'])
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_delete_keyspace'])
    utils.run_vtctl(
        ['GetShardReplication', 'test_nj', 'test_delete_keyspace/0'])

    # Now use RemoveKeyspaceCell to remove all shards.
    utils.run_vtctl(
        ['RemoveKeyspaceCell', '-recursive', 'test_delete_keyspace',
         'test_nj'])
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_delete_keyspace'])

    utils.run_vtctl(
        ['GetShardReplication', 'test_ca', 'test_delete_keyspace/0'])
    utils.run_vtctl(
        ['GetShardReplication', 'test_nj', 'test_delete_keyspace/0'],
        expect_fail=True)
    utils.run_vtctl(
        ['GetShardReplication', 'test_nj', 'test_delete_keyspace/1'],
        expect_fail=True)

    # Clean up.
    utils.run_vtctl(['DeleteKeyspace', '-recursive', 'test_delete_keyspace'])

  def test_shard_count(self):
    sharded_ks = self._read_srv_keyspace(SHARDED_KEYSPACE)
    for db_type in ALL_DB_TYPES:
      self.assertEqual(sharded_ks.get_shard_count(db_type), 2)
    unsharded_ks = self._read_srv_keyspace(UNSHARDED_KEYSPACE)
    for db_type in ALL_DB_TYPES:
      self.assertEqual(unsharded_ks.get_shard_count(db_type), 1)

  def test_shard_names(self):
    sharded_ks = self._read_srv_keyspace(SHARDED_KEYSPACE)
    for db_type in ALL_DB_TYPES:
      self.assertEqual(sharded_ks.get_shard_names(db_type), ['-80', '80-'])
    unsharded_ks = self._read_srv_keyspace(UNSHARDED_KEYSPACE)
    for db_type in ALL_DB_TYPES:
      self.assertEqual(unsharded_ks.get_shard_names(db_type), ['0'])

  def test_keyspace_id_to_shard_name(self):
    # test all keyspace_id in a sharded keyspace go to the right shard
    sharded_ks = self._read_srv_keyspace(SHARDED_KEYSPACE)
    for sn in shard_names:
      for keyspace_id in shard_kid_map[sn]:
        self.assertEqual(
            sharded_ks.keyspace_id_to_shard_name_for_db_type(keyspace_id,
                                                             'master'), sn)

    # take all keyspace_ids, make sure for unsharded they stay on'0'
    unsharded_ks = self._read_srv_keyspace(UNSHARDED_KEYSPACE)
    for sn in shard_names:
      for keyspace_id in shard_kid_map[sn]:
        self.assertEqual(
            unsharded_ks.keyspace_id_to_shard_name_for_db_type(
                keyspace_id, 'master'),
            '0')

  def test_get_srv_keyspace_names(self):
    stdout, _ = utils.run_vtctl(['GetSrvKeyspaceNames', 'test_nj'],
                                trap_output=True)
    self.assertEqual(
        set(stdout.splitlines()), {SHARDED_KEYSPACE, UNSHARDED_KEYSPACE})


if __name__ == '__main__':
  utils.main()
