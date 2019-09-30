#!/usr/bin/env python

# Copyright 2019 The Vitess Authors
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

import datetime
import json
import logging
import os
import unittest

import MySQLdb

import environment
import tablet
import utils

from vtdb import vtgate_client

# initial shard, covers everything
tablet_master = tablet.Tablet()
tablet_replica1 = tablet.Tablet()
tablet_rdonly = tablet.Tablet()
# to use for recovery keyspace
tablet_replica2 = tablet.Tablet()
tablet_replica3 = tablet.Tablet()

# split shards
# range '' - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
shard_0_rdonly = tablet.Tablet()
# range 80 - ''
shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()
shard_1_rdonly = tablet.Tablet()

all_tablets = [tablet_master, tablet_replica1, tablet_replica2, tablet_replica3, tablet_rdonly,
               shard_0_master, shard_0_replica, shard_0_rdonly, shard_1_master, shard_1_replica, shard_1_rdonly]

def setUpModule():
  try:
    environment.topo_server().setup()
    setup_procs = [t.init_mysql() for t in all_tablets]
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise

def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return
  teardown_procs = [t.teardown_mysql() for t in all_tablets]
  utils.wait_procs(teardown_procs, raise_on_error=False)
  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()
  for t in all_tablets:
    t.remove_tree()

def get_connection(timeout=15.0):
  protocol, endpoint = utils.vtgate.rpc_endpoint(python=True)
  try:
    return vtgate_client.connect(protocol, endpoint, timeout)
  except Exception:
    logging.exception('Connection to vtgate (timeout=%s) failed.', timeout)
    raise

class TestShardedRecovery(unittest.TestCase):

  def setUp(self):
    xtra_args = ['-enable_replication_reporter']
    tablet_master.init_tablet('replica', 'test_keyspace', '0', start=True,
                              supports_backups=True,
                              extra_args=xtra_args)
    tablet_replica1.init_tablet('replica', 'test_keyspace', '0', start=True,
                                supports_backups=True,
                                extra_args=xtra_args)
    tablet_rdonly.init_tablet('rdonly', 'test_keyspace', '0', start=True,
                                supports_backups=True,
                                extra_args=xtra_args)
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     tablet_master.tablet_alias])

  def tearDown(self):
    for t in all_tablets:
      t.kill_vttablet()

    tablet.Tablet.check_vttablet_count()
    environment.topo_server().wipe()
    for t in all_tablets:
      t.reset_replication()
      t.set_semi_sync_enabled(master=False, slave=False)
      t.clean_dbs()

    for shard in ['0', '-80', '80-']:
      for backup in self._list_backups(shard):
        self._remove_backup(backup, shard)

  _create_vt_insert_test = '''create table vt_insert_test (
  id bigint,
  msg varchar(64),
  primary key (id)
  ) Engine=InnoDB'''

  _vschema_json = '''{
    "sharded": true,
    "vindexes": {
      "hash": {
        "type": "hash"
      }
    },
    "tables": {
        "vt_insert_test": {
        "column_vindexes": [
          {
            "column": "id",
            "name": "hash"
          }
        ]
      }
    }
}'''

  def _insert_data(self, t, index):
    """Add a single row with value 'index' to the given tablet."""
    t.mquery(
        'vt_test_keyspace',
        "insert into vt_insert_test (id, msg) values (%d, 'test %s')" %
        (index, index), write=True)

  def _check_data(self, t, count, msg):
    """Check that the specified tablet has the expected number of rows."""
    timeout = 10
    while True:
      try:
        result = t.mquery(
            'vt_test_keyspace', 'select count(*) from vt_insert_test')
        if result[0][0] == count:
          break
      except MySQLdb.DatabaseError:
        # ignore exceptions, we'll just timeout (the tablet creation
        # can take some time to replicate, and we get a 'table vt_insert_test
        # does not exist exception in some rare cases)
        logging.exception('exception waiting for data to replicate')
      timeout = utils.wait_step(msg, timeout)

  def _restore(self, t, keyspace, shard):
    """Erase mysql/tablet dir, then start tablet with restore enabled."""
    self._reset_tablet_dir(t)

    # create a recovery keyspace
    utils.run_vtctl(['CreateKeyspace',
                     '-keyspace_type=SNAPSHOT',
                     '-base_keyspace=test_keyspace',
                     '-snapshot_time',
                     datetime.datetime.utcnow().isoformat("T")+"Z",
                     keyspace])
    
    t.start_vttablet(wait_for_state='SERVING',
                     init_tablet_type='replica',
                     init_keyspace=keyspace,
                     init_shard=shard,
                     supports_backups=True,
                     extra_args=tablet.get_backup_storage_flags())

  def _reset_tablet_dir(self, t):
    """Stop mysql, delete everything including tablet dir, restart mysql."""
    utils.wait_procs([t.teardown_mysql()])
    # Specify ignore_options because we want to delete the tree even
    # if the test's -k / --keep-logs was specified on the command line.
    t.remove_tree(ignore_options=True)
    proc = t.init_mysql()
    utils.wait_procs([proc])

  def _list_backups(self, shard):
    """Get a list of backup names for the test shard."""
    backups, _ = utils.run_vtctl(tablet.get_backup_storage_flags() +
                                 ['ListBackups', 'test_keyspace/%s' % shard],
                                 mode=utils.VTCTL_VTCTL, trap_output=True)
    return backups.splitlines()

  def _remove_backup(self, backup, shard):
    """Remove a named backup from the test shard."""
    utils.run_vtctl(
        tablet.get_backup_storage_flags() +
        ['RemoveBackup', 'test_keyspace/%s' % shard, backup],
        auto_log=True, mode=utils.VTCTL_VTCTL)

  def test_unsharded_recovery_after_sharding(self):
    """Test recovery from backup flow.

    test_recovery will:
    - create a shard with master and replica1 only
    - run InitShardMaster
    - insert some data
    - take a backup
    - insert more data on the master
    - perform a resharding
    - create a recovery keyspace
    - bring up tablet_replica2 in the new keyspace
    - check that new tablet does not have data created after backup
    - check that vtgate queries work correctly

    """

    # insert data on master, wait for replica to get it
    utils.run_vtctl(['ApplySchema',
                     '-sql', self._create_vt_insert_test,
                     'test_keyspace'],
                    auto_log=True)
    self._insert_data(tablet_master, 1)
    self._check_data(tablet_replica1, 1, 'replica1 tablet getting data')
    # insert more data on the master
    self._insert_data(tablet_master, 2)

    # backup the replica
    utils.run_vtctl(['Backup', tablet_replica1.tablet_alias], auto_log=True)

    # check that the backup shows up in the listing
    backups = self._list_backups('0')
    logging.debug('list of backups: %s', backups)
    self.assertEqual(len(backups), 1)
    self.assertTrue(backups[0].endswith(tablet_replica1.tablet_alias))

    # insert more data on the master
    self._insert_data(tablet_master, 3)

    utils.run_vtctl(['ApplyVSchema',
                     '-vschema', self._vschema_json,
                     'test_keyspace'],
                    auto_log=True)

    # create the split shards
    shard_0_master.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='-80',
        tablet_index=0)
    shard_0_replica.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='-80',
        tablet_index=1)
    shard_0_rdonly.init_tablet(
        'rdonly',
        keyspace='test_keyspace',
        shard='-80',
        tablet_index=2)
    shard_1_master.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='80-',
        tablet_index=0)
    shard_1_replica.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='80-',
        tablet_index=1)
    shard_1_rdonly.init_tablet(
        'rdonly',
        keyspace='test_keyspace',
        shard='80-',
        tablet_index=2)

    for t in [shard_0_master, shard_0_replica, shard_0_rdonly,
              shard_1_master, shard_1_replica, shard_1_rdonly]:
      t.start_vttablet(wait_for_state=None,
                       binlog_use_v3_resharding_mode=True)

    for t in [shard_0_master, shard_0_replica, shard_0_rdonly,
              shard_1_master, shard_1_replica, shard_1_rdonly]:
      t.wait_for_vttablet_state('NOT_SERVING')

    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/-80',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/80-',
                     shard_1_master.tablet_alias], auto_log=True)

    for t in [shard_0_replica, shard_1_replica]:
      utils.wait_for_tablet_type(t.tablet_alias, 'replica')

    sharded_tablets = [shard_0_master, shard_0_replica, shard_0_rdonly,
                       shard_1_master, shard_1_replica, shard_1_rdonly]
    for t in sharded_tablets:
      t.wait_for_vttablet_state('SERVING')

    # we need to create the schema, and the worker will do data copying
    for keyspace_shard in ('test_keyspace/-80', 'test_keyspace/80-'):
      utils.run_vtctl(['CopySchemaShard',
                       'test_keyspace/0',
                       keyspace_shard],
                       auto_log=True)
    
    utils.run_vtctl(
        ['SplitClone', 'test_keyspace', '0', '-80,80-'], auto_log=True)

    utils.run_vtctl(
        ['MigrateServedTypes', 'test_keyspace/0', 'rdonly'], auto_log=True)
    utils.run_vtctl(
        ['MigrateServedTypes', 'test_keyspace/0', 'replica'], auto_log=True)
    # then serve master from the split shards
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/0', 'master'],
                    auto_log=True)

    # remove the original tablets in the original shard
    tablet.kill_tablets([tablet_master, tablet_replica1, tablet_rdonly])
    for t in [tablet_replica1, tablet_rdonly]:
      utils.run_vtctl(['DeleteTablet', t.tablet_alias], auto_log=True)
    utils.run_vtctl(['DeleteTablet', '-allow_master',
                     tablet_master.tablet_alias], auto_log=True)

    # rebuild the serving graph, all mentions of the old shards should be gone
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    # delete the original shard
    utils.run_vtctl(['DeleteShard', 'test_keyspace/0'], auto_log=True)

    # now bring up the recovery keyspace and a tablet, letting it restore from backup.
    self._restore(tablet_replica2, 'recovery_keyspace', '0')

    # check the new replica does not have the data
    self._check_data(tablet_replica2, 2, 'replica2 tablet should not have new data')

    # start vtgate
    vtgate = utils.VtGate()
    vtgate.start(tablets=[
      shard_0_master, shard_0_replica, shard_1_master, shard_1_replica, tablet_replica2
      ], tablet_types_to_wait='REPLICA')
    utils.vtgate.wait_for_endpoints('test_keyspace.-80.master', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.80-.replica', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.-80.master', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.80-.replica', 1)
    utils.vtgate.wait_for_endpoints('recovery_keyspace.0.replica', 1)

    # check that vtgate doesn't route queries to new tablet
    vtgate_conn = get_connection()
    cursor = vtgate_conn.cursor(
        tablet_type='replica', keyspace=None, writable=True)

    cursor.execute('select count(*) from vt_insert_test', {})
    result = cursor.fetchall()
    if not result:
      self.fail('Result cannot be null')
    else:
      self.assertEqual(result[0][0], 3)

    # check that new tablet is accessible by using ks.table
    cursor.execute('select count(*) from recovery_keyspace.vt_insert_test', {})
    result = cursor.fetchall()
    if not result:
      self.fail('Result cannot be null')
    else:
      self.assertEqual(result[0][0], 2)

    # check that new tablet is accessible with 'use ks'
    cursor.execute('use recovery_keyspace@replica', {})
    cursor.execute('select count(*) from vt_insert_test', {})
    result = cursor.fetchall()
    if not result:
      self.fail('Result cannot be null')
    else:
      self.assertEqual(result[0][0], 2)

    # TODO check that new tablet is accessible with 'use ks:shard'
    # this currently does not work through the python client, though it works from mysql client
    #cursor.execute('use recovery_keyspace:0@replica', {})
    #cursor.execute('select count(*) from vt_insert_test', {})
    #result = cursor.fetchall()
    #if not result:
      #self.fail('Result cannot be null')
    #else:
      #self.assertEqual(result[0][0], 1)

    vtgate_conn.close()
    tablet_replica2.kill_vttablet()
    vtgate.kill()

  def test_sharded_recovery(self):
    """Test recovery from backup flow.

    test_recovery will:
    - create a shard with master and replica1 only
    - run InitShardMaster
    - insert some data
    - perform a resharding
    - take a backup of both new shards
    - insert more data on the masters of both shards
    - create a recovery keyspace
    - bring up tablet_replica2 and tablet_replica3 in the new keyspace
    - check that new tablets do not have data created after backup
    - check that vtgate queries work correctly

    """

    # insert data on master, wait for replica to get it
    utils.run_vtctl(['ApplySchema',
                     '-sql', self._create_vt_insert_test,
                     'test_keyspace'],
                    auto_log=True)
    self._insert_data(tablet_master, 1)
    self._check_data(tablet_replica1, 1, 'replica1 tablet getting data')
    # insert more data on the master
    self._insert_data(tablet_master, 4)

    utils.run_vtctl(['ApplyVSchema',
                     '-vschema', self._vschema_json,
                     'test_keyspace'],
                    auto_log=True)

    # create the split shards
    shard_0_master.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='-80',
        tablet_index=0)
    shard_0_replica.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='-80',
        tablet_index=1)
    shard_0_rdonly.init_tablet(
        'rdonly',
        keyspace='test_keyspace',
        shard='-80',
        tablet_index=2)
    shard_1_master.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='80-',
        tablet_index=0)
    shard_1_replica.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='80-',
        tablet_index=1)
    shard_1_rdonly.init_tablet(
        'rdonly',
        keyspace='test_keyspace',
        shard='80-',
        tablet_index=2)

    for t in [shard_0_master, shard_0_replica, shard_0_rdonly,
              shard_1_master, shard_1_replica, shard_1_rdonly]:
      t.start_vttablet(wait_for_state=None,
                       binlog_use_v3_resharding_mode=True)

    for t in [shard_0_master, shard_0_replica, shard_0_rdonly,
              shard_1_master, shard_1_replica, shard_1_rdonly]:
      t.wait_for_vttablet_state('NOT_SERVING')

    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/-80',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/80-',
                     shard_1_master.tablet_alias], auto_log=True)

    for t in [shard_0_replica, shard_1_replica]:
      utils.wait_for_tablet_type(t.tablet_alias, 'replica')

    sharded_tablets = [shard_0_master, shard_0_replica, shard_0_rdonly,
                       shard_1_master, shard_1_replica, shard_1_rdonly]
    for t in sharded_tablets:
      t.wait_for_vttablet_state('SERVING')

    # we need to create the schema, and the worker will do data copying
    for keyspace_shard in ('test_keyspace/-80', 'test_keyspace/80-'):
      utils.run_vtctl(['CopySchemaShard',
                       'test_keyspace/0',
                       keyspace_shard],
                       auto_log=True)

    utils.run_vtctl(
        ['SplitClone', 'test_keyspace', '0', '-80,80-'], auto_log=True)

    utils.run_vtctl(
        ['MigrateServedTypes', 'test_keyspace/0', 'rdonly'], auto_log=True)
    utils.run_vtctl(
        ['MigrateServedTypes', 'test_keyspace/0', 'replica'], auto_log=True)
    # then serve master from the split shards
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/0', 'master'],
                    auto_log=True)

    # remove the original tablets in the original shard
    tablet.kill_tablets([tablet_master, tablet_replica1, tablet_rdonly])
    for t in [tablet_replica1, tablet_rdonly]:
      utils.run_vtctl(['DeleteTablet', t.tablet_alias], auto_log=True)
    utils.run_vtctl(['DeleteTablet', '-allow_master',
                     tablet_master.tablet_alias], auto_log=True)

    # rebuild the serving graph, all mentions of the old shards should be gone
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    # delete the original shard
    utils.run_vtctl(['DeleteShard', 'test_keyspace/0'], auto_log=True)

    result = shard_0_master.mquery('vt_test_keyspace', "select count(*) from vt_insert_test")
    shard_0_count = result[0][0]
    logging.debug("Shard -80 has %d rows", shard_0_count)
    shard_0_test_id = 0
    if shard_0_count > 0:
      result = shard_0_master.mquery('vt_test_keyspace', "select id from vt_insert_test")
      shard_0_test_id = result[0][0]

    result = shard_1_master.mquery('vt_test_keyspace', "select count(*) from vt_insert_test")
    shard_1_count = result[0][0]
    logging.debug("Shard 80- has %d rows", shard_1_count)
    shard_1_test_id = 0
    if shard_1_count > 0:
      result = shard_1_master.mquery('vt_test_keyspace', "select id from vt_insert_test")
      shard_1_test_id = result[0][0]

    # backup the new shards
    utils.run_vtctl(['Backup', shard_0_replica.tablet_alias], auto_log=True)
    utils.run_vtctl(['Backup', shard_1_replica.tablet_alias], auto_log=True)

    # check that the backup shows up in the listing
    backups = self._list_backups('-80')
    logging.debug('list of backups: %s', backups)
    self.assertEqual(len(backups), 1)
    self.assertTrue(backups[0].endswith(shard_0_replica.tablet_alias))

    backups = self._list_backups('80-')
    logging.debug('list of backups: %s', backups)
    self.assertEqual(len(backups), 1)
    self.assertTrue(backups[0].endswith(shard_1_replica.tablet_alias))

    # start vtgate
    vtgate = utils.VtGate()
    vtgate.start(tablets=[
      shard_0_master, shard_1_master
      ], tablet_types_to_wait='MASTER')
    utils.vtgate.wait_for_endpoints('test_keyspace.-80.master', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.80-.master', 1)

    vtgate_conn = get_connection()
    cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=None, writable=True)
    # insert more data on the masters
    for i in [2, 3]:
      cursor.execute('insert into vt_insert_test (id, msg) values (:id, :msg)', {'id': i, 'msg': 'test %s' % i})

    vtgate_conn.close()
    vtgate.kill()

    # now bring up the recovery keyspace and 2 tablets, letting it restore from backup.
    self._restore(tablet_replica2, 'recovery_keyspace', '-80')
    self._restore(tablet_replica3, 'recovery_keyspace', '80-')

    # check the new replicas have the correct number of rows
    self._check_data(tablet_replica2, shard_0_count, 'replica2 tablet should not have new data')
    self._check_data(tablet_replica3, shard_1_count, 'replica3 tablet should not have new data')

    # start vtgate
    vtgate = utils.VtGate()
    vtgate.start(tablets=[
      shard_0_master, shard_0_replica, shard_1_master, shard_1_replica, tablet_replica2, tablet_replica3
      ], tablet_types_to_wait='REPLICA')
    utils.vtgate.wait_for_endpoints('test_keyspace.-80.master', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.80-.replica', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.-80.master', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.80-.replica', 1)
    utils.vtgate.wait_for_endpoints('recovery_keyspace.-80.replica', 1)
    utils.vtgate.wait_for_endpoints('recovery_keyspace.80-.replica', 1)

    # check that vtgate doesn't route queries to new tablet
    vtgate_conn = get_connection()
    cursor = vtgate_conn.cursor(
        tablet_type='replica', keyspace=None, writable=True)

    cursor.execute('select count(*) from vt_insert_test', {})
    result = cursor.fetchall()
    if not result:
      self.fail('Result cannot be null')
    else:
      self.assertEqual(result[0][0], 4)

    # check that new keyspace is accessible by using ks.table
    cursor.execute('select count(*) from recovery_keyspace.vt_insert_test', {})
    result = cursor.fetchall()
    if not result:
      self.fail('Result cannot be null')
    else:
      self.assertEqual(result[0][0], 2)

    # check that new keyspace is accessible with 'use ks'
    cursor.execute('use recovery_keyspace@replica', {})
    cursor.execute('select count(*) from vt_insert_test', {})
    result = cursor.fetchall()
    if not result:
      self.fail('Result cannot be null')
    else:
      self.assertEqual(result[0][0], 2)

    # TODO check that new tablet is accessible with 'use ks:shard'
    # this currently does not work through the python client, though it works from mysql client
    #cursor.execute('use recovery_keyspace:-80@replica', {})
    #cursor.execute('select count(*) from vt_insert_test', {})
    #result = cursor.fetchall()
    #if not result:
    #  self.fail('Result cannot be null')
    #else:
    #  self.assertEqual(result[0][0], shard_0_count)
    #cursor.execute('select id from vt_insert_test', {})
    #result = cursor.fetchall()
    #if not result:
    #  self.fail('Result cannot be null')
    #else:
    #  self.assertEqual(result[0][0], shard_0_test_id)

    #cursor.execute('use recovery_keyspace:80-@replica', {})
    #cursor.execute('select count(*) from vt_insert_test', {})
    #result = cursor.fetchall()
    #if not result:
    #  self.fail('Result cannot be null')
    #else:
    #  self.assertEqual(result[0][0], shard_1_count)
    #cursor.execute('use recovery_keyspace:80-@replica', {})
    #cursor.execute('select id from vt_insert_test', {})
    #result = cursor.fetchall()
    #if not result:
    #  self.fail('Result cannot be null')
    #else:
    #  self.assertEqual(result[0][0], shard_1_test_id)

    vtgate_conn.close()
    tablet_replica2.kill_vttablet()
    tablet_replica3.kill_vttablet()
    vtgate.kill()

if __name__ == '__main__':
  utils.main()
