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
unsharded_master = tablet.Tablet()
unsharded_replica1 = tablet.Tablet()
unsharded_replica2 = tablet.Tablet()
unsharded_replica3 = tablet.Tablet()

all_tablets = [unsharded_master, unsharded_replica1, unsharded_replica2, unsharded_replica3]
unsharded_tablets = [unsharded_master, unsharded_replica1, unsharded_replica2, unsharded_replica3]

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

class TestVttabletRecovery(unittest.TestCase):

  def setUp(self):
    for t in unsharded_master, unsharded_replica1:
      t.create_db('vt_test_keyspace')

    xtra_args = ['-enable_replication_reporter']
    unsharded_master.init_tablet('replica', 'test_keyspace', '0', start=True,
                              supports_backups=True,
                              extra_args=xtra_args)
    unsharded_replica1.init_tablet('replica', 'test_keyspace', '0', start=True,
                                supports_backups=True,
                                extra_args=xtra_args)
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     unsharded_master.tablet_alias])

  def tearDown(self):
    for t in all_tablets:
      t.kill_vttablet()

    tablet.Tablet.check_vttablet_count()
    environment.topo_server().wipe()
    for t in all_tablets:
      t.reset_replication()
      t.set_semi_sync_enabled(master=False, slave=False)
      t.clean_dbs()

    for backup in self._list_backups():
      self._remove_backup(backup)

  _create_vt_insert_test = '''create table vt_insert_test (
  id bigint auto_increment,
  msg varchar(64),
  primary key (id)
  ) Engine=InnoDB'''

  _vschema_json = '''{
    "tables": {
        "vt_insert_test": {}
    }
}'''


  def _insert_data(self, t, index):
    """Add a single row with value 'index' to the given tablet."""
    t.mquery(
        'vt_test_keyspace',
        "insert into vt_insert_test (msg) values ('test %s')" %
        index, write=True)

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

  def _restore(self, t, keyspace):
    """Erase mysql/tablet dir, then start tablet with restore enabled."""
    self._reset_tablet_dir(t)

    # create a recovery keyspace
    utils.run_vtctl(['CreateKeyspace',
                     '-keyspace_type=SNAPSHOT',
                     '-base_keyspace=test_keyspace',
                     '-snapshot_time',
                     datetime.datetime.utcnow().isoformat("T")+"Z",
                     keyspace])
    
    # set disable_active_reparents to true, otherwise replication_reporter will
    # try to restart replication
    xtra_args = ['-disable_active_reparents',
                 '-enable_replication_reporter=false']
    xtra_args.extend(tablet.get_backup_storage_flags())

    t.start_vttablet(wait_for_state='SERVING',
                     init_tablet_type='replica',
                     init_keyspace=keyspace,
                     init_shard='0',
                     supports_backups=True,
                     extra_args=xtra_args)

  def _reset_tablet_dir(self, t):
    """Stop mysql, delete everything including tablet dir, restart mysql."""
    utils.wait_procs([t.teardown_mysql()])
    # Specify ignore_options because we want to delete the tree even
    # if the test's -k / --keep-logs was specified on the command line.
    t.remove_tree(ignore_options=True)
    proc = t.init_mysql()
    utils.wait_procs([proc])

  def _list_backups(self):
    """Get a list of backup names for the test shard."""
    backups, _ = utils.run_vtctl(tablet.get_backup_storage_flags() +
                                 ['ListBackups', 'test_keyspace/0'],
                                 mode=utils.VTCTL_VTCTL, trap_output=True)
    return backups.splitlines()

  def _remove_backup(self, backup):
    """Remove a named backup from the test shard."""
    utils.run_vtctl(
        tablet.get_backup_storage_flags() +
        ['RemoveBackup', 'test_keyspace/0', backup],
        auto_log=True, mode=utils.VTCTL_VTCTL)

  def test_basic_recovery(self):
    """Test recovery from backup flow.

    test_recovery will:
    - create a shard with master and replica1 only
    - run InitShardMaster
    - insert some data
    - take a backup
    - insert more data on the master
    - create a recovery keyspace
    - bring up unsharded_replica2 in the new keyspace
    - check that new tablet does not have data created after backup
    - check that vtgate queries work correctly

    """

    # insert data on master, wait for slave to get it
    utils.run_vtctl(['ApplySchema',
                     '-sql', self._create_vt_insert_test,
                     'test_keyspace'],
                    auto_log=True)
    self._insert_data(unsharded_master, 1)
    self._check_data(unsharded_replica1, 1, 'replica1 tablet getting data')

    # backup the slave
    utils.run_vtctl(['Backup', unsharded_replica1.tablet_alias], auto_log=True)

    # check that the backup shows up in the listing
    backups = self._list_backups()
    logging.debug('list of backups: %s', backups)
    self.assertEqual(len(backups), 1)
    self.assertTrue(backups[0].endswith(unsharded_replica1.tablet_alias))

    # insert more data on the master
    self._insert_data(unsharded_master, 2)

    utils.run_vtctl(['ApplyVSchema',
                     '-vschema', self._vschema_json,
                     'test_keyspace'],
                    auto_log=True)

    vs = utils.run_vtctl_json(['GetVSchema', 'test_keyspace'])
    logging.debug('test_keyspace vschema: %s', str(vs))
    ks = utils.run_vtctl_json(['GetSrvKeyspace', 'test_nj', 'test_keyspace'])
    logging.debug('Serving keyspace before: %s', str(ks))
    vs = utils.run_vtctl_json(['GetSrvVSchema', 'test_nj'])
    logging.debug('Serving vschema before recovery: %s', str(vs))

    # now bring up the other slave, letting it restore from backup.
    self._restore(unsharded_replica2, 'recovery_keyspace')

    vs = utils.run_vtctl_json(['GetSrvVSchema', 'test_nj'])
    logging.debug('Serving vschema after recovery: %s', str(vs))
    ks = utils.run_vtctl_json(['GetSrvKeyspace', 'test_nj', 'test_keyspace'])
    logging.debug('Serving keyspace after: %s', str(ks))
    vs = utils.run_vtctl_json(['GetVSchema', 'recovery_keyspace'])
    logging.debug('recovery_keyspace vschema: %s', str(vs))

    # check the new slave does not have the data
    self._check_data(unsharded_replica2, 1, 'replica2 tablet should not have new data')

    # check that the restored slave has the right local_metadata
    result = unsharded_replica2.mquery('_vt', 'select * from local_metadata')
    metadata = {}
    for row in result:
      metadata[row[0]] = row[1]
    self.assertEqual(metadata['Alias'], 'test_nj-0000062346')
    self.assertEqual(metadata['ClusterAlias'], 'recovery_keyspace.0')
    self.assertEqual(metadata['DataCenter'], 'test_nj')

    # start vtgate
    vtgate = utils.VtGate()
    vtgate.start(tablets=[
      unsharded_master, unsharded_replica1, unsharded_replica2
      ], tablet_types_to_wait='REPLICA')
    utils.vtgate.wait_for_endpoints('test_keyspace.0.master', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.0.replica', 1)
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
      self.assertEqual(result[0][0], 2)

    # check that new tablet is accessible by using ks.table
    cursor.execute('select count(*) from recovery_keyspace.vt_insert_test', {})
    result = cursor.fetchall()
    if not result:
      self.fail('Result cannot be null')
    else:
      self.assertEqual(result[0][0], 1)

    # check that new tablet is accessible with 'use ks'
    cursor.execute('use recovery_keyspace@replica', {})
    cursor.execute('select count(*) from vt_insert_test', {})
    result = cursor.fetchall()
    if not result:
      self.fail('Result cannot be null')
    else:
      self.assertEqual(result[0][0], 1)

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
    unsharded_replica2.kill_vttablet()
    vtgate.kill()

  def test_multi_recovery(self):
    """Test recovery from backup flow.

    test_multi_recovery will:
    - create a shard with master and replica1 only
    - run InitShardMaster
    - insert some data
    - take a backup
    - insert more data on the master
    - take another backup
    - create a recovery keyspace after first backup
    - bring up unsharded_replica2 in the new keyspace
    - check that new tablet does not have data created after backup1
    - create second recovery keyspace after second backup
    - bring up unsharded_replica3 in second keyspace
    - check that new tablet has data created after backup1 but not data created after backup2
    - check that vtgate queries work correctly

    """

    # insert data on master, wait for slave to get it
    utils.run_vtctl(['ApplySchema',
                     '-sql', self._create_vt_insert_test,
                     'test_keyspace'],
                    auto_log=True)
    self._insert_data(unsharded_master, 1)
    self._check_data(unsharded_replica1, 1, 'replica1 tablet getting data')

    # backup the slave
    utils.run_vtctl(['Backup', unsharded_replica1.tablet_alias], auto_log=True)

    # check that the backup shows up in the listing
    backups = self._list_backups()
    logging.debug('list of backups: %s', backups)
    self.assertEqual(len(backups), 1)
    self.assertTrue(backups[0].endswith(unsharded_replica1.tablet_alias))

    # insert more data on the master
    self._insert_data(unsharded_master, 2)
    # wait for it to replicate
    self._check_data(unsharded_replica1, 2, 'replica1 tablet getting data')

    utils.run_vtctl(['ApplyVSchema',
                     '-vschema', self._vschema_json,
                     'test_keyspace'],
                    auto_log=True)

    vs = utils.run_vtctl_json(['GetVSchema', 'test_keyspace'])
    logging.debug('test_keyspace vschema: %s', str(vs))

    # now bring up the other slave, letting it restore from backup.
    self._restore(unsharded_replica2, 'recovery_ks1')

    # we are not asserting on the contents of vschema here
    # because the later part of the test (vtgate) will fail
    # if the vschema is not copied correctly from the base_keyspace
    vs = utils.run_vtctl_json(['GetVSchema', 'recovery_ks1'])
    logging.debug('recovery_ks1 vschema: %s', str(vs))

    # check the new slave does not have the data
    self._check_data(unsharded_replica2, 1, 'replica2 tablet should not have new data')

    # take another backup on the slave
    utils.run_vtctl(['Backup', unsharded_replica1.tablet_alias], auto_log=True)

    # insert more data on the master
    self._insert_data(unsharded_master, 3)
    # wait for it to replicate
    self._check_data(unsharded_replica1, 3, 'replica1 tablet getting data')

    # now bring up the other slave, letting it restore from backup2.
    self._restore(unsharded_replica3, 'recovery_ks2')

    vs = utils.run_vtctl(['GetVSchema', 'recovery_ks2'])
    logging.debug('recovery_ks2 vschema: %s', str(vs))

    # check the new slave does not have the latest data
    self._check_data(unsharded_replica3, 2, 'replica3 tablet should not have new data')

    # start vtgate
    vtgate = utils.VtGate()
    vtgate.start(tablets=unsharded_tablets, tablet_types_to_wait='REPLICA')
    utils.vtgate.wait_for_endpoints('test_keyspace.0.master', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.0.replica', 1)
    utils.vtgate.wait_for_endpoints('recovery_ks1.0.replica', 1)
    utils.vtgate.wait_for_endpoints('recovery_ks2.0.replica', 1)

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
    cursor.execute('select count(*) from recovery_ks1.vt_insert_test', {})
    result = cursor.fetchall()
    if not result:
      self.fail('Result cannot be null')
    else:
      self.assertEqual(result[0][0], 1)

    # check that new tablet is accessible by using ks.table
    cursor.execute('select count(*) from recovery_ks2.vt_insert_test', {})
    result = cursor.fetchall()
    if not result:
      self.fail('Result cannot be null')
    else:
      self.assertEqual(result[0][0], 2)

    # TODO check that new tablet is accessible with 'use ks:shard'
    # this currently does not work through the python client, though it works from mysql client
    #cursor.execute('use recovery_ks1:0@replica', {})
    #cursor.execute('select count(*) from vt_insert_test', {})
    #result = cursor.fetchall()
    #if not result:
      #self.fail('Result cannot be null')
    #else:
      #self.assertEqual(result[0][0], 1)

    vtgate_conn.close()
    unsharded_replica2.kill_vttablet()
    unsharded_replica3.kill_vttablet()
    vtgate.kill()

if __name__ == '__main__':
  utils.main()
