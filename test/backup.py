#!/usr/bin/env python

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


import json
import logging
import os
import unittest
import datetime

import MySQLdb

import environment
import tablet
import utils
from mysql_flavor import mysql_flavor

use_mysqlctld = False
use_xtrabackup = False
stream_mode = 'tar'
tablet_master = None
tablet_replica1 = None
tablet_replica2 = None
tablet_replica3 = None
xtrabackup_args = []

new_init_db = ''
db_credentials_file = ''


def setUpModule():
  global xtrabackup_args
  xtrabackup_args = ['-backup_engine_implementation',
                   'xtrabackup',
                   '-xtrabackup_stream_mode',
                   stream_mode,
                   '-xtrabackup_user=vt_dba',
                   '-xtrabackup_backup_flags',
                   '--password=VtDbaPass']

  global new_init_db, db_credentials_file
  global tablet_master, tablet_replica1, tablet_replica2, tablet_replica3

  tablet_master = tablet.Tablet(use_mysqlctld=use_mysqlctld,
                                vt_dba_passwd='VtDbaPass')
  tablet_replica1 = tablet.Tablet(use_mysqlctld=use_mysqlctld,
                                  vt_dba_passwd='VtDbaPass')
  tablet_replica2 = tablet.Tablet(use_mysqlctld=use_mysqlctld,
                                  vt_dba_passwd='VtDbaPass')
  tablet_replica3 = tablet.Tablet(use_mysqlctld=use_mysqlctld,
                                  vt_dba_passwd='VtDbaPass')

  try:
    environment.topo_server().setup()

    credentials = {
        'vt_dba': ['VtDbaPass'],
        'vt_app': ['VtAppPass'],
        'vt_allprivs': ['VtAllprivsPass'],
        'vt_repl': ['VtReplPass'],
        'vt_filtered': ['VtFilteredPass'],
    }
    db_credentials_file = environment.tmproot+'/db_credentials.json'
    with open(db_credentials_file, 'w') as fd:
      fd.write(json.dumps(credentials))

    # Determine which column is used for user passwords in this MySQL version.
    proc = tablet_master.init_mysql()
    if use_mysqlctld:
      tablet_master.wait_for_mysqlctl_socket()
    else:
      utils.wait_procs([proc])
    try:
      tablet_master.mquery('mysql', 'select password from mysql.user limit 0',
                           user='root')
      password_col = 'password'
    except MySQLdb.DatabaseError:
      password_col = 'authentication_string'
    utils.wait_procs([tablet_master.teardown_mysql()])
    tablet_master.remove_tree(ignore_options=True)

    # Create a new init_db.sql file that sets up passwords for all users.
    # Then we use a db-credentials-file with the passwords.
    new_init_db = environment.tmproot + '/init_db_with_passwords.sql'
    with open(environment.vttop + '/config/init_db.sql') as fd:
      init_db = fd.read()
    with open(new_init_db, 'w') as fd:
      fd.write(init_db)
      fd.write(mysql_flavor().change_passwords(password_col))

    # start mysql instance external to the test
    setup_procs = [
        tablet_master.init_mysql(init_db=new_init_db,
                                 extra_args=['-db-credentials-file',
                                             db_credentials_file]),
        tablet_replica1.init_mysql(init_db=new_init_db,
                                   extra_args=['-db-credentials-file',
                                               db_credentials_file]),
        tablet_replica2.init_mysql(init_db=new_init_db,
                                   extra_args=['-db-credentials-file',
                                               db_credentials_file]),
    ]
    if use_mysqlctld:
      tablet_master.wait_for_mysqlctl_socket()
      tablet_replica1.wait_for_mysqlctl_socket()
      tablet_replica2.wait_for_mysqlctl_socket()
    else:
      utils.wait_procs(setup_procs)
    logging.debug("done initilizing mysql %s",str(datetime.datetime.now()))      
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  teardown_procs = [
      tablet_master.teardown_mysql(extra_args=['-db-credentials-file',
                                               db_credentials_file]),
      tablet_replica1.teardown_mysql(extra_args=['-db-credentials-file',
                                                 db_credentials_file]),
      tablet_replica2.teardown_mysql(extra_args=['-db-credentials-file',
                                                 db_credentials_file]),
      tablet_replica3.teardown_mysql(extra_args=['-db-credentials-file',
                                                 db_credentials_file]),
  ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  tablet_master.remove_tree()
  tablet_replica1.remove_tree()
  tablet_replica2.remove_tree()
  tablet_replica3.remove_tree()


class TestBackup(unittest.TestCase):

  def setUp(self):
    for t in tablet_master, tablet_replica1:
      t.create_db('vt_test_keyspace')

    xtra_args = ['-db-credentials-file', db_credentials_file]
    if use_xtrabackup:
      xtra_args.extend(xtrabackup_args)
    tablet_master.init_tablet('replica', 'test_keyspace', '0', start=True,
                              supports_backups=True,
                              extra_args=xtra_args)
    tablet_replica1.init_tablet('replica', 'test_keyspace', '0', start=True,
                                supports_backups=True,
                                extra_args=xtra_args)
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     tablet_master.tablet_alias])

  def tearDown(self):
    for t in tablet_master, tablet_replica1, tablet_replica2, tablet_replica3:
      t.kill_vttablet()

    tablet.Tablet.check_vttablet_count()
    environment.topo_server().wipe()
    for t in [tablet_master, tablet_replica1, tablet_replica2]:
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

  def _restore(self, t, tablet_type='replica'):
    """Erase mysql/tablet dir, then start tablet with restore enabled."""
    logging.debug("restoring tablet %s",str(datetime.datetime.now()))
    self._reset_tablet_dir(t)

    xtra_args = ['-db-credentials-file', db_credentials_file]
    if use_xtrabackup:
      xtra_args.extend(xtrabackup_args)

    t.start_vttablet(wait_for_state='SERVING',
                     init_tablet_type=tablet_type,
                     init_keyspace='test_keyspace',
                     init_shard='0',
                     supports_backups=True,
                     extra_args=xtra_args)

    # check semi-sync is enabled for replica, disabled for rdonly.
    if tablet_type == 'replica':
      t.check_db_var('rpl_semi_sync_slave_enabled', 'ON')
      t.check_db_status('rpl_semi_sync_slave_status', 'ON')
    else:
      t.check_db_var('rpl_semi_sync_slave_enabled', 'OFF')
      t.check_db_status('rpl_semi_sync_slave_status', 'OFF')

  def _restore_wait_for_backup(self, t, tablet_type='replica'):
    """Erase mysql/tablet dir, then start tablet with wait_for_restore_interval."""
    self._reset_tablet_dir(t)

    xtra_args = [
      '-db-credentials-file', db_credentials_file,
      '-wait_for_backup_interval', '1s',
    ]
    if use_xtrabackup:
      xtra_args.extend(xtrabackup_args)

    t.start_vttablet(wait_for_state=None,
                     init_tablet_type=tablet_type,
                     init_keyspace='test_keyspace',
                     init_shard='0',
                     supports_backups=True,
                     extra_args=xtra_args)

  def _reset_tablet_dir(self, t):
    """Stop mysql, delete everything including tablet dir, restart mysql."""

    extra_args = ['-db-credentials-file', db_credentials_file]    
    if teardown:
      utils.wait_procs([t.teardown_mysql(extra_args=extra_args)])
    # Specify ignore_options because we want to delete the tree even
    # if the test's -k / --keep-logs was specified on the command line.
    t.remove_tree(ignore_options=True)
    logging.debug("starting mysql %s",str(datetime.datetime.now()))    
    proc = t.init_mysql(init_db=new_init_db, extra_args=extra_args)
    if use_mysqlctld:
      t.wait_for_mysqlctl_socket()
    else:
      utils.wait_procs([proc])
    logging.debug("done starting mysql %s",str(datetime.datetime.now()))          

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

  def test_backup_rdonly(self):
    self._test_backup('rdonly', False)

  def test_backup_replica(self):
    self._test_backup('replica', False)

  def test_backup_master(self):
    """Test backup flow.

    test_backup will:
    - create a shard with master and replica1 only
    - run InitShardMaster
    - insert some data
    - take a backup on master
    - insert more data on the master
    - bring up tablet_replica2 after the fact, let it restore the backup
    - check all data is right (before+after backup data)
    - list the backup, remove it

    """
    # insert data on master, wait for slave to get it
    tablet_master.mquery('vt_test_keyspace', self._create_vt_insert_test)
    self._insert_data(tablet_master, 1)
    self._check_data(tablet_replica1, 1, 'replica1 tablet getting data')

    # This will fail, make sure we get the right error.
    _, err = utils.run_vtctl(['Backup', tablet_master.tablet_alias],
                             auto_log=True, expect_fail=True)
    self.assertIn('type MASTER cannot take backup. if you really need to do this, rerun the backup command with -allow_master', err)

    # And make sure there is no backup left.
    backups = self._list_backups()
    self.assertEqual(len(backups), 0, 'invalid backups: %s' % backups)

    # backup the master
    utils.run_vtctl(['Backup', '-allow_master=true', tablet_master.tablet_alias], auto_log=True)

    # check that the backup shows up in the listing
    backups = self._list_backups()
    logging.debug('list of backups: %s', backups)
    self.assertEqual(len(backups), 1)
    self.assertTrue(backups[0].endswith(tablet_master.tablet_alias))

    # insert more data on the master
    self._insert_data(tablet_master, 2)

    # now bring up the other slave, letting it restore from backup.
    self._restore(tablet_replica2, tablet_type='replica')

    # check the new slave has the data
    self._check_data(tablet_replica2, 2, 'replica2 tablet getting data')

    # check that the restored slave has the right local_metadata
    result = tablet_replica2.mquery('_vt', 'select * from local_metadata')
    metadata = {}
    for row in result:
      metadata[row[0]] = row[1]
    self.assertEqual(metadata['Alias'], 'test_nj-0000062346')
    self.assertEqual(metadata['ClusterAlias'], 'test_keyspace.0')
    self.assertEqual(metadata['DataCenter'], 'test_nj')
    self.assertEqual(metadata['PromotionRule'], 'neutral')

    # remove the backup and check that the list is empty
    self._remove_backup(backups[0])
    backups = self._list_backups()
    logging.debug('list of backups after remove: %s', backups)
    self.assertEqual(len(backups), 0)

    tablet_replica2.kill_vttablet()

  def _test_backup(self, tablet_type, backup_only):
    """Test backup flow.

    test_backup will:
    - create a shard with master and replica1 only
    - run InitShardMaster
    - bring up tablet_replica2 concurrently, telling it to wait for a backup
    - insert some data
    - take a backup
    - insert more data on the master
    - wait for tablet_replica2 to become SERVING
    - check all data is right (before+after backup data)
    - list the backup, remove it

    Args:
      tablet_type: 'replica' or 'rdonly'.
    """

    # bring up another replica concurrently, telling it to wait until a backup
    # is available instead of starting up empty.
    self._restore_wait_for_backup(tablet_replica2, tablet_type=tablet_type)

    # insert data on master, wait for slave to get it
    tablet_master.mquery('vt_test_keyspace', self._create_vt_insert_test)
    self._insert_data(tablet_master, 1)
    self._check_data(tablet_replica1, 1, 'replica1 tablet getting data')

    # backup the slave
    alias = tablet_replica1.tablet_alias
    logging.debug("taking backup %s",str(datetime.datetime.now()))
    if not backup_only:
      utils.run_vtctl(['Backup', tablet_replica1.tablet_alias], auto_log=True)
    else:
      self._backup_only(tablet_replica3,tablet_type='backup')
      alias = tablet_replica3.tablet_alias
    logging.debug("done taking backup %s",str(datetime.datetime.now()))      
    # end if

    # check that the backup shows up in the listing
    backups = self._list_backups()
    logging.debug('list of backups: %s', backups)
    self.assertEqual(len(backups), 1)
    self.assertTrue(backups[0].endswith(alias))

    # insert more data on the master
    self._insert_data(tablet_master, 2)

    # wait for tablet_replica2 to become serving (after restoring)
    utils.pause('wait_for_backup')
    tablet_replica2.wait_for_vttablet_state('SERVING')

    # check the new slave has the data
    self._check_data(tablet_replica2, 2, 'replica2 tablet getting data')

    # check that the restored slave has the right local_metadata
    result = tablet_replica2.mquery('_vt', 'select * from local_metadata')
    metadata = {}
    for row in result:
      metadata[row[0]] = row[1]
    self.assertEqual(metadata['Alias'], 'test_nj-0000062346')
    self.assertEqual(metadata['ClusterAlias'], 'test_keyspace.0')
    self.assertEqual(metadata['DataCenter'], 'test_nj')
    if tablet_type == 'replica':
      self.assertEqual(metadata['PromotionRule'], 'neutral')
    else:
      self.assertEqual(metadata['PromotionRule'], 'must_not')

    for backup in backups:
      self._remove_backup(backup)

    backups = self._list_backups()
    logging.debug('list of backups after remove: %s', backups)
    self.assertEqual(len(backups), 0)

    tablet_replica2.kill_vttablet()

  def test_master_slave_same_backup(self):
    """Test a master and slave from the same backup.

    Check that a slave and master both restored from the same backup
    can replicate successfully.
    """

    # insert data on master, wait for slave to get it
    tablet_master.mquery('vt_test_keyspace', self._create_vt_insert_test)
    self._insert_data(tablet_master, 1)
    self._check_data(tablet_replica1, 1, 'replica1 tablet getting data')

    # backup the slave
    utils.run_vtctl(['Backup', tablet_replica1.tablet_alias], auto_log=True)

    # insert more data on the master
    self._insert_data(tablet_master, 2)

    # now bring up the other slave, letting it restore from backup.
    self._restore(tablet_replica2)

    # check the new slave has the data
    self._check_data(tablet_replica2, 2, 'replica2 tablet getting data')

    # Promote replica2 to master.
    utils.run_vtctl(['PlannedReparentShard',
                     '-keyspace_shard', 'test_keyspace/0',
                     '-new_master', tablet_replica2.tablet_alias])

    # insert more data on replica2 (current master)
    self._insert_data(tablet_replica2, 3)

    # Force replica1 to restore from backup.
    tablet_replica1.kill_vttablet()
    self._restore(tablet_replica1)

    # wait for replica1 to catch up.
    self._check_data(tablet_replica1, 3,
                     'replica1 getting data from restored master')

    # This is to test that replicationPosition is processed correctly
    # while doing backup/restore after a reparent.
    # It is written into the MANIFEST and read back from the MANIFEST.

    # Take another backup on the slave.
    utils.run_vtctl(['Backup', tablet_replica1.tablet_alias], auto_log=True)

    # Insert more data on replica2 (current master).
    self._insert_data(tablet_replica2, 4)

    # Force replica1 to restore from backup.
    tablet_replica1.kill_vttablet()
    self._restore(tablet_replica1)

    # Wait for replica1 to catch up.
    self._check_data(tablet_replica1, 4,
                     'replica1 getting data from master after reparent+backup+restore')

    tablet_replica2.kill_vttablet()

  def _restore_old_master_test(self, restore_method):
    """Test that a former master replicates correctly after being restored.

    - Take a backup.
    - Reparent from old master to new master.
    - Force old master to restore from a previous backup using restore_method.

    Args:
      restore_method: function accepting one parameter of type tablet.Tablet,
          this function is called to force a restore on the provided tablet
    """

    # insert data on master, wait for slave to get it
    tablet_master.mquery('vt_test_keyspace', self._create_vt_insert_test)
    self._insert_data(tablet_master, 1)
    self._check_data(tablet_replica1, 1, 'replica1 tablet getting data')

    # backup the slave
    utils.run_vtctl(['Backup', tablet_replica1.tablet_alias], auto_log=True)

    # insert more data on the master
    self._insert_data(tablet_master, 2)

    # reparent to replica1
    utils.run_vtctl(['PlannedReparentShard',
                     '-keyspace_shard', 'test_keyspace/0',
                     '-new_master', tablet_replica1.tablet_alias])

    # insert more data on new master
    self._insert_data(tablet_replica1, 3)

    # force the old master to restore at the latest backup.
    restore_method(tablet_master)

    # wait for it to catch up.
    self._check_data(tablet_master, 3, 'former master catches up after restore')

  def test_restore_old_master(self):
    def _restore_using_kill(t):
      t.kill_vttablet()
      self._restore(t)

    self._restore_old_master_test(_restore_using_kill)

  def test_in_place_restore(self):
    def _restore_in_place(t):
      utils.run_vtctl(['RestoreFromBackup', t.tablet_alias], auto_log=True)

    self._restore_old_master_test(_restore_in_place)

  def test_terminated_restore(self):
    def _terminated_restore(t):
      for e in utils.vtctld_connection.execute_vtctl_command(
          ['RestoreFromBackup', t.tablet_alias]):
        logging.info('%s', e.value)
        if 'shutdown mysqld' in e.value:
          break
      logging.info('waiting for restore to finish')
      utils.wait_for_tablet_type(t.tablet_alias, 'replica', timeout=30)

    # this test is run standalone with xtrabackup because it fails when run
    # with the other master restore tests
    if use_xtrabackup:
      return

    utils.Vtctld().start()
    self._restore_old_master_test(_terminated_restore)

  def test_backup_transform(self):
    """Use a transform, tests we backup and restore properly."""
    if use_xtrabackup:
      # not supported
      return

    # Insert data on master, make sure slave gets it.
    tablet_master.mquery('vt_test_keyspace', self._create_vt_insert_test)
    self._insert_data(tablet_master, 1)
    self._check_data(tablet_replica1, 1, 'replica1 tablet getting data')

    # Restart the replica with the transform parameter.
    tablet_replica1.kill_vttablet()

    xtra_args = ['-db-credentials-file', db_credentials_file]
    if use_xtrabackup:
      xtra_args.extend(xtrabackup_args)

    hook_args = ['-backup_storage_hook',
                 'test_backup_transform',
                 '-backup_storage_compress=false']
    xtra_args.extend(hook_args)

    tablet_replica1.start_vttablet(supports_backups=True,
                                   extra_args=xtra_args)

    # Take a backup, it should work.
    utils.run_vtctl(['Backup', tablet_replica1.tablet_alias], auto_log=True)

    # Insert more data on the master.
    self._insert_data(tablet_master, 2)

    # Make sure we have the TransformHook in the MANIFEST, and that
    # every file starts with 'header'.
    backups = self._list_backups()
    self.assertEqual(len(backups), 1, 'invalid backups: %s' % backups)
    location = os.path.join(environment.tmproot, 'backupstorage',
                            'test_keyspace', '0', backups[0])
    with open(os.path.join(location, 'MANIFEST')) as fd:
      contents = fd.read()
    manifest = json.loads(contents)
    self.assertEqual(manifest['TransformHook'], 'test_backup_transform')
    self.assertEqual(manifest['SkipCompress'], True)
    for i in xrange(len(manifest['FileEntries'])):
      name = os.path.join(location, '%d' % i)
      with open(name) as fd:
        line = fd.readline()
        self.assertEqual(line, 'header\n', 'wrong file contents for %s' % name)

    # Then start replica2 from backup, make sure that works.
    # Note we don't need to pass in the backup_storage_transform parameter,
    # as it is read from the MANIFEST.
    self._restore(tablet_replica2)

    # Check the new slave has all the data.
    self._check_data(tablet_replica2, 2, 'replica2 tablet getting data')

  def test_backup_transform_error(self):
    """Use a transform, force an error, make sure the backup fails."""
    if use_xtrabackup:
      # not supported
      return

    # Restart the replica with the transform parameter.
    tablet_replica1.kill_vttablet()
    xtra_args = ['-db-credentials-file', db_credentials_file]
    if use_xtrabackup:
      xtra_args.extend(xtrabackup_args)
    hook_args = ['-backup_storage_hook','test_backup_error']
    xtra_args.extend(hook_args)
    tablet_replica1.start_vttablet(supports_backups=True,
                                   extra_args=xtra_args)

    # This will fail, make sure we get the right error.
    _, err = utils.run_vtctl(['Backup', tablet_replica1.tablet_alias],
                             auto_log=True, expect_fail=True)
    self.assertIn('backup is not usable, aborting it', err)

    # And make sure there is no backup left.
    backups = self._list_backups()
    self.assertEqual(len(backups), 0, 'invalid backups: %s' % backups)

if __name__ == '__main__':
  utils.main()
