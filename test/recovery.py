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

import MySQLdb

import environment
import tablet
import utils

use_mysqlctld = False
use_xtrabackup = False
stream_mode = 'tar'
tablet_master = None
tablet_replica1 = None
tablet_replica2 = None
xtrabackup_args = []
new_init_db = ''
db_credentials_file = ''


def setUpModule():
  global new_init_db, db_credentials_file
  global tablet_master, tablet_replica1, tablet_replica2

  tablet_master = tablet.Tablet(use_mysqlctld=use_mysqlctld,
                                vt_dba_passwd='VtDbaPass')
  tablet_replica1 = tablet.Tablet(use_mysqlctld=use_mysqlctld,
                                  vt_dba_passwd='VtDbaPass')
  tablet_replica2 = tablet.Tablet(use_mysqlctld=use_mysqlctld,
                                  vt_dba_passwd='VtDbaPass')

  try:
    environment.topo_server().setup()

    # Create a new init_db.sql file that sets up passwords for all users.
    # Then we use a db-credentials-file with the passwords.
    new_init_db = environment.tmproot + '/init_db_with_passwords.sql'
    with open(environment.vttop + '/config/init_db.sql') as fd:
      init_db = fd.read()
    with open(new_init_db, 'w') as fd:
      fd.write(init_db)
      fd.write('''
# Set real passwords for all users.
ALTER USER 'root'@'localhost' IDENTIFIED BY 'RootPass';
ALTER USER 'vt_dba'@'localhost' IDENTIFIED BY 'VtDbaPass';
ALTER USER 'vt_app'@'localhost' IDENTIFIED BY 'VtAppPass';
ALTER USER 'vt_allprivs'@'localhost' IDENTIFIED BY 'VtAllPrivsPass';
ALTER USER 'vt_repl'@'%' IDENTIFIED BY 'VtReplPass';
ALTER USER 'vt_filtered'@'localhost' IDENTIFIED BY 'VtFilteredPass';
FLUSH PRIVILEGES;
''')
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
  ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  tablet_master.remove_tree()
  tablet_replica1.remove_tree()
  tablet_replica2.remove_tree()


class TestVttabletRecovery(unittest.TestCase):

  def setUp(self):
    for t in tablet_master, tablet_replica1:
      t.create_db('vt_test_keyspace')

    xtra_args = ['-db-credentials-file', db_credentials_file]
    tablet_master.init_tablet('replica', 'test_keyspace', '0', start=True,
                              supports_backups=True,
                              extra_args=xtra_args)
    tablet_replica1.init_tablet('replica', 'test_keyspace', '0', start=True,
                                supports_backups=True,
                                extra_args=xtra_args)
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     tablet_master.tablet_alias])

  def tearDown(self):
    for t in tablet_master, tablet_replica1, tablet_replica2:
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

  def _restore(self, t):
    """Erase mysql/tablet dir, then start tablet with restore enabled."""
    self._reset_tablet_dir(t)

    xtra_args = ['-db-credentials-file', db_credentials_file, '-restore_for_recovery']
    xtra_args.extend(tablet.get_backup_storage_flags())

    backups = self._list_backups()
    # set disable_active_reparents to true, otherwise replication_reporter will
    # try to restart replication
    xtra_args.extend(['-backup_to_use_for_recovery',
                      'test_keyspace/0' + backups[0],
                      '-disable_active_reparents'])
    # supports_backups=False because we don't want to use -restore_from_backup
    # use xtra_args to pass -recover_from_backup
    t.start_vttablet(wait_for_state='SERVING',
                     init_tablet_type='recovery',
                     init_keyspace='test_keyspace',
                     init_shard='0',
                     supports_backups=False,
                     extra_args=xtra_args)

  def _reset_tablet_dir(self, t):
    """Stop mysql, delete everything including tablet dir, restart mysql."""
    extra_args = ['-db-credentials-file', db_credentials_file]
    utils.wait_procs([t.teardown_mysql(extra_args=extra_args)])
    # Specify ignore_options because we want to delete the tree even
    # if the test's -k / --keep-logs was specified on the command line.
    t.remove_tree(ignore_options=True)
    proc = t.init_mysql(init_db=new_init_db, extra_args=extra_args)
    if use_mysqlctld:
      t.wait_for_mysqlctl_socket()
    else:
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

  def test_recovery(self):
    """Test recovery from backup flow.

    test_recovery will:
    - create a shard with master and replica1 only
    - run InitShardMaster
    - insert some data
    - take a backup
    - insert more data on the master
    - bring up tablet_replica2 after the fact, let it restore the backup
    - check all data is right (before+after backup data)
    - list the backup, remove it

    """

    # insert data on master, wait for slave to get it
    tablet_master.mquery('vt_test_keyspace', self._create_vt_insert_test)
    self._insert_data(tablet_master, 1)
    self._check_data(tablet_replica1, 1, 'replica1 tablet getting data')

    # backup the slave
    utils.run_vtctl(['Backup', tablet_replica1.tablet_alias], auto_log=True)

    # check that the backup shows up in the listing
    backups = self._list_backups()
    logging.debug('list of backups: %s', backups)
    self.assertEqual(len(backups), 1)
    self.assertTrue(backups[0].endswith(tablet_replica1.tablet_alias))

    # insert more data on the master
    self._insert_data(tablet_master, 2)

    # now bring up the other slave, letting it restore from backup.
    self._restore(tablet_replica2)

    # check the new slave does not have the data
    self._check_data(tablet_replica2, 1, 'replica2 tablet should not have new data')

    # check that the restored slave has the right local_metadata
    result = tablet_replica2.mquery('_vt', 'select * from local_metadata')
    metadata = {}
    for row in result:
      metadata[row[0]] = row[1]
    self.assertEqual(metadata['Alias'], 'test_nj-0000062346')
    self.assertEqual(metadata['ClusterAlias'], 'test_keyspace.0')
    self.assertEqual(metadata['DataCenter'], 'test_nj')
    self.assertEqual(metadata['PromotionRule'], 'must_not')

    tablet_replica2.kill_vttablet()

if __name__ == '__main__':
  utils.main()
