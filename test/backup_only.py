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
    logging.debug("initilizing mysql %s",str(datetime.datetime.now()))
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
    logging.debug("done restoring tablet %s",str(datetime.datetime.now()))
    
  def _reset_tablet_dir(self, t, teardown=True):
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

  def _backup_only(self, t, tablet_type='replica'):
    """Erase mysql/tablet dir, then start tablet with restore only."""
    logging.debug('starting backup only tablet %s',t.tablet_alias)
    xtra_args = ['-db-credentials-file', db_credentials_file]    
    self._reset_tablet_dir(t,False)

    logging.debug('building command line')

    extra_args = ['-db-credentials-file', db_credentials_file,'-backup_and_exit']
    if use_xtrabackup:
      extra_args.extend(xtrabackup_args)
    logging.debug("starting backup tablet %s",str(datetime.datetime.now()))    
    proc = t.start_vttablet(wait_for_state=False,
                     init_tablet_type=tablet_type,
                     init_keyspace='test_keyspace',
                     init_shard='0',
                     supports_backups=True,
                     extra_args=extra_args)

    logging.debug('tablet started waiting for process to end %s',proc)
    utils.wait_procs([proc],True)
    logging.debug("backup tablet done %s",str(datetime.datetime.now()))        
    utils.wait_procs([t.teardown_mysql(extra_args=xtra_args)])    

  def test_tablet_backup_only(self):
    self._test_backup('replica', True)

  def _test_backup(self, tablet_type, backup_only):
    """Test backup flow.

    test_backup will:
    - create a shard with master and replica1 only
    - run InitShardMaster
    - insert some data
    - take a backup
    - insert more data on the master
    - bring up tablet_replica2 after the fact, let it restore the backup
    - check all data is right (before+after backup data)
    - list the backup, remove it

    Args:
      tablet_type: 'replica' or 'rdonly'.
    """
    
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

    # now bring up the other slave, letting it restore from backup.
    self._restore(tablet_replica2, tablet_type=tablet_type)

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

if __name__ == '__main__':
  utils.main()
