#!/usr/bin/env python

import logging
import unittest

import MySQLdb

import environment
import tablet
import utils

use_mysqlctld = True

tablet_master = tablet.Tablet(use_mysqlctld=use_mysqlctld)
tablet_replica1 = tablet.Tablet(use_mysqlctld=use_mysqlctld)
tablet_replica2 = tablet.Tablet(use_mysqlctld=use_mysqlctld)

setup_procs = []


def setUpModule():
  try:
    environment.topo_server().setup()

    # start mysql instance external to the test
    global setup_procs
    setup_procs = [
        tablet_master.init_mysql(),
        tablet_replica1.init_mysql(),
        tablet_replica2.init_mysql(),
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

  if use_mysqlctld:
    # Try to terminate mysqlctld gracefully, so it kills its mysqld.
    for proc in setup_procs:
      utils.kill_sub_process(proc, soft=True)
    teardown_procs = setup_procs
  else:
    teardown_procs = [
        tablet_master.teardown_mysql(),
        tablet_replica1.teardown_mysql(),
        tablet_replica2.teardown_mysql(),
    ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  tablet_master.remove_tree()
  tablet_replica1.remove_tree()
  tablet_replica2.remove_tree()


class TestBackup(unittest.TestCase):

  def setUp(self):
    for t in tablet_master, tablet_replica1:
      t.create_db('vt_test_keyspace')

    tablet_master.init_tablet('master', 'test_keyspace', '0', start=True,
                              supports_backups=True)
    tablet_replica1.init_tablet('replica', 'test_keyspace', '0', start=True,
                                supports_backups=True)
    utils.run_vtctl(['InitShardMaster', 'test_keyspace/0',
                     tablet_master.tablet_alias])

  def tearDown(self):
    for t in tablet_master, tablet_replica1:
      t.kill_vttablet()

    tablet.Tablet.check_vttablet_count()
    environment.topo_server().wipe()
    for t in [tablet_master, tablet_replica1, tablet_replica2]:
      t.reset_replication()
      t.set_semi_sync_enabled(master=False)
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
    t.start_vttablet(wait_for_state='SERVING',
                     init_tablet_type='replica',
                     init_keyspace='test_keyspace',
                     init_shard='0',
                     supports_backups=True)

  def _reset_tablet_dir(self, t):
    """Stop mysql, delete everything including tablet dir, restart mysql."""
    utils.wait_procs([t.teardown_mysql()])
    t.remove_tree()
    proc = t.init_mysql()
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

  def test_backup(self):
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

    # check that the backup shows up in the listing
    backups = self._list_backups()
    logging.debug('list of backups: %s', backups)
    self.assertEqual(len(backups), 1)
    self.assertTrue(backups[0].endswith(tablet_replica1.tablet_alias))

    # remove the backup and check that the list is empty
    self._remove_backup(backups[0])
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
    utils.run_vtctl(['PlannedReparentShard', 'test_keyspace/0',
                     tablet_replica2.tablet_alias])

    # insert more data on replica2 (current master)
    self._insert_data(tablet_replica2, 3)

    # Force replica1 to restore from backup.
    tablet_replica1.kill_vttablet()
    self._restore(tablet_replica1)

    # wait for replica1 to catch up.
    self._check_data(tablet_replica1, 3,
                     'replica1 getting data from restored master')

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
    utils.run_vtctl(['PlannedReparentShard', 'test_keyspace/0',
                     tablet_replica1.tablet_alias])

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

if __name__ == '__main__':
  utils.main()
