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

  def tearDown(self):
    tablet.Tablet.check_vttablet_count()
    environment.topo_server().wipe()
    for t in [tablet_master, tablet_replica1, tablet_replica2]:
      t.reset_replication()
      t.set_semi_sync_enabled(master=False)
      t.clean_dbs()

  _create_vt_insert_test = '''create table vt_insert_test (
  id bigint auto_increment,
  msg varchar(64),
  primary key (id)
  ) Engine=InnoDB'''

  def _insert_master(self, index):
    tablet_master.mquery(
        'vt_test_keyspace',
        "insert into vt_insert_test (msg) values ('test %s')" %
        index, write=True)

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
    for t in tablet_master, tablet_replica1:
      t.create_db('vt_test_keyspace')

    tablet_master.init_tablet('master', 'test_keyspace', '0', start=True,
                              supports_backups=True)
    tablet_replica1.init_tablet('replica', 'test_keyspace', '0', start=True,
                                supports_backups=True)
    utils.run_vtctl(['InitShardMaster', 'test_keyspace/0',
                     tablet_master.tablet_alias])

    # insert data on master, wait for slave to get it
    tablet_master.mquery('vt_test_keyspace', self._create_vt_insert_test)
    self._insert_master(1)
    timeout = 10
    while True:
      try:
        result = tablet_replica1.mquery(
            'vt_test_keyspace', 'select count(*) from vt_insert_test')
        if result[0][0] == 1:
          break
      except MySQLdb.DatabaseError:
        # ignore exceptions, we'll just timeout (the tablet creation
        # can take some time to replicate, and we get a 'table vt_insert_test
        # does not exist exception in some rare cases)
        logging.exception('exception waiting for data to replicate')
      timeout = utils.wait_step('slave tablet getting data', timeout)

    # backup the slave
    utils.run_vtctl(['Backup', tablet_replica1.tablet_alias], auto_log=True)

    # insert more data on the master
    self._insert_master(2)

    # now bring up the other slave, health check on, init_tablet on, restore on
    tablet_replica2.start_vttablet(wait_for_state='SERVING',
                                   target_tablet_type='replica',
                                   init_keyspace='test_keyspace',
                                   init_shard='0',
                                   supports_backups=True)

    # check the new slave has the data
    timeout = 10
    while True:
      result = tablet_replica2.mquery(
          'vt_test_keyspace', 'select count(*) from vt_insert_test')
      if result[0][0] == 2:
        break
      timeout = utils.wait_step('new slave tablet getting data', timeout)

    # list the backups
    backups, _ = utils.run_vtctl(tablet.get_backup_storage_flags() +
                                 ['ListBackups', 'test_keyspace/0'],
                                 mode=utils.VTCTL_VTCTL, trap_output=True)
    backups = backups.splitlines()
    logging.debug('list of backups: %s', backups)
    self.assertEqual(len(backups), 1)
    self.assertTrue(backups[0].endswith(tablet_replica1.tablet_alias))

    # remove the backup
    utils.run_vtctl(
        tablet.get_backup_storage_flags() +
        ['RemoveBackup', 'test_keyspace/0', backups[0]],
        auto_log=True, mode=utils.VTCTL_VTCTL)

    # make sure the list of backups is empty now
    backups, _ = utils.run_vtctl(tablet.get_backup_storage_flags() +
                                 ['ListBackups', 'test_keyspace/0'],
                                 mode=utils.VTCTL_VTCTL, trap_output=True)
    backups = backups.splitlines()
    logging.debug('list of backups after remove: %s', backups)
    self.assertEqual(len(backups), 0)

    for t in tablet_master, tablet_replica1, tablet_replica2:
      t.kill_vttablet()

if __name__ == '__main__':
  utils.main()
