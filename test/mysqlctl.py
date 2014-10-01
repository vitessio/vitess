#!/usr/bin/env python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import os
import logging
import unittest

import environment
import utils
import tablet

master_tablet = tablet.Tablet()
replica_tablet = tablet.Tablet()

def setUpModule():
  try:
    environment.topo_server_setup()
    utils.Vtctld().start()

    setup_procs = [
        master_tablet.init_mysql(),
        replica_tablet.init_mysql(),
        ]
    utils.wait_procs(setup_procs)

    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    master_tablet.init_tablet('master', 'test_keyspace', '0')
    replica_tablet.init_tablet('replica', 'test_keyspace', '0')
    utils.run_vtctl(['RebuildShardGraph', 'test_keyspace/0'])
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    master_tablet.create_db('vt_test_keyspace')
    replica_tablet.create_db('vt_test_keyspace')
  except:
    tearDownModule()
    raise

def tearDownModule():
  if utils.options.skip_teardown:
    return

  tablet.kill_tablets([master_tablet, replica_tablet])

  teardown_procs = [
      master_tablet.teardown_mysql(),
      replica_tablet.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  master_tablet.remove_tree()
  replica_tablet.remove_tree()

class TestMysqlctl(unittest.TestCase):
  def tearDown(self):
    tablet.Tablet.check_vttablet_count()
    for t in [master_tablet, replica_tablet]:
      t.reset_replication()
      t.clean_dbs()

  def test_mysqlctl_restart(self):
    utils.pause('mysqld initialized')
    utils.wait_procs([master_tablet.shutdown_mysql()])
    utils.wait_procs([master_tablet.start_mysql()])

  def test_auto_detect(self):
    # start up tablets with an empty MYSQL_FLAVOR, which means auto-detect
    master_tablet.start_vttablet(wait_for_state=None,
                                 extra_env={'MYSQL_FLAVOR': ''})
    replica_tablet.start_vttablet(wait_for_state=None,
                                 extra_env={'MYSQL_FLAVOR': ''})
    master_tablet.wait_for_vttablet_state('SERVING')
    replica_tablet.wait_for_vttablet_state('SERVING')

    # reparent tablets, which requires flavor detection
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/0',
                     master_tablet.tablet_alias], auto_log=True)

    master_tablet.kill_vttablet()
    replica_tablet.kill_vttablet()


if __name__ == '__main__':
  utils.main()
