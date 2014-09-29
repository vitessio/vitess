#!/usr/bin/env python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import logging
import unittest

import environment
import utils
import tablet

tablet_62344 = tablet.Tablet(62344)
tablet_31981 = tablet.Tablet(31981)

def setUpModule():
  try:
    # start mysql instance external to the test
    setup_procs = [
        tablet_62344.init_mysql(),
        tablet_31981.init_mysql(),
        ]
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise

def tearDownModule():
  if utils.options.skip_teardown:
    return

  teardown_procs = [
      tablet_62344.teardown_mysql(),
      tablet_31981.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  utils.kill_sub_processes()
  utils.remove_tmp_files()

  tablet_62344.remove_tree()
  tablet_31981.remove_tree()

class TestMysqlctl(unittest.TestCase):
  def tearDown(self):
    tablet.Tablet.check_vttablet_count()
    for t in [tablet_62344, tablet_31981]:
      t.reset_replication()
      t.clean_dbs()

  def test_mysqlctl_restart(self):
    utils.pause('mysqld initialized')
    utils.wait_procs([tablet_62344.shutdown_mysql()])
    utils.wait_procs([tablet_62344.start_mysql()])

if __name__ == '__main__':
  utils.main()
