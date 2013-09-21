#!/usr/bin/python

import logging
import os
import shutil
import sys
import unittest

import utils
import tablet

tablet_62344 = tablet.Tablet(62344)
tablet_62044 = tablet.Tablet(62044)
tablet_41983 = tablet.Tablet(41983)
tablet_31981 = tablet.Tablet(31981)

zkocc_server = None

def setUpModule():
  try:
    utils.zk_setup()
    zkocc_server = utils.zkocc_start()

    # start mysql instance external to the test
    setup_procs = [
        tablet_62344.init_mysql(),
        tablet_62044.init_mysql(),
        tablet_41983.init_mysql(),
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
      tablet_62044.teardown_mysql(),
      tablet_41983.teardown_mysql(),
      tablet_31981.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  if zkocc_server:
    utils.zkocc_kill(zkocc_server)
  utils.zk_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  tablet_62344.remove_tree()
  tablet_62044.remove_tree()
  tablet_41983.remove_tree()
  tablet_31981.remove_tree()

  path = os.path.join(utils.vtdataroot, 'snapshot')
  try:
    shutil.rmtree(path)
  except OSError as e:
    logging.debug("removing snapshot %s: %s", path, str(e))

class TestTabletManager(unittest.TestCase):
  def tearDown(self):
    tablet.Tablet.check_vttablet_count()
    utils.zk_wipe()
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
      t.clean_dbs()

  def test_sanity(self):
    self._test_sanity()
    print "hit enter"
    sys.stdin.readline()

  def _test_sanity(self):
    # Start up a master mysql and vttablet
    utils.run_vtctl('CreateKeyspace -force test_keyspace')
    utils.run_vtctl('CreateShard -force test_keyspace/0')
    tablet_62344.init_tablet('master', 'test_keyspace', '0', parent=False)
    utils.run_vtctl('RebuildShardGraph test_keyspace/0')
    utils.run_vtctl('RebuildKeyspaceGraph test_keyspace')
    utils.validate_topology()

    # if these statements don't run before the tablet it will wedge waiting for the
    # db to become accessible. this is more a bug than a feature.
    tablet_62344.populate('vt_test_keyspace', self._create_vt_select_test,
                          self._populate_vt_select_test)

    tablet_62344.start_vttablet()

    # make sure the query service is started right away
    result, _ = utils.run_vtctl('Query test_nj test_keyspace "select * from vt_select_test"', trap_output=True)
    rows = result.splitlines()
    self.assertEqual(len(rows), 5, "expected 5 rows in vt_select_test: %s %s" % (str(rows), result))

    # check Pings
    utils.run_vtctl('Ping ' + tablet_62344.tablet_alias)
    utils.run_vtctl('RpcPing ' + tablet_62344.tablet_alias)

    utils.validate_topology()
    utils.run_vtctl('ValidateKeyspace test_keyspace')
    # not pinging tablets, as it enables replication checks, and they
    # break because we only have a single master, no slaves
    utils.run_vtctl('ValidateShard -ping-tablets=false test_keyspace/0')

  _create_vt_insert_test = '''create table vt_insert_test (
  id bigint auto_increment,
  msg varchar(64),
  primary key (id)
  ) Engine=InnoDB'''

  _populate_vt_insert_test = [
      "insert into vt_insert_test (msg) values ('test %s')" % x
      for x in xrange(4)]

  _create_vt_select_test = '''create table vt_select_test (
  id bigint auto_increment,
  msg varchar(64),
  primary key (id)
  ) Engine=InnoDB'''

  _populate_vt_select_test = [
      "insert into vt_select_test (msg) values ('test %s')" % x
      for x in xrange(4)]


if __name__ == '__main__':
  utils.main()
