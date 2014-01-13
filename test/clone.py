#!/usr/bin/python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import gzip
import logging
import os
import shutil
from subprocess import call
import unittest

import environment
import utils
import tablet

tablet_62344 = tablet.Tablet(62344)
tablet_62044 = tablet.Tablet(62044)

def setUpModule():
  try:
    environment.topo_server_setup()

    # start mysql instance external to the test
    setup_procs = [
        tablet_62344.init_mysql(),
        tablet_62044.init_mysql(),
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
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  tablet_62344.remove_tree()
  tablet_62044.remove_tree()

  path = os.path.join(environment.vtdataroot, 'snapshot')
  try:
    shutil.rmtree(path)
  except OSError as e:
    logging.debug("removing snapshot %s: %s", path, str(e))

class TestClone(unittest.TestCase):
  def tearDown(self):
    tablet.Tablet.check_vttablet_count()
    environment.topo_server_wipe()
    for t in [tablet_62344, tablet_62044]:
      t.reset_replication()
      t.clean_dbs()

  _create_vt_insert_test = '''create table vt_insert_test (
  id bigint auto_increment,
  msg varchar(64),
  primary key (id)
  ) Engine=InnoDB'''

  _populate_vt_insert_test = [
      "insert into vt_insert_test (msg) values ('test %s')" % x
      for x in xrange(4)]


  def _test_mysqlctl_clone(server_mode):
    if server_mode:
      snapshot_cmd = ['snapshotsourcestart', '-concurrency=8']
      restore_flags = ['-dont-wait-for-slave-start']
    else:
      snapshot_cmd = ['snapshot', '-concurrency=5']
      restore_flags = []

    # Start up a master mysql and vttablet
    utils.run_vtctl(['CreateKeyspace', 'snapshot_test'])

    tablet_62344.init_tablet('master', 'snapshot_test', '0')
    utils.run_vtctl(['RebuildShardGraph', 'snapshot_test/0'])
    utils.validate_topology()

    tablet_62344.populate('vt_snapshot_test', self._create_vt_insert_test,
                          self._populate_vt_insert_test)

    tablet_62344.start_vttablet()

    err = tablet_62344.mysqlctl(snapshot_cmd + ['vt_snapshot_test'],
                                with_ports=True).wait()
    if err != 0:
      self.fail('mysqlctl %s failed' % str(snapshot_cmd))

    utils.pause("%s finished" % str(snapshot_cmd))

    call(["touch", "/tmp/vtSimulateFetchFailures"])
    err = tablet_62044.mysqlctl(['restore',
                                 '-fetch-concurrency=2',
                                 '-fetch-retry-count=4'] +
                                restore_flags +
                                [environment.vtdataroot + '/snapshot/vt_0000062344/snapshot_manifest.json'],
                                with_ports=True).wait()
    if err != 0:
      self.fail('mysqlctl restore failed')

    tablet_62044.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)

    if server_mode:
      err = tablet_62344.mysqlctl(['snapshotsourceend',
                                   '-read-write',
                                   'vt_snapshot_test'], with_ports=True).wait()
      if err != 0:
        self.fail('mysqlctl snapshotsourceend failed')

      # see if server restarted properly
      tablet_62344.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)

    tablet_62344.kill_vttablet()

  # Subsumed by vtctl_clone* tests.
  def _test_mysqlctl_clone(self):
    self._test_mysqlctl_clone(False)

  # Subsumed by vtctl_clone* tests.
  def _test_mysqlctl_clone_server(self):
    self._test_mysqlctl_clone(True)

  def _test_vtctl_snapshot_restore(self, server_mode):
    if server_mode:
      snapshot_flags = ['-server-mode', '-concurrency=8']
      restore_flags = ['-dont-wait-for-slave-start']
    else:
      snapshot_flags = ['-concurrency=4']
      restore_flags = []

    # Start up a master mysql and vttablet
    utils.run_vtctl(['CreateKeyspace', 'snapshot_test'])

    tablet_62344.init_tablet('master', 'snapshot_test', '0')
    utils.run_vtctl(['RebuildShardGraph', 'snapshot_test/0'])
    utils.validate_topology()

    tablet_62344.populate('vt_snapshot_test', self._create_vt_insert_test,
                          self._populate_vt_insert_test)

    tablet_62044.create_db('vt_snapshot_test')

    tablet_62344.start_vttablet()

    # Need to force snapshot since this is a master db.
    out, err = utils.run_vtctl(['Snapshot', '-force'] + snapshot_flags +
                               [tablet_62344.tablet_alias], trap_output=True)
    results = {}
    for name in ['Manifest', 'ParentAlias', 'SlaveStartRequired', 'ReadOnly', 'OriginalType']:
      sepPos = err.find(name + ": ")
      if sepPos != -1:
        results[name] = err[sepPos+len(name)+2:].splitlines()[0]
    if "Manifest" not in results:
      self.fail("Snapshot didn't echo Manifest file: %s" % str(err))
    if "ParentAlias" not in results:
      self.fail("Snapshot didn't echo ParentAlias: %s" % str(err))
    utils.pause("snapshot finished: " + results['Manifest'] + " " + results['ParentAlias'])
    if server_mode:
      if "SlaveStartRequired" not in results:
        self.fail("Snapshot didn't echo SlaveStartRequired: %s" % err)
      if "ReadOnly" not in results:
        self.fail("Snapshot didn't echo ReadOnly %s" % err)
      if "OriginalType" not in results:
        self.fail("Snapshot didn't echo OriginalType: %s" % err)
      if (results['SlaveStartRequired'] != 'false' or
          results['ReadOnly'] != 'true' or
          results['OriginalType'] != 'master'):
        self.fail("Bad values returned by Snapshot: %s" % err)
    tablet_62044.init_tablet('idle', start=True)

    # do not specify a MANIFEST, see if 'default' works
    call(["touch", "/tmp/vtSimulateFetchFailures"])
    utils.run_vtctl(['Restore',
                     '-fetch-concurrency=2',
                     '-fetch-retry-count=4'] +
                    restore_flags +
                    [tablet_62344.tablet_alias, 'default',
                     tablet_62044.tablet_alias, results['ParentAlias']],
                    auto_log=True)
    utils.pause("restore finished")

    tablet_62044.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)

    utils.validate_topology()

    # in server_mode, get the server out of it and check it
    if server_mode:
      utils.run_vtctl(['SnapshotSourceEnd', tablet_62344.tablet_alias,
                       results['OriginalType']], auto_log=True)
      tablet_62344.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)
      utils.validate_topology()

    tablet.kill_tablets([tablet_62344, tablet_62044])

  # Subsumed by vtctl_clone* tests.
  def _test_vtctl_snapshot_restore(self):
    self._test_vtctl_snapshot_restore(server_mode=False)

  # Subsumed by vtctl_clone* tests.
  def _test_vtctl_snapshot_restore_server(self):
    self._test_vtctl_snapshot_restore(server_mode=True)

  def _test_vtctl_clone(self, server_mode):
    if server_mode:
      clone_flags = ['-server-mode']
    else:
      clone_flags = []

    # Start up a master mysql and vttablet
    utils.run_vtctl(['CreateKeyspace', 'snapshot_test'])

    tablet_62344.init_tablet('master', 'snapshot_test', '0')
    utils.run_vtctl(['RebuildShardGraph', 'snapshot_test/0'])
    utils.validate_topology()

    tablet_62344.populate('vt_snapshot_test', self._create_vt_insert_test,
                          self._populate_vt_insert_test)
    tablet_62344.start_vttablet()

    tablet_62044.create_db('vt_snapshot_test')
    tablet_62044.init_tablet('idle', start=True)

    # small test to make sure the directory validation works
    snapshot_dir = os.path.join(environment.vtdataroot, 'snapshot')
    utils.run("rm -rf %s" % snapshot_dir)
    utils.run("mkdir -p %s" % snapshot_dir)
    utils.run("chmod -w %s" % snapshot_dir)
    out, err = utils.run_vtctl(['Clone', '-force'] + clone_flags +
                               [tablet_62344.tablet_alias,
                                tablet_62044.tablet_alias],
                               log_level='INFO', expect_fail=True)
    if "Cannot validate snapshot directory" not in err:
      self.fail("expected validation error: %s" % err)
    if "Un-reserved test_nj-0000062044" not in err:
      self.fail("expected Un-reserved: %s" % err)
    logging.debug("Failed Clone output: " + err)
    utils.run("chmod +w %s" % snapshot_dir)

    call(["touch", "/tmp/vtSimulateFetchFailures"])
    utils.run_vtctl(['Clone', '-force'] + clone_flags +
                    [tablet_62344.tablet_alias, tablet_62044.tablet_alias],
                    auto_log=True)

    utils.pause("look at logs!")
    tablet_62044.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)
    tablet_62344.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)

    utils.validate_topology()

    tablet.kill_tablets([tablet_62344, tablet_62044])

  def test_vtctl_clone(self):
    self._test_vtctl_clone(server_mode=False)

  def test_vtctl_clone_server(self):
    self._test_vtctl_clone(server_mode=True)

  # this test is useful to validate the table specification code works.
  # it will be replaced soon by a vertical split test in resharding.
  def test_multisnapshot_vtctl(self):
    populate = sum([[
      "insert into vt_insert_test_%s (msg) values ('test %s')" % (i, x)
      for x in xrange(4)] for i in range(6)], [])
    create = ['''create table vt_insert_test_%s (
  id bigint auto_increment,
  msg varchar(64),
  primary key (id)
  ) Engine=InnoDB''' % i for i in range(6)]

    # Start up a master mysql and vttablet
    utils.run_vtctl(['CreateKeyspace',
                     '--sharding_column_name', 'id',
                     '--sharding_column_type', 'uint64',
                     'test_keyspace'])

    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    utils.run_vtctl(['RebuildShardGraph', 'test_keyspace/0'])
    utils.validate_topology()

    tablet_62344.populate('vt_test_keyspace', create,
                          populate)

    tablet_62344.start_vttablet()

    utils.run_vtctl(['MultiSnapshot', '--force', '--tables=vt_insert_test_1,vt_insert_test_2,vt_insert_test_3', '--spec=-0000000000000003-', tablet_62344.tablet_alias])

    if os.path.exists(os.path.join(environment.vtdataroot, 'snapshot/vt_0000062344/data/vt_test_keyspace-,0000000000000003/vt_insert_test_4.0.csv.gz')):
      self.fail("Table vt_insert_test_4 wasn't supposed to be dumped.")
    for kr in 'vt_test_keyspace-,0000000000000003', 'vt_test_keyspace-0000000000000003,':
      path = os.path.join(environment.vtdataroot, 'snapshot/vt_0000062344/data/', kr, 'vt_insert_test_1.0.csv.gz')
      with gzip.open(path) as f:
        if len(f.readlines()) != 2:
          self.fail("Data looks wrong in %s" % path)

    tablet_62344.kill_vttablet()

if __name__ == '__main__':
  utils.main()
