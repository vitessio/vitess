#!/usr/bin/python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import gzip
import logging
import os
import shutil
import signal
from subprocess import PIPE, call
import time
import unittest

import MySQLdb

import utils
import tablet
from vtdb import vtgate

tablet_62344 = tablet.Tablet(62344)
tablet_62044 = tablet.Tablet(62044)
tablet_41983 = tablet.Tablet(41983)
tablet_31981 = tablet.Tablet(31981)

def setUpModule():
  try:
    utils.zk_setup(add_bad_host=True)

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

  def _check_db_addr(self, db_addr, expected_addr):
    # Run in the background to capture output.
    proc = utils.run_bg(utils.vtroot+'/bin/vtctl -log_dir '+utils.tmp_root+' --alsologtostderr -zk.local-cell=test_nj Resolve ' + db_addr, stdout=PIPE)
    stdout = proc.communicate()[0].strip()
    self.assertEqual(stdout, expected_addr, 'wrong zk address for %s: %s expected: %s' % (db_addr, stdout, expected_addr))

  # run twice to check behavior with existing znode data
  def test_sanity(self):
    self._test_sanity()
    self._test_sanity()

  def _test_sanity(self):
    # Start up a master mysql and vttablet
    utils.run_vtctl('CreateKeyspace -force test_keyspace')
    utils.run_vtctl('createshard -force test_keyspace/0')
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

    # Quickly check basic actions.
    utils.run_vtctl('SetReadOnly ' + tablet_62344.tablet_alias)
    utils.wait_db_read_only(62344)

    utils.run_vtctl('SetReadWrite ' + tablet_62344.tablet_alias)
    utils.check_db_read_write(62344)

    utils.run_vtctl('DemoteMaster ' + tablet_62344.tablet_alias)
    utils.wait_db_read_only(62344)

    utils.validate_topology()
    utils.run_vtctl('ValidateKeyspace test_keyspace')
    # not pinging tablets, as it enables replication checks, and they
    # break because we only have a single master, no slaves
    utils.run_vtctl('ValidateShard -ping-tablets=false test_keyspace/0')

    tablet_62344.kill_vttablet()

    tablet_62344.init_tablet('idle')
    tablet_62344.scrap(force=True)

  def test_vtgate(self):
    # Start up a master mysql and vttablet
    utils.run_vtctl('CreateKeyspace -force test_keyspace')
    utils.run_vtctl('CreateShard -force test_keyspace/0')
    tablet_62344.init_tablet('master', 'test_keyspace', '0', parent=False)
    utils.run_vtctl('RebuildShardGraph test_keyspace/0')
    utils.run_vtctl('RebuildKeyspaceGraph test_keyspace')
    utils.validate_topology()

    # if these statements don't run before the tablet it will wedge waiting for the
    # db to become accessible. this is more a bug than a feature.
    tablet_62344.mquery("", ["set global read_only = off"])
    tablet_62344.populate('vt_test_keyspace', self._create_vt_select_test,
                          self._populate_vt_select_test)

    tablet_62344.start_vttablet()
    gate_proc, gate_port = utils.vtgate_start()

    try:
      conn = vtgate.connect("localhost:%s"%(gate_port), "master", "test_keyspace", "0", 2.0)

      # _execute
      (result, count, lastrow, fields) = conn._execute("select * from vt_select_test", {})
      self.assertEqual(count, 4, "want 4, got %d" % (count))
      self.assertEqual(len(fields), 2, "want 2, got %d" % (len(fields)))

      # _execute_batch
      queries = [
          "select * from vt_select_test where id = :id",
          "select * from vt_select_test where id = :id",
      ]
      bindvars = [
          {"id": 1},
          {"id": 2},
      ]
      rowsets = conn._execute_batch(queries, bindvars)
      self.assertEqual(rowsets[0][0][0][0], 1)
      self.assertEqual(rowsets[1][0][0][0], 2)

      # _stream_execute
      (result, count, lastrow, fields) = conn._stream_execute("select * from vt_select_test", {})
      self.assertEqual(len(fields), 2, "want 2, got %d" % (len(fields)))
      count = 0
      while 1:
        r = conn._stream_next()
        if not r:
          break
        count += 1
      self.assertEqual(count, 4, "want 4, got %d" % (count))

      # begin-rollback
      conn.begin()
      conn._execute("insert into vt_select_test values(:id, :msg)", {"id": 5, "msg": "test4"})
      conn.rollback()
      (result, count, lastrow, fields) = conn._execute("select * from vt_select_test", {})
      self.assertEqual(count, 4, "want 4, got %d" % (count))

      # begin-commit
      conn.begin()
      conn._execute("insert into vt_select_test values(:id, :msg)", {"id": 5, "msg": "test4"})
      conn.commit()
      (result, count, lastrow, fields) = conn._execute("select * from vt_select_test", {})
      self.assertEqual(count, 5, "want 5, got %d" % (count))

      # close
      conn.close()
    finally:
      utils.vtgate_kill(gate_proc)
      tablet_62344.kill_vttablet()

  def test_scrap(self):
    # Start up a master mysql and vttablet
    utils.run_vtctl('CreateKeyspace test_keyspace')

    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    tablet_62044.init_tablet('replica', 'test_keyspace', '0')
    utils.run_vtctl('RebuildShardGraph test_keyspace/0')
    utils.validate_topology()

    tablet_62044.scrap(force=True)
    utils.validate_topology()


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


  def _test_mysqlctl_clone(server_mode):
    if server_mode:
      snapshot_cmd = "snapshotsourcestart -concurrency=8"
      restore_flags = "-dont-wait-for-slave-start"
    else:
      snapshot_cmd = "snapshot -concurrency=5"
      restore_flags = ""

    # Start up a master mysql and vttablet
    utils.run_vtctl('CreateKeyspace snapshot_test')

    tablet_62344.init_tablet('master', 'snapshot_test', '0')
    utils.run_vtctl('RebuildShardGraph snapshot_test/0')
    utils.validate_topology()

    tablet_62344.populate('vt_snapshot_test', self._create_vt_insert_test,
                          self._populate_vt_insert_test)

    tablet_62344.start_vttablet()

    err = tablet_62344.mysqlctl('-port %u -mysql-port %u %s vt_snapshot_test' % (tablet_62344.port, tablet_62344.mysql_port, snapshot_cmd)).wait()
    if err != 0:
      self.fail('mysqlctl %s failed' % snapshot_cmd)

    utils.pause("%s finished" % snapshot_cmd)

    call(["touch", "/tmp/vtSimulateFetchFailures"])
    err = tablet_62044.mysqlctl('-port %u -mysql-port %u restore -fetch-concurrency=2 -fetch-retry-count=4 %s %s/snapshot/vt_0000062344/snapshot_manifest.json' % (tablet_62044.port, tablet_62044.mysql_port, restore_flags, utils.vtdataroot)).wait()
    if err != 0:
      self.fail('mysqlctl restore failed')

    tablet_62044.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)

    if server_mode:
      err = tablet_62344.mysqlctl('-port %u -mysql-port %u snapshotsourceend -read-write vt_snapshot_test' % (tablet_62344.port, tablet_62344.mysql_port)).wait()
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
      snapshot_flags = '-server-mode -concurrency=8'
      restore_flags = '-dont-wait-for-slave-start'
    else:
      snapshot_flags = '-concurrency=4'
      restore_flags = ''

    # Start up a master mysql and vttablet
    utils.run_vtctl('CreateKeyspace snapshot_test')

    tablet_62344.init_tablet('master', 'snapshot_test', '0')
    utils.run_vtctl('RebuildShardGraph snapshot_test/0')
    utils.validate_topology()

    tablet_62344.populate('vt_snapshot_test', self._create_vt_insert_test,
                          self._populate_vt_insert_test)

    tablet_62044.create_db('vt_snapshot_test')

    tablet_62344.start_vttablet()

    # Need to force snapshot since this is a master db.
    out, err = utils.run_vtctl('Snapshot -force %s %s ' % (snapshot_flags, tablet_62344.tablet_alias), trap_output=True)
    results = {}
    for name in ['Manifest', 'ParentAlias', 'SlaveStartRequired', 'ReadOnly', 'OriginalType']:
      sepPos = err.find(name + ": ")
      if sepPos != -1:
        results[name] = err[sepPos+len(name)+2:].splitlines()[0]
    if "Manifest" not in results:
      raise utils.TestError("Snapshot didn't echo Manifest file", err)
    if "ParentAlias" not in results:
      raise utils.TestError("Snapshot didn't echo ParentAlias", err)
    utils.pause("snapshot finished: " + results['Manifest'] + " " + results['ParentAlias'])
    if server_mode:
      if "SlaveStartRequired" not in results:
        raise utils.TestError("Snapshot didn't echo SlaveStartRequired", err)
      if "ReadOnly" not in results:
        raise utils.TestError("Snapshot didn't echo ReadOnly", err)
      if "OriginalType" not in results:
        raise utils.TestError("Snapshot didn't echo OriginalType", err)
      if (results['SlaveStartRequired'] != 'false' or
          results['ReadOnly'] != 'true' or
          results['OriginalType'] != 'master'):
        raise utils.TestError("Bad values returned by Snapshot", err)
    tablet_62044.init_tablet('idle', start=True)

    # do not specify a MANIFEST, see if 'default' works
    call(["touch", "/tmp/vtSimulateFetchFailures"])
    utils.run_vtctl('Restore -fetch-concurrency=2 -fetch-retry-count=4 %s %s default %s %s' %
                    (restore_flags, tablet_62344.tablet_alias,
                     tablet_62044.tablet_alias, results['ParentAlias']), auto_log=True)
    utils.pause("restore finished")

    tablet_62044.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)

    utils.validate_topology()

    # in server_mode, get the server out of it and check it
    if server_mode:
      utils.run_vtctl('SnapshotSourceEnd %s %s' % (tablet_62344.tablet_alias, results['OriginalType']), auto_log=True)
      tablet_62344.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)
      utils.validate_topology()

    tablet_62344.kill_vttablet()
    tablet_62044.kill_vttablet()

  # Subsumed by vtctl_clone* tests.
  def _test_vtctl_snapshot_restore(self):
    self._test_vtctl_snapshot_restore(server_mode=False)

  # Subsumed by vtctl_clone* tests.
  def _test_vtctl_snapshot_restore_server(self):
    self._test_vtctl_snapshot_restore(server_mode=True)

  def _test_vtctl_clone(self, server_mode):
    if server_mode:
      clone_flags = '-server-mode'
    else:
      clone_flags = ''

    # Start up a master mysql and vttablet
    utils.run_vtctl('CreateKeyspace snapshot_test')

    tablet_62344.init_tablet('master', 'snapshot_test', '0')
    utils.run_vtctl('RebuildShardGraph snapshot_test/0')
    utils.validate_topology()

    tablet_62344.populate('vt_snapshot_test', self._create_vt_insert_test,
                          self._populate_vt_insert_test)
    tablet_62344.start_vttablet()

    tablet_62044.create_db('vt_snapshot_test')
    tablet_62044.init_tablet('idle', start=True)

    # small test to make sure the directory validation works
    snapshot_dir = os.path.join(utils.vtdataroot, 'snapshot')
    utils.run("rm -rf %s" % snapshot_dir)
    utils.run("mkdir -p %s" % snapshot_dir)
    utils.run("chmod -w %s" % snapshot_dir)
    out, err = utils.run('%s/bin/vtctl -log_dir %s --alsologtostderr Clone -force %s %s %s' %
                         (utils.vtroot, utils.tmp_root,
                          clone_flags, tablet_62344.tablet_alias,
                          tablet_62044.tablet_alias),
                         trap_output=True, raise_on_error=False)
    if "Cannot validate snapshot directory" not in err:
      raise utils.TestError("expected validation error", err)
    if "Un-reserved test_nj-0000062044" not in err:
      raise utils.TestError("expected Un-reserved", err)
    logging.debug("Failed Clone output: " + err)
    utils.run("chmod +w %s" % snapshot_dir)

    call(["touch", "/tmp/vtSimulateFetchFailures"])
    utils.run_vtctl('Clone -force %s %s %s' %
                    (clone_flags, tablet_62344.tablet_alias,
                     tablet_62044.tablet_alias), auto_log=True)

    utils.pause("look at logs!")
    tablet_62044.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)
    tablet_62344.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)

    utils.validate_topology()

    tablet_62344.kill_vttablet()
    tablet_62044.kill_vttablet()

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
    utils.run_vtctl('CreateKeyspace test_keyspace')

    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    utils.run_vtctl('RebuildShardGraph test_keyspace/0')
    utils.validate_topology()

    tablet_62344.populate('vt_test_keyspace', create,
                          populate)

    tablet_62344.start_vttablet()

    utils.run_vtctl('MultiSnapshot --force --tables=vt_insert_test_1,vt_insert_test_2,vt_insert_test_3 --spec=-0000000000000003- %s id' % tablet_62344.tablet_alias)

    if os.path.exists(os.path.join(utils.vtdataroot, 'snapshot/vt_0000062344/data/vt_test_keyspace-,0000000000000003/vt_insert_test_4.0.csv.gz')):
      raise utils.TestError("Table vt_insert_test_4 wasn't supposed to be dumped.")
    for kr in 'vt_test_keyspace-,0000000000000003', 'vt_test_keyspace-0000000000000003,':
      path = os.path.join(utils.vtdataroot, 'snapshot/vt_0000062344/data/', kr, 'vt_insert_test_1.0.csv.gz')
      with gzip.open(path) as f:
        if len(f.readlines()) != 2:
          raise utils.TestError("Data looks wrong in %s" % path)

    tablet_62344.kill_vttablet()

  def test_restart_during_action(self):
    # Start up a master mysql and vttablet
    utils.run_vtctl('CreateKeyspace test_keyspace')

    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    utils.run_vtctl('RebuildShardGraph test_keyspace/0')
    utils.validate_topology()
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62344.start_vttablet()

    utils.run_vtctl('Ping ' + tablet_62344.tablet_alias)

    # schedule long action
    utils.run_vtctl('-no-wait Sleep %s 15s' % tablet_62344.tablet_alias, stdout=utils.devnull)
    # ping blocks until the sleep finishes unless we have a schedule race
    action_path, _ = utils.run_vtctl('-no-wait Ping ' + tablet_62344.tablet_alias, trap_output=True)

    # kill agent leaving vtaction running
    tablet_62344.kill_vttablet()

    # restart agent
    tablet_62344.start_vttablet()

    # we expect this action with a short wait time to fail. this isn't the best
    # and has some potential for flakiness.
    utils.run_fail(utils.vtroot+'/bin/vtctl -log_dir '+utils.tmp_root+' --alsologtostderr -wait-time 2s WaitForAction ' + action_path)

    # wait until the background sleep action is done, otherwise there will be
    # a leftover vtaction whose result may overwrite running actions
    # NOTE(alainjobart): Yes, I've seen it happen, it's a pain to debug:
    # the zombie Sleep clobbers the Clone command in the following tests
    utils.run_vtctl('-wait-time 20s WaitForAction ' + action_path,
                    auto_log=True)

    # extra small test: we ran for a while, get the states we were in,
    # make sure they're accounted for properly
    # first the query engine States
    v = utils.get_vars(tablet_62344.port)
    logging.debug("vars: %s" % str(v))
    if v['Voltron']['States']['DurationSERVING'] < 10e9:
      raise utils.TestError('not enough time in Open state', v['Voltron']['States']['DurationSERVING'])
    # then the Zookeeper connections
    if v['ZkMetaConn']['test_nj']['Current'] != 'Connected':
      raise utils.TestError('invalid zk test_nj state: ', v['ZkMetaConn']['test_nj']['Current'])
    if v['ZkMetaConn']['global']['Current'] != 'Connected':
      raise utils.TestError('invalid zk global state: ', v['ZkMetaConn']['global']['Current'])
    if v['ZkMetaConn']['test_nj']['DurationConnected'] < 10e9:
      raise utils.TestError('not enough time in Connected state', v['ZkMetaConn']['test_nj']['DurationConnected'])
    if v['TabletType'] != 'master':
      raise utils.TestError('TabletType not exported correctly')

    tablet_62344.kill_vttablet()

  def test_reparent_down_master(self):
    utils.run_vtctl('CreateKeyspace test_keyspace')

    # create the database so vttablets start, as they are serving
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62044.create_db('vt_test_keyspace')
    tablet_41983.create_db('vt_test_keyspace')
    tablet_31981.create_db('vt_test_keyspace')

    # Start up a master mysql and vttablet
    tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True)

    # Create a few slaves for testing reparenting.
    tablet_62044.init_tablet('replica', 'test_keyspace', '0', start=True)
    tablet_41983.init_tablet('replica', 'test_keyspace', '0', start=True)
    tablet_31981.init_tablet('replica', 'test_keyspace', '0', start=True)

    # Recompute the shard layout node - until you do that, it might not be valid.
    utils.run_vtctl('RebuildShardGraph test_keyspace/0')
    utils.validate_topology()

    # Force the slaves to reparent assuming that all the datasets are identical.
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.run_vtctl('ReparentShard -force test_keyspace/0 ' + tablet_62344.tablet_alias, auto_log=True)
    utils.validate_topology()

    # Make the master agent and database unavailable.
    tablet_62344.kill_vttablet()
    tablet_62344.shutdown_mysql().wait()

    expected_addr = utils.hostname + ':' + str(tablet_62344.port)
    self._check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

    # Perform a reparent operation - the Validate part will try to ping
    # the master and fail somewhat quickly
    stdout, stderr = utils.run_fail(utils.vtroot+'/bin/vtctl -log_dir '+utils.tmp_root+' --alsologtostderr -wait-time 5s ReparentShard test_keyspace/0 ' + tablet_62044.tablet_alias)
    logging.debug("Failed ReparentShard output:\n" + stderr)
    if 'ValidateShard verification failed: timed out during validate' not in stderr:
      raise utils.TestError("didn't find the right error strings in failed ReparentShard: " + stderr)

    # Should timeout and fail
    stdout, stderr = utils.run_fail(utils.vtroot+'/bin/vtctl -log_dir '+utils.tmp_root+' --alsologtostderr -wait-time 5s ScrapTablet ' + tablet_62344.tablet_alias)
    logging.debug("Failed ScrapTablet output:\n" + stderr)
    if 'deadline exceeded' not in stderr:
      raise utils.TestError("didn't find the right error strings in failed ScrapTablet: " + stderr)

    # Should interrupt and fail
    sp = utils.run_bg(utils.vtroot+'/bin/vtctl -log_dir '+utils.tmp_root+' -wait-time 10s ScrapTablet ' + tablet_62344.tablet_alias, stdout=PIPE, stderr=PIPE)
    # Need time for the process to start before killing it.
    time.sleep(3.0)
    os.kill(sp.pid, signal.SIGINT)
    stdout, stderr = sp.communicate()

    logging.debug("Failed ScrapTablet output:\n" + stderr)
    if 'interrupted' not in stderr:
      raise utils.TestError("didn't find the right error strings in failed ScrapTablet: " + stderr)

    # Force the scrap action in zk even though tablet is not accessible.
    tablet_62344.scrap(force=True)

    utils.run_fail(utils.vtroot+'/bin/vtctl -log_dir '+utils.tmp_root+' --alsologtostderr ChangeSlaveType -force %s idle' %
                   tablet_62344.tablet_alias)

    # Remove pending locks (make this the force option to ReparentShard?)
    utils.run_vtctl('PurgeActions /zk/global/vt/keyspaces/test_keyspace/shards/0/action')

    # Re-run reparent operation, this shoud now proceed unimpeded.
    utils.run_vtctl('-wait-time 1m ReparentShard test_keyspace/0 ' + tablet_62044.tablet_alias, auto_log=True)

    utils.validate_topology()
    expected_addr = utils.hostname + ':' + str(tablet_62044.port)
    self._check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

    utils.run_vtctl(['ChangeSlaveType', '-force', tablet_62344.tablet_alias, 'idle'])

    idle_tablets, _ = utils.run_vtctl('ListAllTablets test_nj', trap_output=True)
    if '0000062344 <null> <null> idle' not in idle_tablets:
      raise utils.TestError('idle tablet not found', idle_tablets)

    tablet_62044.kill_vttablet()
    tablet_41983.kill_vttablet()
    tablet_31981.kill_vttablet()

    # sothe other tests don't have any surprise
    tablet_62344.start_mysql().wait()


  def test_reparent_graceful_range_based(self):
    shard_id = '0000000000000000-FFFFFFFFFFFFFFFF'
    self._test_reparent_graceful(shard_id)

  def test_reparent_graceful(self):
    shard_id = '0'
    self._test_reparent_graceful(shard_id)

  def _test_reparent_graceful(self, shard_id):
    utils.run_vtctl('CreateKeyspace test_keyspace')

    # create the database so vttablets start, as they are serving
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62044.create_db('vt_test_keyspace')
    tablet_41983.create_db('vt_test_keyspace')
    tablet_31981.create_db('vt_test_keyspace')

    # Start up a master mysql and vttablet
    tablet_62344.init_tablet('master', 'test_keyspace', shard_id, start=True)
    shard = utils.zk_cat_json('/zk/global/vt/keyspaces/test_keyspace/shards/' + shard_id)
    self.assertEqual(shard['Cells'], ['test_nj'], 'wrong list of cell in Shard: %s' % str(shard['Cells']))

    # Create a few slaves for testing reparenting.
    tablet_62044.init_tablet('replica', 'test_keyspace', shard_id, start=True)
    tablet_41983.init_tablet('replica', 'test_keyspace', shard_id, start=True)
    tablet_31981.init_tablet('replica', 'test_keyspace', shard_id, start=True)
    shard = utils.zk_cat_json('/zk/global/vt/keyspaces/test_keyspace/shards/' + shard_id)
    self.assertEqual(shard['Cells'], ['test_nj', 'test_ny'], 'wrong list of cell in Shard: %s' % str(shard['Cells']))

    # Recompute the shard layout node - until you do that, it might not be valid.
    utils.run_vtctl('RebuildShardGraph test_keyspace/' + shard_id)
    utils.validate_topology()

    # Force the slaves to reparent assuming that all the datasets are identical.
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.pause("force ReparentShard?")
    utils.run_vtctl('ReparentShard -force test_keyspace/%s %s' % (shard_id, tablet_62344.tablet_alias))
    utils.validate_topology(ping_tablets=True)

    expected_addr = utils.hostname + ':' + str(tablet_62344.port)
    self._check_db_addr('test_keyspace.%s.master:_vtocc' % shard_id, expected_addr)

    # Convert two replica to spare. That should leave only one node serving traffic,
    # but still needs to appear in the replication graph.
    utils.run_vtctl(['ChangeSlaveType', tablet_41983.tablet_alias, 'spare'])
    utils.run_vtctl(['ChangeSlaveType', tablet_31981.tablet_alias, 'spare'])
    utils.validate_topology()
    expected_addr = utils.hostname + ':' + str(tablet_62044.port)
    self._check_db_addr('test_keyspace.%s.replica:_vtocc' % shard_id, expected_addr)

    # Run this to make sure it succeeds.
    utils.run_vtctl('ShardReplicationPositions test_keyspace/%s' % shard_id, stdout=utils.devnull)

    # Perform a graceful reparent operation.
    utils.pause("graceful ReparentShard?")
    utils.run_vtctl('ReparentShard test_keyspace/%s %s' % (shard_id, tablet_62044.tablet_alias), auto_log=True)
    utils.validate_topology()

    expected_addr = utils.hostname + ':' + str(tablet_62044.port)
    self._check_db_addr('test_keyspace.%s.master:_vtocc' % shard_id, expected_addr)

    tablet_62344.kill_vttablet()
    tablet_62044.kill_vttablet()
    tablet_41983.kill_vttablet()
    tablet_31981.kill_vttablet()

    # Test address correction.
    new_port = utils.reserve_ports(1)
    tablet_62044.start_vttablet(port=new_port)
    # Wait a moment for address to reregister.
    time.sleep(1.0)

    expected_addr = utils.hostname + ':' + str(new_port)
    self._check_db_addr('test_keyspace.%s.master:_vtocc' % shard_id, expected_addr)

    tablet_62044.kill_vttablet()


  # This is a manual test to check error formatting.
  def _test_reparent_slave_offline(self, shard_id='0'):
    utils.run_vtctl('CreateKeyspace test_keyspace')

    # create the database so vttablets start, as they are serving
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62044.create_db('vt_test_keyspace')
    tablet_41983.create_db('vt_test_keyspace')
    tablet_31981.create_db('vt_test_keyspace')

    # Start up a master mysql and vttablet
    tablet_62344.init_tablet('master', 'test_keyspace', shard_id, start=True)

    # Create a few slaves for testing reparenting.
    tablet_62044.init_tablet('replica', 'test_keyspace', shard_id, start=True)
    tablet_41983.init_tablet('replica', 'test_keyspace', shard_id, start=True)
    tablet_31981.init_tablet('replica', 'test_keyspace', shard_id, start=True)

    # Recompute the shard layout node - until you do that, it might not be valid.
    utils.run_vtctl('RebuildShardGraph test_keyspace/' + shard_id)
    utils.validate_topology()

    # Force the slaves to reparent assuming that all the datasets are identical.
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.run_vtctl('ReparentShard -force test_keyspace/%s %s' % (shard_id, tablet_62344.tablet_alias))
    utils.validate_topology(ping_tablets=True)

    expected_addr = utils.hostname + ':' + str(tablet_62344.port)
    self._check_db_addr('test_keyspace.%s.master:_vtocc' % shard_id, expected_addr)

    # Kill one tablet so we seem offline
    tablet_31981.kill_vttablet()

    # Perform a graceful reparent operation.
    utils.run_vtctl('ReparentShard test_keyspace/%s %s' % (shard_id, tablet_62044.tablet_alias))

    tablet_62344.kill_vttablet()
    tablet_62044.kill_vttablet()
    tablet_41983.kill_vttablet()


  # assume a different entity is doing the reparent, and telling us it was done
  def test_reparent_from_outside(self):
    self._test_reparent_from_outside(False)

  def test_reparent_from_outside_brutal(self):
    self._test_reparent_from_outside(True)

  def _test_reparent_from_outside(self, brutal=False):
    utils.run_vtctl('CreateKeyspace test_keyspace')

    # create the database so vttablets start, as they are serving
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.create_db('vt_test_keyspace')

    # Start up a master mysql and vttablet
    tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True)

    # Create a few slaves for testing reparenting.
    tablet_62044.init_tablet('replica', 'test_keyspace', '0', start=True)
    tablet_41983.init_tablet('replica', 'test_keyspace', '0', start=True)
    tablet_31981.init_tablet('replica', 'test_keyspace', '0', start=True)

    # Reparent as a starting point
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.run_vtctl('ReparentShard -force test_keyspace/0 %s' % tablet_62344.tablet_alias, auto_log=True)

    # now manually reparent 1 out of 2 tablets
    # 62044 will be the new master
    # 31981 won't be re-parented, so it will be busted
    tablet_62044.mquery('', [
        "RESET MASTER",
        "STOP SLAVE",
        "RESET SLAVE",
        "CHANGE MASTER TO MASTER_HOST = ''",
        ])
    new_pos = tablet_62044.mquery('', 'show master status')
    logging.debug("New master position: %s" % str(new_pos))

    # 62344 will now be a slave of 62044
    tablet_62344.mquery('', [
        "RESET MASTER",
        "RESET SLAVE",
        "change master to master_host='%s', master_port=%u, master_log_file='%s', master_log_pos=%u" % (utils.hostname, tablet_62044.mysql_port, new_pos[0][0], new_pos[0][1]),
        'start slave'
        ])

    # 41983 will be a slave of 62044
    tablet_41983.mquery('', [
        'stop slave',
        "change master to master_port=%u, master_log_file='%s', master_log_pos=%u" % (tablet_62044.mysql_port, new_pos[0][0], new_pos[0][1]),
        'start slave'
        ])

    # in brutal mode, we scrap the old master first
    if brutal:
      tablet_62344.scrap(force=True)
      # we have some automated tools that do this too, so it's good to simulate
      utils.run(utils.vtroot+'/bin/zk rm -rf ' + tablet_62344.zk_tablet_path)

    # update zk with the new graph
    utils.run_vtctl('ShardExternallyReparented -scrap-stragglers test_keyspace/0 %s' % tablet_62044.tablet_alias, auto_log=True)

    self._test_reparent_from_outside_check(brutal)

    utils.run_vtctl('RebuildReplicationGraph test_nj test_keyspace')

    self._test_reparent_from_outside_check(brutal)

    tablet_31981.kill_vttablet()
    tablet_62344.kill_vttablet()
    tablet_62044.kill_vttablet()
    tablet_41983.kill_vttablet()

  def _test_reparent_from_outside_check(self, brutal):
    # make sure the shard replication graph is fine
    shard_replication = utils.zk_cat_json('/zk/test_nj/vt/replication/test_keyspace/0')
    hashed_links = {}
    for rl in shard_replication['ReplicationLinks']:
      key = rl['TabletAlias']['Cell'] + "-" + str(rl['TabletAlias']['Uid'])
      value = rl['Parent']['Cell'] + "-" + str(rl['Parent']['Uid'])
      hashed_links[key] = value
    logging.debug("Got replication links: %s", str(hashed_links))
    expected_links = { 'test_nj-41983': 'test_nj-62044' }
    if not brutal:
      expected_links['test_nj-62344'] = 'test_nj-62044'
    self.assertEqual(expected_links, hashed_links, "Got unexpected links: %s != %s" % (str(expected_links), str(hashed_links)))

  # See if a lag slave can be safely reparent.
  def test_reparent_lag_slave(self, shard_id='0'):
    utils.run_vtctl('CreateKeyspace test_keyspace')

    # create the database so vttablets start, as they are serving
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62044.create_db('vt_test_keyspace')
    tablet_41983.create_db('vt_test_keyspace')
    tablet_31981.create_db('vt_test_keyspace')

    # Start up a master mysql and vttablet
    tablet_62344.init_tablet('master', 'test_keyspace', shard_id, start=True)

    # Create a few slaves for testing reparenting.
    tablet_62044.init_tablet('replica', 'test_keyspace', shard_id, start=True)
    tablet_31981.init_tablet('replica', 'test_keyspace', shard_id, start=True)
    tablet_41983.init_tablet('lag', 'test_keyspace', shard_id, start=True)

    # Recompute the shard layout node - until you do that, it might not be valid.
    utils.run_vtctl('RebuildShardGraph test_keyspace/' + shard_id)
    utils.validate_topology()

    # Force the slaves to reparent assuming that all the datasets are identical.
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.run_vtctl('ReparentShard -force test_keyspace/%s %s' % (shard_id, tablet_62344.tablet_alias))
    utils.validate_topology(ping_tablets=True)

    tablet_62344.create_db('vt_test_keyspace')
    tablet_62344.mquery('vt_test_keyspace', self._create_vt_insert_test)

    tablet_41983.mquery('', 'stop slave')
    for q in self._populate_vt_insert_test:
      tablet_62344.mquery('vt_test_keyspace', q, write=True)

    # Perform a graceful reparent operation.
    utils.run_vtctl('ReparentShard test_keyspace/%s %s' % (shard_id, tablet_62044.tablet_alias))

    tablet_41983.mquery('', 'start slave')
    time.sleep(1)

    utils.pause("check orphan")

    utils.run_vtctl('ReparentTablet %s' % tablet_41983.tablet_alias)

    result = tablet_41983.mquery('vt_test_keyspace', 'select msg from vt_insert_test where id=1')
    if len(result) != 1:
      raise utils.TestError('expected 1 row from vt_insert_test', result)

    utils.pause("check lag reparent")

    tablet_62344.kill_vttablet()
    tablet_62044.kill_vttablet()
    tablet_41983.kill_vttablet()
    tablet_31981.kill_vttablet()



  def test_vttablet_authenticated(self):
    utils.run_vtctl('CreateKeyspace test_keyspace')
    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    utils.run_vtctl('RebuildShardGraph test_keyspace/0')
    utils.validate_topology()

    tablet_62344.populate('vt_test_keyspace', self._create_vt_select_test,
                          self._populate_vt_select_test)
    tablet_62344.start_vttablet(auth=True)
    utils.run_vtctl('SetReadWrite ' + tablet_62344.tablet_alias)

    err, out = tablet_62344.vquery('select * from vt_select_test', path='test_keyspace/0', user='ala', password=r'ma kota')
    logging.debug("Got rows: " + out)
    if 'Row count: ' not in out:
      raise utils.TestError("query didn't go through: %s, %s" % (err, out))

    tablet_62344.kill_vttablet()
    # TODO(szopa): Test that non-authenticated queries do not pass
    # through (when we get to that point).

  def _check_string_in_hook_result(self, text, expected):
    if isinstance(expected, basestring):
      expected = [expected]
    for exp in expected:
      if exp in text:
        return
    logging.warning("ExecuteHook output:\n%s", text)
    raise utils.TestError("ExecuteHook returned unexpected result, no string: '" + "', '".join(expected) + "'")

  def _run_hook(self, params, expectedStrings):
    out, err = utils.run(utils.vtroot+'/bin/vtctl -log_dir '+utils.tmp_root+' --alsologtostderr ExecuteHook %s %s' % (tablet_62344.tablet_alias, params), trap_output=True, raise_on_error=False)
    for expected in expectedStrings:
      self._check_string_in_hook_result(err, expected)

  def test_hook(self):
    utils.run_vtctl('CreateKeyspace test_keyspace')

    # create the database so vttablets start, as it is serving
    tablet_62344.create_db('vt_test_keyspace')

    tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True)

    # test a regular program works
    self._run_hook("test.sh --flag1 --param1=hello", [
        '"ExitStatus": 0',
        ['"Stdout": "TABLET_ALIAS: test_nj-0000062344\\nPARAM: --flag1\\nPARAM: --param1=hello\\n"',
         '"Stdout": "TABLET_ALIAS: test_nj-0000062344\\nPARAM: --param1=hello\\nPARAM: --flag1\\n"',
         ],
        '"Stderr": ""',
        ])

    # test stderr output
    self._run_hook("test.sh --to-stderr", [
        '"ExitStatus": 0',
        '"Stdout": "TABLET_ALIAS: test_nj-0000062344\\nPARAM: --to-stderr\\n"',
        '"Stderr": "ERR: --to-stderr\\n"',
        ])

    # test commands that fail
    self._run_hook("test.sh --exit-error", [
        '"ExitStatus": 1',
        '"Stdout": "TABLET_ALIAS: test_nj-0000062344\\nPARAM: --exit-error\\n"',
        '"Stderr": "ERROR: exit status 1\\n"',
        ])

    # test hook that is not present
    self._run_hook("not_here.sh", [
        '"ExitStatus": -1',
        '"Stdout": "Skipping missing hook: /', # cannot go further, local path
        '"Stderr": ""',
        ])

    # test hook with invalid name
    self._run_hook("/bin/ls", [
        "action failed: ExecuteHook hook name cannot have a '/' in it",
        ])

    tablet_62344.kill_vttablet()

  def test_sigterm(self):
    utils.run_vtctl('CreateKeyspace test_keyspace')

    # create the database so vttablets start, as it is serving
    tablet_62344.create_db('vt_test_keyspace')

    tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True)

    # start a 'vtctl Sleep' command in the background
    sp = utils.run_bg(utils.vtroot+'/bin/vtctl -log_dir '+utils.tmp_root+' --alsologtostderr Sleep %s 60s' %
                      tablet_62344.tablet_alias,
                      stdout=PIPE, stderr=PIPE)

    # wait for it to start, and let's kill it
    time.sleep(2.0)
    utils.run(['pkill', 'vtaction'])
    out, err = sp.communicate()

    # check the vtctl command got the right remote error back
    if "vtaction interrupted by signal" not in err:
      raise utils.TestError("cannot find expected output in error:", err)
    logging.debug("vtaction was interrupted correctly:\n" + err)

    tablet_62344.kill_vttablet()

  def test_restart(self):
    utils.run_vtctl('CreateKeyspace test_keyspace')

    # create the database so vttablets start, as it is serving
    tablet_62344.create_db('vt_test_keyspace')

    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    proc1 = tablet_62344.start_vttablet()
    proc2 = tablet_62344.start_vttablet()
    time.sleep(2.0)
    proc1.poll()
    if proc1.returncode is None:
      raise utils.TestError("proc1 still running")
    tablet_62344.kill_vttablet()

  def test_scrap_and_reinit(self):
    utils.run_vtctl('CreateKeyspace test_keyspace')

    tablet_62344.create_db('vt_test_keyspace')
    tablet_62044.create_db('vt_test_keyspace')

    # one master one replica
    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    tablet_62044.init_tablet('replica', 'test_keyspace', '0')

    # make sure the replica is in the replication graph
    before_scrap = utils.zk_cat_json('/zk/test_nj/vt/replication/test_keyspace/0')
    self.assertEqual(1, len(before_scrap['ReplicationLinks']), 'wrong replication links before: %s' % str(before_scrap))

    # scrap and re-init
    utils.run_vtctl('ScrapTablet -force ' + tablet_62044.tablet_alias)
    tablet_62044.init_tablet('replica', 'test_keyspace', '0')

    after_scrap = utils.zk_cat_json('/zk/test_nj/vt/replication/test_keyspace/0')
    self.assertEqual(1, len(after_scrap['ReplicationLinks']), 'wrong replication links after: %s' % str(after_scrap))

    # manually add a bogus entry to the replication graph, and check
    # it is removed by ShardReplicationFix
    utils.run_vtctl('ShardReplicationAdd test_keyspace/0 test_nj-0000066666 test_nj-0000062344', auto_log=True)
    with_bogus = utils.zk_cat_json('/zk/test_nj/vt/replication/test_keyspace/0')
    self.assertEqual(2, len(with_bogus['ReplicationLinks']), 'wrong replication links with bogus: %s' % str(with_bogus))
    utils.run_vtctl('ShardReplicationFix test_nj test_keyspace/0', auto_log=True)
    after_fix = utils.zk_cat_json('/zk/test_nj/vt/replication/test_keyspace/0')
    self.assertEqual(1, len(after_scrap['ReplicationLinks']), 'wrong replication links after fix: %s' % str(after_fix))

if __name__ == '__main__':
  utils.main()
