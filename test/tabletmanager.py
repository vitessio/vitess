#!/usr/bin/python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import logging
import os
import signal
from subprocess import PIPE
import time
import unittest

import environment
import utils
import tablet
from vtdb import vtgate

tablet_62344 = tablet.Tablet(62344)
tablet_62044 = tablet.Tablet(62044)

def setUpModule():
  try:
    if environment.topo_server_implementation == 'zookeeper':
      # this is a one-off test to make sure our zookeeper implementation
      # behaves with a server that is not DNS-resolveable
      environment.topo_server_setup(add_bad_host=True)
    else:
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

class TestTabletManager(unittest.TestCase):
  def tearDown(self):
    tablet.Tablet.check_vttablet_count()
    environment.topo_server_wipe()
    for t in [tablet_62344, tablet_62044]:
      t.reset_replication()
      t.clean_dbs()

  # run twice to check behavior with existing znode data
  def test_sanity(self):
    self._test_sanity()
    self._test_sanity()

  def _test_sanity(self):
    # Start up a master mysql and vttablet
    utils.run_vtctl('CreateKeyspace -force test_keyspace')
    utils.run_vtctl('createshard -force test_keyspace/0')
    tablet_62344.init_tablet('master', 'test_keyspace', '0', parent=False)
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
    utils.run_vtctl('RebuildKeyspaceGraph test_keyspace')
    utils.validate_topology()

    # if these statements don't run before the tablet it will wedge waiting for the
    # db to become accessible. this is more a bug than a feature.
    tablet_62344.mquery("", ["set global read_only = off"])
    tablet_62344.populate('vt_test_keyspace', self._create_vt_select_test,
                          self._populate_vt_select_test)

    tablet_62344.start_vttablet()
    gate_proc, gate_port = utils.vtgate_start()

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


  _create_vt_select_test = '''create table vt_select_test (
  id bigint auto_increment,
  msg varchar(64),
  primary key (id)
  ) Engine=InnoDB'''

  _populate_vt_select_test = [
      "insert into vt_select_test (msg) values ('test %s')" % x
      for x in xrange(4)]


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
    utils.run_vtctl('-wait-time 2s WaitForAction ' + action_path,
                    expect_fail=True)

    # wait until the background sleep action is done, otherwise there will be
    # a leftover vtaction whose result may overwrite running actions
    # NOTE(alainjobart): Yes, I've seen it happen, it's a pain to debug:
    # the zombie Sleep clobbers the Clone command in the following tests
    utils.run_vtctl('-wait-time 20s WaitForAction ' + action_path,
                    auto_log=True)

    if environment.topo_server_implementation == 'zookeeper':
      # extra small test: we ran for a while, get the states we were in,
      # make sure they're accounted for properly
      # first the query engine States
      v = utils.get_vars(tablet_62344.port)
      logging.debug("vars: %s" % str(v))

      # then the Zookeeper connections
      if v['ZkMetaConn']['test_nj']['Current'] != 'Connected':
        raise utils.TestError('invalid zk test_nj state: ',
                              v['ZkMetaConn']['test_nj']['Current'])
      if v['ZkMetaConn']['global']['Current'] != 'Connected':
        raise utils.TestError('invalid zk global state: ',
                              v['ZkMetaConn']['global']['Current'])
      if v['ZkMetaConn']['test_nj']['DurationConnected'] < 10e9:
        raise utils.TestError('not enough time in Connected state',
                              v['ZkMetaConn']['test_nj']['DurationConnected'])
      if v['TabletType'] != 'master':
        raise utils.TestError('TabletType not exported correctly')

    tablet_62344.kill_vttablet()


  def test_vttablet_authenticated(self):
    utils.run_vtctl('CreateKeyspace test_keyspace')
    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    utils.run_vtctl('RebuildShardGraph test_keyspace/0')
    utils.validate_topology()

    tablet_62344.populate('vt_test_keyspace', self._create_vt_select_test,
                          self._populate_vt_select_test)
    tablet_62344.start_vttablet(auth=True)
    utils.run_vtctl('SetReadWrite ' + tablet_62344.tablet_alias)

    err, out = tablet_62344.vquery('select * from vt_select_test',
                                   path='test_keyspace/0',
                                   user='ala', password=r'ma kota')
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
    out, err = utils.run_vtctl('--alsologtostderr ExecuteHook %s %s' % (tablet_62344.tablet_alias, params), trap_output=True, raise_on_error=False)
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
    args = [environment.binary_path('vtctl'),
            '-log_dir', environment.tmproot,
            '--alsologtostderr']
    args.extend(environment.topo_server_flags())
    args.extend(environment.tablet_manager_protocol_flags())
    args.extend(['Sleep', tablet_62344.tablet_alias, '60s'])
    sp = utils.run_bg(args, stdout=PIPE, stderr=PIPE)

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
    for timeout in xrange(20):
      logging.info("Sleeping waiting for first process to die")
      time.sleep(1.0)
      proc1.poll()
      if proc1.returncode is not None:
        break
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
    before_scrap = utils.run_vtctl_json(['GetShardReplication', 'test_nj',
                                         'test_keyspace/0'])
    self.assertEqual(1, len(before_scrap['ReplicationLinks']), 'wrong replication links before: %s' % str(before_scrap))

    # scrap and re-init
    utils.run_vtctl('ScrapTablet -force ' + tablet_62044.tablet_alias)
    tablet_62044.init_tablet('replica', 'test_keyspace', '0')

    after_scrap = utils.run_vtctl_json(['GetShardReplication', 'test_nj',
                                        'test_keyspace/0'])
    self.assertEqual(1, len(after_scrap['ReplicationLinks']), 'wrong replication links after: %s' % str(after_scrap))

    # manually add a bogus entry to the replication graph, and check
    # it is removed by ShardReplicationFix
    utils.run_vtctl('ShardReplicationAdd test_keyspace/0 test_nj-0000066666 test_nj-0000062344', auto_log=True)
    with_bogus = utils.run_vtctl_json(['GetShardReplication', 'test_nj',
                                        'test_keyspace/0'])
    self.assertEqual(2, len(with_bogus['ReplicationLinks']), 'wrong replication links with bogus: %s' % str(with_bogus))
    utils.run_vtctl('ShardReplicationFix test_nj test_keyspace/0', auto_log=True)
    after_fix = utils.run_vtctl_json(['GetShardReplication', 'test_nj',
                                        'test_keyspace/0'])
    self.assertEqual(1, len(after_scrap['ReplicationLinks']), 'wrong replication links after fix: %s' % str(after_fix))

if __name__ == '__main__':
  utils.main()
