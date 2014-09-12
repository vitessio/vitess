#!/usr/bin/python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import logging
import os
import signal
from subprocess import PIPE
import threading
import time
import unittest
import urllib

import environment
import utils
import tablet
from mysql_flavor import mysql_flavor
from vtdb import dbexceptions
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
    utils.Vtctld().start()
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
    utils.run_vtctl(['CreateKeyspace', '-force', 'test_keyspace'])
    utils.run_vtctl(['createshard', '-force', 'test_keyspace/0'])
    tablet_62344.init_tablet('master', 'test_keyspace', '0', parent=False)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'])
    utils.validate_topology()
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_nj',
                                     'test_keyspace/0'])
    self.assertEqual(srvShard['MasterCell'], 'test_nj')

    # if these statements don't run before the tablet it will wedge
    # waiting for the db to become accessible. this is more a bug than
    # a feature.
    tablet_62344.populate('vt_test_keyspace', self._create_vt_select_test,
                          self._populate_vt_select_test)

    tablet_62344.start_vttablet()

    # make sure the query service is started right away
    result, _ = utils.run_vtctl(['Query', 'test_nj', 'test_keyspace',
                                 'select * from vt_select_test'],
                                mode=utils.VTCTL_VTCTL, trap_output=True)
    rows = result.splitlines()
    self.assertEqual(len(rows), 5, "expected 5 rows in vt_select_test: %s %s" %
                     (str(rows), result))

    # make sure direct dba queries work
    query_result = utils.run_vtctl_json(['ExecuteFetch', '-want_fields', tablet_62344.tablet_alias, 'select * from vt_test_keyspace.vt_select_test'])
    self.assertEqual(len(query_result['Rows']), 4, "expected 4 rows in vt_select_test: %s" % str(query_result))
    self.assertEqual(len(query_result['Fields']), 2, "expected 2 fields in vt_select_test: %s" % str(query_result))

    # check Pings
    utils.run_vtctl(['Ping', tablet_62344.tablet_alias])
    utils.run_vtctl(['RpcPing', tablet_62344.tablet_alias])

    # Quickly check basic actions.
    utils.run_vtctl(['SetReadOnly', tablet_62344.tablet_alias])
    utils.wait_db_read_only(62344)

    utils.run_vtctl(['SetReadWrite', tablet_62344.tablet_alias])
    utils.check_db_read_write(62344)

    utils.run_vtctl(['DemoteMaster', tablet_62344.tablet_alias])
    utils.wait_db_read_only(62344)

    utils.validate_topology()
    utils.run_vtctl(['ValidateKeyspace', 'test_keyspace'])
    # not pinging tablets, as it enables replication checks, and they
    # break because we only have a single master, no slaves
    utils.run_vtctl(['ValidateShard', '-ping-tablets=false',
                     'test_keyspace/0'])
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_nj',
                                     'test_keyspace/0'])
    self.assertEqual(srvShard['MasterCell'], 'test_nj')

    tablet_62344.kill_vttablet()

    tablet_62344.init_tablet('idle')
    tablet_62344.scrap(force=True)

  def test_vtgate(self):
    # Start up a master mysql and vttablet
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])
    utils.run_vtctl(['CreateShard', 'test_keyspace/0'])
    tablet_62344.init_tablet('master', 'test_keyspace', '0', parent=False)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'])
    utils.validate_topology()
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_nj',
                                     'test_keyspace/0'])
    self.assertEqual(srvShard['MasterCell'], 'test_nj')

    # if these statements don't run before the tablet it will wedge
    # waiting for the db to become accessible. this is more a bug than
    # a feature.
    tablet_62344.mquery("", ["set global read_only = off"])
    tablet_62344.populate('vt_test_keyspace', self._create_vt_select_test,
                          self._populate_vt_select_test)

    tablet_62344.start_vttablet()
    gate_proc, gate_port = utils.vtgate_start()

    conn = vtgate.connect("localhost:%s"%(gate_port), "master", "test_keyspace",
                          "0", 2.0)

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

    # error on dml. We still need to get a transaction id
    conn.begin()
    with self.assertRaises(dbexceptions.IntegrityError):
      conn._execute("insert into vt_select_test values(:id, :msg)", {"id": 5, "msg": "test4"})
    self.assertTrue(conn.session["ShardSessions"][0]["TransactionId"] != 0)
    conn.commit()

    # interleaving
    conn2 = vtgate.connect("localhost:%s"%(gate_port), "master",
                           "test_keyspace", "0", 2.0)
    thd = threading.Thread(target=self._query_lots, args=(conn2,))
    thd.start()
    for i in xrange(250):
      (result, count, lastrow, fields) = conn._execute("select id from vt_select_test where id = 2", {})
      self.assertEqual(result, [(2,)])
      if i % 10 == 0:
        conn._stream_execute("select id from vt_select_test where id = 3", {})
        while 1:
          result = conn._stream_next()
          if not result:
            break
          self.assertEqual(result, (3,))
    thd.join()

    # close
    conn.close()

    utils.vtgate_kill(gate_proc)
    tablet_62344.kill_vttablet()

  def _query_lots(self, conn2):
    for i in xrange(500):
      (result, count, lastrow, fields) = conn2._execute("select id from vt_select_test where id = 1", {})
      self.assertEqual(result, [(1,)])

  def test_scrap(self):
    # Start up a master mysql and vttablet
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    tablet_62044.init_tablet('replica', 'test_keyspace', '0')
    utils.run_vtctl(['RebuildShardGraph', 'test_keyspace/*'])
    utils.validate_topology()
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_nj',
                                     'test_keyspace/0'])
    self.assertEqual(srvShard['MasterCell'], 'test_nj')

    tablet_62044.scrap(force=True)
    utils.validate_topology()
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_nj',
                                     'test_keyspace/0'])
    self.assertEqual(srvShard['MasterCell'], 'test_nj')


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
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    utils.run_vtctl(['RebuildShardGraph', 'test_keyspace/0'])
    utils.validate_topology()
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_nj',
                                     'test_keyspace/0'])
    self.assertEqual(srvShard['MasterCell'], 'test_nj')
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62344.start_vttablet()

    utils.run_vtctl(['Ping', tablet_62344.tablet_alias])

    # schedule long action
    utils.run_vtctl(['-no-wait', 'Sleep', tablet_62344.tablet_alias, '15s'],
                    mode=utils.VTCTL_VTCTL, stdout=utils.devnull)
    # ping blocks until the sleep finishes unless we have a schedule race
    action_path, _ = utils.run_vtctl(['-no-wait', 'Ping',
                                      tablet_62344.tablet_alias],
                                     mode=utils.VTCTL_VTCTL, trap_output=True)
    action_path = action_path.strip()

    # kill agent leaving vtaction running
    tablet_62344.kill_vttablet()

    # restart agent
    tablet_62344.start_vttablet()

    # we expect this action with a short wait time to fail. this isn't the best
    # and has some potential for flakiness.
    utils.run_vtctl(['-wait-time', '2s', 'WaitForAction', action_path],
                    mode=utils.VTCTL_VTCTL, expect_fail=True)

    # wait until the background sleep action is done, otherwise there will be
    # a leftover vtaction whose result may overwrite running actions
    # NOTE(alainjobart): Yes, I've seen it happen, it's a pain to debug:
    # the zombie Sleep clobbers the Clone command in the following tests
    utils.run_vtctl(['-wait-time', '20s', 'WaitForAction', action_path],
                    mode=utils.VTCTL_VTCTL, auto_log=True)

    if environment.topo_server_implementation == 'zookeeper':
      # extra small test: we ran for a while, get the states we were in,
      # make sure they're accounted for properly
      # first the query engine States
      v = utils.get_vars(tablet_62344.port)
      logging.debug("vars: %s" % str(v))

      # then the Zookeeper connections
      if v['ZkMetaConn']['test_nj']['Current'] != 'Connected':
        self.fail('invalid zk test_nj state: %s' %
                  v['ZkMetaConn']['test_nj']['Current'])
      if v['ZkMetaConn']['global']['Current'] != 'Connected':
        self.fail('invalid zk global state: %s' %
                  v['ZkMetaConn']['global']['Current'])
      if v['ZkMetaConn']['test_nj']['DurationConnected'] < 10e9:
        self.fail('not enough time in Connected state: %u',
                  v['ZkMetaConn']['test_nj']['DurationConnected'])
      if v['TabletType'] != 'master':
        self.fail('TabletType not exported correctly')

    tablet_62344.kill_vttablet()


  def test_vttablet_authenticated(self):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])
    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    utils.run_vtctl(['RebuildShardGraph', 'test_keyspace/0'])
    utils.validate_topology()
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_nj',
                                     'test_keyspace/0'])
    self.assertEqual(srvShard['MasterCell'], 'test_nj')

    tablet_62344.populate('vt_test_keyspace', self._create_vt_select_test,
                          self._populate_vt_select_test)
    tablet_62344.start_vttablet(auth=True)
    utils.run_vtctl(['SetReadWrite', tablet_62344.tablet_alias])

    out, err = tablet_62344.vquery('select * from vt_select_test',
                                   path='test_keyspace/0', verbose=True,
                                   user='ala', password=r'ma kota')
    logging.debug("Got rows: " + err)
    if 'Row count: 4' not in err:
      self.fail("query didn't go through: %s, %s" % (err, out))

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
    self.fail("ExecuteHook returned unexpected result, no string: '" + "', '".join(expected) + "'")

  def _run_hook(self, params, expectedStrings):
    out, err = utils.run_vtctl(['--alsologtostderr', 'ExecuteHook',
                                tablet_62344.tablet_alias] + params,
                               mode=utils.VTCTL_VTCTL, trap_output=True,
                               raise_on_error=False)
    for expected in expectedStrings:
      self._check_string_in_hook_result(err, expected)

  def test_hook(self):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # create the database so vttablets start, as it is serving
    tablet_62344.create_db('vt_test_keyspace')

    tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True)

    # test a regular program works
    self._run_hook(['test.sh', '--flag1', '--param1=hello'], [
        '"ExitStatus": 0',
        ['"Stdout": "TABLET_ALIAS: test_nj-0000062344\\nPARAM: --flag1\\nPARAM: --param1=hello\\n"',
         '"Stdout": "TABLET_ALIAS: test_nj-0000062344\\nPARAM: --param1=hello\\nPARAM: --flag1\\n"',
         ],
        '"Stderr": ""',
        ])

    # test stderr output
    self._run_hook(['test.sh', '--to-stderr'], [
        '"ExitStatus": 0',
        '"Stdout": "TABLET_ALIAS: test_nj-0000062344\\nPARAM: --to-stderr\\n"',
        '"Stderr": "ERR: --to-stderr\\n"',
        ])

    # test commands that fail
    self._run_hook(['test.sh', '--exit-error'], [
        '"ExitStatus": 1',
        '"Stdout": "TABLET_ALIAS: test_nj-0000062344\\nPARAM: --exit-error\\n"',
        '"Stderr": "ERROR: exit status 1\\n"',
        ])

    # test hook that is not present
    self._run_hook(['not_here.sh'], [
        '"ExitStatus": -1',
        '"Stdout": "Skipping missing hook: /', # cannot go further, local path
        '"Stderr": ""',
        ])

    # test hook with invalid name
    self._run_hook(['/bin/ls'], [
        "action failed: ExecuteHook hook name cannot have a '/' in it",
        ])

    tablet_62344.kill_vttablet()

  def test_sigterm(self):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # create the database so vttablets start, as it is serving
    tablet_62344.create_db('vt_test_keyspace')

    tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True)

    # start a 'vtctl Sleep' command, don't wait for it
    action_path, _ = utils.run_vtctl(['-no-wait', 'Sleep',
                                      tablet_62344.tablet_alias, '60s'],
                                     mode=utils.VTCTL_VTCTL, trap_output=True)
    action_path = action_path.strip()

    # wait for the action to be 'Running', capture its pid
    timeout = 10
    while True:
      an = utils.run_vtctl_json(['ReadTabletAction', action_path])
      if an.get('State', None) == 'Running':
        pid = an['Pid']
        logging.debug("Action is running with pid %u, good", pid)
        break
      timeout = utils.wait_step('sleep action to run', timeout)

    # let's kill the vtaction process with a regular SIGTERM
    os.kill(pid, signal.SIGTERM)

    # check the vtctl command got the right remote error back
    out, err = utils.run_vtctl(['WaitForAction', action_path], trap_output=True,
                               mode=utils.VTCTL_VTCTL, raise_on_error=False)
    if "vtaction interrupted by signal" not in err:
      self.fail("cannot find expected output in error: " + err)
    logging.debug("vtaction was interrupted correctly:\n" + err)

    tablet_62344.kill_vttablet()

  # test_vtaction_dies_hard makes sure that the recovery code works
  # properly when action dies hard (with a crash for instance)
  def test_vtaction_dies_hard(self):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # create the database so vttablets start, as it is serving
    tablet_62344.create_db('vt_test_keyspace')

    tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True)

    # start a 'vtctl Sleep' command, don't wait for it
    action_path, _ = utils.run_vtctl(['-no-wait', 'Sleep',
                                      tablet_62344.tablet_alias, '60s'],
                                     mode=utils.VTCTL_VTCTL, trap_output=True)
    action_path = action_path.strip()

    # wait for the action to be 'Running', capture its pid
    timeout = 10
    while True:
      an = utils.run_vtctl_json(['ReadTabletAction', action_path])
      if an.get('State', None) == 'Running':
        pid = an['Pid']
        logging.debug("Action is running with pid %u, good", pid)
        break
      timeout = utils.wait_step('sleep action to run', timeout)

    # let's kill it hard, wait until it's gone for good
    os.kill(pid, signal.SIGKILL)
    try:
      os.waitpid(pid, 0)
    except OSError:
      # this means the process doesn't exist any more, we're good
      pass

    # Then let's make sure the next action cleans up properly and can execute.
    # If that doesn't work, this will time out and the test will fail.
    utils.run_vtctl(['Ping', tablet_62344.tablet_alias])

    tablet_62344.kill_vttablet()

  def test_restart(self):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # create the database so vttablets start, as it is serving
    tablet_62344.create_db('vt_test_keyspace')

    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    proc1 = tablet_62344.start_vttablet()
    proc2 = tablet_62344.start_vttablet()
    for timeout in xrange(20):
      logging.debug("Sleeping waiting for first process to die")
      time.sleep(1.0)
      proc1.poll()
      if proc1.returncode is not None:
        break
    if proc1.returncode is None:
      self.fail("proc1 still running")
    tablet_62344.kill_vttablet()

  def test_scrap_and_reinit(self):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

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
    utils.run_vtctl(['ScrapTablet', '-force', tablet_62044.tablet_alias])
    tablet_62044.init_tablet('replica', 'test_keyspace', '0')

    after_scrap = utils.run_vtctl_json(['GetShardReplication', 'test_nj',
                                        'test_keyspace/0'])
    self.assertEqual(1, len(after_scrap['ReplicationLinks']), 'wrong replication links after: %s' % str(after_scrap))

    # manually add a bogus entry to the replication graph, and check
    # it is removed by ShardReplicationFix
    utils.run_vtctl(['ShardReplicationAdd', 'test_keyspace/0',
                     'test_nj-0000066666', 'test_nj-0000062344'], auto_log=True)
    with_bogus = utils.run_vtctl_json(['GetShardReplication', 'test_nj',
                                        'test_keyspace/0'])
    self.assertEqual(2, len(with_bogus['ReplicationLinks']),
                     'wrong replication links with bogus: %s' % str(with_bogus))
    utils.run_vtctl(['ShardReplicationFix', 'test_nj', 'test_keyspace/0'],
                    auto_log=True)
    after_fix = utils.run_vtctl_json(['GetShardReplication', 'test_nj',
                                        'test_keyspace/0'])
    self.assertEqual(1, len(after_scrap['ReplicationLinks']),
                     'wrong replication links after fix: %s' % str(after_fix))

  def test_health_check(self):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # one master, one replica that starts in spare
    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    tablet_62044.init_tablet('spare', 'test_keyspace', '0')

    for t in tablet_62344, tablet_62044:
      t.create_db('vt_test_keyspace')

    tablet_62344.start_vttablet(wait_for_state=None,
                                target_tablet_type='replica')
    tablet_62044.start_vttablet(wait_for_state=None,
                                target_tablet_type='replica',
                                lameduck_period='5s')

    tablet_62344.wait_for_vttablet_state('SERVING')
    tablet_62044.wait_for_vttablet_state('NOT_SERVING')

    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/0',
                     tablet_62344.tablet_alias])

    # make sure the 'spare' slave goes to 'replica'
    timeout = 10
    while True:
      ti = utils.run_vtctl_json(['GetTablet', tablet_62044.tablet_alias])
      if ti['Type'] == "replica":
        logging.debug("Slave tablet went to replica, good")
        break
      timeout = utils.wait_step('slave tablet going to replica', timeout)

    # make sure the master is still master
    ti = utils.run_vtctl_json(['GetTablet', tablet_62344.tablet_alias])
    self.assertEqual(ti['Type'], 'master',
                     "unexpected master type: %s" % ti['Type'])

    # stop replication on the slave, see it trigger the slave going
    # slightly unhealthy
    tablet_62044.mquery('', 'stop slave')
    timeout = 10
    while True:
      ti = utils.run_vtctl_json(['GetTablet', tablet_62044.tablet_alias])
      if 'Health' in ti and ti['Health']:
        if 'replication_lag' in ti['Health']:
          if ti['Health']['replication_lag'] == 'high':
            logging.debug("Slave tablet replication_lag went to high, good")
            break
      timeout = utils.wait_step('slave has high replication lag', timeout)

    # make sure the serving graph was updated
    ep = utils.run_vtctl_json(['GetEndPoints', 'test_nj', 'test_keyspace/0',
                               'replica'])
    if not ep['entries'][0]['health']:
      self.fail('Replication lag parameter not propagated to serving graph: %s' % str(ep))
    self.assertEqual(ep['entries'][0]['health']['replication_lag'], 'high', 'Replication lag parameter not propagated to serving graph: %s' % str(ep))

    # make sure status web page is unhappy
    self.assertIn('>unhappy</span></div>', tablet_62044.get_status())

    # make sure the vars is updated
    v = utils.get_vars(tablet_62044.port)
    self.assertEqual(v['LastHealthMapCount'], 1)

    # then restart replication, make sure we go back to healthy
    tablet_62044.mquery('', 'start slave')
    timeout = 10
    while True:
      ti = utils.run_vtctl_json(['GetTablet', tablet_62044.tablet_alias])
      if 'Health' in ti and ti['Health']:
        if 'replication_lag' in ti['Health']:
          if ti['Health']['replication_lag'] == 'high':
            timeout = utils.wait_step('slave has no replication lag', timeout)
            continue
      logging.debug("Slave tablet replication_lag is gone, good")
      break

    # make sure status web page is healthy
    self.assertIn('>healthy</span></div>', tablet_62044.get_status())

    # make sure the vars is updated
    v = utils.get_vars(tablet_62044.port)
    self.assertEqual(v['LastHealthMapCount'], 0)

    # kill the tablets
    tablet.kill_tablets([tablet_62344, tablet_62044])

    # the replica was in lameduck for 5 seconds, should have been enough
    # to reset its state to spare
    ti = utils.run_vtctl_json(['GetTablet', tablet_62044.tablet_alias])
    self.assertEqual(ti['Type'], 'spare', "tablet didn't go to spare while in lameduck mode: %s" % str(ti))

  def test_no_mysql_healthcheck(self):
    """This test starts a vttablet with no mysql port, while mysql is down.
    It makes sure vttablet will start properly and be unhealthy.
    Then we start mysql, and make sure vttablet becomes healthy.
    """
    # we need replication to be enabled, so the slave tablet can be healthy.
    for t in tablet_62344, tablet_62044:
      t.create_db('vt_test_keyspace')
    pos = mysql_flavor().master_position(tablet_62344)
    changeMasterCmds = mysql_flavor().change_master_commands(
                            utils.hostname,
                            tablet_62344.mysql_port,
                            pos)
    tablet_62044.mquery('', ['RESET MASTER', 'RESET SLAVE'] +
                        changeMasterCmds +
                        ['START SLAVE'])

    # now shutdown all mysqld
    shutdown_procs = [
        tablet_62344.shutdown_mysql(),
        tablet_62044.shutdown_mysql(),
        ]
    utils.wait_procs(shutdown_procs)

    # start the tablets, wait for them to be NOT_SERVING (mysqld not there)
    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    tablet_62044.init_tablet('spare', 'test_keyspace', '0',
                             include_mysql_port=False)
    for t in tablet_62344, tablet_62044:
      t.start_vttablet(wait_for_state=None,
                       target_tablet_type='replica',
                       full_mycnf_args=True, include_mysql_port=False)
    for t in tablet_62344, tablet_62044:
      t.wait_for_vttablet_state('NOT_SERVING')

    # restart mysqld
    start_procs = [
        tablet_62344.start_mysql(),
        tablet_62044.start_mysql(),
        ]
    utils.wait_procs(start_procs)

    # wait for the tablets to become healthy and fix their mysql port
    for t in tablet_62344, tablet_62044:
      t.wait_for_vttablet_state('SERVING')
    for t in tablet_62344, tablet_62044:
      ti = utils.run_vtctl_json(['GetTablet', t.tablet_alias])
      if not 'mysql' in ti['Portmap']:
        self.assertFalse('No mysql port in tablet record: %s', str(ti))
      self.assertEqual(ti['Portmap']['mysql'], t.mysql_port)

    # all done
    tablet.kill_tablets([tablet_62344, tablet_62044])

  def test_fallback_policy(self):
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    proc1 = tablet_62344.start_vttablet(security_policy="bogus")
    f = urllib.urlopen('http://localhost:%u/queryz' % int(tablet_62344.port))
    response = f.read()
    f.close()
    self.assertIn('not allowed', response)
    tablet_62344.kill_vttablet()

if __name__ == '__main__':
  utils.main()
