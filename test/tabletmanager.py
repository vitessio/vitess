#!/usr/bin/env python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import json
import logging
import os
import signal
from subprocess import PIPE
import threading
import time
import unittest
import urllib
import urllib2

import environment
import utils
import tablet
from mysql_flavor import mysql_flavor
from protocols_flavor import protocols_flavor
from vtdb import dbexceptions

tablet_62344 = tablet.Tablet(62344)
tablet_62044 = tablet.Tablet(62044)

def setUpModule():
  try:
    if environment.topo_server().flavor() == 'zookeeper':
      # this is a one-off test to make sure our zookeeper implementation
      # behaves with a server that is not DNS-resolveable
      environment.topo_server().setup(add_bad_host=True)
    else:
      environment.topo_server().setup()

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

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  tablet_62344.remove_tree()
  tablet_62044.remove_tree()

class TestTabletManager(unittest.TestCase):
  def tearDown(self):
    tablet.Tablet.check_vttablet_count()
    environment.topo_server().wipe()
    for t in [tablet_62344, tablet_62044]:
      t.reset_replication()
      t.clean_dbs()

  def _check_srv_shard(self):
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_nj',
                                     'test_keyspace/0'])
    self.assertEqual(srvShard['master_cell'], 'test_nj')

  # run twice to check behavior with existing znode data
  def test_sanity(self):
    self._test_sanity()
    self._test_sanity()

  def _test_sanity(self):
    # Start up a master mysql and vttablet
    utils.run_vtctl(['CreateKeyspace', '-force', 'test_keyspace'])
    utils.run_vtctl(['createshard', '-force', 'test_keyspace/0'])
    tablet_62344.init_tablet('master', 'test_keyspace', '0', parent=False)
    utils.run_vtctl(['RebuildKeyspaceGraph', '-rebuild_srv_shards', 'test_keyspace'])
    utils.validate_topology()
    self._check_srv_shard()

    # if these statements don't run before the tablet it will wedge
    # waiting for the db to become accessible. this is more a bug than
    # a feature.
    tablet_62344.populate('vt_test_keyspace', self._create_vt_select_test,
                          self._populate_vt_select_test)

    tablet_62344.start_vttablet()

    # make sure the query service is started right away
    qr = tablet_62344.execute('select * from vt_select_test')
    self.assertEqual(len(qr['Rows']), 4,
                     "expected 4 rows in vt_select_test: %s" % str(qr))

    # make sure direct dba queries work
    query_result = utils.run_vtctl_json(['ExecuteFetchAsDba', '-want_fields', tablet_62344.tablet_alias, 'select * from vt_test_keyspace.vt_select_test'])
    self.assertEqual(len(query_result['Rows']), 4, "expected 4 rows in vt_select_test: %s" % str(query_result))
    self.assertEqual(len(query_result['Fields']), 2, "expected 2 fields in vt_select_test: %s" % str(query_result))

    # check Ping / RefreshState
    utils.run_vtctl(['Ping', tablet_62344.tablet_alias])
    utils.run_vtctl(['RefreshState', tablet_62344.tablet_alias])

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
    self._check_srv_shard()

    tablet_62344.kill_vttablet()

    tablet_62344.init_tablet('idle')
    tablet_62344.scrap(force=True)

  def test_scrap(self):
    # Start up a master mysql and vttablet
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    tablet_62044.init_tablet('replica', 'test_keyspace', '0')
    utils.run_vtctl(['RebuildShardGraph', 'test_keyspace/*'])
    utils.validate_topology()
    self._check_srv_shard()

    tablet_62044.scrap(force=True)
    utils.validate_topology()
    self._check_srv_shard()


  _create_vt_select_test = '''create table vt_select_test (
  id bigint auto_increment,
  msg varchar(64),
  primary key (id)
  ) Engine=InnoDB'''

  _populate_vt_select_test = [
      "insert into vt_select_test (msg) values ('test %s')" % x
      for x in xrange(4)]


  def test_actions_and_timeouts(self):
    # Start up a master mysql and vttablet
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    utils.run_vtctl(['RebuildShardGraph', 'test_keyspace/0'])
    utils.validate_topology()
    self._check_srv_shard()
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62344.start_vttablet()

    utils.run_vtctl(['Ping', tablet_62344.tablet_alias])

    # schedule long action in the background, sleep a little bit to make sure
    # it started to run
    args = (environment.binary_args('vtctl') +
            environment.topo_server().flags() +
            ['-tablet_manager_protocol',
             protocols_flavor().tablet_manager_protocol(),
             '-tablet_protocol', protocols_flavor().tabletconn_protocol(),
             '-log_dir', environment.vtlogroot,
             'Sleep', tablet_62344.tablet_alias, '10s'])
    bg = utils.run_bg(args)
    time.sleep(3)

    # try a frontend RefreshState that should timeout as the tablet is busy
    # running the other one
    stdout, stderr = utils.run_vtctl(['-wait-time', '3s',
                                      'RefreshState', tablet_62344.tablet_alias],
                                     expect_fail=True)
    self.assertIn(protocols_flavor().rpc_timeout_message(), stderr)

    # wait for the background vtctl
    bg.wait()

    if environment.topo_server().flavor() == 'zookeeper':
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
    self._check_srv_shard()

    tablet_62344.populate('vt_test_keyspace', self._create_vt_select_test,
                          self._populate_vt_select_test)
    tablet_62344.start_vttablet(auth=True)
    utils.run_vtctl(['SetReadWrite', tablet_62344.tablet_alias])

    # make sure we can connect using secure connection
    conn = tablet_62344.conn(user='ala', password=r'ma kota')
    results, rowcount, lastrowid, fields = conn._execute('select * from vt_select_test', {})
    logging.debug("Got results: %s", str(results))
    self.assertEqual(len(results), 4, 'got wrong result length: %s' % str(results))
    conn.close()

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

  def test_restart(self):
    """test_restart tests that when starting a second vttablet with the same
    configuration as another one, it will kill the previous process
    and take over listening on the socket.

    If vttablet listens to other ports (like gRPC), this feature will
    break. We believe it is not widely used, so we're OK with this for now.
    (container based installations usually handle tablet restarts
    by using a different set of servers, and do not rely on this feature
    at all).
    """
    if environment.topo_server().flavor() != 'zookeeper':
      logging.info("Skipping this test in non-github tree")
      return
    if tablet_62344.grpc_enabled():
      logging.info("Skipping this test as second gRPC port interferes")
      return

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
    self.assertEqual(2, len(before_scrap['nodes']),
                     'wrong shard replication nodes before: %s' %
                     str(before_scrap))

    # scrap and re-init
    utils.run_vtctl(['ScrapTablet', '-force', tablet_62044.tablet_alias])
    tablet_62044.init_tablet('replica', 'test_keyspace', '0')

    after_scrap = utils.run_vtctl_json(['GetShardReplication', 'test_nj',
                                        'test_keyspace/0'])
    self.assertEqual(2, len(after_scrap['nodes']),
                     'wrong shard replication nodes after: %s' %
                     str(after_scrap))

    # manually add a bogus entry to the replication graph, and check
    # it is removed by ShardReplicationFix
    utils.run_vtctl(['ShardReplicationAdd', 'test_keyspace/0',
                     'test_nj-0000066666'], auto_log=True)
    with_bogus = utils.run_vtctl_json(['GetShardReplication', 'test_nj',
                                        'test_keyspace/0'])
    self.assertEqual(3, len(with_bogus['nodes']),
                     'wrong shard replication nodes with bogus: %s' %
                     str(with_bogus))
    utils.run_vtctl(['ShardReplicationFix', 'test_nj', 'test_keyspace/0'],
                    auto_log=True)
    after_fix = utils.run_vtctl_json(['GetShardReplication', 'test_nj',
                                        'test_keyspace/0'])
    self.assertEqual(2, len(after_scrap['nodes']),
                     'wrong shard replication nodes after fix: %s' %
                     str(after_fix))

  def check_healthz(self, tablet, expected):
    if expected:
      self.assertEqual("ok\n", tablet.get_healthz())
    else:
      with self.assertRaises(urllib2.HTTPError):
        tablet.get_healthz()

  def wait_for_tablet_type_change(self, tablet_alias, expected_type):
    timeout = 10
    while True:
      ti = utils.run_vtctl_json(['GetTablet', tablet_alias])
      if ti['Type'] == expected_type:
        logging.debug("Slave tablet went to %s, good" % expected_type)
        break
      timeout = utils.wait_step('slave becomes ' + expected_type, timeout)

  def test_health_check(self):
    # one master, one replica that starts in spare
    # (for the replica, we let vttablet do the InitTablet)
    tablet_62344.init_tablet('master', 'test_keyspace', '0')

    for t in tablet_62344, tablet_62044:
      t.create_db('vt_test_keyspace')

    tablet_62344.start_vttablet(wait_for_state=None,
                                target_tablet_type='replica')
    tablet_62044.start_vttablet(wait_for_state=None,
                                target_tablet_type='replica',
                                lameduck_period='5s',
                                init_keyspace='test_keyspace',
                                init_shard='0')

    tablet_62344.wait_for_vttablet_state('SERVING')
    tablet_62044.wait_for_vttablet_state('NOT_SERVING')
    self.check_healthz(tablet_62044, False)

    utils.run_vtctl(['InitShardMaster', 'test_keyspace/0',
                     tablet_62344.tablet_alias])

    # make sure the 'spare' slave goes to 'replica'
    self.wait_for_tablet_type_change(tablet_62044.tablet_alias, "replica")
    self.check_healthz(tablet_62044, True)

    # make sure the master is still master
    ti = utils.run_vtctl_json(['GetTablet', tablet_62344.tablet_alias])
    self.assertEqual(ti['Type'], 'master',
                     "unexpected master type: %s" % ti['Type'])

    # stop replication, make sure we go unhealthy.
    utils.run_vtctl(['StopSlave', tablet_62044.tablet_alias])
    self.wait_for_tablet_type_change(tablet_62044.tablet_alias, "spare")
    self.check_healthz(tablet_62044, False)

    # make sure the serving graph was updated
    timeout = 10
    while True:
      try:
        utils.run_vtctl_json(['GetEndPoints', 'test_nj', 'test_keyspace/0',
                              'replica'])
      except:
        logging.debug("Tablet is gone from serving graph, good")
        break
      timeout = utils.wait_step('Stopped replication didn\'t trigger removal from serving graph', timeout)

    # make sure status web page is unhappy
    self.assertIn('>unhealthy: replication_reporter: Replication is not running</span></div>', tablet_62044.get_status())

    # make sure the health stream is updated
    health = utils.run_vtctl_json(['VtTabletStreamHealth',
                                   '-count', '1',
                                   tablet_62044.tablet_alias])
    self.assertIn('replication_reporter: Replication is not running', health['realtime_stats']['health_error'])

    # then restart replication, and write data, make sure we go back to healthy
    utils.run_vtctl(['StartSlave', tablet_62044.tablet_alias])
    self.wait_for_tablet_type_change(tablet_62044.tablet_alias, "replica")

    # make sure status web page is healthy
    self.assertIn('>healthy</span></div>', tablet_62044.get_status())

    # make sure the vars is updated
    v = utils.get_vars(tablet_62044.port)
    self.assertEqual(v['LastHealthMapCount'], 0)

    # now test VtTabletStreamHealth returns the right thing
    stdout, stderr = utils.run_vtctl(['VtTabletStreamHealth',
                                      '-count', '2',
                                      tablet_62044.tablet_alias],
                                     trap_output=True, auto_log=True)
    lines = stdout.splitlines()
    self.assertEqual(len(lines), 2)
    for line in lines:
      logging.debug("Got health: %s", line)
      data = json.loads(line)
      self.assertIn('realtime_stats', data)
      self.assertNotIn('health_error', data['realtime_stats'])
      self.assertNotIn('tablet_externally_reparented_timestamp', data)
      self.assertEqual('test_keyspace', data['target']['keyspace'])
      self.assertEqual('0', data['target']['shard'])
      self.assertEqual(3, data['target']['tablet_type'])

    # kill the tablets
    tablet.kill_tablets([tablet_62344, tablet_62044])

    # the replica was in lameduck for 5 seconds, should have been enough
    # to reset its state to spare
    ti = utils.run_vtctl_json(['GetTablet', tablet_62044.tablet_alias])
    self.assertEqual(ti['Type'], 'spare', "tablet didn't go to spare while in lameduck mode: %s" % str(ti))
  
  def test_health_check_worker_state_does_not_shutdown_query_service(self):
    # This test is similar to test_health_check, but has the following differences:
    # - the second tablet is an "rdonly" and not a "replica"
    # - the second tablet will be set to "worker" and we expect that the query service won't be shutdown
    
    # Setup master and rdonly tablets.
    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    
    for t in tablet_62344, tablet_62044:
      t.create_db('vt_test_keyspace')
      
    tablet_62344.start_vttablet(wait_for_state=None,
                                target_tablet_type='replica')
    tablet_62044.start_vttablet(wait_for_state=None,
                                target_tablet_type='rdonly',
                                init_keyspace='test_keyspace',
                                init_shard='0')
    
    tablet_62344.wait_for_vttablet_state('SERVING')
    tablet_62044.wait_for_vttablet_state('NOT_SERVING')
    self.check_healthz(tablet_62044, False)
    
    # Enable replication.
    utils.run_vtctl(['InitShardMaster', 'test_keyspace/0',
                     tablet_62344.tablet_alias])
    # Trigger healthcheck to save time waiting for the next interval.
    utils.run_vtctl(["RunHealthCheck", tablet_62044.tablet_alias, "rdonly"])
    self.wait_for_tablet_type_change(tablet_62044.tablet_alias, "rdonly")
    self.check_healthz(tablet_62044, True)
    tablet_62044.wait_for_vttablet_state('SERVING')
    
    # Change from rdonly to worker and stop replication. (These actions are similar to the SplitClone vtworker command implementation.)
    # The tablet will become unhealthy, but the query service is still running.
    utils.run_vtctl(["ChangeSlaveType", tablet_62044.tablet_alias, "worker"])
    utils.run_vtctl(['StopSlave', tablet_62044.tablet_alias])
    # Trigger healthcheck explicitly to avoid waiting for the next interval.
    utils.run_vtctl(["RunHealthCheck", tablet_62044.tablet_alias, "rdonly"])
    self.wait_for_tablet_type_change(tablet_62044.tablet_alias, "worker")
    self.check_healthz(tablet_62044, False)
    # Make sure that replication got disabled.
    self.assertIn('>unhealthy: replication_reporter: Replication is not running</span></div>', tablet_62044.get_status())
    # Query service is still running.
    tablet_62044.wait_for_vttablet_state('SERVING')

    # Restart replication. Tablet will become healthy again.
    utils.run_vtctl(["ChangeSlaveType", tablet_62044.tablet_alias, "spare"])
    self.wait_for_tablet_type_change(tablet_62044.tablet_alias, "spare")
    utils.run_vtctl(['StartSlave', tablet_62044.tablet_alias])
    utils.run_vtctl(["RunHealthCheck", tablet_62044.tablet_alias, "rdonly"])
    self.wait_for_tablet_type_change(tablet_62044.tablet_alias, "rdonly")
    self.check_healthz(tablet_62044, True)
    tablet_62044.wait_for_vttablet_state('SERVING')

    # kill the tablets
    tablet.kill_tablets([tablet_62344, tablet_62044])

  def test_no_mysql_healthcheck(self):
    """This test starts a vttablet with no mysql port, while mysql is down.
    It makes sure vttablet will start properly and be unhealthy.
    Then we start mysql, and make sure vttablet becomes healthy.
    """
    # we need replication to be enabled, so the slave tablet can be healthy.
    for t in tablet_62344, tablet_62044:
      t.create_db('vt_test_keyspace')
    pos = mysql_flavor().master_position(tablet_62344)
    # Use "localhost" as hostname because Travis CI worker hostnames are too long for MySQL replication.
    changeMasterCmds = mysql_flavor().change_master_commands(
                            "localhost",
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
      self.check_healthz(t, False)

    # restart mysqld
    start_procs = [
        tablet_62344.start_mysql(),
        tablet_62044.start_mysql(),
        ]
    utils.wait_procs(start_procs)

    # the master should still be healthy
    utils.run_vtctl(['RunHealthCheck', tablet_62344.tablet_alias, 'replica'],
                      auto_log=True)
    self.check_healthz(tablet_62344, True)

    # the slave won't be healthy at first, as replication is not running
    utils.run_vtctl(['RunHealthCheck', tablet_62044.tablet_alias, 'replica'],
                      auto_log=True)
    self.check_healthz(tablet_62044, False)
    tablet_62044.wait_for_vttablet_state('NOT_SERVING')

    # restart replication
    tablet_62044.mquery('', ['START SLAVE'])

    # wait for the tablet to become healthy and fix its mysql port
    utils.run_vtctl(['RunHealthCheck', tablet_62044.tablet_alias, 'replica'],
                    auto_log=True)
    tablet_62044.wait_for_vttablet_state('SERVING')
    self.check_healthz(tablet_62044, True)

    for t in tablet_62344, tablet_62044:
      # wait for mysql port to show up
      timeout = 10
      while True:
        ti = utils.run_vtctl_json(['GetTablet', t.tablet_alias])
        if 'mysql' in ti['Portmap']:
          break
        timeout = utils.wait_step('mysql port in tablet record', timeout)
      self.assertEqual(ti['Portmap']['mysql'], t.mysql_port)

    # all done
    tablet.kill_tablets([tablet_62344, tablet_62044])

  def test_repeated_init_shard_master(self):
    for t in tablet_62344, tablet_62044:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(wait_for_state=None,
                       target_tablet_type='replica',
                       lameduck_period='5s',
                       init_keyspace='test_keyspace',
                       init_shard='0')

    # tablets are not replicating, so they won't be healthy
    for t in tablet_62344, tablet_62044:
      t.wait_for_vttablet_state('NOT_SERVING')
      self.check_healthz(t, False)

    # pick one master out of the two
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     tablet_62344.tablet_alias])

    # run health check on both, make sure they are both healthy
    for t in tablet_62344, tablet_62044:
          utils.run_vtctl(['RunHealthCheck', t.tablet_alias, 'replica'],
                          auto_log=True)
          self.check_healthz(t, True)

    # pick the other one as master, make sure they are still healthy
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     tablet_62044.tablet_alias])

    # run health check on both, make sure they are both healthy
    for t in tablet_62344, tablet_62044:
          utils.run_vtctl(['RunHealthCheck', t.tablet_alias, 'replica'],
                          auto_log=True)
          self.check_healthz(t, True)

    # and come back to the original guy
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     tablet_62344.tablet_alias])

    # run health check on both, make sure they are both healthy
    for t in tablet_62344, tablet_62044:
          utils.run_vtctl(['RunHealthCheck', t.tablet_alias, 'replica'],
                          auto_log=True)
          self.check_healthz(t, True)

    # and done
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
