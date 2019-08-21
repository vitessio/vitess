#!/usr/bin/env python

# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# vim: tabstop=8 expandtab shiftwidth=2 softtabstop=2

import json
import logging
import os
import time
import unittest
import urllib.request, urllib.parse, urllib.error
import urllib.request, urllib.error, urllib.parse
import re

import MySQLdb

import environment
import utils
import tablet
from mysql_flavor import mysql_flavor
from protocols_flavor import protocols_flavor

from vtproto import topodata_pb2

tablet_62344 = tablet.Tablet(62344)
tablet_62044 = tablet.Tablet(62044)

# regexp to check if the tablet status page reports healthy,
# regardless of actual replication lag
healthy_expr = re.compile(r'Current status: <span.+?>healthy')


def setUpModule():
  try:
    topo_flavor = environment.topo_server().flavor()
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
  utils.required_teardown()
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
      t.set_semi_sync_enabled(master=False)
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

    # if these statements don't run before the tablet it will wedge
    # waiting for the db to become accessible. this is more a bug than
    # a feature.
    tablet_62344.populate('vt_test_keyspace', self._create_vt_select_test,
                          self._populate_vt_select_test)

    tablet_62344.start_vttablet()

    # make sure the query service is started right away.
    qr = tablet_62344.execute('select id, msg from vt_select_test')
    self.assertEqual(len(qr['rows']), 4,
                     'expected 4 rows in vt_select_test: %s' % str(qr))
    self.assertEqual(qr['fields'][0]['name'], 'id')
    self.assertEqual(qr['fields'][1]['name'], 'msg')

    # test exclude_field_names to vttablet works as expected.
    qr = tablet_62344.execute('select id, msg from vt_select_test',
                              execute_options='included_fields:TYPE_ONLY ')
    self.assertEqual(len(qr['rows']), 4,
                     'expected 4 rows in vt_select_test: %s' % str(qr))
    self.assertNotIn('name', qr['fields'][0])
    self.assertNotIn('name', qr['fields'][1])

    # make sure direct dba queries work
    query_result = utils.run_vtctl_json(
        ['ExecuteFetchAsDba', '-json', tablet_62344.tablet_alias,
         'select * from vt_test_keyspace.vt_select_test'])
    self.assertEqual(
        len(query_result['rows']), 4,
        'expected 4 rows in vt_select_test: %s' % str(query_result))
    self.assertEqual(
        len(query_result['fields']), 2,
        'expected 2 fields in vt_select_test: %s' % str(query_result))

    # check Ping / RefreshState / RefreshStateByShard
    utils.run_vtctl(['Ping', tablet_62344.tablet_alias])
    utils.run_vtctl(['RefreshState', tablet_62344.tablet_alias])
    utils.run_vtctl(['RefreshStateByShard', 'test_keyspace/0'])
    utils.run_vtctl(['RefreshStateByShard', '--cells=test_nj',
                     'test_keyspace/0'])

    # Quickly check basic actions.
    utils.run_vtctl(['SetReadOnly', tablet_62344.tablet_alias])
    utils.wait_db_read_only(62344)

    utils.run_vtctl(['SetReadWrite', tablet_62344.tablet_alias])
    utils.check_db_read_write(62344)

    utils.validate_topology()
    utils.run_vtctl(['ValidateKeyspace', 'test_keyspace'])
    # not pinging tablets, as it enables replication checks, and they
    # break because we only have a single master, no slaves
    utils.run_vtctl(['ValidateShard', '-ping-tablets=false',
                     'test_keyspace/0'])

    tablet_62344.kill_vttablet()

  _create_vt_select_test = '''create table vt_select_test (
  id bigint auto_increment,
  msg varchar(64),
  primary key (id)
  ) Engine=InnoDB'''

  _populate_vt_select_test = [
      "insert into vt_select_test (msg) values ('test %s')" % x
      for x in range(4)]

  # Test if a vttablet can be pointed at an existing mysql
  # We point 62044 at 62344's mysql and try to read from it.
  def test_command_line(self):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])
    tablet_62044.init_tablet('master', 'test_keyspace', '0')
    tablet_62344.populate('vt_test_keyspace', self._create_vt_select_test,
                          self._populate_vt_select_test)

    # mycnf_server_id prevents vttablet from reading the mycnf
    extra_args = [
        '-mycnf_server_id', str(tablet_62044.tablet_uid),
        '-db_socket', os.path.join(tablet_62344.tablet_dir, 'mysql.sock')]
    # supports_backup=False prevents vttablet from trying to restore
    tablet_62044.start_vttablet(extra_args=extra_args, supports_backups=False)
    qr = tablet_62044.execute('select id, msg from vt_select_test')
    self.assertEqual(len(qr['rows']), 4,
                     'expected 4 rows in vt_select_test: %s' % str(qr))

    # Verify backup fails
    try:
      utils.run_vtctl(['Backup', tablet_62044.tablet_alias])
    except Exception as e:
      self.assertIn('cannot perform backup without my.cnf', str(e))
    else:
        self.assertFail('did not get an exception')

    tablet_62044.kill_vttablet()

  def test_actions_and_timeouts(self):
    # Start up a master mysql and vttablet
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    utils.validate_topology()
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
    _, stderr = utils.run_vtctl(
        ['-wait-time', '3s', 'RefreshState', tablet_62344.tablet_alias],
        expect_fail=True)
    self.assertIn(protocols_flavor().rpc_timeout_message(), stderr)

    # wait for the background vtctl
    bg.wait()

    tablet_62344.kill_vttablet()

  def _run_hook(self, params, expected_status, expected_stdout,
                expected_stderr):
    hr = utils.run_vtctl_json(['ExecuteHook', tablet_62344.tablet_alias] +
                              params)
    self.assertEqual(hr['ExitStatus'], expected_status)
    if isinstance(expected_stdout, str):
      self.assertEqual(hr['Stdout'], expected_stdout)
    else:
      found = False
      for exp in expected_stdout:
        if hr['Stdout'] == exp:
          found = True
          break
      if not found:
        self.assertFail(
            'cannot find expected %s in %s' %
            (str(expected_stdout), hr['Stdout']))
    if expected_stderr[-1:] == '%':
      self.assertEqual(
          hr['Stderr'][:len(expected_stderr)-1],
          expected_stderr[:len(expected_stderr)-1])
    else:
      self.assertEqual(hr['Stderr'], expected_stderr)

  def test_hook(self):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # create the database so vttablets start, as it is serving
    tablet_62344.create_db('vt_test_keyspace')

    tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True)

    # test a regular program works
    self._run_hook(['test.sh', '--flag1', '--param1=hello'], 0,
                   ['TABLET_ALIAS: test_nj-0000062344\n'
                    'PARAM: --flag1\n'
                    'PARAM: --param1=hello\n',
                    'TABLET_ALIAS: test_nj-0000062344\n'
                    'PARAM: --param1=hello\n'
                    'PARAM: --flag1\n'],
                   '')

    # test stderr output
    self._run_hook(['test.sh', '--to-stderr'], 0,
                   'TABLET_ALIAS: test_nj-0000062344\n'
                   'PARAM: --to-stderr\n',
                   'ERR: --to-stderr\n')

    # test commands that fail
    self._run_hook(['test.sh', '--exit-error'], 1,
                   'TABLET_ALIAS: test_nj-0000062344\n'
                   'PARAM: --exit-error\n',
                   'ERROR: exit status 1\n')

    # test hook that is not present
    self._run_hook(['not_here.sh'], -1,
                   '',
                   'missing hook /%')  # cannot go further, local path

    # test hook with invalid name
    _, err = utils.run_vtctl(['--alsologtostderr', 'ExecuteHook',
                              tablet_62344.tablet_alias,
                              '/bin/ls'],
                             mode=utils.VTCTL_VTCTL, trap_output=True,
                             raise_on_error=False)
    expected = "action failed: ExecuteHook hook name cannot have a '/' in it"
    self.assertIn(expected, err)

    tablet_62344.kill_vttablet()

  def test_shard_replication_fix(self):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    tablet_62344.create_db('vt_test_keyspace')
    tablet_62044.create_db('vt_test_keyspace')

    # one master one replica
    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    tablet_62044.init_tablet('replica', 'test_keyspace', '0')

    # make sure the replica is in the replication graph
    before_bogus = utils.run_vtctl_json(['GetShardReplication', 'test_nj',
                                         'test_keyspace/0'])
    self.assertEqual(2, len(before_bogus['nodes']),
                     'wrong shard replication nodes before: %s' %
                     str(before_bogus))

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
    self.assertEqual(2, len(after_fix['nodes']),
                     'wrong shard replication nodes after fix: %s' %
                     str(after_fix))

  def check_healthz(self, t, expected):
    if expected:
      self.assertEqual('ok\n', t.get_healthz())
    else:
      with self.assertRaises(urllib.error.HTTPError):
        t.get_healthz()

  def test_health_check(self):
    # one master, one replica that starts not initialized
    # (for the replica, we let vttablet do the InitTablet)
    tablet_62344.init_tablet('replica', 'test_keyspace', '0')

    for t in tablet_62344, tablet_62044:
      t.create_db('vt_test_keyspace')

    tablet_62344.start_vttablet(wait_for_state=None)
    tablet_62044.start_vttablet(wait_for_state=None,
                                lameduck_period='5s',
                                init_tablet_type='replica',
                                init_keyspace='test_keyspace',
                                init_shard='0')

    tablet_62344.wait_for_vttablet_state('NOT_SERVING')
    tablet_62044.wait_for_vttablet_state('NOT_SERVING')
    self.check_healthz(tablet_62044, False)

    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     tablet_62344.tablet_alias])

    # make sure the unhealthy slave goes to healthy
    tablet_62044.wait_for_vttablet_state('SERVING')
    utils.run_vtctl(['RunHealthCheck', tablet_62044.tablet_alias])
    self.check_healthz(tablet_62044, True)

    # make sure the master is still master
    ti = utils.run_vtctl_json(['GetTablet', tablet_62344.tablet_alias])
    self.assertEqual(ti['type'], topodata_pb2.MASTER,
                     'unexpected master type: %s' % ti['type'])

    # stop replication at the mysql level.
    tablet_62044.mquery('', 'stop slave')
    # vttablet replication_reporter should restart it.
    utils.run_vtctl(['RunHealthCheck', tablet_62044.tablet_alias])
    # insert something on the master and wait for it on the slave.
    tablet_62344.mquery('vt_test_keyspace', [
        'create table repl_test_table (id int)',
        'insert into repl_test_table values (123)'], write=True)
    timeout = 10.0
    while True:
      try:
        result = tablet_62044.mquery('vt_test_keyspace',
                                     'select * from repl_test_table')
        if result:
          self.assertEqual(result[0][0], 123)
          break
      except MySQLdb.ProgrammingError:
        # Maybe the create table hasn't gone trough yet, we wait more
        logging.exception('got this exception waiting for data, ignoring it')
      timeout = utils.wait_step(
          'slave replication repaired by replication_reporter', timeout)

    # stop replication, make sure we don't go unhealthy.
    # (we have a baseline as well, so the time should be good).
    utils.run_vtctl(['StopSlave', tablet_62044.tablet_alias])
    utils.run_vtctl(['RunHealthCheck', tablet_62044.tablet_alias])
    self.check_healthz(tablet_62044, True)

    # make sure status web page is healthy
    self.assertRegex(tablet_62044.get_status(), healthy_expr)

    # make sure the health stream is updated
    health = utils.run_vtctl_json(['VtTabletStreamHealth',
                                   '-count', '1',
                                   tablet_62044.tablet_alias])
    self.assertTrue(('seconds_behind_master' not in health['realtime_stats']) or
                    (health['realtime_stats']['seconds_behind_master'] < 30),
                    'got unexpected health: %s' % str(health))
    self.assertIn('serving', health)

    # then restart replication, make sure we stay healthy
    utils.run_vtctl(['StartSlave', tablet_62044.tablet_alias])
    utils.run_vtctl(['RunHealthCheck', tablet_62044.tablet_alias])

    # make sure status web page is healthy
    self.assertRegex(tablet_62044.get_status(), healthy_expr)

    # now test VtTabletStreamHealth returns the right thing
    stdout, _ = utils.run_vtctl(['VtTabletStreamHealth',
                                 '-count', '2',
                                 tablet_62044.tablet_alias],
                                trap_output=True, auto_log=True)
    lines = stdout.splitlines()
    self.assertEqual(len(lines), 2)
    for line in lines:
      logging.debug('Got health: %s', line)
      data = json.loads(line)
      self.assertIn('realtime_stats', data)
      self.assertIn('serving', data)
      self.assertTrue(data['serving'])
      self.assertNotIn('health_error', data['realtime_stats'])
      self.assertNotIn('tablet_externally_reparented_timestamp', data)
      self.assertEqual('test_keyspace', data['target']['keyspace'])
      self.assertEqual('0', data['target']['shard'])
      self.assertEqual(topodata_pb2.REPLICA, data['target']['tablet_type'])

    # Test that VtTabletStreamHealth reports a QPS >0.0.
    # Therefore, issue several reads first.
    # NOTE: This may be potentially flaky because we'll observe a QPS >0.0
    #       exactly "once" for the duration of one sampling interval (5s) and
    #       after that we'll see 0.0 QPS rates again. If this becomes actually
    #       flaky, we need to read continuously in a separate thread.
    for _ in range(10):
      tablet_62044.execute('select 1 from dual')
    # This may take up to 5 seconds to become true because we sample the query
    # counts for the rates only every 5 seconds (see query_service_stats.go).
    timeout = 10
    while True:
      health = utils.run_vtctl_json(['VtTabletStreamHealth', '-count', '1',
                                     tablet_62044.tablet_alias])
      if health['realtime_stats'].get('qps', 0.0) > 0.0:
        break
      timeout = utils.wait_step('QPS >0.0 seen', timeout)

    # kill the tablets
    tablet.kill_tablets([tablet_62344, tablet_62044])

  def test_health_check_drained_state_does_not_shutdown_query_service(self):
    # This test is similar to test_health_check, but has the following
    # differences:
    # - the second tablet is an 'rdonly' and not a 'replica'
    # - the second tablet will be set to 'drained' and we expect that
    #   the query service won't be shutdown

    # Setup master and rdonly tablets.
    tablet_62344.init_tablet('replica', 'test_keyspace', '0')

    for t in tablet_62344, tablet_62044:
      t.create_db('vt_test_keyspace')

    # Note we only have a master and a rdonly. So we can't enable
    # semi-sync in this case, as the rdonly slaves don't semi-sync ack.
    tablet_62344.start_vttablet(wait_for_state=None, enable_semi_sync=False)
    tablet_62044.start_vttablet(wait_for_state=None,
                                init_tablet_type='rdonly',
                                init_keyspace='test_keyspace',
                                init_shard='0',
                                enable_semi_sync=False)

    tablet_62344.wait_for_vttablet_state('NOT_SERVING')
    tablet_62044.wait_for_vttablet_state('NOT_SERVING')
    self.check_healthz(tablet_62044, False)

    # Enable replication.
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     tablet_62344.tablet_alias])

    # Trigger healthcheck to save time waiting for the next interval.
    utils.run_vtctl(['RunHealthCheck', tablet_62044.tablet_alias])
    tablet_62044.wait_for_vttablet_state('SERVING')
    self.check_healthz(tablet_62044, True)

    # Change from rdonly to drained and stop replication. (These
    # actions are similar to the SplitClone vtworker command
    # implementation.)  The tablet will stay healthy, and the
    # query service is still running.
    utils.run_vtctl(['ChangeSlaveType', tablet_62044.tablet_alias, 'drained'])
    # Trying to drain the same tablet again, should error
    try:
      utils.run_vtctl(['ChangeSlaveType', tablet_62044.tablet_alias, 'drained'])
    except Exception as e:
      s = str(e)
      self.assertIn("already drained", s)
    utils.run_vtctl(['StopSlave', tablet_62044.tablet_alias])
    # Trigger healthcheck explicitly to avoid waiting for the next interval.
    utils.run_vtctl(['RunHealthCheck', tablet_62044.tablet_alias])
    utils.wait_for_tablet_type(tablet_62044.tablet_alias, 'drained')
    self.check_healthz(tablet_62044, True)
    # Query service is still running.
    tablet_62044.wait_for_vttablet_state('SERVING')

    # Restart replication. Tablet will become healthy again.
    utils.run_vtctl(['ChangeSlaveType', tablet_62044.tablet_alias, 'rdonly'])
    utils.run_vtctl(['StartSlave', tablet_62044.tablet_alias])
    utils.run_vtctl(['RunHealthCheck', tablet_62044.tablet_alias])
    self.check_healthz(tablet_62044, True)

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
    # Use 'localhost' as hostname because Travis CI worker hostnames
    # are too long for MySQL replication.
    change_master_cmds = mysql_flavor().change_master_commands(
        'localhost',
        tablet_62344.mysql_port,
        pos)
    tablet_62044.mquery('', ['RESET MASTER', 'RESET SLAVE'] +
                        change_master_cmds + ['START SLAVE'])

    # now shutdown all mysqld
    shutdown_procs = [
        tablet_62344.shutdown_mysql(),
        tablet_62044.shutdown_mysql(),
        ]
    utils.wait_procs(shutdown_procs)

    # start the tablets, wait for them to be NOT_SERVING (mysqld not there)
    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    tablet_62044.init_tablet('replica', 'test_keyspace', '0',
                             include_mysql_port=False)
    for t in tablet_62344, tablet_62044:
      # Since MySQL is down at this point and we want the tablet to start up
      # successfully, we have to use supports_backups=False.
      t.start_vttablet(wait_for_state=None, supports_backups=False,
                       full_mycnf_args=True, include_mysql_port=False)
    for t in tablet_62344, tablet_62044:
      t.wait_for_vttablet_state('NOT_SERVING')
      self.check_healthz(t, False)

    # Tell slave to not try to repair replication in healthcheck.
    # The StopSlave will ultimately fail because mysqld is not running,
    # But vttablet should remember that it's not supposed to fix replication.
    utils.run_vtctl(['StopSlave', tablet_62044.tablet_alias], expect_fail=True)

    # The above notice to not fix replication should survive tablet restart.
    tablet_62044.kill_vttablet()
    tablet_62044.start_vttablet(wait_for_state='NOT_SERVING',
                                full_mycnf_args=True, include_mysql_port=False,
                                supports_backups=False)

    # restart mysqld
    start_procs = [
        tablet_62344.start_mysql(),
        tablet_62044.start_mysql(),
        ]
    utils.wait_procs(start_procs)

    # the master should still be healthy
    utils.run_vtctl(['RunHealthCheck', tablet_62344.tablet_alias],
                    auto_log=True)
    self.check_healthz(tablet_62344, True)

    # the slave will now be healthy, but report a very high replication
    # lag, because it can't figure out what it exactly is.
    utils.run_vtctl(['RunHealthCheck', tablet_62044.tablet_alias],
                    auto_log=True)
    tablet_62044.wait_for_vttablet_state('SERVING')
    self.check_healthz(tablet_62044, True)

    health = utils.run_vtctl_json(['VtTabletStreamHealth',
                                   '-count', '1',
                                   tablet_62044.tablet_alias])
    self.assertIn('seconds_behind_master', health['realtime_stats'])
    self.assertEqual(health['realtime_stats']['seconds_behind_master'], 7200)
    self.assertIn('serving', health)

    # restart replication, wait until health check goes small
    # (a value of zero is default and won't be in structure)
    utils.run_vtctl(['StartSlave', tablet_62044.tablet_alias])
    timeout = 10
    while True:
      utils.run_vtctl(['RunHealthCheck', tablet_62044.tablet_alias],
                      auto_log=True)
      health = utils.run_vtctl_json(['VtTabletStreamHealth',
                                     '-count', '1',
                                     tablet_62044.tablet_alias])
      if 'serving' in health and (
          ('seconds_behind_master' not in health['realtime_stats']) or
          (health['realtime_stats']['seconds_behind_master'] < 30)):
        break
      timeout = utils.wait_step('health delay goes back down', timeout)

    # wait for the tablet to fix its mysql port
    for t in tablet_62344, tablet_62044:
      # wait for mysql port to show up
      timeout = 10
      while True:
        ti = utils.run_vtctl_json(['GetTablet', t.tablet_alias])
        if 'mysql' in ti['port_map']:
          break
        timeout = utils.wait_step('mysql port in tablet record', timeout)
      self.assertEqual(ti['port_map']['mysql'], t.mysql_port)

    # all done
    tablet.kill_tablets([tablet_62344, tablet_62044])

  def test_repeated_init_shard_master(self):
    """Test that using InitShardMaster can go back and forth between 2 hosts."""
    for t in tablet_62344, tablet_62044:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(wait_for_state=None,
                       lameduck_period='5s',
                       init_tablet_type='replica',
                       init_keyspace='test_keyspace',
                       init_shard='0')

    # Tablets are not replicating, so they won't be healthy.
    for t in tablet_62344, tablet_62044:
      t.wait_for_vttablet_state('NOT_SERVING')
      self.check_healthz(t, False)

    # Pick one master out of the two.
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     tablet_62344.tablet_alias])

    # Run health check on both, make sure they are both healthy.
    # Also make sure the types are correct.
    for t in tablet_62344, tablet_62044:
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias], auto_log=True)
      self.check_healthz(t, True)
    utils.wait_for_tablet_type(tablet_62344.tablet_alias, 'master', timeout=0)
    utils.wait_for_tablet_type(tablet_62044.tablet_alias, 'replica', timeout=0)

    # Pick the other one as master, make sure they are still healthy.
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     tablet_62044.tablet_alias])

    # Run health check on both, make sure they are both healthy.
    # Also make sure the types are correct.
    for t in tablet_62344, tablet_62044:
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias], auto_log=True)
      self.check_healthz(t, True)
    utils.wait_for_tablet_type(tablet_62344.tablet_alias, 'replica', timeout=0)
    utils.wait_for_tablet_type(tablet_62044.tablet_alias, 'master', timeout=0)

    # Come back to the original guy.
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     tablet_62344.tablet_alias])

    # Run health check on both, make sure they are both healthy.
    # Also make sure the types are correct.
    for t in tablet_62344, tablet_62044:
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias], auto_log=True)
      self.check_healthz(t, True)
    utils.wait_for_tablet_type(tablet_62344.tablet_alias, 'master', timeout=0)
    utils.wait_for_tablet_type(tablet_62044.tablet_alias, 'replica', timeout=0)

    # And done.
    tablet.kill_tablets([tablet_62344, tablet_62044])

  def test_fallback_policy(self):
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62344.init_tablet('master', 'test_keyspace', '0')
    tablet_62344.start_vttablet(security_policy='bogus')
    f = urllib.request.urlopen('http://localhost:%d/queryz' % int(tablet_62344.port))
    response = f.read()
    f.close()
    self.assertIn('not allowed', response)
    tablet_62344.kill_vttablet()

  def test_ignore_health_error(self):
    tablet_62344.create_db('vt_test_keyspace')

    # Starts unhealthy because of "no slave status" (not replicating).
    tablet_62344.start_vttablet(wait_for_state='NOT_SERVING',
                                init_tablet_type='replica',
                                init_keyspace='test_keyspace',
                                init_shard='0')

    # Force it healthy.
    utils.run_vtctl(['IgnoreHealthError', tablet_62344.tablet_alias,
                     '.*no slave status.*'])
    utils.run_vtctl(['RunHealthCheck', tablet_62344.tablet_alias],
                    auto_log=True)
    tablet_62344.wait_for_vttablet_state('SERVING')
    self.check_healthz(tablet_62344, True)

    # Turn off the force-healthy.
    utils.run_vtctl(['IgnoreHealthError', tablet_62344.tablet_alias, ''])
    utils.run_vtctl(['RunHealthCheck', tablet_62344.tablet_alias],
                    auto_log=True)
    tablet_62344.wait_for_vttablet_state('NOT_SERVING')
    self.check_healthz(tablet_62344, False)

    tablet_62344.kill_vttablet()

  def test_master_restart_sets_ter_timestamp(self):
    """Test that TER timestamp is set when we restart the MASTER vttablet.

    TER = TabletExternallyReparented.
    See StreamHealthResponse.tablet_externally_reparented_timestamp for details.
    """
    master, replica = tablet_62344, tablet_62044
    tablets = [master, replica]
    # Start vttablets. Our future master is initially a REPLICA.
    for t in tablets:
      t.create_db('vt_test_keyspace')
    for t in tablets:
      t.start_vttablet(wait_for_state='NOT_SERVING',
                       init_tablet_type='replica',
                       init_keyspace='test_keyspace',
                       init_shard='0')

    # Initialize tablet as MASTER.
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     master.tablet_alias])
    master.wait_for_vttablet_state('SERVING')

    # Capture the current TER.
    health = utils.run_vtctl_json(['VtTabletStreamHealth',
                                   '-count', '1',
                                   master.tablet_alias])
    self.assertEqual(topodata_pb2.MASTER, health['target']['tablet_type'])
    self.assertIn('tablet_externally_reparented_timestamp', health)
    self.assertGreater(health['tablet_externally_reparented_timestamp'], 0,
                       'TER on MASTER must be set after InitShardMaster')

    # Restart the MASTER vttablet.
    master.kill_vttablet()
    master.start_vttablet(wait_for_state='SERVING',
                          init_tablet_type='replica',
                          init_keyspace='test_keyspace',
                          init_shard='0')

    # Make sure that the TER increased i.e. it was set to the current time.
    health_after_restart = utils.run_vtctl_json(['VtTabletStreamHealth',
                                                 '-count', '1',
                                                 master.tablet_alias])
    self.assertEqual(topodata_pb2.MASTER,
                     health_after_restart['target']['tablet_type'])
    self.assertIn('tablet_externally_reparented_timestamp',
                  health_after_restart)
    self.assertGreater(
        health_after_restart['tablet_externally_reparented_timestamp'],
        health['tablet_externally_reparented_timestamp'],
        'When the MASTER vttablet was restarted, the TER timestamp must be set'
        ' to the current time.')

    # Shutdown.
    for t in tablets:
      t.kill_vttablet()

  def test_topocustomrule(self):
    # Empty rule file.
    topocustomrule_file = environment.tmproot+'/rules.json'
    with open(topocustomrule_file, 'w') as fd:
      fd.write('[]\n')

    # Start up a master mysql and vttablet
    utils.run_vtctl(['CreateKeyspace', '-force', 'test_keyspace'])
    utils.run_vtctl(['createshard', '-force', 'test_keyspace/0'])
    tablet_62344.init_tablet('master', 'test_keyspace', '0', parent=False)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'])
    utils.validate_topology()

    # Copy config file into topo.
    topocustomrule_path = '/keyspaces/test_keyspace/configs/CustomRules'
    utils.run_vtctl(['TopoCp', '-to_topo', topocustomrule_file,
                     topocustomrule_path])

    # Put some data in, start master.
    tablet_62344.populate('vt_test_keyspace', self._create_vt_select_test,
                          self._populate_vt_select_test)
    tablet_62344.start_vttablet(topocustomrule_path=topocustomrule_path)

    # make sure the query service is working
    qr = tablet_62344.execute('select id, msg from vt_select_test')
    self.assertEqual(len(qr['rows']), 4,
                     'expected 4 rows in vt_select_test: %s' % str(qr))

    # Now update the topocustomrule file.
    with open(topocustomrule_file, 'w') as fd:
      fd.write('''
        [{
          "Name": "rule1",
          "Description": "disallow select on table vt_select_test",
          "TableNames" : ["vt_select_test"],
          "Query" : "(select)|(SELECT)"
        }]''')
    utils.run_vtctl(['TopoCp', '-to_topo', topocustomrule_file,
                     topocustomrule_path])

    # And wait until the query fails with the right error.
    timeout = 10.0
    while True:
      try:
        tablet_62344.execute('select id, msg from vt_select_test')
        timeout = utils.wait_step('query rule in place', timeout)
      except Exception as e:
        print(e)
        expected = ('disallowed due to rule: disallow select'
                    ' on table vt_select_test')
        self.assertIn(expected, str(e))
        break

    # Cleanup.
    tablet_62344.kill_vttablet()


if __name__ == '__main__':
  utils.main()
