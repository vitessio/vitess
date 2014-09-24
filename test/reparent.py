#!/usr/bin/python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter('ignore')

import logging
import os
import shutil
import signal
from subprocess import PIPE
import time
import unittest

import environment
import utils
import tablet
from mysql_flavor import mysql_flavor

tablet_62344 = tablet.Tablet(62344)
tablet_62044 = tablet.Tablet(62044)
tablet_41983 = tablet.Tablet(41983)
tablet_31981 = tablet.Tablet(31981)


def setUpModule():
  try:
    environment.topo_server_setup()

    # start mysql instance external to the test
    setup_procs = [
        tablet_62344.init_mysql(),
        tablet_62044.init_mysql(),
        tablet_41983.init_mysql(),
        tablet_31981.init_mysql(),
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
      tablet_41983.teardown_mysql(),
      tablet_31981.teardown_mysql(),
  ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  tablet_62344.remove_tree()
  tablet_62044.remove_tree()
  tablet_41983.remove_tree()
  tablet_31981.remove_tree()


class TestReparent(unittest.TestCase):

  def tearDown(self):
    tablet.Tablet.check_vttablet_count()
    environment.topo_server_wipe()
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
      t.clean_dbs()
    super(TestReparent, self).tearDown()

  def _check_db_addr(self, shard, db_type, expected_port, cell='test_nj'):
    ep = utils.run_vtctl_json(['GetEndPoints', cell, 'test_keyspace/' + shard,
                               db_type])
    self.assertEqual(
        len(ep['entries']), 1, 'Wrong number of entries: %s' % str(ep))
    port = ep['entries'][0]['named_port_map']['_vtocc']
    self.assertEqual(port, expected_port,
                     'Unexpected port: %u != %u from %s' % (port, expected_port,
                                                            str(ep)))
    host = ep['entries'][0]['host']
    if not host.startswith(utils.hostname):
      self.fail(
          'Invalid hostname %s was expecting something starting with %s' %
          (host, utils.hostname))

  def test_master_to_spare_state_change_impossible(self):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # create the database so vttablets start, as they are serving
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True,
                             wait_for_start=True)

    utils.run_vtctl(['ChangeSlaveType', tablet_62344.tablet_alias, 'spare'],
                    expect_fail=True)
    utils.run_vtctl(['ChangeSlaveType', '--force', tablet_62344.tablet_alias,
                     'spare'], expect_fail=True)
    tablet_62344.kill_vttablet()

  def test_reparent_down_master(self):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # create the database so vttablets start, as they are serving
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62044.create_db('vt_test_keyspace')
    tablet_41983.create_db('vt_test_keyspace')
    tablet_31981.create_db('vt_test_keyspace')

    # Start up a master mysql and vttablet
    tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True,
                             wait_for_start=False)

    # Create a few slaves for testing reparenting.
    tablet_62044.init_tablet('replica', 'test_keyspace', '0', start=True,
                             wait_for_start=False)
    tablet_41983.init_tablet('replica', 'test_keyspace', '0', start=True,
                             wait_for_start=False)
    tablet_31981.init_tablet('replica', 'test_keyspace', '0', start=True,
                             wait_for_start=False)

    # wait for all tablets to start
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.wait_for_vttablet_state('SERVING')

    # Recompute the shard layout node - until you do that, it might not be
    # valid.
    utils.run_vtctl(['RebuildShardGraph', 'test_keyspace/0'])
    utils.validate_topology()

    # Force the slaves to reparent assuming that all the datasets are identical.
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/0',
                     tablet_62344.tablet_alias], auto_log=True)
    utils.validate_topology()

    # Make the master agent and database unavailable.
    tablet_62344.kill_vttablet()
    tablet_62344.shutdown_mysql().wait()

    self._check_db_addr('0', 'master', tablet_62344.port)

    # Perform a reparent operation - the Validate part will try to ping
    # the master and fail somewhat quickly
    stdout, stderr = utils.run_vtctl(['-wait-time', '5s', 'ReparentShard',
                                      'test_keyspace/0',
                                      tablet_62044.tablet_alias],
                                     expect_fail=True)
    logging.debug('Failed ReparentShard output:\n' + stderr)
    if 'ValidateShard verification failed' not in stderr:
      self.fail(
          "didn't find the right error strings in failed ReparentShard: " +
          stderr)

    # Should timeout and fail
    stdout, stderr = utils.run_vtctl(['-wait-time', '5s', 'ScrapTablet',
                                      tablet_62344.tablet_alias],
                                     expect_fail=True)
    logging.debug('Failed ScrapTablet output:\n' + stderr)
    if 'deadline exceeded' not in stderr:
      self.fail("didn't find the right error strings in failed ScrapTablet: " +
                stderr)

    # Should interrupt and fail
    args = environment.binary_args('vtctl') + [
        '-log_dir', environment.vtlogroot,
        '-wait-time', '10s']
    args.extend(environment.topo_server_flags())
    args.extend(environment.tablet_manager_protocol_flags())
    args.extend(['ScrapTablet', tablet_62344.tablet_alias])
    sp = utils.run_bg(args, stdout=PIPE, stderr=PIPE)
    # Need time for the process to start before killing it.
    time.sleep(3.0)
    os.kill(sp.pid, signal.SIGINT)
    stdout, stderr = sp.communicate()

    logging.debug('Failed ScrapTablet output:\n' + stderr)
    if 'interrupted' not in stderr:
      self.fail("didn't find the right error strings in failed ScrapTablet: " +
                stderr)

    # Force the scrap action in zk even though tablet is not accessible.
    tablet_62344.scrap(force=True)

    utils.run_vtctl(['ChangeSlaveType', '-force', tablet_62344.tablet_alias,
                     'idle'], expect_fail=True)

    # Remove pending locks (make this the force option to ReparentShard?)
    if environment.topo_server_implementation == 'zookeeper':
      utils.run_vtctl(['PurgeActions',
                       '/zk/global/vt/keyspaces/test_keyspace/shards/0/action'])

    # Re-run reparent operation, this should now proceed unimpeded.
    utils.run_vtctl(['-wait-time', '1m', 'ReparentShard', 'test_keyspace/0',
                     tablet_62044.tablet_alias],
                    mode=utils.VTCTL_VTCTL, auto_log=True)

    utils.validate_topology()
    self._check_db_addr('0', 'master', tablet_62044.port)

    utils.run_vtctl(['ChangeSlaveType', '-force', tablet_62344.tablet_alias,
                     'idle'])

    idle_tablets, _ = utils.run_vtctl(['ListAllTablets', 'test_nj'],
                                      trap_output=True)
    if '0000062344 <null> <null> idle' not in idle_tablets:
      self.fail('idle tablet not found: %s' % idle_tablets)

    tablet.kill_tablets([tablet_62044, tablet_41983, tablet_31981])

    # so the other tests don't have any surprise
    tablet_62344.start_mysql().wait()

  def test_reparent_cross_cell(self, shard_id='0'):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # create the database so vttablets start, as they are serving
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62044.create_db('vt_test_keyspace')
    tablet_41983.create_db('vt_test_keyspace')
    tablet_31981.create_db('vt_test_keyspace')

    # Start up a master mysql and vttablet
    tablet_62344.init_tablet('master', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    shard = utils.run_vtctl_json(['GetShard', 'test_keyspace/' + shard_id])
    self.assertEqual(shard['Cells'], ['test_nj'],
                     'wrong list of cell in Shard: %s' % str(shard['Cells']))

    # Create a few slaves for testing reparenting.
    tablet_62044.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    tablet_41983.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    tablet_31981.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.wait_for_vttablet_state('SERVING')
    shard = utils.run_vtctl_json(['GetShard', 'test_keyspace/' + shard_id])
    self.assertEqual(
        shard['Cells'], ['test_nj', 'test_ny'],
        'wrong list of cell in Shard: %s' % str(shard['Cells']))

    # Recompute the shard layout node - until you do that, it might not be
    # valid.
    utils.run_vtctl(['RebuildShardGraph', 'test_keyspace/' + shard_id])
    utils.validate_topology()

    # Force the slaves to reparent assuming that all the datasets are identical.
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.pause('force ReparentShard?')
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/' + shard_id,
                     tablet_62344.tablet_alias])
    utils.validate_topology(ping_tablets=True)

    self._check_db_addr(shard_id, 'master', tablet_62344.port)

    # Verify MasterCell is properly set
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_nj',
                                     'test_keyspace/%s' % (shard_id)])
    self.assertEqual(srvShard['MasterCell'], 'test_nj')
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_ny',
                                     'test_keyspace/%s' % (shard_id)])
    self.assertEqual(srvShard['MasterCell'], 'test_nj')

    # Perform a graceful reparent operation to another cell.
    utils.pause('graceful ReparentShard?')
    utils.run_vtctl(['ReparentShard', 'test_keyspace/' + shard_id,
                     tablet_31981.tablet_alias], auto_log=True)
    utils.validate_topology()

    self._check_db_addr(shard_id, 'master', tablet_31981.port, cell='test_ny')

    # Verify MasterCell is set to new cell.
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_nj',
                                     'test_keyspace/%s' % (shard_id)])
    self.assertEqual(srvShard['MasterCell'], 'test_ny')
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_ny',
                                     'test_keyspace/%s' % (shard_id)])
    self.assertEqual(srvShard['MasterCell'], 'test_ny')

    tablet.kill_tablets([tablet_62344, tablet_62044, tablet_41983,
                         tablet_31981])

  def test_reparent_graceful_range_based(self):
    shard_id = '0000000000000000-ffffffffffffffff'
    self._test_reparent_graceful(shard_id)

  def test_reparent_graceful(self):
    shard_id = '0'
    self._test_reparent_graceful(shard_id)

  def _test_reparent_graceful(self, shard_id):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # create the database so vttablets start, as they are serving
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62044.create_db('vt_test_keyspace')
    tablet_41983.create_db('vt_test_keyspace')
    tablet_31981.create_db('vt_test_keyspace')

    # Start up a master mysql and vttablet
    tablet_62344.init_tablet('master', 'test_keyspace', shard_id, start=True)
    if environment.topo_server_implementation == 'zookeeper':
      shard = utils.run_vtctl_json(['GetShard', 'test_keyspace/' + shard_id])
      self.assertEqual(shard['Cells'], ['test_nj'],
                       'wrong list of cell in Shard: %s' % str(shard['Cells']))

    # Create a few slaves for testing reparenting.
    tablet_62044.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    tablet_41983.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    tablet_31981.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    for t in [tablet_62044, tablet_41983, tablet_31981]:
      t.wait_for_vttablet_state('SERVING')
    if environment.topo_server_implementation == 'zookeeper':
      shard = utils.run_vtctl_json(['GetShard', 'test_keyspace/' + shard_id])
      self.assertEqual(shard['Cells'], ['test_nj', 'test_ny'],
                       'wrong list of cell in Shard: %s' % str(shard['Cells']))

    # Recompute the shard layout node - until you do that, it might not be
    # valid.
    utils.run_vtctl(['RebuildShardGraph', 'test_keyspace/' + shard_id])
    utils.validate_topology()

    # Force the slaves to reparent assuming that all the datasets are identical.
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.pause('force ReparentShard?')
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/' + shard_id,
                     tablet_62344.tablet_alias])
    utils.validate_topology(ping_tablets=True)

    self._check_db_addr(shard_id, 'master', tablet_62344.port)

    # Verify MasterCell is set to new cell.
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_nj',
                                     'test_keyspace/%s' % (shard_id)])
    self.assertEqual(srvShard['MasterCell'], 'test_nj')
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_ny',
                                     'test_keyspace/%s' % (shard_id)])
    self.assertEqual(srvShard['MasterCell'], 'test_nj')

    # Convert two replica to spare. That should leave only one node serving traffic,
    # but still needs to appear in the replication graph.
    utils.run_vtctl(['ChangeSlaveType', tablet_41983.tablet_alias, 'spare'])
    utils.run_vtctl(['ChangeSlaveType', tablet_31981.tablet_alias, 'spare'])
    utils.validate_topology()
    self._check_db_addr(shard_id, 'replica', tablet_62044.port)

    # Run this to make sure it succeeds.
    utils.run_vtctl(['ShardReplicationPositions', 'test_keyspace/' + shard_id],
                    stdout=utils.devnull)

    # Perform a graceful reparent operation.
    utils.pause('graceful ReparentShard?')
    utils.run_vtctl(['ReparentShard', 'test_keyspace/' + shard_id,
                     tablet_62044.tablet_alias], auto_log=True)
    utils.validate_topology()

    self._check_db_addr(shard_id, 'master', tablet_62044.port)

    # Verify MasterCell is set to new cell.
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_nj',
                                     'test_keyspace/%s' % (shard_id)])
    self.assertEqual(srvShard['MasterCell'], 'test_nj')
    srvShard = utils.run_vtctl_json(['GetSrvShard', 'test_ny',
                                     'test_keyspace/%s' % (shard_id)])
    self.assertEqual(srvShard['MasterCell'], 'test_nj')

    tablet.kill_tablets([tablet_62344, tablet_62044, tablet_41983,
                         tablet_31981])

    # Test address correction.
    new_port = environment.reserve_ports(1)
    tablet_62044.start_vttablet(port=new_port)

    # Wait until the new address registers.
    timeout = 30.0
    while True:
      try:
        self._check_db_addr(shard_id, 'master', new_port)
        break
      except:
        timeout = utils.wait_step('waiting for new port to register',
                                  timeout, sleep_time=0.1)

    tablet_62044.kill_vttablet()

  # This is a manual test to check error formatting.
  def _test_reparent_slave_offline(self, shard_id='0'):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # create the database so vttablets start, as they are serving
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62044.create_db('vt_test_keyspace')
    tablet_41983.create_db('vt_test_keyspace')
    tablet_31981.create_db('vt_test_keyspace')

    # Start up a master mysql and vttablet
    tablet_62344.init_tablet('master', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)

    # Create a few slaves for testing reparenting.
    tablet_62044.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    tablet_41983.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    tablet_31981.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)

    # wait for all tablets to start
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.wait_for_vttablet_state('SERVING')

    # Recompute the shard layout node - until you do that, it might not be
    # valid.
    utils.run_vtctl(['RebuildShardGraph', 'test_keyspace/' + shard_id])
    utils.validate_topology()

    # Force the slaves to reparent assuming that all the datasets are identical.
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/' + shard_id,
                     tablet_62344.tablet_alias])
    utils.validate_topology(ping_tablets=True)

    self._check_db_addr(shard_id, 'master', tablet_62344.port)

    # Kill one tablet so we seem offline
    tablet_31981.kill_vttablet()

    # Perform a graceful reparent operation.
    utils.run_vtctl(['ReparentShard', 'test_keyspace/' + shard_id,
                     tablet_62044.tablet_alias])

    tablet.kill_tablets([tablet_62344, tablet_62044, tablet_41983])

  # assume a different entity is doing the reparent, and telling us it was done
  def test_reparent_from_outside(self):
    self._test_reparent_from_outside(brutal=False)

  def test_reparent_from_outside_brutal(self):
    self._test_reparent_from_outside(brutal=True)

  def test_reparent_from_outside_rpc(self):
    self._test_reparent_from_outside(brutal=False, rpc=True)

  def test_reparent_from_outside_brutal_rpc(self):
    self._test_reparent_from_outside(brutal=True, rpc=True)

  def _test_reparent_from_outside(self, brutal=False, rpc=False):
    """This test will start a master and 3 slaves. Then:
    - one slave will be the new master
    - one slave will be reparented to that new master
    - one slave will be busted and ded in the water
    and we'll call ShardExternallyReparented.

    Args:
      brutal: scraps the old master first
      rpc: sends an RPC to the new master instead of doing the work.
    """
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # create the database so vttablets start, as they are serving
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.create_db('vt_test_keyspace')

    # Start up a master mysql and vttablet
    tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True,
                             wait_for_start=False)

    # Create a few slaves for testing reparenting.
    tablet_62044.init_tablet('replica', 'test_keyspace', '0', start=True,
                             wait_for_start=False)
    tablet_41983.init_tablet('replica', 'test_keyspace', '0', start=True,
                             wait_for_start=False)
    tablet_31981.init_tablet('replica', 'test_keyspace', '0', start=True,
                             wait_for_start=False)

    # wait for all tablets to start
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.wait_for_vttablet_state('SERVING')

    # Reparent as a starting point
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/0',
                     tablet_62344.tablet_alias], auto_log=True)

    # now manually reparent 1 out of 2 tablets
    # 62044 will be the new master
    # 31981 won't be re-parented, so it will be busted
    tablet_62044.mquery('', mysql_flavor().promote_slave_commands())
    new_pos = mysql_flavor().master_position(tablet_62044)
    logging.debug('New master position: %s', str(new_pos))
    changeMasterCmds = mysql_flavor().change_master_commands(
                            utils.hostname,
                            tablet_62044.mysql_port,
                            new_pos)

    # 62344 will now be a slave of 62044
    tablet_62344.mquery('', ['RESET MASTER', 'RESET SLAVE'] +
                        changeMasterCmds +
                        ['START SLAVE'])

    # 41983 will be a slave of 62044
    tablet_41983.mquery('', ['STOP SLAVE'] +
                        changeMasterCmds +
                        ['START SLAVE'])

    # in brutal mode, we scrap the old master first
    if brutal:
      tablet_62344.scrap(force=True)
      # we have some automated tools that do this too, so it's good to simulate
      if environment.topo_server_implementation == 'zookeeper':
        utils.run(environment.binary_args('zk') + ['rm', '-rf',
                                                   tablet_62344.zk_tablet_path])

    # update zk with the new graph
    extra_args = []
    if rpc:
      extra_args = ['-use_rpc']
    utils.run_vtctl(['ShardExternallyReparented'] + extra_args +
                    ['test_keyspace/0', tablet_62044.tablet_alias],
                    mode=utils.VTCTL_VTCTL, auto_log=True)

    self._test_reparent_from_outside_check(brutal)

    utils.run_vtctl(['RebuildReplicationGraph', 'test_nj', 'test_keyspace'])

    self._test_reparent_from_outside_check(brutal)

    tablet.kill_tablets([tablet_31981, tablet_62344, tablet_62044,
                         tablet_41983])

  def _test_reparent_from_outside_check(self, brutal):
    if environment.topo_server_implementation != 'zookeeper':
      return
    # make sure the shard replication graph is fine
    shard_replication = utils.run_vtctl_json(['GetShardReplication', 'test_nj',
                                              'test_keyspace/0'])
    hashed_links = {}
    for rl in shard_replication['ReplicationLinks']:
      key = rl['TabletAlias']['Cell'] + '-' + str(rl['TabletAlias']['Uid'])
      value = rl['Parent']['Cell'] + '-' + str(rl['Parent']['Uid'])
      hashed_links[key] = value
    logging.debug('Got replication links: %s', str(hashed_links))
    expected_links = {'test_nj-41983': 'test_nj-62044'}
    if not brutal:
      expected_links['test_nj-62344'] = 'test_nj-62044'
    self.assertEqual(expected_links, hashed_links,
                     'Got unexpected links: %s != %s' % (str(expected_links),
                                                         str(hashed_links)))

    tablet_62044_master_status = tablet_62044.get_status()
    self.assertIn('Serving graph: test_keyspace 0 master', tablet_62044_master_status)

  _create_vt_insert_test = '''create table vt_insert_test (
  id bigint auto_increment,
  msg varchar(64),
  primary key (id)
  ) Engine=InnoDB'''

  _populate_vt_insert_test = [
      "insert into vt_insert_test (msg) values ('test %s')" % x
      for x in xrange(4)]

  # See if a lag slave can be safely reparent.
  def test_reparent_lag_slave(self, shard_id='0'):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # create the database so vttablets start, as they are serving
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62044.create_db('vt_test_keyspace')
    tablet_41983.create_db('vt_test_keyspace')
    tablet_31981.create_db('vt_test_keyspace')

    # Start up a master mysql and vttablet
    tablet_62344.init_tablet('master', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)

    # Create a few slaves for testing reparenting.
    tablet_62044.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    tablet_31981.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    tablet_41983.init_tablet('lag', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)

    # wait for all tablets to start
    for t in [tablet_62344, tablet_62044, tablet_31981]:
      t.wait_for_vttablet_state('SERVING')
    tablet_41983.wait_for_vttablet_state('NOT_SERVING')

    # Recompute the shard layout node - until you do that, it might not be
    # valid.
    utils.run_vtctl(['RebuildShardGraph', 'test_keyspace/' + shard_id])
    utils.validate_topology()

    # Force the slaves to reparent assuming that all the datasets are identical.
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/' + shard_id,
                     tablet_62344.tablet_alias])
    utils.validate_topology(ping_tablets=True)

    tablet_62344.mquery('vt_test_keyspace', self._create_vt_insert_test)

    tablet_41983.mquery('', 'stop slave')
    for q in self._populate_vt_insert_test:
      tablet_62344.mquery('vt_test_keyspace', q, write=True)

    # Perform a graceful reparent operation.
    utils.run_vtctl(['ReparentShard', 'test_keyspace/' + shard_id,
                     tablet_62044.tablet_alias])

    tablet_41983.mquery('', 'start slave')
    time.sleep(1)

    utils.pause('check orphan')

    utils.run_vtctl(['ReparentTablet', tablet_41983.tablet_alias])

    result = tablet_41983.mquery('vt_test_keyspace',
                                 'select msg from vt_insert_test where id=1')
    if len(result) != 1:
      self.fail('expected 1 row from vt_insert_test: %s' % str(result))

    utils.pause('check lag reparent')

    tablet.kill_tablets([tablet_62344, tablet_62044, tablet_41983,
                         tablet_31981])


if __name__ == '__main__':
  utils.main()
