#!/usr/bin/python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import json
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

  def _check_db_addr(self, shard, db_type, expected_port):
    stdout, stderr = utils.run_vtctl(['GetEndPoints', 'test_nj', 'test_keyspace/'+shard, db_type],
                                     trap_output=True, auto_log=True)
    ep = json.loads(stdout)
    self.assertEqual(len(ep['entries']), 1 , 'Wrong number of entries: %s' % str(ep))
    port = ep['entries'][0]['named_port_map']['_vtocc']
    self.assertEqual(port, expected_port, 'Unexpected port: %u != %u from %s' % (port, expected_port, str(ep)))
    host = ep['entries'][0]['host']
    if not host.startswith(utils.hostname):
      self.fail('Invalid hostname %s was expecting something starting with %s' % host, utils.hostname)

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

    self._check_db_addr('0', 'master', tablet_62344.port)

    # Perform a reparent operation - the Validate part will try to ping
    # the master and fail somewhat quickly
    stdout, stderr = utils.run_vtctl('-wait-time 5s ReparentShard test_keyspace/0 ' + tablet_62044.tablet_alias, expect_fail=True)
    logging.debug("Failed ReparentShard output:\n" + stderr)
    if 'ValidateShard verification failed' not in stderr:
      raise utils.TestError("didn't find the right error strings in failed ReparentShard: " + stderr)

    # Should timeout and fail
    stdout, stderr = utils.run_vtctl('-wait-time 5s ScrapTablet ' + tablet_62344.tablet_alias, expect_fail=True)
    logging.debug("Failed ScrapTablet output:\n" + stderr)
    if 'deadline exceeded' not in stderr:
      raise utils.TestError("didn't find the right error strings in failed ScrapTablet: " + stderr)

    # Should interrupt and fail
    args = [environment.binary_path('vtctl'),
            '-log_dir', environment.tmproot,
            '-wait-time', '10s']
    args.extend(environment.topo_server_flags())
    args.extend(environment.tablet_manager_protocol_flags())
    args.extend(['ScrapTablet', tablet_62344.tablet_alias])
    sp = utils.run_bg(args, stdout=PIPE, stderr=PIPE)
    # Need time for the process to start before killing it.
    time.sleep(3.0)
    os.kill(sp.pid, signal.SIGINT)
    stdout, stderr = sp.communicate()

    logging.debug("Failed ScrapTablet output:\n" + stderr)
    if 'interrupted' not in stderr:
      raise utils.TestError("didn't find the right error strings in failed ScrapTablet: " + stderr)

    # Force the scrap action in zk even though tablet is not accessible.
    tablet_62344.scrap(force=True)

    utils.run_vtctl('ChangeSlaveType -force %s idle' %
                    tablet_62344.tablet_alias, expect_fail=True)

    # Remove pending locks (make this the force option to ReparentShard?)
    if environment.topo_server_implementation == 'zookeeper':
      utils.run_vtctl('PurgeActions /zk/global/vt/keyspaces/test_keyspace/shards/0/action')

    # Re-run reparent operation, this shoud now proceed unimpeded.
    utils.run_vtctl('-wait-time 1m ReparentShard test_keyspace/0 ' + tablet_62044.tablet_alias, auto_log=True)

    utils.validate_topology()
    self._check_db_addr('0', 'master', tablet_62044.port)

    utils.run_vtctl(['ChangeSlaveType', '-force', tablet_62344.tablet_alias, 'idle'])

    idle_tablets, _ = utils.run_vtctl('ListAllTablets test_nj', trap_output=True)
    if '0000062344 <null> <null> idle' not in idle_tablets:
      raise utils.TestError('idle tablet not found', idle_tablets)

    tablet_62044.kill_vttablet()
    tablet_41983.kill_vttablet()
    tablet_31981.kill_vttablet()

    # so the other tests don't have any surprise
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
    if environment.topo_server_implementation == 'zookeeper':
      shard = utils.zk_cat_json('/zk/global/vt/keyspaces/test_keyspace/shards/' + shard_id)
      self.assertEqual(shard['Cells'], ['test_nj'], 'wrong list of cell in Shard: %s' % str(shard['Cells']))

    # Create a few slaves for testing reparenting.
    tablet_62044.init_tablet('replica', 'test_keyspace', shard_id, start=True)
    tablet_41983.init_tablet('replica', 'test_keyspace', shard_id, start=True)
    tablet_31981.init_tablet('replica', 'test_keyspace', shard_id, start=True)
    if environment.topo_server_implementation == 'zookeeper':
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

    self._check_db_addr(shard_id, 'master', tablet_62344.port)

    # Convert two replica to spare. That should leave only one node serving traffic,
    # but still needs to appear in the replication graph.
    utils.run_vtctl(['ChangeSlaveType', tablet_41983.tablet_alias, 'spare'])
    utils.run_vtctl(['ChangeSlaveType', tablet_31981.tablet_alias, 'spare'])
    utils.validate_topology()
    self._check_db_addr(shard_id, 'replica', tablet_62044.port)

    # Run this to make sure it succeeds.
    utils.run_vtctl('ShardReplicationPositions test_keyspace/%s' % shard_id, stdout=utils.devnull)

    # Perform a graceful reparent operation.
    utils.pause("graceful ReparentShard?")
    utils.run_vtctl('ReparentShard test_keyspace/%s %s' % (shard_id, tablet_62044.tablet_alias), auto_log=True)
    utils.validate_topology()

    self._check_db_addr(shard_id, 'master', tablet_62044.port)

    tablet_62344.kill_vttablet()
    tablet_62044.kill_vttablet()
    tablet_41983.kill_vttablet()
    tablet_31981.kill_vttablet()

    # Test address correction.
    new_port = environment.reserve_ports(1)
    tablet_62044.start_vttablet(port=new_port)
    # Wait a moment for address to reregister.
    time.sleep(1.0)

    self._check_db_addr(shard_id, 'master', new_port)

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

    self._check_db_addr(shard_id, 'master', tablet_62344.port)

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
      if environment.topo_server_implementation == 'zookeeper':
        utils.run(environment.binary_path('zk')+' rm -rf ' + tablet_62344.zk_tablet_path)

    # try to pretend the wrong host is the master, should fail
    stdout, stderr = utils.run_vtctl('ShardExternallyReparented -scrap-stragglers test_keyspace/0 %s' % tablet_41983.tablet_alias, auto_log=True, expect_fail=True)
    if not "new master is a slave" in stderr:
      self.fail('Unexpected error message in output: %v' % stderr)

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
    if environment.topo_server_implementation != 'zookeeper':
      return
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


if __name__ == '__main__':
  utils.main()
