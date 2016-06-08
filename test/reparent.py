#!/usr/bin/env python

import logging
import time
import unittest

from vtproto import topodata_pb2

import environment
import utils
import tablet
from mysql_flavor import mysql_flavor
from protocols_flavor import protocols_flavor


tablet_62344 = tablet.Tablet(62344)
tablet_62044 = tablet.Tablet(62044)
tablet_41983 = tablet.Tablet(41983)
tablet_31981 = tablet.Tablet(31981)


def setUpModule():
  try:
    environment.topo_server().setup()

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
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  teardown_procs = [
      tablet_62344.teardown_mysql(),
      tablet_62044.teardown_mysql(),
      tablet_41983.teardown_mysql(),
      tablet_31981.teardown_mysql(),
  ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  tablet_62344.remove_tree()
  tablet_62044.remove_tree()
  tablet_41983.remove_tree()
  tablet_31981.remove_tree()


class TestReparent(unittest.TestCase):

  def tearDown(self):
    tablet.Tablet.check_vttablet_count()
    environment.topo_server().wipe()
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
      t.set_semi_sync_enabled(master=False)
      t.clean_dbs()
    super(TestReparent, self).tearDown()

  _create_vt_insert_test = '''create table vt_insert_test (
  id bigint,
  msg varchar(64),
  primary key (id)
  ) Engine=InnoDB'''

  def _populate_vt_insert_test(self, master_tablet, index):
    q = ("insert into vt_insert_test(id, msg) values (%d, 'test %d')" %
         (index, index))
    master_tablet.mquery('vt_test_keyspace', q, write=True)

  def _check_vt_insert_test(self, tablet_obj, index):
    # wait until it gets the data
    timeout = 10.0
    while True:
      result = tablet_obj.mquery(
          'vt_test_keyspace',
          'select msg from vt_insert_test where id=%d' % index)
      if len(result) == 1:
        break
      timeout = utils.wait_step('waiting for replication to catch up on %s' %
                                tablet_obj.tablet_alias,
                                timeout, sleep_time=0.1)

  def _check_master_tablet(self, t, port=None):
    """Makes sure the tablet type is master, and its health check agrees."""
    ti = utils.run_vtctl_json(['GetTablet', t.tablet_alias])
    self.assertEqual(ti['type'], topodata_pb2.MASTER)
    if port:
      self.assertEqual(ti['port_map']['vt'], port)

    # make sure the health stream is updated
    health = utils.run_vtctl_json(['VtTabletStreamHealth', '-count', '1',
                                   t.tablet_alias])
    self.assertIn('serving', health)
    self.assertEqual(health['target']['tablet_type'], topodata_pb2.MASTER)

  def test_master_to_spare_state_change_impossible(self):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # create the database so vttablets start, as they are serving
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True,
                             wait_for_start=True)

    utils.run_vtctl(['ChangeSlaveType', tablet_62344.tablet_alias, 'spare'],
                    expect_fail=True)
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
    tablet_62344.wait_for_vttablet_state('SERVING')
    for t in [tablet_62044, tablet_41983, tablet_31981]:
      t.wait_for_vttablet_state('NOT_SERVING')

    utils.validate_topology()

    # Force the slaves to reparent assuming that all the datasets are
    # identical.
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.run_vtctl(['InitShardMaster', 'test_keyspace/0',
                     tablet_62344.tablet_alias], auto_log=True)
    utils.validate_topology()
    tablet_62344.mquery('vt_test_keyspace', self._create_vt_insert_test)

    # Make the current master agent and database unavailable.
    tablet_62344.kill_vttablet()
    tablet_62344.shutdown_mysql().wait()

    # Perform a planned reparent operation, will try to contact
    # the current master and fail somewhat quickly
    _, stderr = utils.run_vtctl(['-wait-time', '5s',
                                 'PlannedReparentShard', 'test_keyspace/0',
                                 tablet_62044.tablet_alias],
                                expect_fail=True)
    self.assertIn('DemoteMaster failed', stderr)

    # Run forced reparent operation, this should now proceed unimpeded.
    utils.run_vtctl(['EmergencyReparentShard', 'test_keyspace/0',
                     tablet_62044.tablet_alias], auto_log=True)

    utils.validate_topology()
    self._check_master_tablet(tablet_62044)

    # insert data into the new master, check the connected slaves work
    self._populate_vt_insert_test(tablet_62044, 2)
    self._check_vt_insert_test(tablet_41983, 2)
    self._check_vt_insert_test(tablet_31981, 2)

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
    self.assertEqual(shard['cells'], ['test_nj'],
                     'wrong list of cell in Shard: %s' % str(shard['cells']))

    # Create a few slaves for testing reparenting. Won't be healthy
    # as replication is not running.
    tablet_62044.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    tablet_41983.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    tablet_31981.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    tablet_62344.wait_for_vttablet_state('SERVING')
    for t in [tablet_62044, tablet_41983, tablet_31981]:
      t.wait_for_vttablet_state('NOT_SERVING')
    shard = utils.run_vtctl_json(['GetShard', 'test_keyspace/' + shard_id])
    self.assertEqual(
        shard['cells'], ['test_nj', 'test_ny'],
        'wrong list of cell in Shard: %s' % str(shard['cells']))

    utils.validate_topology()

    # Force the slaves to reparent assuming that all the datasets are
    # identical.
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.run_vtctl(['InitShardMaster', 'test_keyspace/' + shard_id,
                     tablet_62344.tablet_alias], auto_log=True)
    utils.validate_topology(ping_tablets=True)

    self._check_master_tablet(tablet_62344)

    # Perform a graceful reparent operation to another cell.
    utils.run_vtctl(['PlannedReparentShard', 'test_keyspace/' + shard_id,
                     tablet_31981.tablet_alias], auto_log=True)
    utils.validate_topology()

    self._check_master_tablet(tablet_31981)

    tablet.kill_tablets([tablet_62344, tablet_62044, tablet_41983,
                         tablet_31981])

  def test_reparent_graceful_range_based(self):
    utils.run_vtctl(['CreateKeyspace',
                     '--sharding_column_name', 'keyspace_id',
                     '--sharding_column_type', 'uint64',
                     'test_keyspace'])
    self._test_reparent_graceful('0000000000000000-ffffffffffffffff')

  def test_reparent_graceful(self):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])
    self._test_reparent_graceful('0')

  def _test_reparent_graceful(self, shard_id):
    # create the database so vttablets start, as they are serving
    tablet_62344.create_db('vt_test_keyspace')
    tablet_62044.create_db('vt_test_keyspace')
    tablet_41983.create_db('vt_test_keyspace')
    tablet_31981.create_db('vt_test_keyspace')

    # Start up a master mysql and vttablet
    tablet_62344.init_tablet('master', 'test_keyspace', shard_id, start=True)
    if environment.topo_server().flavor() == 'zookeeper':
      shard = utils.run_vtctl_json(['GetShard', 'test_keyspace/' + shard_id])
      self.assertEqual(shard['cells'], ['test_nj'],
                       'wrong list of cell in Shard: %s' % str(shard['cells']))

    # Create a few slaves for testing reparenting.
    tablet_62044.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    tablet_41983.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    tablet_31981.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)
    for t in [tablet_62044, tablet_41983, tablet_31981]:
      t.wait_for_vttablet_state('NOT_SERVING')
    if environment.topo_server().flavor() == 'zookeeper':
      shard = utils.run_vtctl_json(['GetShard', 'test_keyspace/' + shard_id])
      self.assertEqual(shard['cells'], ['test_nj', 'test_ny'],
                       'wrong list of cell in Shard: %s' % str(shard['cells']))

    utils.validate_topology()

    # Force the slaves to reparent assuming that all the datasets are
    # identical.
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.run_vtctl(['InitShardMaster', 'test_keyspace/' + shard_id,
                     tablet_62344.tablet_alias])
    utils.validate_topology(ping_tablets=True)
    tablet_62344.mquery('vt_test_keyspace', self._create_vt_insert_test)

    self._check_master_tablet(tablet_62344)

    utils.validate_topology()

    # Run this to make sure it succeeds.
    stdout, _ = utils.run_vtctl(['ShardReplicationPositions',
                                 'test_keyspace/' + shard_id],
                                trap_output=True)
    lines = stdout.splitlines()
    self.assertEqual(len(lines), 4)  # one master, three slaves
    self.assertIn('master', lines[0])  # master first

    # Perform a graceful reparent operation.
    utils.run_vtctl(['PlannedReparentShard', 'test_keyspace/' + shard_id,
                     tablet_62044.tablet_alias], auto_log=True)
    utils.validate_topology()

    self._check_master_tablet(tablet_62044)

    # insert data into the new master, check the connected slaves work
    self._populate_vt_insert_test(tablet_62044, 1)
    self._check_vt_insert_test(tablet_41983, 1)
    self._check_vt_insert_test(tablet_62344, 1)

    tablet.kill_tablets([tablet_62344, tablet_62044, tablet_41983,
                         tablet_31981])

    # Test address correction.
    new_port = environment.reserve_ports(1)
    tablet_62044.start_vttablet(port=new_port)

    # Wait until the new address registers.
    timeout = 30.0
    while True:
      try:
        self._check_master_tablet(tablet_62044, port=new_port)
        break
      except protocols_flavor().client_error_exception_type():
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
    tablet_62344.wait_for_vttablet_state('SERVING')
    for t in [tablet_62044, tablet_41983, tablet_31981]:
      t.wait_for_vttablet_state('NOT_SERVING')

    utils.validate_topology()

    # Force the slaves to reparent assuming that all the datasets are
    # identical.
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/' + shard_id,
                     tablet_62344.tablet_alias])
    utils.validate_topology(ping_tablets=True)

    self._check_master_tablet(tablet_62344)

    # Kill one tablet so we seem offline
    tablet_31981.kill_vttablet()

    # Perform a graceful reparent operation.
    utils.run_vtctl(['PlannedReparentShard', 'test_keyspace/' + shard_id,
                     tablet_62044.tablet_alias])
    self._check_master_tablet(tablet_62044)

    tablet.kill_tablets([tablet_62344, tablet_62044, tablet_41983])

  # assume a different entity is doing the reparent, and telling us it was done
  def test_reparent_from_outside(self):
    self._test_reparent_from_outside(brutal=False)

  def test_reparent_from_outside_brutal(self):
    self._test_reparent_from_outside(brutal=True)

  def _test_reparent_from_outside(self, brutal=False):
    """This test will start a master and 3 slaves.

    Then:
    - one slave will be the new master
    - one slave will be reparented to that new master
    - one slave will be busted and dead in the water
    and we'll call TabletExternallyReparented.

    Args:
      brutal: kills the old master first
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
    tablet_62344.wait_for_vttablet_state('SERVING')
    for t in [tablet_62044, tablet_41983, tablet_31981]:
      t.wait_for_vttablet_state('NOT_SERVING')

    # Reparent as a starting point
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.run_vtctl(['InitShardMaster', 'test_keyspace/0',
                     tablet_62344.tablet_alias], auto_log=True)

    # now manually reparent 1 out of 2 tablets
    # 62044 will be the new master
    # 31981 won't be re-parented, so it will be busted
    tablet_62044.mquery('', mysql_flavor().promote_slave_commands())
    new_pos = mysql_flavor().master_position(tablet_62044)
    logging.debug('New master position: %s', str(new_pos))
    # Use 'localhost' as hostname because Travis CI worker hostnames
    # are too long for MySQL replication.
    change_master_cmds = mysql_flavor().change_master_commands(
        'localhost',
        tablet_62044.mysql_port,
        new_pos)

    # 62344 will now be a slave of 62044
    tablet_62344.mquery('', ['RESET MASTER', 'RESET SLAVE'] +
                        change_master_cmds +
                        ['START SLAVE'])

    # 41983 will be a slave of 62044
    tablet_41983.mquery('', ['STOP SLAVE'] +
                        change_master_cmds +
                        ['START SLAVE'])

    # in brutal mode, we kill the old master first
    # and delete its tablet record
    if brutal:
      tablet_62344.kill_vttablet()
      utils.run_vtctl(['DeleteTablet', '-allow_master',
                       tablet_62344.tablet_alias], auto_log=True)

    base_time = time.time()

    # update topology with the new server
    utils.run_vtctl(['TabletExternallyReparented', tablet_62044.tablet_alias],
                    mode=utils.VTCTL_VTCTL, auto_log=True)

    self._test_reparent_from_outside_check(brutal, base_time)

    # RebuildReplicationGraph will rebuild the topo data from
    # the tablet records. It is an emergency command only.
    utils.run_vtctl(['RebuildReplicationGraph', 'test_nj', 'test_keyspace'])

    self._test_reparent_from_outside_check(brutal, base_time)

    if not brutal:
      tablet_62344.kill_vttablet()
    tablet.kill_tablets([tablet_31981, tablet_62044, tablet_41983])

  def _test_reparent_from_outside_check(self, brutal, base_time):

    # make sure the shard replication graph is fine
    shard_replication = utils.run_vtctl_json(['GetShardReplication', 'test_nj',
                                              'test_keyspace/0'])
    hashed_nodes = {}
    for node in shard_replication['nodes']:
      key = node['tablet_alias']['cell']+'-'+str(node['tablet_alias']['uid'])
      hashed_nodes[key] = True
    logging.debug('Got shard replication nodes: %s', str(hashed_nodes))
    expected_nodes = {
        'test_nj-41983': True,
        'test_nj-62044': True,
        }
    if not brutal:
      expected_nodes['test_nj-62344'] = True
    self.assertEqual(expected_nodes, hashed_nodes,
                     'Got unexpected nodes: %s != %s' % (str(expected_nodes),
                                                         str(hashed_nodes)))

    # make sure the master status page says it's the master
    tablet_62044_master_status = tablet_62044.get_status()
    self.assertIn('Keyspace: test_keyspace Shard: 0 Tablet Type: MASTER',
                  tablet_62044_master_status)

    # make sure the master health stream says it's the master too
    # (health check is disabled on these servers, force it first)
    utils.run_vtctl(['RunHealthCheck', tablet_62044.tablet_alias])
    health = utils.run_vtctl_json(['VtTabletStreamHealth',
                                   '-count', '1',
                                   tablet_62044.tablet_alias])
    self.assertEqual(health['target']['tablet_type'], topodata_pb2.MASTER)
    # have to compare the int version, or the rounding errors can break
    self.assertTrue(
        health['tablet_externally_reparented_timestamp'] >= int(base_time))

  # See if a missing slave can be safely reparented after the fact.
  def test_reparent_with_down_slave(self, shard_id='0'):
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
    tablet_41983.init_tablet('replica', 'test_keyspace', shard_id, start=True,
                             wait_for_start=False)

    # wait for all tablets to start
    tablet_62344.wait_for_vttablet_state('SERVING')
    for t in [tablet_62044, tablet_31981, tablet_41983]:
      t.wait_for_vttablet_state('NOT_SERVING')

    utils.validate_topology()

    # Force the slaves to reparent assuming that all the datasets are identical.
    for t in [tablet_62344, tablet_62044, tablet_41983, tablet_31981]:
      t.reset_replication()
    utils.run_vtctl(['InitShardMaster', 'test_keyspace/' + shard_id,
                     tablet_62344.tablet_alias])
    utils.validate_topology(ping_tablets=True)
    tablet_62344.mquery('vt_test_keyspace', self._create_vt_insert_test)

    utils.wait_procs([tablet_41983.shutdown_mysql()])

    # Perform a graceful reparent operation. It will fail as one tablet is down.
    _, stderr = utils.run_vtctl(['PlannedReparentShard',
                                 'test_keyspace/' + shard_id,
                                 tablet_62044.tablet_alias],
                                expect_fail=True)
    self.assertIn('TabletManager.SetMaster on test_nj-0000041983 error', stderr)

    # insert data into the new master, check the connected slaves work
    self._populate_vt_insert_test(tablet_62044, 3)
    self._check_vt_insert_test(tablet_31981, 3)
    self._check_vt_insert_test(tablet_62344, 3)

    # restart mysql on the old slave, should still be connecting to the
    # old master
    utils.wait_procs([tablet_41983.start_mysql()])

    utils.pause('check orphan')

    # reparent the tablet (will not start replication, so we have to
    # do it ourselves), then it should catch up on replication really quickly
    utils.run_vtctl(['ReparentTablet', tablet_41983.tablet_alias])
    utils.run_vtctl(['StartSlave', tablet_41983.tablet_alias])

    # wait until it gets the data
    self._check_vt_insert_test(tablet_41983, 3)

    tablet.kill_tablets([tablet_62344, tablet_62044, tablet_41983,
                         tablet_31981])


if __name__ == '__main__':
  utils.main()
