#!/usr/bin/env python
import logging
import unittest
import re

from vtctl import vtctl_client

import environment
import tablet
import utils


# range '' - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
shard_0_spare = tablet.Tablet()
# range 80 - ''
shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()
# all tablets
tablets = [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica,
           shard_0_spare]


def setUpModule():
  try:
    environment.topo_server().setup()

    setup_procs = [t.init_mysql() for t in tablets]
    utils.Vtctld().start()
    utils.wait_procs(setup_procs)

  except:
    tearDownModule()
    raise


def tearDownModule():
  if utils.options.skip_teardown:
    return

  teardown_procs = [t.teardown_mysql() for t in tablets]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  for t in tablets:
    t.remove_tree()


class TestVtctld(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    utils.run_vtctl(['CreateKeyspace',
                     '--sharding_column_name', 'keyspace_id',
                     '--sharding_column_type', 'uint64',
                     'test_keyspace'])
    utils.run_vtctl(
        ['CreateKeyspace', '--served_from',
         'master:test_keyspace,replica:test_keyspace,rdonly:test_keyspace',
         'redirected_keyspace'])

    shard_0_master.init_tablet('master', 'test_keyspace', '-80')
    shard_0_replica.init_tablet('spare', 'test_keyspace', '-80')
    shard_0_spare.init_tablet('spare', 'test_keyspace', '-80')
    shard_1_master.init_tablet('master', 'test_keyspace', '80-')
    shard_1_replica.init_tablet('replica', 'test_keyspace', '80-')

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'redirected_keyspace'],
                    auto_log=True)

    # start running all the tablets
    for t in [shard_0_master, shard_1_master, shard_1_replica]:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(wait_for_state=None,
                       extra_args=utils.vtctld.process_args())
    shard_0_replica.create_db('vt_test_keyspace')
    shard_0_replica.start_vttablet(extra_args=utils.vtctld.process_args(),
                                   target_tablet_type='replica',
                                   wait_for_state=None)

    shard_0_spare.start_vttablet(wait_for_state=None,
                                 extra_args=utils.vtctld.process_args())

    # wait for the right states
    for t in [shard_0_master, shard_1_master, shard_1_replica]:
      t.wait_for_vttablet_state('SERVING')
    for t in [shard_0_replica, shard_0_spare]:
      t.wait_for_vttablet_state('NOT_SERVING')

    for t in [shard_0_master, shard_0_replica, shard_0_spare,
              shard_1_master, shard_1_replica]:
      t.reset_replication()
    utils.run_vtctl(['InitShardMaster', 'test_keyspace/-80',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', 'test_keyspace/80-',
                     shard_1_master.tablet_alias], auto_log=True)
    shard_0_replica.wait_for_vttablet_state('SERVING')

    # run checks now before we start the tablets
    utils.validate_topology()

  def _check_all_tablets(self, result):
    lines = result.splitlines()
    self.assertEqual(len(lines), len(tablets), 'got lines:\n%s' % lines)
    line_map = {}
    for line in lines:
      parts = line.split()
      alias = parts[0]
      line_map[alias] = parts
    for t in tablets:
      if t.tablet_alias not in line_map:
        self.assertFalse('tablet %s is not in the result: %s' % (
            t.tablet_alias, str(line_map)))

  def test_vtctl(self):
    # standalone RPC client to vtctld
    out, _ = utils.run_vtctl(
        ['ListAllTablets', 'test_nj'], mode=utils.VTCTL_VTCTLCLIENT)
    self._check_all_tablets(out)

    # vtctl querying the topology directly
    out, _ = utils.run_vtctl(['ListAllTablets', 'test_nj'],
                             mode=utils.VTCTL_VTCTL,
                             trap_output=True, auto_log=True)
    self._check_all_tablets(out)

    # python RPC client to vtctld
    out, _ = utils.run_vtctl(['ListAllTablets', 'test_nj'],
                             mode=utils.VTCTL_RPC)
    self._check_all_tablets(out)

  def test_tablet_status(self):
    # the vttablet that has a health check has a bit more, so using it
    shard_0_replica_status = shard_0_replica.get_status()
    self.assertTrue(
        re.search(r'Polling health information from.+MySQLReplicationLag',
                  shard_0_replica_status))
    self.assertIn('Alias: <a href="http://localhost:', shard_0_replica_status)
    self.assertIn('</html>', shard_0_replica_status)

  def test_interrupt_vtctl_command(self):
    """An interrupted streaming vtctl command should work."""
    protocol, endpoint = utils.vtctld.rpc_endpoint(python=True)
    vtctld_connection = vtctl_client.connect(protocol, endpoint, 30)
    for i, event in enumerate(
        vtctld_connection.execute_vtctl_command(['ListAllTablets', 'test_nj'])):
      logging.debug('got event %d %s', i, event.value)
      if i == 1:
        break
    vtctld_connection.close()

if __name__ == '__main__':
  utils.main()
