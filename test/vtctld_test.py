#!/usr/bin/env python
import json
import logging
import os
import unittest
import urllib2

import environment
import tablet
import utils
from zk import zkocc


# range "" - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
shard_0_spare = tablet.Tablet()
# range 80 - ""
shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()
# not assigned
idle = tablet.Tablet()
scrap = tablet.Tablet()
# all tablets
tablets = [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica,
           idle, scrap, shard_0_spare]
# vtgate
vtgate_server = None
vtgate_port = None

# vtctld
vtctld = utils.Vtctld()


def setUpModule():
  try:
    environment.topo_server_setup()

    setup_procs = [t.init_mysql() for t in tablets]
    utils.wait_procs(setup_procs)
    vtctld.start()

  except:
    tearDownModule()
    raise


def tearDownModule():
  if utils.options.skip_teardown:
    return

  utils.vtgate_kill(vtgate_server)

  teardown_procs = [t.teardown_mysql() for t in tablets]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  for t in tablets:
    t.remove_tree()


class TestVtctld(unittest.TestCase):

  @classmethod
  def setUpClass(klass):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])
    utils.run_vtctl(['CreateKeyspace',
                     '--served-from', 'master:test_keyspace,replica:test_keyspace,rdonly:test_keyspace',
                     'redirected_keyspace'])

    shard_0_master.init_tablet( 'master',  'test_keyspace', '-80')
    shard_0_replica.init_tablet('spare',   'test_keyspace', '-80')
    shard_0_spare.init_tablet(  'spare',   'test_keyspace', '-80')
    shard_1_master.init_tablet( 'master',  'test_keyspace', '80-')
    shard_1_replica.init_tablet('replica', 'test_keyspace', '80-')
    idle.init_tablet('idle')
    scrap.init_tablet('idle')

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'redirected_keyspace'],
                    auto_log=True)

    for t in [shard_0_master, shard_1_master, shard_1_replica]:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(extra_args=vtctld.process_args())
    shard_0_replica.create_db('vt_test_keyspace')
    shard_0_replica.start_vttablet(extra_args=vtctld.process_args(),
                                   target_tablet_type='replica',
                                   wait_for_state='NOT_SERVING')

    for t in scrap, idle, shard_0_spare:
      t.start_vttablet(wait_for_state='NOT_SERVING',
                       extra_args=vtctld.process_args())

    scrap.scrap()

    for t in [shard_0_master, shard_0_replica, shard_0_spare,
              shard_1_master, shard_1_replica, idle, scrap]:
      t.reset_replication()
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/-80',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/80-',
                     shard_1_master.tablet_alias], auto_log=True)
    shard_0_replica.wait_for_vttablet_state('SERVING')

    # run checks now before we start the tablets
    utils.validate_topology()

    # start a vtgate server too
    global vtgate_server, vtgate_port
    vtgate_server, vtgate_port = utils.vtgate_start(
        cache_ttl='0s', extra_args=vtctld.process_args())

  def setUp(self):
    self.data = vtctld.dbtopo()
    self.serving_data = vtctld.serving_graph()

  def _check_all_tablets(self, result):
    lines = result.splitlines()
    self.assertEqual(len(lines), len(tablets))
    line_map = {}
    for line in lines:
      parts = line.split()
      alias = parts[0]
      line_map[alias] = parts
    for tablet in tablets:
      if not tablet.tablet_alias in line_map:
         self.assertFalse('tablet %s is not in the result: %s' % (
                          tablet.tablet_alias, str(line_map)))
    
  def test_vtctl(self):
    # standalone RPC client to vtctld
    result = vtctld.vtctl_client(['ListAllTablets', 'test_nj'])
    self._check_all_tablets(result)

    # vtctl querying the topology directly
    out, err = utils.run_vtctl(['ListAllTablets', 'test_nj'],
                               trap_output=True, auto_log=True)
    self._check_all_tablets(out)

  def test_assigned(self):
    logging.debug("test_assigned: %s", str(self.data))
    self.assertItemsEqual(self.data["Assigned"].keys(), ["test_keyspace"])
    s0 = self.data["Assigned"]["test_keyspace"]['ShardNodes'][0]
    self.assertItemsEqual(s0['Name'], "-80")
    s1 = self.data["Assigned"]["test_keyspace"]['ShardNodes'][1]
    self.assertItemsEqual(s1['Name'], "80-")

  def test_not_assigned(self):
    self.assertEqual(len(self.data["Idle"]), 1)
    self.assertEqual(len(self.data["Scrap"]), 1)

  def test_partial(self):
    utils.pause("You can now run a browser and connect to http://localhost:%u to manually check topology" % vtctld.port)
    self.assertEqual(self.data["Partial"], True)

  def test_explorer_redirects(self):
    base = 'http://localhost:%u' % vtctld.port
    self.assertEqual(urllib2.urlopen(base + '/explorers/redirect?type=keyspace&explorer=zk&keyspace=test_keyspace').geturl(),
                     base + '/zk/global/vt/keyspaces/test_keyspace')
    self.assertEqual(urllib2.urlopen(base + '/explorers/redirect?type=shard&explorer=zk&keyspace=test_keyspace&shard=-80').geturl(),
                     base + '/zk/global/vt/keyspaces/test_keyspace/shards/-80')
    self.assertEqual(urllib2.urlopen(base + '/explorers/redirect?type=tablet&explorer=zk&alias=%s' % shard_0_replica.tablet_alias).geturl(),
                     base + shard_0_replica.zk_tablet_path)

    self.assertEqual(urllib2.urlopen(base + '/explorers/redirect?type=srv_keyspace&explorer=zk&keyspace=test_keyspace&cell=test_nj').geturl(),
                     base + '/zk/test_nj/vt/ns/test_keyspace')
    self.assertEqual(urllib2.urlopen(base + '/explorers/redirect?type=srv_shard&explorer=zk&keyspace=test_keyspace&shard=-80&cell=test_nj').geturl(),
                     base + '/zk/test_nj/vt/ns/test_keyspace/-80')
    self.assertEqual(urllib2.urlopen(base + '/explorers/redirect?type=srv_type&explorer=zk&keyspace=test_keyspace&shard=-80&tablet_type=replica&cell=test_nj').geturl(),
                     base + '/zk/test_nj/vt/ns/test_keyspace/-80/replica')

    self.assertEqual(urllib2.urlopen(base + '/explorers/redirect?type=replication&explorer=zk&keyspace=test_keyspace&shard=-80&cell=test_nj').geturl(),
                     base + '/zk/test_nj/vt/replication/test_keyspace/-80')

  def test_serving_graph(self):
    self.assertItemsEqual(sorted(self.serving_data.keys()),
                          ["redirected_keyspace", "test_keyspace"])
    s0 = self.serving_data["test_keyspace"]['ShardNodes'][0]
    self.assertItemsEqual(s0['Name'], "-80")
    self.assertItemsEqual(s0['ServedTypes'], ['master', 'replica', 'rdonly'])
    s1 = self.serving_data["test_keyspace"]['ShardNodes'][1]
    self.assertItemsEqual(s1['Name'], "80-")
    self.assertItemsEqual(sorted(s0['TabletNodes'].keys()),
                          ["master", "replica"])
    self.assertEqual(len(s0['TabletNodes']['master']), 1)
    self.assertEqual(self.serving_data["redirected_keyspace"]['ServedFrom']['master'],
                     'test_keyspace')

  def test_tablet_status(self):
    # the vttablet that has a health check has a bit more, so using it
    shard_0_replica_status = shard_0_replica.get_status()
    self.assertIn('Polling health information from MySQLReplicationLag(allowedLag=30)', shard_0_replica_status)
    self.assertIn('Alias: <a href="http://localhost:', shard_0_replica_status)
    self.assertIn('</html>', shard_0_replica_status)

  def test_vtgate(self):
    # do a few vtgate topology queries to prime the cache
    vtgate_client = zkocc.ZkOccConnection("localhost:%u" % vtgate_port,
                                          "test_nj", 30.0)
    vtgate_client.dial()
    vtgate_client.get_srv_keyspace_names("test_nj")
    vtgate_client.get_srv_keyspace("test_nj", "test_keyspace")
    vtgate_client.get_end_points("test_nj", "test_keyspace", "-80", "master")
    vtgate_client.close()

    status = utils.get_status(vtgate_port)
    self.assertIn('</html>', status) # end of page
    self.assertIn('/serving_graph/test_nj">test_nj', status) # vtctld link

    utils.pause("You can now run a browser and connect to http://localhost:%u%s to manually check vtgate status page" % (vtgate_port, environment.status_url))

if __name__ == '__main__':
  utils.main()
