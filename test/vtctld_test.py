#!/usr/bin/env python
import json
import logging
import os
import unittest
import urllib2

import environment
import tablet
import utils


# range "" - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
shard_0_spare = tablet.Tablet()
# range 80 - ""
shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()

idle = tablet.Tablet()
scrap = tablet.Tablet()
assigned = [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica]
tablets = assigned + [idle, scrap, shard_0_spare]


class VtctldError(Exception): pass


class Vtctld(object):

  def dbtopo(self):
    data = json.load(urllib2.urlopen('http://localhost:8080/dbtopo?format=json'))
    if data["Error"]:
      raise VtctldError(data)
    return data["Topology"]

  def serving_graph(self):
    data = json.load(urllib2.urlopen('http://localhost:8080/serving_graph/test_nj?format=json'))
    if data["Error"]:
      raise VtctldError(data)
    return data["ServingGraph"]["Keyspaces"]

  def start(self):
    args = [environment.binary_path('vtctld'), '-debug', '-templates', environment.vttop + '/go/cmd/vtctld/templates', '-log_dir', environment.tmproot]
    stderr_fd = open(os.path.join(environment.tmproot, "vtctld.stderr"), "w")
    self.proc = utils.run_bg(args, stderr=stderr_fd)
    return self.proc


vtctld = Vtctld()

def setUpModule():
  try:
    utils.zk_setup()

    setup_procs = [t.init_mysql() for t in tablets]
    utils.wait_procs(setup_procs)
    vtctld.start()

  except:
    tearDownModule()
    raise

def tearDownModule():
  if utils.options.skip_teardown:
    return

  teardown_procs = [t.teardown_mysql() for t in tablets]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  utils.zk_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  for t in tablets:
    t.remove_tree()


class TestVtctld(unittest.TestCase):

  @classmethod
  def setUpClass(klass):
    utils.run_vtctl('CreateKeyspace test_keyspace')

    shard_0_master.init_tablet( 'master',  'test_keyspace', '-80')
    shard_0_replica.init_tablet('replica', 'test_keyspace', '-80')
    shard_0_spare.init_tablet('spare', 'test_keyspace', '-80')
    shard_1_master.init_tablet( 'master',  'test_keyspace', '80-')
    shard_1_replica.init_tablet('replica', 'test_keyspace', '80-')
    idle.init_tablet('idle')
    scrap.init_tablet('idle')

    utils.run_vtctl('RebuildShardGraph /zk/global/vt/keyspaces/test_keyspace/shards/*', auto_log=True)
    utils.run_vtctl('RebuildKeyspaceGraph /zk/global/vt/keyspaces/*', auto_log=True)

    for t in assigned:
      t.create_db('vt_test_keyspace')
      t.start_vttablet()

    for t in scrap, idle, shard_0_spare:
      t.start_vttablet(wait_for_state='NOT_SERVING')

    scrap.scrap()

    for t in [shard_0_master, shard_0_replica, shard_0_spare,
              shard_1_master, shard_1_replica, idle, scrap]:
      t.reset_replication()
    utils.run_vtctl('ReparentShard -force test_keyspace/-80 ' + shard_0_master.tablet_alias, auto_log=True)
    utils.run_vtctl('ReparentShard -force test_keyspace/80- ' + shard_1_master.tablet_alias, auto_log=True)


    # run checks now before we start the tablets
    utils.validate_topology()

  def setUp(self):
    self.data = vtctld.dbtopo()
    self.serving_data = vtctld.serving_graph()

  def test_assigned(self):
    logging.debug("test_assigned: %s", str(self.data))
    self.assertItemsEqual(self.data["Assigned"].keys(), ["test_keyspace"])
    self.assertItemsEqual(self.data["Assigned"]["test_keyspace"].keys(), ["-80", "80-"])

  def test_not_assigned(self):
    self.assertEqual(len(self.data["Idle"]), 1)
    self.assertEqual(len(self.data["Scrap"]), 1)

  def test_partial(self):
    utils.pause("You can now run a browser and connect to http://localhost:8080 to manually check topology")
    self.assertEqual(self.data["Partial"], True)

  def test_explorer_redirects(self):
    self.assertEqual(urllib2.urlopen('http://localhost:8080/explorers/redirect?type=keyspace&explorer=zk&keyspace=test_keyspace').geturl(),
                     'http://localhost:8080/zk/global/vt/keyspaces/test_keyspace')
    self.assertEqual(urllib2.urlopen('http://localhost:8080/explorers/redirect?type=shard&explorer=zk&keyspace=test_keyspace&shard=-80').geturl(),
                     'http://localhost:8080/zk/global/vt/keyspaces/test_keyspace/shards/-80')
    self.assertEqual(urllib2.urlopen('http://localhost:8080/explorers/redirect?type=tablet&explorer=zk&alias=%s' % shard_0_replica.tablet_alias).geturl(),
                     'http://localhost:8080' + shard_0_replica.zk_tablet_path)

    self.assertEqual(urllib2.urlopen('http://localhost:8080/explorers/redirect?type=srv_keyspace&explorer=zk&keyspace=test_keyspace&cell=test_nj').geturl(),
                     'http://localhost:8080/zk/test_nj/vt/ns/test_keyspace')
    self.assertEqual(urllib2.urlopen('http://localhost:8080/explorers/redirect?type=srv_shard&explorer=zk&keyspace=test_keyspace&shard=-80&cell=test_nj').geturl(),
                     'http://localhost:8080/zk/test_nj/vt/ns/test_keyspace/-80')
    self.assertEqual(urllib2.urlopen('http://localhost:8080/explorers/redirect?type=srv_type&explorer=zk&keyspace=test_keyspace&shard=-80&tablet_type=replica&cell=test_nj').geturl(),
                     'http://localhost:8080/zk/test_nj/vt/ns/test_keyspace/-80/replica')

    self.assertEqual(urllib2.urlopen('http://localhost:8080/explorers/redirect?type=replication&explorer=zk&keyspace=test_keyspace&shard=-80&cell=test_nj').geturl(),
                     'http://localhost:8080/zk/test_nj/vt/replication/test_keyspace/-80')


  def test_serving_graph(self):
    self.assertItemsEqual(self.serving_data.keys(), ["test_keyspace"])
    self.assertItemsEqual(self.serving_data["test_keyspace"].keys(), ["-80", "80-"])
    self.assertItemsEqual(self.serving_data["test_keyspace"]["-80"].keys(), ["master", "replica"])
    self.assertEqual(len(self.serving_data["test_keyspace"]["-80"]["master"]), 1)

if __name__ == '__main__':
  utils.main()
