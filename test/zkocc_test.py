#!/usr/bin/env python
import datetime
import json
import logging
import os
import re
import tempfile
import time
import unittest

import environment
import tablet
import utils
from zk import zkocc


# We check in this test that we can achieve at least this QPS.
# Sometimes on slow machines this won't work.
# We used to observe 30k+ QPS on workstations. This has gone down to 13k.
# So now the value we check is 5k, and we have an action item to look into it.
MIN_QPS = 5000


def setUpModule():
  try:
    environment.topo_server().setup()

  except:
    tearDownModule()
    raise


def tearDownModule():
  if utils.options.skip_teardown:
    return

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()


class TopoOccTest(unittest.TestCase):
  def setUp(self):
    environment.topo_server().wipe()
    self.vtgate_zk, self.vtgate_zk_port = utils.vtgate_start()

  def tearDown(self):
    utils.vtgate_kill(self.vtgate_zk)

  def rebuild(self):
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

  def test_get_srv_keyspace_names(self):
    utils.run_vtctl('CreateKeyspace test_keyspace1')
    utils.run_vtctl('CreateKeyspace test_keyspace2')
    t1 = tablet.Tablet(tablet_uid=1, cell="nj")
    t1.init_tablet("master", "test_keyspace1", "0")
    t1.update_addrs()
    t2 = tablet.Tablet(tablet_uid=2, cell="nj")
    t2.init_tablet("master", "test_keyspace2", "0")
    t2.update_addrs()
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace*'], auto_log=True)

    # vtgate API test
    out, err = utils.run(environment.binary_argstr('zkclient2')+' -server localhost:%u -mode getSrvKeyspaceNames test_nj' % self.vtgate_zk_port, trap_output=True)
    self.assertEqual(err, "KeyspaceNames[0] = test_keyspace1\n" +
                          "KeyspaceNames[1] = test_keyspace2\n")

  def test_get_srv_keyspace(self):
    utils.run_vtctl('CreateKeyspace test_keyspace')
    t = tablet.Tablet(tablet_uid=1, cell="nj")
    t.init_tablet("master", "test_keyspace", "0")
    utils.run_vtctl('UpdateTabletAddrs -hostname localhost -ip-addr 127.0.0.1 -mysql-port %s -vts-port %s %s' % (t.mysql_port, t.port + 500, t.tablet_alias))
    self.rebuild()

    # vtgate zk API test
    out, err = utils.run(environment.binary_argstr('zkclient2')+' -server localhost:%u -mode getSrvKeyspace test_nj test_keyspace' % self.vtgate_zk_port, trap_output=True)
    self.assertEqual(err, "Partitions[master] =\n" +
                     "  Shards[0]={Start: , End: }\n" +
                     "Partitions[rdonly] =\n" +
                     "  Shards[0]={Start: , End: }\n" +
                     "Partitions[replica] =\n" +
                     "  Shards[0]={Start: , End: }\n" +
                     "TabletTypes[0] = master\n",
                     "Got wrong content: %s" % err)

  def test_get_end_points(self):
    utils.run_vtctl('CreateKeyspace test_keyspace')
    t = tablet.Tablet(tablet_uid=1, cell="nj")
    t.init_tablet("master", "test_keyspace", "0")
    t.update_addrs()
    self.rebuild()

    # vtgate zk API test
    out, err = utils.run(environment.binary_argstr('zkclient2')+' -server localhost:%u -mode getEndPoints test_nj test_keyspace 0 master' % self.vtgate_zk_port, trap_output=True)
    self.assertEqual(err, "Entries[0] = 1 localhost\n")


class TestTopo(unittest.TestCase):
  longMessage = True
  def setUp(self):
    environment.topo_server().wipe()

  # test_vtgate_qps can be run to profile vtgate:
  # Just run:
  #   ./zkocc_test.py -v TestTopo.test_vtgate_qps --skip-teardown
  # Then run:
  #   go tool pprof $VTROOT/bin/vtgate $VTDATAROOT/tmp/vtgate.pprof
  # (or with zkclient2 for the client side)
  # and for instance type 'web' in the prompt.
  def test_vtgate_qps(self):
    # create the topology
    utils.run_vtctl('CreateKeyspace test_keyspace')
    t = tablet.Tablet(tablet_uid=1, cell="nj")
    t.init_tablet("master", "test_keyspace", "0")
    t.update_addrs()
    utils.run_vtctl('RebuildKeyspaceGraph test_keyspace', auto_log=True)

    # start vtgate and the qps-er
    vtgate_proc, vtgate_port = utils.vtgate_start(
        extra_args=['-cpu_profile', os.path.join(environment.tmproot,
                                                 'vtgate.pprof')])
    qpser = utils.run_bg(environment.binary_args('zkclient2') + [
        '-server', 'localhost:%u' % vtgate_port,
        '-mode', 'qps',
        '-cpu_profile', os.path.join(environment.tmproot, 'zkclient2.pprof'),
        'test_nj', 'test_keyspace'])
    qpser.wait()

    # get the vtgate vars, make sure we have what we need
    v = utils.get_vars(vtgate_port)

    # some checks on performance / stats
    rpcCalls = v['TopoReaderRpcQueryCount']['test_nj']
    if rpcCalls < MIN_QPS * 10:
      self.fail('QPS is too low: %u < %u' % (rpcCalls / 10, MIN_QPS))
    else:
      logging.debug("Recorded qps: %u", rpcCalls / 10)
    utils.vtgate_kill(vtgate_proc)

  def test_fake_zkocc_connection(self):
    fkc = zkocc.FakeZkOccConnection.from_data_path("testing", environment.vttop + "/test/fake_zkocc_config.json")
    fkc.replace_zk_data("3306", "3310")
    fkc.replace_zk_data("127.0.0.1", "my.cool.hostname")

    # new style API tests
    keyspaces = fkc.get_srv_keyspace_names('testing')
    self.assertEqual(keyspaces, ["test_keyspace"], "get_srv_keyspace_names doesn't work")
    keyspace = fkc.get_srv_keyspace('testing', 'test_keyspace')
    self.assertEqual({
        'Shards': [{
            'AddrsByType': None,
            'KeyRange': {'End': '\xd0', 'Start': '\xc0'},
            'ReadOnly': False}],
        'TabletTypes': ['rdonly', 'replica', 'master']},
                     keyspace, "keyspace reading is wrong")
    end_points = fkc.get_end_points("testing", "test_keyspace", "0", "master")
    self.assertEqual({
        'entries': [{'host': 'my.cool.hostname',
                     'named_port_map': {'_mysql': 3310, '_vtocc': 6711},
                     'port': 0,
                     'uid': 0}]},
                     end_points, "end points are wrong")

if __name__ == '__main__':
  utils.main()
