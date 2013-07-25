#!/usr/bin/env python
import optparse
import sys
import unittest

import tablet
import utils
from zk import zkocc


def setUpModule():
  try:
    utils.zk_setup()

  except:
    tearDownModule()
    raise


def tearDownModule():
  if utils.options.skip_teardown:
    return

  utils.zk_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()


class TopoOccTest(unittest.TestCase):
  def setUp(self):
    utils.zk_wipe()
    self.zkocc_server = utils.zkocc_start()
    self.topo = zkocc.TopoOccConnection("localhost:%u" % utils.zkocc_port_base, 30)
    self.topo.dial()

  def tearDown(self):
    utils.kill_sub_process(self.zkocc_server)

  def rebuild(self):
    utils.run_vtctl('RebuildShardGraph /zk/global/vt/keyspaces/test_keyspace/shards/0', auto_log=True)
    utils.run_vtctl('RebuildKeyspaceGraph /zk/global/vt/keyspaces/*', auto_log=True)

  def test_get_keyspaces(self):
    utils.run_vtctl('CreateKeyspace test_keyspace1')
    utils.run_vtctl('CreateKeyspace test_keyspace2')
    self.assertItemsEqual(self.topo.get_keyspaces(), ["test_keyspace1", "test_keyspace2"])

  def test_get_srv_keyspace(self):
    utils.run_vtctl('CreateKeyspace test_keyspace')
    t = tablet.Tablet(tablet_uid=1, cell="nj")
    t.init_tablet("master", "test_keyspace", "0")
    utils.run_vtctl('UpdateTabletAddrs %s -mysql-ip-addr 127.0.0.1:%s -secure-addr 127.0.0.1:%s' % (t.zk_tablet_path, t.mysql_port, t.port + 500))
    self.rebuild()
    reply = self.topo.get_srv_keyspace("test_nj", "test_keyspace")
    self.assertEqual(reply['TabletTypes'], ['master'])

  def test_get_end_points(self):
    utils.run_vtctl('CreateKeyspace test_keyspace')
    t = tablet.Tablet(tablet_uid=1, cell="nj")
    t.init_tablet("master", "test_keyspace", "0")
    t.update_addrs(mysql_ip_addr="127.0.0.1:%s" % t.mysql_port, secure_addr="localhost:%s" % (t.port + 500))
    self.rebuild()
    self.assertEqual(len(self.topo.get_end_points("test_nj", "test_keyspace", "0", "master")['Entries']), 1)

def main():
  parser = optparse.OptionParser(usage="usage: %prog [options] [test_names]")
  parser.add_option('--skip-teardown', action='store_true')
  parser.add_option('--teardown', action='store_true')
  parser.add_option("-q", "--quiet", action="store_const", const=0, dest="verbose", default=1)
  parser.add_option("-v", "--verbose", action="store_const", const=2, dest="verbose", default=1)
  parser.add_option("--no-build", action="store_true")

  (options, args) = parser.parse_args()

  utils.options = options
  global skip_teardown
  skip_teardown = options.skip_teardown
  if options.teardown:
    tearDownModule()
    sys.exit()
  unittest.main(argv=sys.argv[:1] + ['-f'])


if __name__ == '__main__':
  main()
