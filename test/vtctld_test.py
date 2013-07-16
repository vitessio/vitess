#!/usr/bin/env python
import json
import optparse
import os
import pprint
import socket
import sys
import unittest
import urllib2

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


class VtcldError(Exception): pass


class Vtctld(object):

  def dbtopo(self):
    data = json.load(urllib2.urlopen('http://localhost:8080/dbtopo?format=json'))
    if data["Error"]:
      raise VtcldError(data)
    return data["Topology"]

  def start(self):
    utils.prog_compile(['vtctld'])
    args = [os.path.join(utils.vtroot, 'bin', 'vtctld'), '-debug', '-templates', utils.vttop + '/go/cmd/vtctld/templates']
    stderr_fd = open(os.path.join(utils.tmp_root, "vtctld.stderr"), "w")
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

class TestDbTopo(unittest.TestCase):

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

    utils.run_vtctl('ReparentShard -force test_keyspace/-80 ' + shard_0_master.tablet_alias, auto_log=True)
    utils.run_vtctl('ReparentShard -force test_keyspace/80- ' + shard_1_master.tablet_alias, auto_log=True)


  # run checks now before we start the tablets
    utils.validate_topology()

  def setUp(self):
    self.data = vtctld.dbtopo()

  def test_assigned(self):
    if utils.options.verbose:
      pprint.pprint(self.data)
    self.assertItemsEqual(self.data["Assigned"].keys(), ["test_keyspace"])
    self.assertItemsEqual(self.data["Assigned"]["test_keyspace"].keys(), ["-80", "80-"])

  def test_not_assigned(self):
    self.assertEqual(len(self.data["Idle"]), 1)
    self.assertEqual(len(self.data["Scrap"]), 1)

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
