#!/usr/bin/env python
"""
Script to set up a Vitess environment for Java client integration
tests. Every shard gets a master instance. For extra instances,
use the tablet-config option. Upon successful start up, the port for
VtGate is written to stdout.

Start up steps include:
- start MySQL instances
- configure keyspace
- start VtTablets and ensure SERVING mode
- start VtGate instance

Usage:
java_vtgate_test_helper.py --shards=-80,80- --tablet-config='{"rdonly":1, "replica":1}' --keyspace=test_keyspace setup
starts 1 VtGate and 6 vttablets - 1 master, replica and rdonly each per shard

java_vtgate_test_helper.py --shards=-80,80- --tablet-config='{"rdonly":1, "replica":1}' --keyspace=test_keyspace teardown
shuts down the tablets and VtGate instances
"""

import utils
import json
import optparse
import sys

import environment
import tablet

from vtdb import topology
from zk import zkocc

class Tablet(tablet.Tablet):
  def __init__(self, shard, type):
    super(Tablet, self).__init__()
    self.shard = shard
    self.type = type

class TestEnv(object):
  vtgate_server = None
  vtgate_port = None
  def __init__(self, options):
    self.keyspace = options.keyspace
    self.tablets = []
    tablet_config = json.loads(options.tablet_config)
    for shard in options.shards.split(','):
      self.tablets.append(Tablet(shard, "master"))
      for tablet_type, count in tablet_config.iteritems():
        for i in range(count):
          self.tablets.append(Tablet(shard, tablet_type))

  def set_up(self):
    try:
      environment.topo_server_setup()
      utils.wait_procs([t.init_mysql() for t in self.tablets])
      utils.run_vtctl(['CreateKeyspace', self.keyspace])
      utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', self.keyspace, 'keyspace_id', 'uint64'])
      for t in self.tablets:
        t.init_tablet(t.type, keyspace=self.keyspace, shard=t.shard)
      utils.run_vtctl(['RebuildKeyspaceGraph', self.keyspace], auto_log=True)
      for t in self.tablets:
        t.create_db('vt_' + self.keyspace)
        t.start_vttablet(wait_for_state=None)
      for t in self.tablets:
        t.wait_for_vttablet_state('SERVING')
      for t in self.tablets:
        if t.type == "master":
          utils.run_vtctl(['ReparentShard', '-force', self.keyspace+'/'+t.shard, t.tablet_alias], auto_log=True)
      utils.run_vtctl(['RebuildKeyspaceGraph', self.keyspace], auto_log=True)
      self.vtgate_server, self.vtgate_port = utils.vtgate_start(cache_ttl='500s')
      vtgate_client = zkocc.ZkOccConnection("localhost:%u" % self.vtgate_port, "test_nj", 30.0)
      topology.read_topology(vtgate_client)
    except:
      self.shutdown()
      raise

  def shutdown(self):
    utils.vtgate_kill(self.vtgate_server)
    tablet.kill_tablets(self.tablets)
    teardown_procs = [t.teardown_mysql() for t in self.tablets]
    utils.wait_procs(teardown_procs, raise_on_error=False)
    environment.topo_server_teardown()
    utils.kill_sub_processes()
    utils.remove_tmp_files()
    for t in self.tablets:
      t.remove_tree()


def main():
  parser = optparse.OptionParser(usage="usage: %prog [options]")
  parser.add_option("--shards", action="store", type="string",
                    help="comma separated list of shard names, e.g: '-80,80-'")
  parser.add_option("--tablet-config", action="store", type="string",
                    help="json config for for non-master tablets. e.g {'replica':2, 'rdonly':1}")
  parser.add_option("--keyspace", action="store", type="string")

  # remaining options are required to run the helper functions in utils.py
  parser.add_option('-d', '--debug', action='store_true', help='utils.pause() statements will wait for user input')
  parser.add_option('--skip-teardown', action='store_true')
  parser.add_option('-k', '--keep-logs', action='store_true',
                    help="Don't delete log files on teardown.")
  parser.add_option("-q", "--quiet", action="store_const", const=0, dest="verbose", default=1)
  parser.add_option("-v", "--verbose", action="store_const", const=2, dest="verbose", default=1)
  parser.add_option("--mysql-flavor", action="store", type="string")
  (options, args) = parser.parse_args()
  utils.options = options
  utils.set_mysql_flavor(options.mysql_flavor)
  env = TestEnv(options)
  if args[0] == 'setup':
    env.set_up()
    sys.stdout.write(json.dumps({
      "port": env.vtgate_port,
      }) + "\n")
    sys.stdout.flush()
  elif args[0] == 'teardown':
    env.shutdown()


if __name__ == '__main__':
  main()

