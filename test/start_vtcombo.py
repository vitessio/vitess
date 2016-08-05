"""Start a vtcombo."""

import argparse
import json
import os
import sharding_utils
import subprocess

from google.protobuf import text_format

from vtproto import vttest_pb2

import environment


def start(topo, web_dir='/web/vtctld', blocking=False):
  """Start vtcombo."""
  port = environment.reserve_ports(1)
  args = [
      environment.run_local_database, '--port', str(port),
      '--proto_topo', text_format.MessageToString(topo, as_one_line=True),
      '--schema_dir', os.path.join(environment.vttop, 'test', 'vttest_schema'),
      '--web_dir', environment.vttop + web_dir,
      '--default_schema_dir', os.path.join(
          environment.vttop, 'test', 'vttest_schema', 'default')]
  if blocking:
    # Runs vtcombo, waits until the user enters a newline
    subprocess.check_output(args)
  else:
    # Returns the process and the config json so that tests can close
    sp = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    config = json.loads(sp.stdout.readline())
    return sp, config


if __name__ == '__main__':
  parser = argparse.ArgumentParser(
      description='Start a custom vtcombo instance.')
  parser.add_argument('--cells', type=str, nargs='*', default=[])
  parser.add_argument(
      '--keyspaces', type=str, nargs='*', default=['test_keyspace'])
  parser.add_argument(
      '--num_shards', type=int, nargs='*', default=[2])
  parser.add_argument('--replica_count', type=int, default=2)
  parser.add_argument('--rdonly_count', type=int, default=1)
  parser.add_argument('--web_dir', type=str, default='/web/vtctld')

  pargs = parser.parse_args()

  topology = vttest_pb2.VTTestTopology()
  for cell in pargs.cells:
    topology.cells.append(cell)

  for keyspace, num_shards in zip(pargs.keyspaces, pargs.num_shards):
    ks = topology.keyspaces.add(name=keyspace)
    for shard in sharding_utils.get_shard_names(num_shards):
      ks.shards.add(name=shard)
    ks.replica_count = pargs.replica_count
    ks.rdonly_count = pargs.rdonly_count

  start(topology, pargs.web_dir, True)
