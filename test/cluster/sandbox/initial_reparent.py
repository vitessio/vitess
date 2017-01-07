#!/usr/bin/env python
"""Performs the first reparent on every shard of a keyspace."""

import json
import logging
import optparse
from vtproto import topodata_pb2
from vttest import sharding_utils
import sandbox_utils
import vtctl_sandbox


def is_master(tablet, namespace):
  tablet_info = (
      vtctl_sandbox.execute_vtctl_command(
          ['GetTablet', tablet], namespace=namespace))
  if json.loads(tablet_info[0])['type'] == topodata_pb2.MASTER:
    return True


def initial_reparent(keyspace, master_cell, num_shards, namespace):
  """Performs the first reparent."""
  successfully_reparented = []
  master_tablets = {}
  while len(master_tablets) < num_shards:
    for shard_name in sharding_utils.get_shard_names(num_shards):
      shard_name = sandbox_utils.fix_shard_name(shard_name)
      tablets = vtctl_sandbox.execute_vtctl_command(
          ['ListShardTablets', '%s/%s' % (
              keyspace, sandbox_utils.fix_shard_name(shard_name))],
          namespace=namespace)[0].split('\n')
      tablets = [x.split(' ') for x in tablets if x]
      potential_masters = [
          x[0] for x in tablets if x[3] == 'replica'
          and x[0].split('-')[0] == master_cell]
      if potential_masters:
        master_tablets[shard_name] = potential_masters[0]

  while len(successfully_reparented) < num_shards:
    for shard_name in sharding_utils.get_shard_names(num_shards):
      shard_name = sandbox_utils.fix_shard_name(shard_name)
      master_tablet_id = master_tablets[shard_name]
      if is_master(master_tablet_id, namespace):
        logging.info('Tablet %s is the master of %s/%s.',
                     master_tablet_id, keyspace, shard_name)
        successfully_reparented.append(shard_name)
      if shard_name in successfully_reparented:
        continue
      logging.info('Setting tablet %s as master for %s/%s.',
                   master_tablet_id, keyspace, shard_name)
      vtctl_sandbox.execute_vtctl_command(
          ['InitShardMaster', '-force', '%s/%s' % (keyspace, shard_name),
           master_tablet_id], namespace=namespace, timeout_s=5)
  logging.info('Done with initial reparent.')


def main():
  parser = optparse.OptionParser(usage='usage: %prog [options] [test_names]')
  parser.add_option('-n', '--namespace', help='Kubernetes namespace',
                    default='vitess')
  parser.add_option('-k', '--keyspace', help='Keyspace name',
                    default='test_keyspace')
  parser.add_option('-m', '--master_cell', help='Master cell')
  parser.add_option('-s', '--shard_count', help='Number of shards', default=2)
  logging.getLogger().setLevel(logging.INFO)

  options, _ = parser.parse_args()
  initial_reparent(options.keyspace, options.master_cell,
                   int(options.shard_count), options.namespace)


if __name__ == '__main__':
  main()
