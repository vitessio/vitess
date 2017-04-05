#!/usr/bin/env python
"""Fixes served types for each keyspace.

This is only useful in the scenario where multiple keyspaces with the same name
are created. This will happen in resharding tests. Since helm brings up all
vttablets at the same time, there is a race condition that determines which
shards are considered serving. In a resharding test, the unsharded keyspace
should be serving, and the sharded keyspace should not be.

This script iterates through all keyspaces, and ensures that for any set of
keyspaces that have the same name, the keyspace with the fewest shards will be
set to serving.
"""

import collections
import json
import logging
import optparse
import sys
from vtproto import topodata_pb2
from vttest import sharding_utils
import vtctl_sandbox

# Use OrderedDict in order to ensure master is set last.
tablet_types = collections.OrderedDict([
    (topodata_pb2.REPLICA, 'replica'),
    (topodata_pb2.RDONLY, 'rdonly'),
    (topodata_pb2.MASTER, 'master'),
])


def get_vtctl_commands(keyspace, shards):
  """Gets a list of vtctl SetShardServedTypes commands.

  Args:
    keyspace: (string) Keyspace name to obtain commands for.
    shards: (json) Struct containing sharding info from FindAllShardsInKeyspace
            vtctl command.

  Returns:
    List of vtctl commands.
  """
  # Skip keyspaces with non-overlapping sharding schemes.
  lowest_sharding_count = min([
      sharding_utils.get_shard_index(x)[1] for x in shards.keys()])
  if lowest_sharding_count == len(shards):
    return []

  logging.info('Keyspace %s has overlapping sharding schemes', keyspace)
  vtctl_commands = []
  for shard_name, shard_info in shards.iteritems():
    served_tablet_types = [
        x['tablet_type'] for x in shard_info.get('served_types', [])]
    _, num_shards = sharding_utils.get_shard_index(shard_name)
    is_lowest_sharding = num_shards == lowest_sharding_count

    for tablet_type, tablet_type_name in tablet_types.iteritems():
      # Setting served types when a shard is already serving is allowable,
      # but only remove served types from the more sharded keyspace shards if
      # they are serving.
      if not (is_lowest_sharding or tablet_type in served_tablet_types):
        continue

      vtctl_command = [
          'SetShardServedTypes', '%s/%s' % (keyspace, shard_name),
          tablet_type_name]
      if not is_lowest_sharding:
        vtctl_command.insert(1, '--remove')

      vtctl_commands.append(vtctl_command)

  return vtctl_commands


def fix_served_types(keyspaces, namespace):
  """Ensures the least-sharded keyspace is serving.

  Args:
    keyspaces: [string], list of keyspaces to fix.
    namespace: string, Kubernetes namespace for vtctld calls.

  Returns:
    0 for success, 1 for failure
  """
  for keyspace in keyspaces:
    output, success = vtctl_sandbox.execute_vtctl_command(
        ['FindAllShardsInKeyspace', keyspace], namespace=namespace)

    if not success:
      logging.error('Failed to execute FindAllShardsInKeyspace: %s', output)
      return 1

    for cmd in get_vtctl_commands(keyspace, json.loads(output)):
      logging.info('Executing %s', ' '.join(cmd))
      vtctl_sandbox.execute_vtctl_command(cmd, namespace=namespace)

    logging.info('Rebuilding keyspace %s', keyspace)
    vtctl_sandbox.execute_vtctl_command(
        ['RebuildKeyspaceGraph', keyspace], namespace=namespace)
  return 0


def main():
  parser = optparse.OptionParser(usage='usage: %prog [options] [test_names]')
  parser.add_option('-n', '--namespace', help='Kubernetes namespace',
                    default='vitess')
  parser.add_option('-k', '--keyspaces', help='Comma delimited keyspaces',
                    default='test_keyspace')
  logging.getLogger().setLevel(logging.INFO)

  options, _ = parser.parse_args()
  sys.exit(
      fix_served_types(set(options.keyspaces.split(',')), options.namespace))


if __name__ == '__main__':
  main()
