#!/usr/bin/env python

# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Fixes served types for each keyspace.

This is only useful in the scenario where a set of overlapping shards is
created. This will happen in resharding tests. Since helm brings up all
vttablets at the same time, there is a race condition that determines which
shards are considered serving. In a resharding test, the unsharded set
should be serving, and the sharded set should not be.

This script iterates through all keyspaces, and ensures that for any set of
shards within the same keyspace, the set with the fewest shards will be set to
serving.
"""

import json
import logging
import optparse
import sys
from vtproto import topodata_pb2
from vttest import sharding_utils
from . import vtctl_sandbox

TABLET_TYPES = [
    (topodata_pb2.REPLICA, 'replica'),
    (topodata_pb2.RDONLY, 'rdonly'),
    (topodata_pb2.MASTER, 'master'),
]


def get_vtctl_commands(keyspace, shards):
  """Gets a list of vtctl SetShardServedTypes commands.

  Args:
    keyspace: (string) Keyspace name to obtain commands for.
    shards: Dict from shard name to shard info object from
            FindAllShardsInKeyspace vtctl command.

  Returns:
    List of vtctl commands.
  """
  lowest_sharding_count = min(
      sharding_utils.get_shard_index(x)[1] for x in list(shards.keys()))
  if lowest_sharding_count == len(shards):
    # Skip keyspaces with non-overlapping shards.
    return []

  logging.info('Keyspace %s has overlapping shards', keyspace)
  vtctl_commands = []
  for shard_name, shard_info in shards.items():
    served_tablet_types = [
        x['tablet_type'] for x in shard_info.get('served_types', [])]
    _, num_shards = sharding_utils.get_shard_index(shard_name)
    is_lowest_sharding = num_shards == lowest_sharding_count

    for tablet_type, tablet_type_name in TABLET_TYPES:
      # Setting served types when a shard is already serving is allowable,
      # but only remove served types from the sharded set if they are serving.
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
  """Ensures the smallest set of non-overlapping shards is serving.

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
