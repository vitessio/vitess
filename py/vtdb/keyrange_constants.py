# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""Constants related to keyspaces and shard names."""

# This is the shard name for when the keyrange covers the entire space
# for unsharded database.
SHARD_ZERO = '0'

# Keyrange that spans the entire space, used
# for unsharded database.
NON_PARTIAL_KEYRANGE = ''
MIN_KEY = ''
MAX_KEY = ''

KIT_UNSET = ''
KIT_UINT64 = 'uint64'
KIT_BYTES = 'bytes'

# Map from proto3 integer keyspace id type to lower case string version
PROTO3_KIT_TO_STRING = {
    0: KIT_UNSET,
    1: KIT_UINT64,
    2: KIT_BYTES,
}

# Map from proto3 integer tablet type value to the lower case string
# (Eventually we will use the proto3 version of this)
PROTO3_TABLET_TYPE_TO_STRING = {
    0: 'unknown',
    1: 'master',
    2: 'replica',
    3: 'rdonly',
    4: 'spare',
    5: 'experimental',
    6: 'backup',
    7: 'restore',
    8: 'worker',
    9: 'scrap',
}


# (Eventually this will just go away, as keyspace.Keyspace will use
# the proto3 version directly).
def srv_keyspace_proto3_to_old(sk):
  """Converts a bson-encoded proto3 SrvKeyspace.

  Args:
    sk: bson-encoded proto3 SrvKeyspace.

  Returns:
    sk with converted values.
  """
  if 'ShardingColumnType' in sk:
    if sk['ShardingColumnType'] == 1:
      sk['ShardingColumnType'] = KIT_UINT64
    elif sk['ShardingColumnType'] == 2:
      sk['ShardingColumnType'] = KIT_BYTES
    else:
      sk['ShardingColumnType'] = KIT_UNSET
  if 'ServedFrom' in sk:
    sfmap = {}
    for sf in sk['ServedFrom']:
      tt = PROTO3_TABLET_TYPE_TO_STRING[sf['TabletType']]
      sfmap[tt] = sf['Keyspace']
    sk['ServedFrom'] = sfmap
  if 'Partitions' in sk:
    pmap = {}
    for p in sk['Partitions']:
      tt = PROTO3_TABLET_TYPE_TO_STRING[p['ServedType']]
      pmap[tt] = {
          'ShardReferences': p['ShardReferences'],
          }
    sk['Partitions'] = pmap
  return sk
