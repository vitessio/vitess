# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""Constants related to keyspaces and shard names."""

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
