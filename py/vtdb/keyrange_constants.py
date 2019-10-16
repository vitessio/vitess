# Copyright 2019 The Vitess Authors.
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
