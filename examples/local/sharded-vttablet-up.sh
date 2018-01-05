#!/bin/bash

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

# This is an example script that creates a sharded vttablet deployment.

set -e

script_root=`dirname "${BASH_SOURCE}"`

# Shard -80 contains all entries whose keyspace ID has a first byte < 0x80.
# See: http://vitess.io/overview/concepts/#keyspace-id
SHARD=-80 UID_BASE=200 $script_root/vttablet-up.sh "$@"

# Shard 80- contains all entries whose keyspace ID has a first byte >= 0x80.
SHARD=80- UID_BASE=300 $script_root/vttablet-up.sh "$@"

