#!/bin/bash -e

# Copyright 2022 The Vitess Authors.
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
set -u

keyspaces=${KEYSPACES:-'test_keyspace lookup_keyspace'}
sleeptime=${SLEEPTIME:-'10'}

sleep $sleeptime

# set the correct durability policy for the keyspace
for keyspace in $keyspaces; do
  echo "Set Keyspace Durability Policy ${keyspace} to none"
  /vt/bin/vtctldclient --server vtctld:$GRPC_PORT SetKeyspaceDurabilityPolicy --durability-policy=none $keyspace
done
