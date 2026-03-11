#!/bin/bash

# Copyright 2026 The Vitess Authors.
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
set -euo pipefail

GRPC_PORT=${GRPC_PORT:-"15999"}
KEYSPACES=${KEYSPACES:-"test_keyspace lookup_keyspace"}

keyspaces=$KEYSPACES

# set the correct durability policy for the keyspace
for keyspace in $keyspaces; do
  echo "Set Keyspace Durability Policy ${keyspace} to semi_sync"
  max_retries=20
  retry_count=0
  until /vt/bin/vtctldclient --server "vtctld:$GRPC_PORT" SetKeyspaceDurabilityPolicy --durability-policy=semi_sync "$keyspace"; do
    retry_count=$((retry_count + 1))
    if [ "$retry_count" -ge "$max_retries" ]; then
      echo "Failed to set durability policy for ${keyspace} after ${max_retries} attempts"
      exit 1
    fi
    echo "Retrying SetKeyspaceDurabilityPolicy for ${keyspace} (attempt ${retry_count}/${max_retries})..."
    sleep 10
  done
done
