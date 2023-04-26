#!/bin/bash

# Copyright 2020 The Vitess Authors.
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

source ./env.sh

# start vttablets for new shards. we start only one tablet each (primary)
CELL=zone1 TABLET_UID=200 ./scripts/mysqlctl-up.sh
SHARD=-40 CELL=zone1 KEYSPACE=main TABLET_UID=200 ./scripts/vttablet-up.sh
CELL=zone1 TABLET_UID=300 ./scripts/mysqlctl-up.sh
SHARD=40-80 CELL=zone1 KEYSPACE=main TABLET_UID=300 ./scripts/vttablet-up.sh
CELL=zone1 TABLET_UID=400 ./scripts/mysqlctl-up.sh
SHARD=80-c0 CELL=zone1 KEYSPACE=main TABLET_UID=400 ./scripts/vttablet-up.sh
CELL=zone1 TABLET_UID=500 ./scripts/mysqlctl-up.sh
SHARD=c0- CELL=zone1 KEYSPACE=main TABLET_UID=500 ./scripts/vttablet-up.sh

for shard in "-40" "40-80" "80-c0" "c0-"; do
  # Wait for a primary tablet to be elected in the shard
  for _ in $(seq 0 200); do
  	vtctldclient GetTablets --keyspace main --shard $shard | grep -q "primary" && break
  	sleep 1
  done;
  vtctldclient GetTablets --keyspace main --shard $shard | grep "primary" || (echo "Timed out waiting for primary to be elected in main/$shard" && exit 1)
done;
