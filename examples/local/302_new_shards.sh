#!/bin/bash

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

# this script brings up new tablets for the two new shards that we will
# be creating in the customer keyspace and copies the schema

source ./env.sh

for i in 300 301 302; do
	CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
	SHARD=-80 CELL=zone1 KEYSPACE=customer TABLET_UID=$i ./scripts/vttablet-up.sh
done

for i in 400 401 402; do
	CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
	SHARD=80- CELL=zone1 KEYSPACE=customer TABLET_UID=$i ./scripts/vttablet-up.sh
done

for shard in "-80" "80-"; do
  # Wait for all the tablets to be up and registered in the topology server
  for _ in $(seq 0 200); do
  	vtctldclient GetTablets --keyspace customer --shard $shard | wc -l | grep -q "3" && break
  	sleep 1
  done;
  vtctldclient GetTablets --keyspace customer --shard $shard | wc -l | grep -q "3" || (echo "Timed out waiting for tablets to be up in customer/$shard" && exit 1)

  # Wait for a primary tablet to be elected in the shard
  for _ in $(seq 0 200); do
  	vtctldclient GetTablets --keyspace customer --shard $shard | grep -q "primary" && break
  	sleep 1
  done;
  vtctldclient GetTablets --keyspace customer --shard $shard | grep "primary" || (echo "Timed out waiting for primary to be elected in customer/$shard" && exit 1)
done;
