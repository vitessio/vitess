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

# this script creates the tablets and initializes them for vertical
# resharding it also splits the vschema between the two keyspaces
# old (commerce) and new (customer)

source ./env.sh

for i in 200 201 202; do
	CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
	CELL=zone1 KEYSPACE=customer TABLET_UID=$i ./scripts/vttablet-up.sh
done

# set the correct durability policy for the keyspace
vtctldclient --server localhost:15999 SetKeyspaceDurabilityPolicy --durability-policy=semi_sync customer

# Wait for all the tablets to be up and registered in the topology server
for _ in $(seq 0 200); do
	vtctldclient GetTablets --keyspace customer --shard 0 | wc -l | grep -q "3" && break
	sleep 1
done;
vtctldclient GetTablets --keyspace customer --shard 0 | wc -l | grep -q "3" || (echo "Timed out waiting for tablets to be up in customer/0" && exit 1)

# Wait for a primary tablet to be elected in the shard
for _ in $(seq 0 200); do
	vtctldclient GetTablets --keyspace customer --shard 0 | grep -q "primary" && break
	sleep 1
done;
vtctldclient GetTablets --keyspace customer --shard 0 | grep "primary" || (echo "Timed out waiting for primary to be elected in customer/0" && exit 1)
