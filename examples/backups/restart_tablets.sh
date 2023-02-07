#!/bin/bash

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

# this script brings up new tablets for the two new shards that we will
# be creating in the customer keyspace and copies the schema

source ../common/env.sh

for i in 100 101 102; do
  CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-up.sh
  CELL=zone1 KEYSPACE=commerce TABLET_UID=$i ../common/scripts/vttablet-up.sh
done

for i in 200 201 202; do
  CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-up.sh
  SHARD=-80 CELL=zone1 KEYSPACE=customer TABLET_UID=$i ../common/scripts/vttablet-up.sh
done

for i in 300 301 302; do
  CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-up.sh
  SHARD=80- CELL=zone1 KEYSPACE=customer TABLET_UID=$i ../common/scripts/vttablet-up.sh
done
sleep 5

# Wait for all the replica tablets to be in the serving state before initiating
# InitShardPrimary. This is essential, since we want the RESTORE phase to be
# complete before we start InitShardPrimary, otherwise we end up reading the
# tablet type to RESTORE and do not set semi-sync, which leads to the primary
# hanging on writes.
totalTime=600
for i in 101 201 301; do
  while [ $totalTime -gt 0 ]; do
    status=$(curl "http://$hostname:15$i/debug/status_details")
    echo "$status" | grep "REPLICA: Serving" && break
    totalTime=$((totalTime-1))
    sleep 0.1
  done
done

# Check that all the replica tablets have reached REPLICA: Serving state
for i in 101 201 301; do
  status=$(curl "http://$hostname:15$i/debug/status_details")
  echo "$status" | grep "REPLICA: Serving" && continue
  echo "tablet-$i did not reach REPLICA: Serving state. Exiting due to failure."
  exit 1
done

vtctldclient InitShardPrimary --force commerce/0 zone1-100
vtctldclient InitShardPrimary --force customer/-80 zone1-200
vtctldclient InitShardPrimary --force customer/80- zone1-300
