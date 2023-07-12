#!/bin/bash

# Copyright 2023 The Vitess Authors.
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

# Restart the replica tablets so that they come up with new vttablet versions
for i in 101 102; do
  echo "Shutting down tablet zone1-$i"
  CELL=zone1 TABLET_UID=$i ../common/scripts/vttablet-down.sh
  echo "Shutting down mysql zone1-$i"
  CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-down.sh
  echo "Removing tablet directory zone1-$i"
  vtctlclient DeleteTablet -- --allow_primary=true zone1-$i
  rm -Rf $VTDATAROOT/vt_0000000$i
  echo "Starting tablet zone1-$i again"
  CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-up.sh
  CELL=zone1 KEYSPACE=commerce TABLET_UID=$i ../common/scripts/vttablet-up.sh
done

for i in 201 202; do
  echo "Shutting down tablet zone1-$i"
  CELL=zone1 TABLET_UID=$i ../common/scripts/vttablet-down.sh
  echo "Shutting down mysql zone1-$i"
  CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-down.sh
  echo "Removing tablet directory zone1-$i"
  vtctlclient DeleteTablet -- --allow_primary=true zone1-$i
  rm -Rf $VTDATAROOT/vt_0000000$i
  echo "Starting tablet zone1-$i again"
  CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-up.sh
  SHARD=-80 CELL=zone1 KEYSPACE=customer TABLET_UID=$i ../common/scripts/vttablet-up.sh
done

for i in 301 302; do
  echo "Shutting down tablet zone1-$i"
  CELL=zone1 TABLET_UID=$i ../common/scripts/vttablet-down.sh
  echo "Shutting down mysql zone1-$i"
  CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-down.sh
  echo "Removing tablet directory zone1-$i"
  vtctlclient DeleteTablet -- --allow_primary=true zone1-$i
  rm -Rf $VTDATAROOT/vt_0000000$i
  echo "Starting tablet zone1-$i again"
  CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-up.sh
  SHARD=80- CELL=zone1 KEYSPACE=customer TABLET_UID=$i ../common/scripts/vttablet-up.sh
done

# Wait for all the replica tablets to be in the serving state before reparenting to them.
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

# Promote the replica tablets to primary
vtctldclient PlannedReparentShard commerce/0 --new-primary "zone1-101"
vtctldclient PlannedReparentShard customer/-80 --new-primary "zone1-201"
vtctldclient PlannedReparentShard customer/80- --new-primary "zone1-301"

# Restart the old primary tablets so that they are on the latest version of vttablet too.
echo "Restarting tablet zone1-100"
CELL=zone1 TABLET_UID=100 ../common/scripts/vttablet-down.sh
CELL=zone1 KEYSPACE=commerce TABLET_UID=100 ../common/scripts/vttablet-up.sh

echo "Restarting tablet zone1-200"
CELL=zone1 TABLET_UID=200 ../common/scripts/vttablet-down.sh
SHARD=-80 CELL=zone1 KEYSPACE=customer TABLET_UID=200 ../common/scripts/vttablet-up.sh

echo "Restarting tablet zone1-300"
CELL=zone1 TABLET_UID=300 ../common/scripts/vttablet-down.sh
SHARD=80- CELL=zone1 KEYSPACE=customer TABLET_UID=300 ../common/scripts/vttablet-up.sh