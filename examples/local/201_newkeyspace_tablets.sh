#!/bin/bash
set -eEo pipefail

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

# This script creates a primary, replica, and rdonly tablet in the given
# keyspace and initializes them.

# Let's allow this to be run from anywhere
pushd "$(dirname "${0}")" >/dev/null

source ./env.sh

KEYSPACE=${1:-test}
BASETABLETNUM=${2:-2}
SHARD=${3:-"-"}

for i in ${BASETABLETNUM}00 ${BASETABLETNUM}01 ${BASETABLETNUM}02; do
        CELL=zone1 TABLET_UID="${i}" ./scripts/mysqlctl-up.sh
        SHARD=${SHARD} CELL=zone1 KEYSPACE=${KEYSPACE} TABLET_UID=$i ./scripts/vttablet-up.sh
done

# Wait for all the tablets to be up and registered in the topology server
for _ in $(seq 0 200); do
	vtctldclient GetTablets --keyspace "${KEYSPACE}" --shard "${SHARD}" | wc -l | grep -q "3" && break
	sleep 1
done;
vtctldclient GetTablets --keyspace "${KEYSPACE}" --shard "${SHARD}" | wc -l | grep -q "3" || (echo "Timed out waiting for tablets to be up in ${KEYSPACE}/${SHARD}" && exit 1)

# Wait for a primary tablet to be elected in the shard
for _ in $(seq 0 200); do
	vtctldclient GetTablets --keyspace "${KEYSPACE}" --shard "${SHARD}" | grep -q "primary" && break
	sleep 1
done;
vtctldclient GetTablets --keyspace "${KEYSPACE}" --shard "${SHARD}" | grep "primary" || (echo "Timed out waiting for primary to be elected in ${KEYSPACE}/${SHARD}" && exit 1)

# Go back to the original ${PWD} in the parent shell
popd >/dev/null
