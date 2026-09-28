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

source ../common/env.sh

# start vttablets for new shards. we start only one tablet each (primary)
CELL=zone1 TABLET_UID=200 ../common/scripts/mysqlctl-up.sh
SHARD=-40 CELL=zone1 KEYSPACE=main TABLET_UID=200 ../common/scripts/vttablet-up.sh
CELL=zone1 TABLET_UID=300 ../common/scripts/mysqlctl-up.sh
SHARD=40-80 CELL=zone1 KEYSPACE=main TABLET_UID=300 ../common/scripts/vttablet-up.sh
CELL=zone1 TABLET_UID=400 ../common/scripts/mysqlctl-up.sh
SHARD=80-c0 CELL=zone1 KEYSPACE=main TABLET_UID=400 ../common/scripts/vttablet-up.sh
CELL=zone1 TABLET_UID=500 ../common/scripts/mysqlctl-up.sh
SHARD=c0- CELL=zone1 KEYSPACE=main TABLET_UID=500 ../common/scripts/vttablet-up.sh

for shard in "-40" "40-80" "80-c0" "c0-"; do
	# Wait for all the tablets to be up and registered in the topology server
	# and for a primary tablet to be elected in the shard and become healthy/serving.
	wait_for_healthy_shard main "${shard}" 1 || exit 1
done;
