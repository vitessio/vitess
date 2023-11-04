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

# this script brings up topo server and all the vitess components
# required for a single shard deployment.

source ../common/env.sh

# start topo server
if [ "${TOPO}" = "zk2" ]; then
	CELL=zone1 ../common/scripts/zk-up.sh
else
	CELL=zone1 ../common/scripts/etcd-up.sh
fi

# start vtctld
CELL=zone1 ../common/scripts/vtctld-up.sh

# start unsharded keyspace and tablet
CELL=zone1 TABLET_UID=100 ../common/scripts/mysqlctl-up.sh
SHARD=0 CELL=zone1 KEYSPACE=main TABLET_UID=100 ../common/scripts/vttablet-up.sh

# set the correct durability policy for the keyspace
vtctldclient --server localhost:15999 SetKeyspaceDurabilityPolicy --durability-policy=none main || fail "Failed to set keyspace durability policy on the main keyspace"

# start vtorc
../common/scripts/vtorc-up.sh

# Wait for a primary tablet to be elected in the shard and for it
# to become healthy/sherving.
wait_for_healthy_shard main 0 1 || exit 1

# create the schema
vtctldclient ApplySchema --sql-file create_main_schema.sql main || fail "Failed to apply schema for the main keyspace"

# create the vschema
vtctldclient ApplyVSchema --vschema-file main_vschema_initial.json main ||  fail "Failed to apply vschema for the main keyspace"

# start vtgate
CELL=zone1 ../common/scripts/vtgate-up.sh

# start vtadmin
../common/scripts/vtadmin-up.sh

