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

# this script brings up zookeeper and all the vitess components
# required for a single shard deployment.

source ../common/env.sh

# This is done here as a means to support testing the experimental
# custom sidecar database name work in a wide variety of scenarios
# as the local examples are used to test many features locally.
# This is NOT here to indicate that you should normally use a
# non-default (_vt) value or that it is somehow a best practice
# to do so. In production, you should ONLY use a non-default
# sidecar database name when it's truly needed.
SIDECAR_DB_NAME=${SIDECAR_DB_NAME:-"_vt"}

# start topo server
if [ "${TOPO}" = "zk2" ]; then
	CELL=zone1 ../common/scripts/zk-up.sh
elif [ "${TOPO}" = "consul" ]; then
	CELL=zone1 ../common/scripts/consul-up.sh
else
	CELL=zone1 ../common/scripts/etcd-up.sh
fi

# start vtctld
CELL=zone1 ../common/scripts/vtctld-up.sh

# Create the keyspace with the sidecar database name and set the
# correct durability policy. Please see the comment above for
# more context on using a custom sidecar database name in your
# Vitess clusters.
vtctldclient --server localhost:15999 CreateKeyspace --sidecar-db-name="${SIDECAR_DB_NAME}" --durability-policy=semi_sync commerce || fail "Failed to create and configure the commerce keyspace"

# start vttablets for keyspace commerce
for i in 100 101 102; do
	CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-up.sh
	CELL=zone1 KEYSPACE=commerce TABLET_UID=$i ../common/scripts/vttablet-up.sh
done

# start vtorc
../common/scripts/vtorc-up.sh

# Wait for all the tablets to be up and registered in the topology server
# and for a primary tablet to be elected in the shard and become healthy/serving.
wait_for_healthy_shard commerce 0 || exit 1

# create the schema
vtctldclient ApplySchema --sql-file create_commerce_schema.sql commerce || fail "Failed to apply schema for the commerce keyspace"

# create the vschema
vtctldclient ApplyVSchema --vschema-file vschema_commerce_initial.json commerce || fail "Failed to apply vschema for the commerce keyspace"

# start vtgate
CELL=zone1 ../common/scripts/vtgate-up.sh

# start vtadmin
if [[ -n ${SKIP_VTADMIN} ]]; then
	echo -e "\nSkipping VTAdmin! If this is not what you want then please unset the SKIP_VTADMIN env variable in your shell."
else
	../common/scripts/vtadmin-up.sh
fi

