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

# start topo server
if [ "${TOPO}" = "zk2" ]; then
	CELL=zone1 ../common/scripts/zk-up.sh
else
	CELL=zone1 ../common/scripts/etcd-up.sh
fi

# start vtctld
CELL=zone1 ../common/scripts/vtctld-up.sh

# Create keyspace and set the semi_sync durability policy.
vtctldclient CreateKeyspace --durability-policy=semi_sync commerce || fail "Failed to create and configure the commerce keyspace"

# start vttablets for keyspace commerce
for i in 100 101 102; do
	CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-up.sh
	CELL=zone1 KEYSPACE=commerce TABLET_UID=$i ../common/scripts/vttablet-up.sh
done

# set one of the replicas to primary
vtctldclient PlannedReparentShard commerce/0 --new-primary "zone1-100"

# create the schema for commerce
vtctldclient ApplySchema --sql-file ./create_commerce_schema.sql commerce || fail "Could not apply schema for the commerce keyspace"
vtctldclient ApplyVSchema --vschema-file ../local/vschema_commerce_seq.json commerce || fail "Could not apply vschema for the commerce keyspace"

# Create keyspace and set the semi_sync durability policy.
vtctldclient CreateKeyspace --durability-policy=semi_sync customer || fail "Failed to create and configure the customer keyspace"

# start vttablets for keyspace customer
for i in 200 201 202; do
	CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-up.sh
	SHARD=-80 CELL=zone1 KEYSPACE=customer TABLET_UID=$i ../common/scripts/vttablet-up.sh
done
for i in 300 301 302; do
	CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-up.sh
	SHARD=80- CELL=zone1 KEYSPACE=customer TABLET_UID=$i ../common/scripts/vttablet-up.sh
done

# set one of the replicas to primary
vtctldclient PlannedReparentShard customer/-80 --new-primary "zone1-200"
vtctldclient PlannedReparentShard customer/80- --new-primary "zone1-300"

for shard in "-80" "80-"; do
	wait_for_healthy_shard customer "${shard}" || exit 1
done

# create the schema for customer
vtctldclient ApplySchema --sql-file ./create_customer_schema.sql customer || fail "Could not apply schema for the customer keyspace"
vtctldclient ApplyVSchema --vschema-file ../local/vschema_customer_sharded.json customer || fail "Could not apply vschema for the customer keyspace"


# start vtgate
CELL=zone1 ../common/scripts/vtgate-up.sh

sleep 5

mysql < ../common/insert_commerce_data.sql
