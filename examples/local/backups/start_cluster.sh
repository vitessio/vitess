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

source ./env.sh

# start topo server
if [ "${TOPO}" = "zk2" ]; then
	CELL=zone1 ./scripts/zk-up.sh
elif [ "${TOPO}" = "k8s" ]; then
	CELL=zone1 ./scripts/k3s-up.sh
else
	CELL=zone1 ./scripts/etcd-up.sh
fi

# start vtctld
CELL=zone1 ./scripts/vtctld-up.sh


# start vttablets for keyspace commerce
for i in 100 101 102; do
	CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
	CELL=zone1 KEYSPACE=commerce TABLET_UID=$i ./scripts/vttablet-up.sh
done

# set one of the replicas to primary
vtctldclient InitShardPrimary --force commerce/0 zone1-100

# create the schema for commerce
vtctlclient ApplySchema -- --sql-file backups/create_commerce_schema.sql commerce
vtctlclient ApplyVSchema -- --vschema_file ./vschema_commerce_seq.json commerce


# start vttablets for keyspace customer
for i in 200 201 202; do
	CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
	SHARD=-80 CELL=zone1 KEYSPACE=customer TABLET_UID=$i ./scripts/vttablet-up.sh
done
for i in 300 301 302; do
	CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
	SHARD=80- CELL=zone1 KEYSPACE=customer TABLET_UID=$i ./scripts/vttablet-up.sh
done

# set one of the replicas to primary
vtctldclient InitShardPrimary --force customer/-80 zone1-200
vtctldclient InitShardPrimary --force customer/80- zone1-300

# create the schema for customer
vtctlclient ApplySchema -- --sql-file backups/create_customer_schema.sql customer
vtctlclient ApplyVSchema -- --vschema_file ./vschema_customer_sharded.json customer


# start vtgate
CELL=zone1 ./scripts/vtgate-up.sh

sleep 5

mysql < ../common/insert_commerce_data.sql
