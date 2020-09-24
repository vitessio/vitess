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

# this script brings up zookeeper and all the vitess components
# required for a single shard deployment.

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

vtctlclient AddCellInfo -root /vitess/zone2 -server_address "${ETCD_SERVER}" zone2

# start vttablets for keyspace commerce
for i in 100 101 102; do
	CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
	CELL=zone1 KEYSPACE=commerce TABLET_UID=$i ./scripts/ovttablet-up.sh
done

for i in 110 111; do
	CELL=zone2 TABLET_UID=$i ./scripts/mysqlctl-up.sh
	CELL=zone2 KEYSPACE=commerce TABLET_UID=$i ./scripts/ovttablet-up.sh
done

# set one of the replicas to master
#vtctlclient InitShardMaster -force commerce/0 zone1-100

# create the schema
#vtctlclient ApplySchema -sql-file create_commerce_schema.sql commerce

# create the vschema
#vtctlclient ApplyVSchema -vschema_file vschema_commerce_initial.json commerce

# start vtgate
CELL=zone1 ./scripts/vtgate-up.sh
