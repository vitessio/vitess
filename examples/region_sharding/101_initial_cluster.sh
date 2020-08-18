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

# start vttablets for main keyspace. we start only one tablet each (master)
CELL=zone1 TABLET_UID=100 ./scripts/mysqlctl-up.sh
SHARD=-40 CELL=zone1 KEYSPACE=main TABLET_UID=100 ./scripts/vttablet-up.sh
CELL=zone1 TABLET_UID=200 ./scripts/mysqlctl-up.sh
SHARD=40-80 CELL=zone1 KEYSPACE=main TABLET_UID=200 ./scripts/vttablet-up.sh
CELL=zone1 TABLET_UID=300 ./scripts/mysqlctl-up.sh
SHARD=80-c0 CELL=zone1 KEYSPACE=main TABLET_UID=300 ./scripts/vttablet-up.sh
CELL=zone1 TABLET_UID=400 ./scripts/mysqlctl-up.sh
SHARD=c0- CELL=zone1 KEYSPACE=main TABLET_UID=400 ./scripts/vttablet-up.sh

# set master
vtctlclient InitShardMaster -force main/-40 zone1-100
vtctlclient InitShardMaster -force main/40-80 zone1-200
vtctlclient InitShardMaster -force main/80-c0 zone1-300
vtctlclient InitShardMaster -force main/c0- zone1-400

# create the schema
vtctlclient ApplySchema -sql-file create_main_schema.sql main

# create the vschema
vtctlclient ApplyVSchema -vschema_file main_vschema.json main

# start vtgate
CELL=zone1 ./scripts/vtgate-up.sh
