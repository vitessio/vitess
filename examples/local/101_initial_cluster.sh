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

set -e

# start topo server
if [ "${TOPO}" = "zk2" ]; then
 CELL=zone1 ./zk-up.sh
else
 CELL=zone1 ./etcd-up.sh
fi

# start vtctld
CELL=zone1 ./vtctld-up.sh &

# start vttablets for keyspace commerce
for i in 100 101 102; do
 CELL=zone1 TABLET_UID=$i ./mysqlctl-up.sh
 CELL=zone1 KEYSPACE=commerce TABLET_UID=$i ./vttablet-up.sh &
done

sleep 20

# set one of the replicas to master
vtctlclient -server localhost:15999 InitShardMaster -force commerce/0 zone1-100

# create the schema
vtctlclient -server localhost:15999 ApplySchema -sql-file create_commerce_schema.sql commerce

# create the vschema
vtctlclient -server localhost:15999 ApplyVSchema -vschema_file vschema_commerce_initial.json commerce

# start vtgate
CELL=zone1 ./vtgate-up.sh &

sleep 20
