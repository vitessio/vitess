#!/bin/bash

# Copyright 2018 The Vitess Authors.
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

set -e

# shellcheck disable=SC2128
script_root=$(dirname "${BASH_SOURCE}")

# start topo server
if [ "${TOPO}" = "etcd2" ]; then
    CELL=zone1 "$script_root/etcd-up.sh"
else
    CELL=zone1 "$script_root/zk-up.sh"
fi

# start vtctld
CELL=zone1 "$script_root/vtctld-up.sh"

# start vttablets for keyspace commerce
CELL=zone1 KEYSPACE=commerce UID_BASE=100 "$script_root/vttablet-up.sh"
sleep 15

# set one of the replicas to master
./lvtctl.sh InitShardMaster -force commerce/0 zone1-100

# create the schema
./lvtctl.sh ApplySchema -sql-file create_commerce_schema.sql commerce

# create the vschema
./lvtctl.sh ApplyVSchema -vschema_file vschema_commerce_initial.json commerce

# start vtgate
CELL=zone1 "$script_root/vtgate-up.sh"

disown -a
