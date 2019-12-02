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

# This runs client tests. It used to be part of local_example,
# but has been moved to its own test. It hijacks the public examples scripts

source build.env

set -xe
cd "$VTROOT/examples/local"

CELL=test ./etcd-up.sh
CELL=test ./vtctld-up.sh

CELL=test KEYSPACE=test_keyspace UID_BASE=100 ./vttablet-up.sh

./lvtctl.sh InitShardMaster -force test_keyspace/0 test-100

./lvtctl.sh ApplySchema -sql-file create_test_table.sql test_keyspace
./lvtctl.sh RebuildVSchemaGraph

CELL=test ./vtgate-up.sh

echo "Run Python client script.."
./client.sh

echo "Run Go client script..."
go run client.go -server=localhost:15991

echo "Run Java client script..."
./client_java.sh

echo "Run JDBC client script..."
./client_jdbc.sh

# Clean up

./vtgate-down.sh
CELL=test UID_BASE=100 ./vttablet-down.sh
./vtctld-down.sh
CELL=test ./etcd-down.sh

