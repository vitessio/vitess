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

CELL=test ./scripts/etcd-up.sh
CELL=test ./scripts/vtctld-up.sh

for i in 100 101 102; do
	CELL=test TABLET_UID=$i ./scripts/mysqlctl-up.sh
	CELL=test KEYSPACE=test_keyspace TABLET_UID=$i ./scripts/vttablet-up.sh
done

vtctlclient -server localhost:15999 InitShardMaster -force test_keyspace/0 test-100

vtctlclient -server localhost:15999 ApplySchema -sql-file create_test_table.sql test_keyspace
vtctlclient -server localhost:15999 RebuildVSchemaGraph

CELL=test ./scripts/vtgate-up.sh

echo "Run Go client script..."
go run $VTROOT/test/client.go -server=localhost:15991

echo "Run Java client script..."
$VTROOT/test/client_java.sh

echo "Run JDBC client script..."
$VTROOT/test/client_jdbc.sh

# Clean up

./scripts/vtgate-down.sh

for i in 100 101 102; do
	CELL=test TABLET_UID=$i ./scripts/vttablet-down.sh
	CELL=test TABLET_UID=$i ./scripts/mysqlctl-down.sh
done

./scripts/vtctld-down.sh
CELL=test ./scripts/etcd-down.sh
