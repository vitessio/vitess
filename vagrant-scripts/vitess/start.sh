#!/usr/bin/env bash

set -e

printf "\nStarting Vitess cluster\n"

export VTROOT=/vagrant
export VTDATAROOT=/tmp/vtdata-dev
export MYSQL_FLAVOR=MySQL56
cd "$VITESS_WORKSPACE"/examples/local
export SHARD="-"
export TOPO="zk2"

./zk-up.sh
./vtctld-up.sh --enable-grpc-static-auth
./vttablet-up.sh --enable-grpc-static-auth
./vtgate-up.sh --enable-grpc-static-auth
sleep 3
./lvtctl.sh InitShardMaster -force test_keyspace/- test-100
./lvtctl.sh ApplySchema -sql "$(cat create_test_table.sql)" test_keyspace
./lvtctl.sh ApplyVSchema -vschema_file vschema.json test_keyspace
./lvtctl.sh RebuildVSchemaGraph

printf "\nVitess cluster started successfully.\n\n"
