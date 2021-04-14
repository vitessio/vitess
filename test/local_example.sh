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

# This test runs through the scripts in examples/local to make sure they work.
# It should be kept in sync with the steps in https://vitess.io/docs/get-started/local/
# So we can detect if a regression affecting a tutorial is introduced.

source build.env

set -xe

cd "$VTROOT/examples/local"
unset VTROOT # ensure that the examples can run without VTROOT now.

source ./env.sh # Required so that "mysql" works from alias

./101_initial_cluster.sh

sleep 5 # Give vtgate time to really start.

mysql < ../common/insert_commerce_data.sql
mysql --table < ../common/select_commerce_data.sql

./201_customer_tablets.sh

for shard in "customer/0"; do
 while true; do
  mysql "$shard" -e 'show tables' && break || echo "waiting for shard: $shard!"
  sleep 1
 done;
done;

./202_move_tables.sh
sleep 3 # required for now

./203_switch_reads.sh

./204_switch_writes.sh

mysql --table < ../common/select_customer0_data.sql
# Expected to fail!
mysql --table < ../common/select_commerce_data.sql || echo "Blacklist working as expected"
./205_clean_commerce.sh
# Expected to fail!
mysql --table < ../common/select_commerce_data.sql || echo "Tables missing as expected"


./301_customer_sharded.sh
./302_new_shards.sh

# Wait for the schema to be targetable before proceeding
# TODO: Eliminate this race in the examples' scripts
for shard in "customer/-80" "customer/80-"; do
 while true; do
  mysql "$shard" -e 'show tables' && break || echo "waiting for shard: $shard!"
  sleep 1
 done;
done;

./303_reshard.sh

sleep 3 # TODO: Required for now!


./304_switch_reads.sh
./305_switch_writes.sh

mysql --table < ../common/select_customer-80_data.sql
mysql --table < ../common/select_customer80-_data.sql

./401_teardown.sh

