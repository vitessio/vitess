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

# This test runs through the scripts in examples/legacy_local to make sure they work.
# This is currently a copy of examples/local which will be updated soon to use the
# new resharding workflows

source build.env

set -xe

cd "$VTROOT/examples/legacy_local"

unset VTROOT # ensure that the examples can run without VTROOT now.

./101_initial_cluster.sh

sleep 5 # Give vtgate time to really start.

mysql -h 127.0.0.1 -P 15306 commerce < ../common/insert_commerce_data.sql
mysql -h 127.0.0.1 -P 15306 commerce --table < ../common/select_commerce_data.sql
./201_customer_keyspace.sh
./202_customer_tablets.sh
./203_vertical_split.sh
mysql -h 127.0.0.1 -P 15306 customer/0 --table < ../common/select_customer_data.sql

./204_vertical_migrate_replicas.sh
./205_vertical_migrate_master.sh
# Expected to fail!
mysql -h 127.0.0.1 -P 15306 commerce --table < ../common/select_commerce_data.sql || echo "Blacklist working as expected"
./206_clean_commerce.sh
# Expected to fail!
mysql -h 127.0.0.1 -P 15306 commerce --table < ../common/select_commerce_data.sql || echo "Tables missing as expected"

./301_customer_sharded.sh
./302_new_shards.sh

# Wait for the schema to be targetable before proceeding
# TODO: Eliminate this race in the examples' scripts
for shard in "customer/-80" "customer/80-"; do
 while true; do
  mysql -h 127.0.0.1 -P 15306 "$shard" -e 'show tables' && break || echo "waiting for shard: $shard!"
  sleep 1
 done;
done;

mysql -h 127.0.0.1 -P 15306 --table customer/-80 < ../common/select_customer_data.sql
mysql -h 127.0.0.1 -P 15306 --table customer/80- < ../common/select_customer_data.sql

./303_horizontal_split.sh

./304_migrate_replicas.sh
./305_migrate_master.sh

mysql -h 127.0.0.1 -P 15306 --table customer/-80 < ../common/select_customer_data.sql

./401_teardown.sh

