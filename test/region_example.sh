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

# This test runs through the scripts in examples/region_sharding to make sure they work.
# It should be kept in sync with the steps in https://vitess.io/docs/user-guides/region-sharding/
# So we can detect if a regression affecting a tutorial is introduced.

source build.env

set -xe

cd "$VTROOT/examples/region_sharding"
unset VTROOT # ensure that the examples can run without VTROOT now.

source ./env.sh # Required so that "mysql" works from alias

./101_initial_cluster.sh

sleep 5 # Give vtgate time to really start.

mysql < insert_customers.sql
mysql --table < show_initial_data.sql

# create schema and vschema for sharding (+lookup vindex)
./201_main_sharded.sh

# bring up shards and tablets
./202_new_tablets.sh

# reshard
./203_reshard.sh

# SwitchReads
./204_switch_reads.sh

# SwitchWrites
./205_switch_writes.sh

# down shard
./206_down_shard_0.sh

# delete shard 0
./207_delete_shard_0.sh

# Down cluster
./301_teardown.sh
