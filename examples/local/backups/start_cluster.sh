#!/bin/bash

# Copyright 2022 The Vitess Authors.
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

# this script brings up new tablets for the two new shards that we will
# be creating in the customer keyspace and copies the schema

source ./env.sh

./101_initial_cluster.sh
sleep 5

mysql < ../common/insert_commerce_data.sql

./201_customer_tablets.sh
sleep 5

./202_move_tables.sh
sleep 3

./203_switch_reads.sh
./204_switch_writes.sh

./205_clean_commerce.sh

./301_customer_sharded.sh

./302_new_shards.sh
sleep 5

./303_reshard.sh
sleep 3

./304_switch_reads.sh
./305_switch_writes.sh
sleep 5