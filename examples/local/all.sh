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

# this script execute all the local example scripts from 101 to 307.
# after this script's execution, you will have a sharded cluster running

./101_initial_cluster.sh
./201_customer_tablets.sh
./201_newkeyspace_tablets.sh
./202_move_tables.sh
./203_switch_reads.sh
./204_switch_writes.sh
./205_clean_commerce.sh
./301_customer_sharded.sh
./302_new_shards.sh
./303_reshard.sh
./304_switch_reads.sh
./305_switch_writes.sh
./306_down_shard_0.sh
./307_delete_shard_0.sh