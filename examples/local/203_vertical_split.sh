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

# this script copies over all the data from commerce keyspace to
# customer keyspace for the customer and corder tables

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

$VTROOT/bin/vtworker \
    $TOPOLOGY_FLAGS \
    -cell zone1 \
    -log_dir $VTDATAROOT/tmp \
    -alsologtostderr \
    -use_v3_resharding_mode \
    VerticalSplitClone -min_healthy_rdonly_tablets=1 -tables=customer,corder customer/0

sleep 2
# check that customer and order data has been copied to customer keyspace
mysql -h 127.0.0.1 -P 15306 -u mysql_user --table < $script_root/../common/select_customer0_data.sql

disown -a
