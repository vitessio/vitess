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

# this script creates the tablets and initializes them for vertical
# resharding it also splits the vschema between the two keyspaces
# old (commerce) and new (customer)

set -e

script_root=`dirname "${BASH_SOURCE}"`

CELL=zone1 KEYSPACE=customer UID_BASE=200 $script_root/vttablet-up.sh
sleep 15
./lvtctl.sh InitShardMaster -force customer/0 zone1-200
./lvtctl.sh CopySchemaShard -tables customer,corder commerce/0 customer/0
./lvtctl.sh ApplyVSchema -vschema_file vschema_commerce_vsplit.json commerce
./lvtctl.sh ApplyVSchema -vschema_file vschema_customer_vsplit.json customer

disown -a
