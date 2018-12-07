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

# this script removes the customer and corder tables from the commerce
# keyspace

set -e

script_root=`dirname "${BASH_SOURCE}"`

./lvtctl.sh ApplySchema -sql-file drop_commerce_tables.sql commerce
./lvtctl.sh SetShardTabletControl -blacklisted_tables=customer,corder -remove commerce/0 rdonly
./lvtctl.sh SetShardTabletControl -blacklisted_tables=customer,corder -remove commerce/0 replica
./lvtctl.sh SetShardTabletControl -blacklisted_tables=customer,corder -remove commerce/0 master

# customer and order tables have now been deleted from commerce keyspace
mysql -h 127.0.0.1 -P 15306 -u mysql_user --table < $script_root/../common/select_commerce_data.sql

disown -a
