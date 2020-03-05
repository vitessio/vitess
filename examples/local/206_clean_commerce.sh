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

# this script removes the customer and corder tables from the commerce
# keyspace

vtctlclient -server localhost:15999 ApplySchema -sql-file drop_commerce_tables.sql commerce
vtctlclient -server localhost:15999 SetShardTabletControl -blacklisted_tables=customer,corder -remove commerce/0 rdonly
vtctlclient -server localhost:15999 SetShardTabletControl -blacklisted_tables=customer,corder -remove commerce/0 replica
vtctlclient -server localhost:15999 SetShardTabletControl -blacklisted_tables=customer,corder -remove commerce/0 master
