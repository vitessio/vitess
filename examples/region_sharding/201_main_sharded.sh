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

source ../common/env.sh

# apply sharding vschema
vtctldclient ApplyVSchema --vschema-file main_vschema_sharded.json main || fail "Failed to apply vschema for the sharded main keyspace"

# optional: create the schema needed for lookup vindex
#vtctldclient ApplySchema --sql-file create_lookup_schema.sql main

# create the lookup vindex
vtctldclient LookupVindex --name customer_region_lookup --table-keyspace main create --keyspace main --type consistent_lookup_unique --table-owner customer --table-owner-columns=id --tablet-types=PRIMARY || fail "Failed to create lookup vindex in main keyspace"

# we have to wait for replication to catch up
# Can see on vttablet status page Vreplication that copy is complete
sleep 5

# externalize vindex
vtctldclient LookupVindex --name customer_region_lookup --table-keyspace main externalize --keyspace main || fail "Failed to externalize customer_region_lookup vindex in the main keyspace"
