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

source ./env.sh

# apply sharding vschema
vtctlclient ApplyVSchema -vschema_file main_vschema_sharded.json main

# optional: create the schema needed for lookup vindex
#vtctlclient ApplySchema -sql-file create_lookup_schema.sql main

# create the lookup vindex
vtctlclient CreateLookupVindex -tablet_types=MASTER main "$(cat lookup_vindex.json)"

# we have to wait for replication to catch up
# Can see on vttablet status page Vreplication that copy is complete
sleep 5

#externalize vindex
vtctlclient ExternalizeVindex main.customer_region_lookup
