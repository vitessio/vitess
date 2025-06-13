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

# this script creates the tablets and initializes them for vertical
# resharding it also splits the vschema between the two keyspaces
# old (commerce) and new (customer)

source ./init.d/utils.sh

# You can either modify /etc/vitess.yaml directly to add the new tablets,
# or you can copy vitess.yaml.201-example to /etc/vitess.yaml or ~/.vitess.yaml
# We will do the latter here, but if you diff the 101 and 201 files,
# you can see all we are doing is adding 3 tablets.
cp ../common/config/vitess.yaml.201-example ~/.vitess.yaml

# Once the tablets are added to the config file, we can start them.
# The init script will re-read the tablets from the conf and
# only needs to start the tablets that are not already running.
./init.d/mysqlctl-vttablet start

# set the correct durability policy for the keyspace
vtctldclient --server=localhost:15999 SetKeyspaceDurabilityPolicy --durability-policy=semi_sync customer || fail "Failed to set keyspace durability policy on the customer keyspace"

# Wait for all the tablets to be up and registered in the topology server
# and for a primary tablet to be elected in the shard and become healthy/serving.
wait_for_healthy_shard customer 0 || exit 1
