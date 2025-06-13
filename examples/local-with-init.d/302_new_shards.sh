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

# this script brings up new tablets for the two new shards that we will
# be creating in the customer keyspace and copies the schema

source ./init.d/utils.sh

# Add the new 300 301 302 and 400 401 402 tablets to /etc/vitess.yaml or ~/.vitess.yaml.
# We can then re-run start on the init script, which will detect some tablets have not started.

cp ../common/config/vitess.yaml.302-example ~/.vitess.yaml

# Once the tablets are added to the config file, we can start them.
# The init script will read the tablets from /etc/vitess.yaml and
# only needs to start the tablets that are not already running.

./init.d/mysqlctl-vttablet start

for shard in "-80" "80-"; do
	# Wait for all the tablets to be up and registered in the topology server
	# and for a primary tablet to be elected in the shard and become healthy/serving.
	wait_for_healthy_shard customer "${shard}" || exit 1
done;
