#!/bin/bash

# Copyright 2026 The Vitess Authors.
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

# This script brings up the initial Vitess cluster with the commerce keyspace.

source ./env.sh

docker compose --profile commerce up -d

vtctldclient CreateKeyspace --durability-policy=semi_sync commerce || fail "Failed to create commerce keyspace. If the compose example was previously running, please run ./501_teardown.sh to clean it up and then re-run this step."

wait_for_healthy_shard commerce 0 || exit 1

vtctldclient ApplySchema --sql="$(cat ../common/create_commerce_schema.sql)" commerce || fail "Failed to apply schema for the commerce keyspace"
vtctldclient ApplyVSchema --vschema="$(cat ../common/vschema_commerce_initial.json)" commerce || fail "Failed to apply vschema for the commerce keyspace"

echo "Vitess commerce cluster is up!"
echo "  - vtgate MySQL:  mysql -h 127.0.0.1 -P 15306"
echo "  - vtctld web:    http://localhost:15000"
echo "  - vtadmin:       http://localhost:14201"
