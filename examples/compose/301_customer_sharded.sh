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

# This script prepares the customer keyspace for sharding by creating
# sequence tables and applying the sharded vschema.

source ./env.sh

vtctldclient ApplySchema --sql="$(cat ../common/create_commerce_seq.sql)" commerce || fail "Failed to create sequence tables in the commerce keyspace"
vtctldclient ApplyVSchema --vschema="$(cat ../common/vschema_commerce_seq.json)" commerce || fail "Failed to create vschema sequences in the commerce keyspace"
vtctldclient ApplyVSchema --vschema="$(cat ../common/vschema_customer_sharded.json)" customer || fail "Failed to create vschema in sharded customer keyspace"
vtctldclient ApplySchema --sql="$(cat ../common/create_customer_sharded.sql)" customer || fail "Failed to create schema in sharded customer keyspace"
