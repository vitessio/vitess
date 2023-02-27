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

# this script creates vitess sequences for the auto_increment fields
# and alters the fields to no longer be auto_increment in preparation
# for horizontal sharding
# it also changes the customer vschema from unsharded to sharded and
# sets up the necessary vindexes

source ../common/env.sh

vtctldclient ApplySchema --sql-file create_commerce_seq.sql commerce || fail "Failed to create sequence tables in the commerce keyspace"
vtctldclient ApplyVSchema --vschema-file vschema_commerce_seq.json commerce || fail "Failed to create vschema sequences in the commerce keyspace"
vtctldclient ApplyVSchema --vschema-file vschema_customer_sharded.json customer || fail "Failed to create vschema in sharded customer keyspace"
vtctldclient ApplySchema --sql-file create_customer_sharded.sql customer || fail "Failed to create schema in sharded customer keyspace"
