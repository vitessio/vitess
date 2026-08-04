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

# This script brings up tablets for the two new shards (-80 and 80-)
# in the customer keyspace.

source ./env.sh

docker compose --profile sharded up -d

wait_for_healthy_shard customer "-80" || exit 1
wait_for_healthy_shard customer "80-" || exit 1
