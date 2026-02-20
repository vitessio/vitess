#!/bin/bash

# Copyright 2025 The Vitess Authors.
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

set -u

echo "Waiting for vtctld..."
until vtctldclient --server vtctld:15999 GetKeyspaces; do
  sleep 1
done

echo "Waiting for tablets..."
# We expect 6 tablets
while [ $(vtctldclient --server vtctld:15999 GetTablets | wc -l) -lt 6 ]; do
  sleep 1
done

echo "Initializing shards..."

# Initialize test_keyspace/-80
echo "Initializing test_keyspace/-80..."
vtctldclient --server vtctld:15999 PlannedReparentShard --new-primary test-0000000101 test_keyspace/-80 || echo "Failed to init test_keyspace/-80 or already initialized"

# Initialize test_keyspace/80-
echo "Initializing test_keyspace/80-..."
vtctldclient --server vtctld:15999 PlannedReparentShard --new-primary test-0000000201 test_keyspace/80- || echo "Failed to init test_keyspace/80- or already initialized"

# Initialize lookup_keyspace/-
echo "Initializing lookup_keyspace/-..."
vtctldclient --server vtctld:15999 PlannedReparentShard --new-primary test-0000000301 lookup_keyspace/- || echo "Failed to init lookup_keyspace/- or already initialized"

echo "Cluster initialization complete."
