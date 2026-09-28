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

# Load common environment variables and functions
source ../common/env.sh  # Import necessary environment variables and functions from a common script

# Set keyspace and shard details for the 'customer' keyspace
KEYSPACE="customer"  # Define the keyspace to work with
SHARDS=("-80" "80-")  # Define the shards within the keyspace to list backups for

# List backups for each shard
for shard in "${SHARDS[@]}"; do  # Loop through each shard defined earlier
    echo "Listing available backups for keyspace $KEYSPACE and shard $shard..."  # Log the start of the backup listing
    vtctldclient GetBackups "$KEYSPACE/$shard" || echo "Failed to list backups for keyspace $KEYSPACE and shard $shard"  # Attempt to list backups; log failure if it occurs
done

echo "Backup listing process completed."  # Log completion of the backup listing process
