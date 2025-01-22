# Copyright 2025 The Vitess Authors.

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

# This script restores the first replica tablet from backups for the 'customer' keyspace.

# Load common environment variables and functions
source ../common/env.sh  # Import necessary environment variables and functions from a common script

# Set keyspace and shard details for the 'customer' keyspace
KEYSPACE="customer"  # Define the keyspace to work with
SHARDS=("-80" "80-")  # Define the shards within the keyspace to restore

# Restore all shards of the customer keyspace from backups
for shard in "${SHARDS[@]}"; do  # Loop through each shard defined earlier
    echo "Finding replica tablets for shard $shard..."  # Log the start of the tablet search

    # Fetch the list of replica tablets for the current shard
    REPLICA_TABLETS=$(vtctldclient GetTablets --keyspace="$KEYSPACE" --shard="$shard" --tablet-type=replica | awk '{print $1}')  # Extract the first column containing tablet names
    REPLICA_COUNT=$(echo "$REPLICA_TABLETS" | wc -l)  # Count the number of replica tablets found

    # Check if any replica tablets were found
    if [ "$REPLICA_COUNT" -lt 1 ]; then  # If the count is less than 1, no replicas were found
        echo "No replica tablets found for shard $shard. Exiting..."  # Log a message and exit if none are found
        exit 1  # Exit the script with an error code
    fi

    # Choose the first replica for restoration
    RESTORE_TABLET=$(echo "$REPLICA_TABLETS" | head -n 1)  # Select the first replica tablet from the list
    echo "Restoring tablet $RESTORE_TABLET from backup for shard $shard..."  # Log the restoration action

    # Restore from backup and handle any failures
    vtctldclient RestoreFromBackup "$RESTORE_TABLET" || fail "Restore failed for tablet $RESTORE_TABLET"  # Attempt to restore from backup and log an error message if it fails
done

echo "Restore process completed successfully for $KEYSPACE."  # Log completion of the restore process
