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
# This script demonstrates how to create backups and restore tablets in Vitess.

# Load common environment variables and functions from the specified file
source ../common/env.sh

# Set the keyspace and shard variables
KEYSPACE="commerce"  # Define the keyspace we will work with, which is 'commerce'
SHARD="0"            # Define the shard we will operate on, which is shard '0'

# Helper function for logging messages with timestamps
log() {
    # Print the current date, time, and log message to standard output
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1"
}

# Ensure the keyspace exists and is healthy
log "Ensuring keyspace $KEYSPACE exists and shard $SHARD is healthy..."
# Wait for the specified shard to be healthy. If it is not healthy, exit the script with an error.
wait_for_healthy_shard $KEYSPACE $SHARD || exit 1

# Find the replica tablets dynamically
log "Finding replica tablets..."
# Use vtctldclient to get a list of replica tablets in the specified keyspace and shard,
# extracting just the tablet names using awk.
REPLICA_TABLETS=$(vtctldclient GetTablets --keyspace=$KEYSPACE --shard=$SHARD --tablet-type=replica | awk '{print $1}')
# Count the number of replica tablets found
REPLICA_COUNT=$(echo "$REPLICA_TABLETS" | wc -l)

# Check if any replica tablets were found
if [ "$REPLICA_COUNT" -lt 1 ]; then
    log "No replica tablets found. Attempting to temporarily demote primary for restore..."
    
    # If no replica tablets exist, find the primary tablet
    PRIMARY_TABLET=$(vtctldclient GetTablets --keyspace=$KEYSPACE --shard=$SHARD --tablet-type=primary | awk '{print $1}')
    log "Demoting primary tablet ($PRIMARY_TABLET) to replica mode for restore..."
    
    # Temporarily demote the primary tablet to a replica so it can be restored
    vtctldclient ChangeTabletType $PRIMARY_TABLET replica || fail "Failed to demote primary to replica"
    
    # Set the restore tablet to the primary tablet (now demoted)
    RESTORE_TABLET=$PRIMARY_TABLET
else
    # If there are replicas, select the first replica tablet for the restore process
    RESTORE_TABLET=$(echo "$REPLICA_TABLETS" | head -n 1)
fi

# Restore from the backup on the chosen tablet
log "Restoring tablet $RESTORE_TABLET from backup..."
# Call the RestoreFromBackup command on the selected tablet. If it fails, exit with an error.
vtctldclient RestoreFromBackup $RESTORE_TABLET || fail "Restore failed for tablet $RESTORE_TABLET"

# Check if the primary tablet was the one that was restored
if [ "$PRIMARY_TABLET" = "$RESTORE_TABLET" ]; then
    log "Promoting tablet $PRIMARY_TABLET back to primary..."
    # If the restored tablet was the primary, promote it back to primary status
    vtctldclient ChangeTabletType $PRIMARY_TABLET primary || fail "Failed to promote replica back to primary"
fi

# Complete the process with a success message
log "Backup and Restore example completed successfully."
