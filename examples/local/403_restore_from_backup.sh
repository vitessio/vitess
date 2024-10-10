#!/bin/bash

# Copyright 2024 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# This script restores the first replica tablet from backups for the 'customer' keyspace.

# Load common environment variables and functions
source ../common/env.sh  # Import necessary environment variables and functions from a common script

# Set keyspace and shard details for the 'customer' keyspace
KEYSPACE="customer"  # Define the keyspace to work with
SHARDS=("-80" "80-")  # Define the shards within the keyspace to restore

# Restore all shards of the customer keyspace from backups
for SHARD in "${SHARDS[@]}"; do  # Loop through each shard defined earlier
    echo "Finding replica tablets for shard $SHARD..."  # Log the start of the tablet search
    REPLICA_TABLETS=$(vtctldclient GetTablets --keyspace=$KEYSPACE --shard=$SHARD --tablet-type=replica | awk '{print $1}')  # Fetch replica tablets for the current shard
    REPLICA_COUNT=$(echo "$REPLICA_TABLETS" | wc -l)  # Count the number of replica tablets found

    if [ "$REPLICA_COUNT" -lt 1 ]; then  # Check if no replica tablets were found
        echo "No replica tablets found for shard $SHARD. Exiting..."  # Log a message and exit if none are found
        exit 1  # Exit the script with an error code
    fi

    # Choose the first replica for restoration
    RESTORE_TABLET=$(echo "$REPLICA_TABLETS" | head -n 1)  # Select the first replica tablet from the list
    echo "Restoring tablet $RESTORE_TABLET from backup for shard $SHARD..."  # Log the restoration action
    vtctldclient RestoreFromBackup $RESTORE_TABLET || fail "Restore failed for tablet $RESTORE_TABLET"  # Restore from backup and handle any failures
done

echo "Restore process completed successfully for $KEYSPACE."  # Log completion of the restore process
