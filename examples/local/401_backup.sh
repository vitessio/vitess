#!/bin/bash

# Copyright 2019 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# This script takes backups of the 'customer' keyspace and all its shards.

# Load common environment variables and functions
source ../common/env.sh

# Set keyspace and shard details for the 'customer' keyspace
KEYSPACE="customer"
SHARDS=("-80" "80-")

# Helper function for logging messages with timestamps
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1"
}

# Ensure the keyspace and shards are healthy
log "Ensuring keyspace $KEYSPACE exists and shards are healthy..."
for SHARD in "${SHARDS[@]}"; do
    wait_for_healthy_shard $KEYSPACE $SHARD || exit 1
done

# Backup all shards of the customer keyspace
for SHARD in "${SHARDS[@]}"; do
    log "Backing up shard $SHARD in keyspace $KEYSPACE..."
    vtctldclient BackupShard $KEYSPACE/$SHARD || fail "Backup failed for shard $SHARD"
done

log "Backup process completed successfully for all shards in $KEYSPACE."