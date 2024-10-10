#!/bin/bash

# Copyright 2024 The Vitess Authors.
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

# Ensure the keyspace and shards are healthy
echo "Ensuring keyspace $KEYSPACE exists and shards are healthy..."
for shard in "${shards[@]}"; do
    wait_for_healthy_shard "$KEYSPACE" "$shard" || exit 1
done

# Backup all shards of the customer keyspace
for shard in "${shards[@]}"; do
    echo "Backing up shard $shard in keyspace $KEYSPACE..."
    vtctldclient BackupShard "$KEYSPACE/$shard" || fail "Backup failed for shard $shard"
done

echo "Backup process completed successfully for all shards in $KEYSPACE."