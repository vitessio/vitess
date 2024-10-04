#!/bin/bash

# Copyright 2024 The Vitess Authors.
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

# This script is part of the 101 example series and demonstrates how to create backups and restore tablets in Vitess.

source ../common/env.sh

# Set the keyspace and shard variables
KEYSPACE="commerce"
SHARD="0"
SIDECAR_DB_NAME="_vt"
TABLETS="100 101 102"

# Helper function for logging
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1"
}

# Start the topo server
log "Starting Topo server..."
if [ "${TOPO}" = "zk2" ]; then
	CELL=zone1 ../common/scripts/zk-up.sh
elif [ "${TOPO}" = "consul" ]; then
	CELL=zone1 ../common/scripts/consul-up.sh
else
	CELL=zone1 ../common/scripts/etcd-up.sh
fi

# Start vtctld
log "Starting vtctld..."
CELL=zone1 ../common/scripts/vtctld-up.sh

# Initialize the keyspace for backup/restore
if vtctldclient GetKeyspace $KEYSPACE > /dev/null 2>&1 ; then
	# Keyspace exists
	log "Keyspace $KEYSPACE already exists. Setting durability policy..."
	vtctldclient SetKeyspaceDurabilityPolicy --durability-policy=semi_sync $KEYSPACE || fail "Failed to set durability policy"
else
	# Create the keyspace
	log "Creating keyspace $KEYSPACE with durability policy semi_sync..."
	vtctldclient CreateKeyspace --sidecar-db-name="$SIDECAR_DB_NAME" --durability-policy=semi_sync $KEYSPACE || fail "Failed to create keyspace"
fi

# Start mysqlctl for the tablets
log "Starting MySQL instances..."
for tablet in $TABLETS; do
	CELL=zone1 TABLET_UID=$tablet ../common/scripts/mysqlctl-up.sh &
done
sleep 2
wait
log "MySQL instances started."

# Start vttablets for each tablet in the keyspace
log "Starting vttablets..."
for tablet in $TABLETS; do
	CELL=zone1 KEYSPACE=$KEYSPACE TABLET_UID=$tablet ../common/scripts/vttablet-up.sh
done

# Wait for the primary tablet to be elected and healthy
log "Waiting for a healthy primary tablet..."
wait_for_healthy_shard $KEYSPACE $SHARD || exit 1

# Apply schema to the keyspace
log "Applying schema to keyspace $KEYSPACE..."
vtctldclient ApplySchema --sql-file create_commerce_schema.sql $KEYSPACE || fail "Failed to apply schema"

# Apply vschema to the keyspace
log "Applying vschema to keyspace $KEYSPACE..."
vtctldclient ApplyVSchema --vschema-file vschema_commerce_initial.json $KEYSPACE || fail "Failed to apply vschema"

# New backup logic
log "Finding a replica tablet to backup..."
REPLICA_TABLET=$(vtctldclient GetTablet zone1-0000000101 | grep 'type: REPLICA' > /dev/null && echo "zone1-0000000101" || echo "zone1-0000000102")

if [ -z "$REPLICA_TABLET" ]; then
    log "No replica tablet found. Attempting to backup primary with --allow_primary flag..."
    vtctldclient Backup --allow_primary zone1-0000000100 || fail "Backup failed for primary tablet"
else
    log "Backing up replica tablet ($REPLICA_TABLET)..."
    vtctldclient Backup $REPLICA_TABLET || fail "Backup failed for replica tablet"
fi

# Restore another tablet (tablet 201) from the backup
RESTORE_TABLET="zone1-0000000102"
if [ "$RESTORE_TABLET" = "$REPLICA_TABLET" ]; then
    RESTORE_TABLET="zone1-0000000101"
fi

log "Restoring tablet $RESTORE_TABLET from backup..."
vtctldclient RestoreFromBackup $RESTORE_TABLET || fail "Restore failed for tablet $RESTORE_TABLET"

# Verify the backup exists
log "Verifying backups exist for keyspace $KEYSPACE, shard $SHARD..."
vtctldclient GetBackups $KEYSPACE/$SHARD || fail "No backups found"

# Complete
log "Backup and Restore example completed successfully."