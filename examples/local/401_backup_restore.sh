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

# This script demonstrates how to create backups and restore tablets in Vitess.

source ../common/env.sh

# Start the topo server
if [ "${TOPO}" = "zk2" ]; then
	CELL=zone1 ../common/scripts/zk-up.sh
elif [ "${TOPO}" = "consul" ]; then
	CELL=zone1 ../common/scripts/consul-up.sh
else
	CELL=zone1 ../common/scripts/etcd-up.sh
fi

# Start vtctld
CELL=zone1 ../common/scripts/vtctld-up.sh

# Initialize the keyspace for backup/restore
if vtctldclient GetKeyspace backup_restore > /dev/null 2>&1 ; then
	# Keyspace exists
	vtctldclient SetKeyspaceDurabilityPolicy --durability-policy=semi_sync backup_restore || fail "Failed to set durability policy"
else
	# Create the keyspace
	vtctldclient CreateKeyspace --sidecar-db-name="_vt" --durability-policy=semi_sync backup_restore || fail "Failed to create keyspace"
fi

# Start mysqlctl and vttablets
for i in 200 201 202; do
	CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-up.sh &
done
sleep 2
wait
echo "MySQL servers started."

# Start vttablets
for i in 200 201 202; do
	CELL=zone1 KEYSPACE=backup_restore TABLET_UID=$i ../common/scripts/vttablet-up.sh
done

# Wait for a healthy primary tablet
wait_for_healthy_shard backup_restore 0 || exit 1

# Apply schema and vschema
vtctldclient ApplySchema --sql-file create_backup_schema.sql backup_restore || fail "Failed to apply schema"
vtctldclient ApplyVSchema --vschema-file vschema_backup.json backup_restore || fail "Failed to apply vschema"

# Backup the primary tablet (tablet 200)
vtctldclient Backup zone1-0000000200 || fail "Backup failed"

# Restore tablet 201 from backup
vtctldclient RestoreFromBackup zone1-0000000201 || fail "Restore failed"

# Verify backup exists
vtctldclient GetBackups backup_restore/0 || fail "No backups found"

# Complete
echo "Backup and Restore example completed successfully."
