/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package clone

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/log"
)

func TestCloneBackup(t *testing.T) {
	// The primary and replica1 come up with the shard; replica2 joins later and
	// restores from the backup this test takes.
	cluster := startCluster(
		t,
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames(shardName).
			WithReplicas(1).
			WithDurabilityPolicy("semi_sync"),
	)
	t.Cleanup(func() { removeBackups(t, cluster) })

	// Disable VTOrc recoveries, so that it's not racing with the primary
	// election.
	disableVTOrcRecoveries(t, cluster)

	ctx := t.Context()
	shard := cluster.Keyspace(keyspaceName).Shard(shardName)
	primary := shard.Primary()
	replica1 := shard.Replicas()[0]

	// Set up clean test data (table may have data from previous tests).
	_, err := primary.QueryTablet(ctx, vtInsertTest)
	require.NoError(t, err)
	_, err = primary.QueryTablet(ctx, "TRUNCATE TABLE vt_insert_test")
	require.NoError(t, err)
	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('clone_test_1')")
	require.NoError(t, err)
	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('clone_test_2')")
	require.NoError(t, err)

	// Verify data exists on primary.
	verifyRowsInTablet(t, primary, 2)

	// Wait for replica to catch up.
	waitInsertedRows(
		t,
		replica1,
		[]string{"clone_test_1", "clone_test_2"},
		30*time.Second,
		100*time.Millisecond,
	)

	// Take a backup using clone from primary.
	log.Info("Starting vtbackup with --clone-from-primary")
	err = vtbackupWithClone(t, cluster)
	require.NoError(t, err)

	// Verify a backup was created.
	backups := verifyBackupCount(t, cluster, 1)
	assert.NotEmpty(t, backups)

	// Insert more data AFTER the backup was taken.
	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('after_backup')")
	require.NoError(t, err)
	verifyRowsInTablet(t, primary, 3)

	// Now bring up replica2 and restore from the backup we just created.
	// This verifies the clone-based backup actually contains the data.
	log.Info("Restoring replica2 from backup to verify clone worked")
	replica2, err := cluster.AddTablet(ctx, cell, keyspaceName, shardName, "replica")
	require.NoError(t, err)
	require.NoError(t, replica2.WaitForTabletStatus(ctx, time.Minute, "SERVING"))

	// Verify replica2 has ALL the data (2 rows from before backup + 1 from after).
	// The 2 pre-backup rows prove the clone-based backup worked.
	// The 3rd row proves replication is working after restore.
	waitInsertedRows(
		t,
		replica2,
		[]string{"clone_test_1", "clone_test_2", "after_backup"},
		30*time.Second,
		100*time.Millisecond,
	)
	log.Info("Clone backup verification successful: replica2 has all data")
}

func vtbackupWithClone(t *testing.T, cluster *vitesst.Cluster) error {
	extraArgs := []string{
		"--allow-first-backup",
		"--mysql-clone-enabled",
		// Clone from primary instead of restoring from backup.
		"--restore-with-clone",
		"--clone-from-primary",
		// Clone credentials - use vt_clone user which is created with @'%' host
		// and BACKUP_ADMIN privilege in init_db.sql (no password).
		"--db-clone-user", "vt_clone",
		"--db-clone-password", "",
		"--db-clone-use-ssl=false",
	}

	log.Info(fmt.Sprintf("Starting vtbackup with clone args: %v", extraArgs))
	_, err := cluster.RunVtbackup(t.Context(), vitesst.VtbackupSpec{
		Keyspace:  keyspaceName,
		Shard:     shardName,
		ExtraArgs: extraArgs,
	})
	return err
}

func verifyBackupCount(t *testing.T, cluster *vitesst.Cluster, expected int) []string {
	backups, err := cluster.ListBackups(t.Context(), keyspaceName, shardName)
	require.NoError(t, err)

	assert.Len(t, backups, expected, "expected %d backups, got %d", expected, len(backups))
	return backups
}
