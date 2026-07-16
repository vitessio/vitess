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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
)

// cloneRestoreArgs make a vttablet get its data with MySQL CLONE from the
// shard primary instead of restoring from a backup.
var cloneRestoreArgs = []string{
	// Enable restore with clone - this triggers the clone logic.
	"--restore-from-backup=false",
	"--restore-with-clone",
	// Clone configuration - tells vttablet to clone instead of restoring from backup.
	"--clone-from-primary",
	"--db-clone-user", "vt_clone",
	"--db-clone-password", "",
	"--db-clone-use-ssl=false",
}

// TestCloneRestore tests clone-based replica provisioning via vttablet's
// --restore-with-clone flag. This simulates the workflow where a new replica
// is provisioned by cloning data from the primary instead of restoring from backup.
func TestCloneRestore(t *testing.T) {
	// The clone flags are cluster wide, so the replica that joins the shard
	// later picks them up. The primary and replica1 come up with the shard,
	// before there is a donor to clone from, and turn the clone off.
	cluster := startCluster(
		t,
		vitesst.WithVTTabletArgs(cloneRestoreArgs...),
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames(shardName).
			WithReplicas(1).
			WithDurabilityPolicy("semi_sync").
			WithTabletSpec(func(spec *vitesst.TabletSpec) {
				spec.ExtraArgs = []string{"--restore-with-clone=false"}
			}),
	)
	t.Cleanup(func() { removeBackups(t, cluster) })

	// Disable VTOrc recoveries, so that it's not racing with the primary
	// election.
	disableVTOrcRecoveries(t, cluster)

	ctx := t.Context()
	shard := cluster.Keyspace(keyspaceName).Shard(shardName)
	primary := shard.Primary()

	// Set up clean test data (table may have data from previous tests).
	_, err := primary.QueryTablet(ctx, vtInsertTest)
	require.NoError(t, err)
	_, err = primary.QueryTablet(ctx, "TRUNCATE TABLE vt_insert_test")
	require.NoError(t, err)
	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('clone_restore_1')")
	require.NoError(t, err)
	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('clone_restore_2')")
	require.NoError(t, err)
	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('clone_restore_3')")
	require.NoError(t, err)

	// Verify data exists on primary.
	verifyRowsInTablet(t, primary, 3)

	// Bring up replica2 using clone from primary.
	replica2, err := cluster.AddTablet(t, ctx, cell, keyspaceName, shardName, "replica")
	require.NoError(t, err)
	require.NoError(t, replica2.WaitForTabletStatus(ctx, time.Minute, "SERVING"))

	// Verify clone worked: clone_status confirms, replication is set up.
	waitInsertedRows(
		t,
		replica2,
		[]string{"clone_restore_1", "clone_restore_2", "clone_restore_3"},
		30*time.Second,
		100*time.Millisecond,
	)
	verifyCloneWasUsed(t, replica2)
	waitReplicationTopology(t, replica2)

	// Insert rows on primary and verify they replicate to the cloned replica.
	for i := 1; i <= 5; i++ {
		_, err = primary.QueryTablet(ctx,
			fmt.Sprintf("insert into vt_insert_test (msg) values ('after_clone_%d')", i))
		require.NoError(t, err)
	}

	// Wait for replica to catch up.
	waitInsertedRows(
		t,
		replica2,
		[]string{"clone_restore_1", "clone_restore_2", "clone_restore_3", "after_clone_1", "after_clone_2", "after_clone_3", "after_clone_4", "after_clone_5"},
		30*time.Second,
		100*time.Millisecond,
	)

	verifyPostCloneReplication(t, replica2)
}

// waitReplicationTopology checks that the cloned replica has properly joined
// the replication topology and is replicating from the primary.
func waitReplicationTopology(t *testing.T, tablet *vitesst.Tablet) {
	require.Eventually(t, func() bool {
		qr, err := tablet.QueryTablet(t.Context(), "SHOW REPLICA STATUS")
		if err != nil {
			return false
		}
		if len(qr.Rows) == 0 {
			return false
		}

		// Find column indices.
		ioRunningIdx, sqlRunningIdx := -1, -1
		for i, field := range qr.Fields {
			switch field.Name {
			case "Replica_IO_Running":
				ioRunningIdx = i
			case "Replica_SQL_Running":
				sqlRunningIdx = i
			}
		}

		row := qr.Rows[0]
		return strings.EqualFold(row[ioRunningIdx].ToString(), "Yes") && strings.EqualFold(row[sqlRunningIdx].ToString(), "Yes")
	}, 10*time.Second, 100*time.Millisecond)
}

// verifyPostCloneReplication checks that data inserted after the clone
// was properly replicated to the cloned replica.
func verifyPostCloneReplication(t *testing.T, tablet *vitesst.Tablet) {
	qr, err := tablet.QueryTablet(t.Context(),
		"SELECT msg FROM vt_insert_test WHERE msg LIKE 'after_clone_%' ORDER BY id")
	require.NoError(t, err)
	require.Len(t, qr.Rows, 5, "Expected 5 post-clone rows via replication")

	for i, row := range qr.Rows {
		expected := fmt.Sprintf("after_clone_%d", i+1)
		assert.Equal(t, expected, row[0].ToString())
	}
}

// verifyCloneWasUsed checks performance_schema.clone_status to verify that
// MySQL CLONE was actually used to restore the tablet.
func verifyCloneWasUsed(t *testing.T, tablet *vitesst.Tablet) {
	qr, err := tablet.QueryTablet(t.Context(),
		"SELECT STATE, SOURCE, ERROR_NO FROM performance_schema.clone_status")
	require.NoError(t, err)
	require.NotEmpty(t, qr.Rows, "clone_status is empty - CLONE was not used")

	row := qr.Rows[0]
	assert.Equal(t, "Completed", row[0].ToString(), "Clone did not complete")
	assert.NotEmpty(t, row[1].ToString(), "Clone source is empty")
	assert.Equal(t, "0", row[2].ToString(), "Clone had an error")
}
