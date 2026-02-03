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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

// TestCloneRestore tests clone-based replica provisioning via vttablet's
// --restore-with-clone flag. This simulates the workflow where a new replica
// is provisioned by cloning data from the primary instead of restoring from backup.
func TestCloneRestore(t *testing.T) {
	t.Cleanup(func() { removeBackups(t) })
	t.Cleanup(tearDownRestoreTest)

	// Disable VTOrc recoveries, so that it's not racing with InitShardPrimary
	// call to set the primary.
	localCluster.DisableVTOrcRecoveries(t)

	// Initialize primary and replica1 first (need replica for semi-sync durability).
	for _, tablet := range []*cluster.Vttablet{primary, replica1} {
		err := localCluster.InitTablet(tablet, keyspaceName, shardName)
		require.NoError(t, err)
		err = tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}

	// Initialize shard primary.
	err := localCluster.VtctldClientProcess.InitShardPrimary(keyspaceName, shardName, cell, primary.TabletUID)
	require.NoError(t, err)

	// Now check if MySQL version supports clone (need vttablet running to query).
	if !mysqlVersionSupportsClone(t, primary) {
		ci, ok := os.LookupEnv("CI")
		if !ok || strings.ToLower(ci) != "true" {
			t.Skip("Skipping clone test: MySQL version does not support CLONE (requires 8.0.17+)")
		} else {
			require.FailNow(t, "CI should be running versions of mysqld that support CLONE")
		}
	}

	// Check if clone plugin is available.
	if !clonePluginAvailable(t, primary) {
		t.Skip("Skipping clone test: clone plugin not available")
	}

	// Set up clean test data (table may have data from previous tests).
	_, err = primary.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	require.NoError(t, err)
	_, err = primary.VttabletProcess.QueryTablet("TRUNCATE TABLE vt_insert_test", keyspaceName, true)
	require.NoError(t, err)
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('clone_restore_1')", keyspaceName, true)
	require.NoError(t, err)
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('clone_restore_2')", keyspaceName, true)
	require.NoError(t, err)
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('clone_restore_3')", keyspaceName, true)
	require.NoError(t, err)

	// Verify data exists on primary.
	cluster.VerifyRowsInTablet(t, primary, keyspaceName, 3)

	// Clean up replica2's MySQL data from previous test so clone can work.
	// Stop MySQL, remove data directory, and restart with fresh data.
	err = replica2.MysqlctlProcess.Stop()
	require.NoError(t, err)
	err = os.RemoveAll(replica2.VttabletProcess.Directory)
	require.NoError(t, err)
	proc, err := replica2.MysqlctlProcess.StartProcess()
	require.NoError(t, err)
	err = proc.Wait()
	require.NoError(t, err)

	// Bring up replica2 using clone from primary.
	err = localCluster.InitTablet(replica2, keyspaceName, shardName)
	require.NoError(t, err)
	restoreWithClone(t, replica2, "replica", "SERVING")

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
		_, err = primary.VttabletProcess.QueryTablet(
			fmt.Sprintf("insert into vt_insert_test (msg) values ('after_clone_%d')", i),
			keyspaceName, true)
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

// restoreWithClone starts a tablet that will use MySQL CLONE to get its data.
func restoreWithClone(t *testing.T, tablet *cluster.Vttablet, tabletType string, waitForState string) {
	// Start with the base vttablet flags (includes replication flags)
	cloneArgs := append([]string{}, vttabletExtraArgs...)
	// Add clone-specific flags
	cloneArgs = append(cloneArgs,
		// Enable restore with clone - this triggers the clone logic.
		"--restore-from-backup=false",
		"--restore-with-clone",
		// Clone configuration - tells vttablet to clone instead of restoring from backup.
		"--clone-from-primary",
		"--db-clone-user", "vt_clone",
		"--db-clone-password", "",
		"--db-clone-use-ssl=false",
	)
	tablet.VttabletProcess.ExtraArgs = cloneArgs
	tablet.VttabletProcess.TabletType = tabletType
	tablet.VttabletProcess.ServingStatus = waitForState
	tablet.VttabletProcess.SupportsBackup = true

	err := tablet.VttabletProcess.Setup()
	require.NoError(t, err)
}

// waitReplicationTopology checks that the cloned replica has properly joined
// the replication topology and is replicating from the primary.
func waitReplicationTopology(t *testing.T, tablet *cluster.Vttablet) {
	require.Eventually(t, func() bool {
		qr, err := tablet.VttabletProcess.QueryTablet("SHOW REPLICA STATUS", keyspaceName, true)
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
func verifyPostCloneReplication(t *testing.T, tablet *cluster.Vttablet) {
	qr, err := tablet.VttabletProcess.QueryTablet(
		"SELECT msg FROM vt_insert_test WHERE msg LIKE 'after_clone_%' ORDER BY id",
		keyspaceName,
		true,
	)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 5, "Expected 5 post-clone rows via replication")

	for i, row := range qr.Rows {
		expected := fmt.Sprintf("after_clone_%d", i+1)
		assert.Equal(t, expected, row[0].ToString())
	}
}

// verifyCloneWasUsed checks performance_schema.clone_status to verify that
// MySQL CLONE was actually used to restore the tablet.
func verifyCloneWasUsed(t *testing.T, tablet *cluster.Vttablet) {
	qr, err := tablet.VttabletProcess.QueryTablet(
		"SELECT STATE, SOURCE, ERROR_NO FROM performance_schema.clone_status",
		keyspaceName,
		true,
	)
	require.NoError(t, err)
	require.NotEmpty(t, qr.Rows, "clone_status is empty - CLONE was not used")

	row := qr.Rows[0]
	assert.Equal(t, "Completed", row[0].ToString(), "Clone did not complete")
	assert.NotEmpty(t, row[1].ToString(), "Clone source is empty")
	assert.Equal(t, "0", row[2].ToString(), "Clone had an error")
}

// tearDownRestoreTest cleans up tablets created during the restore test.
func tearDownRestoreTest() {
	for _, tablet := range []*cluster.Vttablet{primary, replica1, replica2} {
		if tablet != nil && tablet.VttabletProcess != nil {
			_ = tablet.VttabletProcess.TearDown()
		}
		if tablet != nil {
			_ = localCluster.VtctldClientProcess.ExecuteCommand("DeleteTablets", "--allow-primary", tablet.Alias)
		}
	}
}
