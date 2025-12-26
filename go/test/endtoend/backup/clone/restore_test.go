/*
Copyright 2025 The Vitess Authors.

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

	"vitess.io/vitess/go/test/endtoend/cluster"
)

// TestCloneRestore tests clone-based replica provisioning via vttablet's
// --restore-with-clone flag. This simulates the workflow where a new replica
// is provisioned by cloning data from the primary instead of restoring from backup.
func TestCloneRestore(t *testing.T) {
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

	// Wait for replica1 to catch up.
	time.Sleep(2 * time.Second)

	// Now check if MySQL version supports clone (need vttablet running to query).
	if !mysqlVersionSupportsClone(t, primary) {
		t.Skip("Skipping clone test: MySQL version does not support CLONE (requires 8.0.17+)")
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

	// Bring up replica2 using clone from primary.
	err = localCluster.InitTablet(replica2, keyspaceName, shardName)
	require.NoError(t, err)
	restoreWithClone(t, replica2, "replica", "SERVING")

	// Wait for data to exist.
	require.Eventually(t, func() bool {
		qr, _ := replica2.VttabletProcess.QueryTablet("select * from vt_insert_test", keyspaceName, true)
		if qr != nil {
			if len(qr.Rows) == 3 {
				return true
			}
		}
		return false
	}, 5*time.Minute, 10*time.Second)

	// Verify clone worked: clone_status confirms, replication is set up.
	verifyClonedData(t, replica2)
	verifyCloneWasUsed(t, replica2)
	verifyReplicationTopology(t, replica2)

	// Insert rows on primary and verify they replicate to the cloned replica.
	for i := 1; i <= 5; i++ {
		_, err = primary.VttabletProcess.QueryTablet(
			fmt.Sprintf("insert into vt_insert_test (msg) values ('after_clone_%d')", i),
			keyspaceName, true)
		require.NoError(t, err)
	}
	time.Sleep(5 * time.Second)

	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 8)
	verifyPostCloneReplication(t, replica2)

	// Cleanup.
	tearDownRestoreTest()
}

// restoreWithClone starts a tablet that will use MySQL CLONE to get its data.
func restoreWithClone(t *testing.T, tablet *cluster.Vttablet, tabletType string, waitForState string) {
	tablet.VttabletProcess.ExtraArgs = []string{
		"--db-credentials-file", dbCredentialFile,
		"--mysql-clone-enabled",
		// Enable restore with clone - this triggers the clone logic.
		"--restore-from-backup=false",
		"--restore-with-clone",
		// Clone configuration - tells vttablet to clone instead of restoring from backup.
		"--clone-from-primary",
		"--db-clone-user", "vt_clone",
		"--db-clone-password", "",
		"--db-clone-use-ssl=false",
	}
	tablet.VttabletProcess.TabletType = tabletType
	tablet.VttabletProcess.ServingStatus = waitForState
	tablet.VttabletProcess.SupportsBackup = true

	err := tablet.VttabletProcess.Setup()
	require.NoError(t, err)
}

// verifyClonedData checks that the specific test data we inserted on primary
// exists on the cloned replica. This proves data was actually transferred.
func verifyClonedData(t *testing.T, tablet *cluster.Vttablet) {
	qr, err := tablet.VttabletProcess.QueryTablet(
		"SELECT msg FROM vt_insert_test ORDER BY id",
		keyspaceName,
		true,
	)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 3, "Expected 3 rows from clone")

	expectedValues := []string{"clone_restore_1", "clone_restore_2", "clone_restore_3"}
	for i, row := range qr.Rows {
		assert.Equal(t, expectedValues[i], row[0].ToString())
	}
}

// verifyReplicationTopology checks that the cloned replica has properly joined
// the replication topology and is replicating from the primary.
func verifyReplicationTopology(t *testing.T, tablet *cluster.Vttablet) {
	qr, err := tablet.VttabletProcess.QueryTablet("SHOW REPLICA STATUS", keyspaceName, true)
	require.NoError(t, err)
	require.NotEmpty(t, qr.Rows, "Replica status is empty - not replicating")

	// Find column indices.
	var ioRunningIdx, sqlRunningIdx = -1, -1
	for i, field := range qr.Fields {
		switch field.Name {
		case "Replica_IO_Running":
			ioRunningIdx = i
		case "Replica_SQL_Running":
			sqlRunningIdx = i
		}
	}

	row := qr.Rows[0]
	assert.Equal(t, "Yes", row[ioRunningIdx].ToString(), "Replica IO thread not running")
	assert.Equal(t, "Yes", row[sqlRunningIdx].ToString(), "Replica SQL thread not running")
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
