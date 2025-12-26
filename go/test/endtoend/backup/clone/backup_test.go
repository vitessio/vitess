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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	vtutils "vitess.io/vitess/go/vt/utils"
)

func TestCloneBackup(t *testing.T) {
	t.Cleanup(func() { removeBackups(t) })
	t.Cleanup(tearDown)

	// Initialize tablets first so we can connect to MySQL.
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
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('clone_test_1')", keyspaceName, true)
	require.NoError(t, err)
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('clone_test_2')", keyspaceName, true)
	require.NoError(t, err)

	// Verify data exists on primary.
	cluster.VerifyRowsInTablet(t, primary, keyspaceName, 2)

	// Wait for replica to catch up.
	time.Sleep(2 * time.Second)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 2)

	// Take a backup using clone from primary.
	log.Infof("Starting vtbackup with --clone-from-primary")
	err = vtbackupWithClone(t)
	require.NoError(t, err)

	// Verify a backup was created.
	backups := verifyBackupCount(t, shardKsName, 1)
	assert.NotEmpty(t, backups)

	// Insert more data AFTER the backup was taken.
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('after_backup')", keyspaceName, true)
	require.NoError(t, err)
	cluster.VerifyRowsInTablet(t, primary, keyspaceName, 3)

	// Now bring up replica2 and restore from the backup we just created.
	// This verifies the clone-based backup actually contains the data.
	log.Infof("Restoring replica2 from backup to verify clone worked")
	err = localCluster.InitTablet(replica2, keyspaceName, shardName)
	require.NoError(t, err)
	restore(t, replica2, "replica", "SERVING")

	// Give replica2 time to catch up via replication.
	time.Sleep(5 * time.Second)

	// Verify replica2 has ALL the data (2 rows from before backup + 1 from after).
	// The 2 pre-backup rows prove the clone-based backup worked.
	// The 3rd row proves replication is working after restore.
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 3)
	log.Infof("Clone backup verification successful: replica2 has all data")
}

func vtbackupWithClone(t *testing.T) error {
	mysqlSocket, err := os.CreateTemp("", "vtbackup_clone_test_mysql.sock")
	require.NoError(t, err)
	defer os.Remove(mysqlSocket.Name())

	extraArgs := []string{
		"--allow_first_backup",
		"--db-credentials-file", dbCredentialFile,
		"--mysql-clone-enabled",
		vtutils.GetFlagVariantForTests("--mysql-socket"), mysqlSocket.Name(),
		// Clone from primary instead of restoring from backup.
		"--restore-with-clone",
		"--clone-from-primary",
		// Clone credentials - use vt_clone user which is created with @'%' host
		// and BACKUP_ADMIN privilege in init_db.sql (no password).
		"--db-clone-user", "vt_clone",
		"--db-clone-password", "",
		"--db-clone-use-ssl=false",
	}

	log.Infof("Starting vtbackup with clone args: %v", extraArgs)
	return localCluster.StartVtbackup(newInitDBFile, false, keyspaceName, shardName, cell, extraArgs...)
}

func verifyBackupCount(t *testing.T, shardKsName string, expected int) []string {
	backups, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("GetBackups", shardKsName)
	require.NoError(t, err)

	var result []string
	for _, line := range splitLines(backups) {
		if line != "" {
			result = append(result, line)
		}
	}
	assert.Equalf(t, expected, len(result), "expected %d backups, got %d", expected, len(result))
	return result
}

func removeBackups(t *testing.T) {
	backups, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("GetBackups", shardKsName)
	require.NoError(t, err)
	for _, backup := range splitLines(backups) {
		if backup != "" {
			_, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("RemoveBackup", shardKsName, backup)
			require.NoError(t, err)
		}
	}
}

func splitLines(s string) []string {
	var result []string
	for _, line := range []byte(s) {
		if line == '\n' {
			continue
		}
	}
	// Simple split by newline.
	start := 0
	for i, c := range s {
		if c == '\n' {
			if i > start {
				result = append(result, s[start:i])
			}
			start = i + 1
		}
	}
	if start < len(s) {
		result = append(result, s[start:])
	}
	return result
}

func restore(t *testing.T, tablet *cluster.Vttablet, tabletType string, waitForState string) {
	// Start tablet with restore enabled. MySQL is already running from TestMain.
	log.Infof("restoring tablet %s", time.Now())
	tablet.VttabletProcess.ExtraArgs = []string{"--db-credentials-file", dbCredentialFile}
	tablet.VttabletProcess.TabletType = tabletType
	tablet.VttabletProcess.ServingStatus = waitForState
	tablet.VttabletProcess.SupportsBackup = true
	err := tablet.VttabletProcess.Setup()
	require.NoError(t, err)
}

func tearDown() {
	for _, tablet := range []*cluster.Vttablet{primary, replica1, replica2} {
		if tablet != nil && tablet.VttabletProcess != nil {
			_ = tablet.VttabletProcess.TearDown()
		}
		if tablet != nil {
			_ = localCluster.VtctldClientProcess.ExecuteCommand("DeleteTablets", "--allow-primary", tablet.Alias)
		}
	}
}
