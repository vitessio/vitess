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
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

func TestCloneBackup(t *testing.T) {
	t.Cleanup(func() { removeBackups(t) })
	t.Cleanup(tearDown)

	// Disable VTOrc recoveries, so that it's not racing with InitShardPrimary
	// call to set the primary.
	localCluster.DisableVTOrcRecoveries(t)

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
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('clone_test_1')", keyspaceName, true)
	require.NoError(t, err)
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('clone_test_2')", keyspaceName, true)
	require.NoError(t, err)

	// Verify data exists on primary.
	cluster.VerifyRowsInTablet(t, primary, keyspaceName, 2)

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
	log.Info("Restoring replica2 from backup to verify clone worked")
	err = localCluster.InitTablet(replica2, keyspaceName, shardName)
	require.NoError(t, err)
	restore(t, replica2, "replica", "SERVING")

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

func vtbackupWithClone(t *testing.T) error {
	mysqlSocket, err := os.CreateTemp("", "vtbackup_clone_test_mysql.sock")
	require.NoError(t, err)
	defer os.Remove(mysqlSocket.Name())

	// Boot the recipient mysqld with super-read-only enabled, mirroring the
	// stock mycnf templates used in production. The init_db file restores the
	// boot value at the end of initialization, so the recipient is read-only
	// when the clone starts.
	sroCnf := path.Join(t.TempDir(), "super_read_only.cnf")
	require.NoError(t, os.WriteFile(sroCnf, []byte("super-read-only = 1\n"), 0o666))
	extraMyCnf := sroCnf
	if existing := os.Getenv("EXTRA_MY_CNF"); existing != "" {
		// Append last so super-read-only wins over any earlier settings.
		extraMyCnf = existing + ":" + sroCnf
	}
	t.Setenv("EXTRA_MY_CNF", extraMyCnf)

	// Build a recipient-specific init DB file that also creates an extra user
	// database, reproducing the production failure mode: a recipient initialized
	// with --init-db-sql-file containing CREATE DATABASE boots super-read-only,
	// and CLONE INSTANCE cannot drop the database (errno 1290). Without the
	// fix to disable super_read_only before cloning, this test fails.
	vtbackupInitDB := buildVtbackupInitDBFile(t, newInitDBFile)

	extraArgs := []string{
		"--allow-first-backup",
		"--db-credentials-file", dbCredentialFile,
		"--mysql-clone-enabled",
		"--mysql-socket", mysqlSocket.Name(),
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
	return localCluster.StartVtbackup(vtbackupInitDB, false, keyspaceName, shardName, cell, extraArgs...)
}

// buildVtbackupInitDBFile creates a copy of the base init DB file that also
// creates an extra user database, mirroring an operator-supplied
// --init-db-sql-file with a CREATE DATABASE statement. The database is
// created while super_read_only is OFF (during init), then super_read_only is
// restored to the boot value, so CLONE INSTANCE must drop it under
// super_read_only.
func buildVtbackupInitDBFile(t *testing.T, baseFile string) string {
	content, err := os.ReadFile(baseFile)
	require.NoError(t, err)

	const restoreStmt = "SET GLOBAL super_read_only=IFNULL(@original_super_read_only, 'ON')"
	idx := strings.LastIndex(string(content), restoreStmt)
	require.NotEqual(t, -1, idx, "could not find super_read_only restore in init DB file")

	updated := string(content[:idx]) +
		"CREATE DATABASE IF NOT EXISTS extra_clone_test;\n" +
		string(content[idx:])

	vtbackupInitDB := path.Join(t.TempDir(), "init_db_vtbackup.sql")
	require.NoError(t, os.WriteFile(vtbackupInitDB, []byte(updated), 0o666))
	return vtbackupInitDB
}

func verifyBackupCount(t *testing.T, shardKsName string, expected int) []string {
	backups, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("GetBackups", shardKsName)
	require.NoError(t, err)

	var result []string
	for line := range strings.SplitSeq(backups, "\n") {
		if line != "" {
			result = append(result, line)
		}
	}
	assert.Len(t, result, expected, "expected %d backups, got %d", expected, len(result))
	return result
}

func restore(t *testing.T, tablet *cluster.Vttablet, tabletType string, waitForState string) {
	// Start tablet with restore enabled. MySQL is already running from TestMain.
	log.Info(fmt.Sprintf("restoring tablet %s", time.Now()))
	tablet.VttabletProcess.ExtraArgs = vttabletExtraArgs
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
