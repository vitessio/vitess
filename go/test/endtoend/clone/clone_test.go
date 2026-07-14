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
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/vitesst"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	clusterInstance   *vitesst.Cluster
	donorTablet       *vitesst.Tablet
	recipientTablet   *vitesst.Tablet
	cell              = "zone1"
	donorKeyspace     = "donor"
	recipientKeyspace = "recipient"
	shardName         = "0"

	// cloneArgs give every vttablet the CLONE plugin and the credentials the
	// recipient connects to the donor with.
	cloneArgs = []string{
		"--mysql-clone-enabled",
		"--db-clone-user", "vt_clone",
		"--db-clone-password", "",
		"--db-clone-use-ssl=false",
	}
)

const (
	// cloneCnfPath is the clone plugin configuration the image carries; every
	// mysqld picks it up through EXTRA_MY_CNF.
	cloneCnfPath = "/vt/config/mycnf/clone.cnf"

	// initCloneSQL creates the user MySQL CLONE operations connect to the
	// donor with. BACKUP_ADMIN is required on the donor for clone operations.
	initCloneSQL = `CREATE USER IF NOT EXISTS 'vt_clone'@'%';
GRANT BACKUP_ADMIN ON *.* TO 'vt_clone'@'%';`

	// cloneStatusQuery reads the outcome of the last clone on the recipient.
	cloneStatusQuery = "SELECT STATE, ERROR_NO, ERROR_MESSAGE FROM performance_schema.clone_status ORDER BY ID DESC LIMIT 1"

	// cloneWaitTimeout bounds how long the recipient may take to clone the
	// donor, restart its mysqld and report completion.
	cloneWaitTimeout = 5 * time.Minute
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithCells(cell),
			vitesst.WithoutVTGate(),
			vitesst.WithTabletEnv(map[string]string{"EXTRA_MY_CNF": cloneCnfPath}),
			vitesst.WithInitDBSQLExtra(initCloneSQL),
			vitesst.WithVTTabletArgs(cloneArgs...),
			// The donor keyspace holds the data the clone transfers.
			vitesst.WithKeyspace(donorKeyspace).WithShardNames(shardName),
			// The recipient keyspace has no primary, so its tablet stays an
			// empty replica: nothing creates a database on its mysqld and
			// nothing replicates into it before the clone runs.
			vitesst.WithKeyspace(recipientKeyspace).WithShardNames(shardName).WithoutPrimaryElection(),
		)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}

		cleanup, err := cluster.Start(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer func() {
			if err := cleanup(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "cluster teardown:", err)
			}
		}()

		clusterInstance = cluster
		donorTablet = cluster.Keyspace(donorKeyspace).Shard(shardName).Primary()
		recipientTablet = cluster.Keyspace(recipientKeyspace).Shard(shardName).Replicas()[0]

		return m.Run()
	}()
	os.Exit(exitCode)
}

// stopMysqldSafeForTablet stops mysqld_safe for a tablet without stopping
// mysqld itself. This is used to exercise the case where MySQL finishes the
// clone but cannot restart itself.
func stopMysqldSafeForTablet(ctx context.Context, tablet *vitesst.Tablet) error {
	mysqldSafePattern := fmt.Sprintf("mysqld_safe.*vt_%010d", tablet.UID)

	exitCode, output, err := tablet.Exec(ctx, "pkill", "-9", "-f", mysqldSafePattern)
	if err != nil {
		return vterrors.Wrapf(err, "failed to stop mysqld_safe for tablet %s", tablet.Alias())
	}

	// pkill exits 1 when no process matched. That is fine if the MySQL install
	// in the image already starts mysqld directly.
	if exitCode != 0 && exitCode != 1 {
		return vterrors.Errorf(
			vtrpcpb.Code_INTERNAL,
			"failed to stop mysqld_safe for tablet %s: pkill exited %d, output: %s",
			tablet.Alias(),
			exitCode,
			output,
		)
	}

	return nil
}

// waitForCloneResult polls the recipient until its last clone reaches a final
// state and returns that row of performance_schema.clone_status. The recipient
// is unreachable while it restarts its mysqld, so connection failures are part
// of the wait.
func waitForCloneResult(ctx context.Context, t *testing.T, tablet *vitesst.Tablet) []sqltypes.Value {
	t.Helper()

	var row []sqltypes.Value
	require.Eventuallyf(t, func() bool {
		qr, err := tablet.QueryTabletWithDB(ctx, cloneStatusQuery, "")
		if err != nil || len(qr.Rows) == 0 {
			return false
		}

		state := qr.Rows[0][0].ToString()
		if state == "Not Started" || state == "In Progress" {
			return false
		}

		row = qr.Rows[0]
		return true
	}, cloneWaitTimeout, time.Second, "clone on %s did not finish", tablet.Alias())

	return row
}

// TestCloneRemote tests MySQL CLONE INSTANCE functionality
func TestCloneRemote(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()

	t.Cleanup(func() {
		if t.Failed() {
			clusterInstance.DumpDiagnostics(context.WithoutCancel(ctx), t.Logf)
		}
	})

	// Disable super_read_only so we can create test data
	_, err := donorTablet.QueryTabletWithDB(ctx, "SET GLOBAL super_read_only = OFF", "")
	require.NoError(t, err, "Failed to disable super_read_only")
	_, err = donorTablet.QueryTabletWithDB(ctx, "SET GLOBAL read_only = OFF", "")
	require.NoError(t, err, "Failed to disable read_only")

	// Create test database and table on donor
	_, err = donorTablet.QueryTabletWithDB(ctx, "CREATE DATABASE IF NOT EXISTS test_clone", "")
	require.NoError(t, err, "Failed to create test database")

	_, err = donorTablet.QueryTabletWithDB(ctx, `
		CREATE TABLE IF NOT EXISTS test_clone.clone_test (
			id INT AUTO_INCREMENT PRIMARY KEY,
			msg VARCHAR(255)
		) ENGINE=InnoDB
	`, "")
	require.NoError(t, err, "Failed to create test table")

	// Insert test data
	for i := 1; i <= 10; i++ {
		_, err = donorTablet.QueryTabletWithDB(ctx, fmt.Sprintf(
			"INSERT INTO test_clone.clone_test (msg) VALUES ('test message %d')", i,
		), "")
		require.NoError(t, err, "Failed to insert test data row %d", i)
	}

	// Verify donor has the data
	qr, err := donorTablet.QueryTabletWithDB(ctx, "SELECT COUNT(*) FROM test_clone.clone_test", "")
	require.NoError(t, err, "Failed to count rows on donor")
	require.Len(t, qr.Rows, 1)
	require.Equal(t, "10", qr.Rows[0][0].ToString(), "Donor should have 10 rows")

	// Pre-clone verification: ensure recipient does NOT have the test database
	// This proves the clone actually transfers data, not that it was already there
	qr, err = recipientTablet.QueryTabletWithDB(ctx, "SHOW DATABASES LIKE 'test_clone'", "")
	require.NoError(t, err, "Failed to check for test_clone database on recipient")
	require.Len(t, qr.Rows, 0, "Recipient should NOT have test_clone database before clone")

	// Stop only mysqld_safe. The mysqld process must stay alive for CLONE to
	// run, but mysqld_safe should not restart it when CLONE finishes. The
	// recipient's vttablet is then the one that starts mysqld again.
	err = stopMysqldSafeForTablet(ctx, recipientTablet)
	require.NoError(t, err, "Failed to stop recipient mysqld_safe")

	// Execute the clone: the recipient's vttablet restores with MySQL CLONE
	// from the donor tablet instead of from a backup.
	err = recipientTablet.StopVttablet(ctx)
	require.NoError(t, err, "Failed to stop recipient vttablet")

	restoreArgs := append([]string{
		"--restore-with-clone",
		"--clone-from-tablet", donorTablet.Alias(),
	}, cloneArgs...)
	err = recipientTablet.StartVttablet(ctx, restoreArgs...)
	require.NoError(t, err, "Failed to start recipient vttablet with clone restore")

	// Verify clone succeeded at MySQL level using performance_schema.clone_status
	cloneRow := waitForCloneResult(ctx, t, recipientTablet)
	require.Len(t, cloneRow, 3, "Expected one clone_status row")
	cloneState := cloneRow[0].ToString()
	cloneErrorNo := cloneRow[1].ToString()
	cloneErrorMsg := cloneRow[2].ToString()
	require.Equal(t, "Completed", cloneState, "Clone state should be Completed")
	require.Equal(t, "0", cloneErrorNo, "Clone should have no error, got: %s", cloneErrorMsg)

	// Verify recipient has the cloned data
	qr, err = recipientTablet.QueryTabletWithDB(ctx, "SELECT COUNT(*) FROM test_clone.clone_test", "")
	require.NoError(t, err, "Failed to count rows on recipient")
	require.Len(t, qr.Rows, 1)
	require.Equal(t, "10", qr.Rows[0][0].ToString(), "Recipient should have 10 rows after clone")

	// Verify actual data content matches
	donorData, err := donorTablet.QueryTabletWithDB(ctx, "SELECT id, msg FROM test_clone.clone_test ORDER BY id", "")
	require.NoError(t, err)

	recipientData, err := recipientTablet.QueryTabletWithDB(ctx, "SELECT id, msg FROM test_clone.clone_test ORDER BY id", "")
	require.NoError(t, err)

	require.Equal(t, len(donorData.Rows), len(recipientData.Rows), "Row counts should match")
	for i := range donorData.Rows {
		assert.Equal(t, donorData.Rows[i][0].ToString(), recipientData.Rows[i][0].ToString(), "IDs should match at row %d", i)
		assert.Equal(t, donorData.Rows[i][1].ToString(), recipientData.Rows[i][1].ToString(), "Messages should match at row %d", i)
	}

	t.Logf("Clone test passed: successfully cloned 10 rows from donor (tablet %s) to recipient (tablet %s)",
		donorTablet.Alias(), recipientTablet.Alias())
}
