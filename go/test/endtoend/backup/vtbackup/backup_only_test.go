/*
Copyright 2019 The Vitess Authors.

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

package vtbackup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/stats/opentsdb"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/log"
)

var vtInsertTest = `
		create table if not exists vt_insert_test (
		id bigint auto_increment,
		msg varchar(64),
		primary key (id)
		) Engine=InnoDB;`

// superReadOnlyError is what MySQL answers a write with while the server is
// read-only, which every tablet is until it is made the shard primary.
const superReadOnlyError = "The MySQL server is running with the --super-read-only option so it cannot execute this statement"

func TestFailingReplication(t *testing.T) {
	cluster, primary, replica1 := prepareCluster(t)
	ctx := t.Context()

	// Run the entire backup test
	firstBackupTest(t, cluster, primary, replica1, false)

	// Insert one more row, the primary will be ahead of the last backup
	_, err := primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test_failure')")
	require.NoError(t, err)

	// Disable replication from the primary by removing the grants to 'vt_repl'.
	_, err = primary.QueryTablet(ctx, "REVOKE REPLICATION SLAVE ON *.* FROM 'vt_repl'@'%';")
	require.NoError(t, err)
	_, err = primary.QueryTablet(ctx, "FLUSH PRIVILEGES;")
	require.NoError(t, err)

	// Take a backup with vtbackup: the process should fail entirely as it cannot replicate from the primary.
	_, err = startVtBackup(t, cluster, false, false, false)
	require.Error(t, err)

	// keep in mind how many backups we have right now
	backups, err := listBackups(t, cluster)
	require.NoError(t, err)

	// In 30 seconds, grant the replication permission again to 'vt_repl'.
	// This will mean that vtbackup should fail to replicate for ~30 seconds, until we grant the permission again.
	go func() {
		<-time.After(30 * time.Second)
		_, err := primary.QueryTablet(ctx, "GRANT REPLICATION SLAVE ON *.* TO 'vt_repl'@'%';")
		if !assert.NoError(t, err) {
			return
		}
		_, err = primary.QueryTablet(ctx, "FLUSH PRIVILEGES;")
		if !assert.NoError(t, err) {
			return
		}
	}()

	startTime := time.Now()
	// this will initially be stuck trying to replicate from the primary, and once we re-grant the permission in
	// the goroutine above, the process will work and complete successfully.
	_ = vtBackup(t, cluster, false, false, false)

	require.GreaterOrEqual(t, time.Since(startTime).Seconds(), float64(30))

	verifyBackupCount(t, cluster, len(backups)+1)

	removeBackups(t, cluster)
	verifyBackupCount(t, cluster, 0)
}

func TestTabletInitialBackup(t *testing.T) {
	// Test Initial Backup Flow
	//    TestTabletInitialBackup will:
	//    - Create a shard using vtbackup and --initial-backup
	//    - Create the rest of the cluster restoring from backup
	//    - Externally Reparenting to a primary tablet
	//    - Insert Some data
	//    - Verify that the cluster is working
	//    - Take a Second Backup
	//    - Bring up a second replica, and restore from the second backup
	//    - list the backups, remove them

	cluster, primary, replica1 := prepareCluster(t)

	// Run the entire backup test
	firstBackupTest(t, cluster, primary, replica1, true)
}

// prepareCluster seeds a shard with an initial backup taken by vtbackup from an
// empty database, and then brings up the shard's primary and first replica,
// both restoring from that backup.
func prepareCluster(t *testing.T) (*vitesst.Cluster, *vitesst.Tablet, *vitesst.Tablet) {
	t.Helper()

	cluster := startCluster(
		t,
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames(shardName).
			WithDurabilityPolicy("semi_sync").
			WithoutPrimaryElection(),
	)
	ctx := t.Context()

	// vtbackup seeds the shard's initial backup before any tablet is deployed,
	// so the shard has to start out empty.
	shard := cluster.Keyspace(keyspaceName).Shard(shardName)
	for _, tablet := range shard.Tablets() {
		require.NoError(t, tablet.Remove(ctx))
	}
	require.NoError(t, cluster.Vtctld().ExecuteCommand(ctx,
		"DeleteShards", "--recursive", "--even-if-serving", keyspaceName+"/"+shardName))

	dataPointReader := vtBackup(t, cluster, true, false, false)
	verifyBackupCount(t, cluster, 1)
	verifyBackupStats(t, dataPointReader, true /* initialBackup */)

	// Bring up the primary, restoring it from the initial backup. It stays
	// NOT_SERVING and read-only until it is made the shard primary.
	primary, err := cluster.AddTablet(ctx, cell, keyspaceName, shardName, "replica")
	require.NoError(t, err)
	waitForRestoreComplete(t, primary, 180*time.Second)

	err = createDB(ctx, primary, "testDB")
	require.ErrorContains(t, err, superReadOnlyError)

	// Vitess expects that the user has set the database into ReadWrite mode before calling
	// TabletExternallyReparented
	err = cluster.Vtctld().ExecuteCommand(ctx, "SetWritable", primary.Alias(), "true")
	require.NoError(t, err)
	err = cluster.Vtctld().ExecuteCommand(ctx, "TabletExternallyReparented", primary.Alias())
	require.NoError(t, err)

	// Bring up the first replica, restoring it from the same backup, and let it
	// replicate from the new primary.
	replica1, err := cluster.AddTablet(ctx, cell, keyspaceName, shardName, "replica")
	require.NoError(t, err)
	require.NoError(t, replica1.WaitForTabletStatus(ctx, 180*time.Second, "SERVING"))

	err = createDB(ctx, replica1, "testDB")
	require.ErrorContains(t, err, superReadOnlyError)

	return cluster, primary, replica1
}

func TestTabletBackupOnly(t *testing.T) {
	// Test Backup Flow
	//    TestTabletBackupOnly will:
	//    - Create a shard using regular init & start tablet
	//    - Run InitShardPrimary to start replication
	//    - Insert Some data
	//    - Verify that the cluster is working
	//    - Take a Second Backup
	//    - Bring up a second replica, and restore from the second backup
	//    - list the backups, remove them

	cluster := startCluster(
		t,
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames(shardName).
			WithReplicas(1).
			WithDurabilityPolicy("semi_sync"),
	)

	shard := cluster.Keyspace(keyspaceName).Shard(shardName)
	firstBackupTest(t, cluster, shard.Primary(), shard.Replicas()[0], true)
}

func firstBackupTest(t *testing.T, cluster *vitesst.Cluster, primary, replica1 *vitesst.Tablet, removeBackup bool) {
	// Test First Backup flow.
	//
	//    firstBackupTest will:
	//    - create a shard with primary and replica1 only
	//    - run InitShardPrimary
	//    - insert some data
	//    - take a backup
	//    - insert more data on the primary
	//    - bring up replica2 after the fact, let it restore the backup
	//    - check all data is right (before+after backup data)
	//    - list the backup, remove it

	ctx := t.Context()

	// Store initial backup counts
	backups, err := listBackups(t, cluster)
	require.NoError(t, err)

	// insert data on primary, wait for replica to get it
	_, err = primary.QueryTablet(ctx, vtInsertTest)
	require.NoError(t, err)
	// Add a single row with value 'test1' to the primary tablet
	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test1')")
	require.NoError(t, err)

	// Check that the specified tablet has the expected number of rows
	verifyRowsInTablet(t, replica1, 1)

	// backup the replica
	log.Info(fmt.Sprintf("taking backup %s", time.Now()))
	dataPointReader := vtBackup(t, cluster, false, true, true)
	log.Info(fmt.Sprintf("done taking backup %s", time.Now()))

	// check that the backup shows up in the listing
	verifyBackupCount(t, cluster, len(backups)+1)
	// check that backup stats are what we expect
	verifyBackupStats(t, dataPointReader, false /* initialBackup */)

	// insert more data on the primary
	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test2')")
	require.NoError(t, err)
	verifyRowsInTablet(t, replica1, 2)

	// now bring up the other replica, letting it restore from backup.
	replica2, err := cluster.AddTablet(ctx, cell, keyspaceName, shardName, "replica")
	require.NoError(t, err)
	// check the new replica has the data
	verifyRowsInTablet(t, replica2, 2)

	if removeBackup {
		removeBackups(t, cluster)
		verifyBackupCount(t, cluster, 0)
	}
}

// startVtBackup runs one vtbackup to completion and returns a reader over the
// opentsdb stats it wrote while running.
func startVtBackup(t *testing.T, cluster *vitesst.Cluster, initialBackup, restartBeforeBackup, disableRedoLog bool) (*opentsdb.DataPointReader, error) {
	// Take the back using vtbackup executable
	extraArgs := []string{
		"--allow-first-backup",

		// Use opentsdb for stats.
		"--stats-backend", "opentsdb",
		// Write stats to the process output for reading afterwards.
		"--opentsdb-uri", "file:///dev/stdout",
	}
	if restartBeforeBackup {
		extraArgs = append(extraArgs, "--restart-before-backup")
	}
	if disableRedoLog {
		extraArgs = append(extraArgs, "--disable-redo-log")
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	log.Info(fmt.Sprintf("starting backup tablet %s", time.Now()))
	vtbackup, err := cluster.StartVtbackup(ctx, vitesst.VtbackupSpec{
		Keyspace:      keyspaceName,
		Shard:         shardName,
		InitialBackup: initialBackup,
		ExtraArgs:     extraArgs,
	})
	if err != nil {
		return nil, err
	}

	if !initialBackup && disableRedoLog {
		go verifyDisableEnableRedoLogs(ctx, t, vtbackup)
	}

	output, err := vtbackup.Wait(ctx)
	if err != nil {
		return nil, err
	}
	return opentsdb.NewDataPointReader(strings.NewReader(dataPoints(output))), nil
}

func vtBackup(t *testing.T, cluster *vitesst.Cluster, initialBackup, restartBeforeBackup, disableRedoLog bool) *opentsdb.DataPointReader {
	dataPointReader, err := startVtBackup(t, cluster, initialBackup, restartBeforeBackup, disableRedoLog)
	require.NoError(t, err)
	return dataPointReader
}

// dataPoints keeps the opentsdb data point lines of a vtbackup run's output and
// drops its log lines, which the data point reader cannot parse.
func dataPoints(output string) string {
	var points strings.Builder
	for line := range strings.SplitSeq(output, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, "vtbackup.") {
			points.WriteString(line)
			points.WriteString("\n")
		}
	}
	return points.String()
}

func verifyBackupCount(t *testing.T, cluster *vitesst.Cluster, expected int) []string {
	backups, err := listBackups(t, cluster)
	require.NoError(t, err)
	assert.Equalf(t, expected, len(backups), "invalid number of backups")
	return backups
}

func listBackups(t *testing.T, cluster *vitesst.Cluster) ([]string, error) {
	return cluster.ListBackups(t.Context(), keyspaceName, shardName)
}

func removeBackups(t *testing.T, cluster *vitesst.Cluster) {
	// Remove all the backups from the shard
	require.NoError(t, cluster.RemoveAllBackups(t.Context(), keyspaceName, shardName))
}

// createDB creates a database on a tablet's mysqld, without selecting one first.
func createDB(ctx context.Context, tablet *vitesst.Tablet, dbName string) error {
	_, err := tablet.QueryTabletWithDB(ctx, "create database "+dbName, "")
	return err
}

// waitForRestoreComplete waits for a vttablet's background restore to finish.
// The tablet type transitions replica -> restore -> replica during a restore.
// This function polls until it observes "restore" and then sees "replica" again.
// If the restore completes faster than the polling interval and "restore" is
// never observed, the function returns since the restore is already done.
func waitForRestoreComplete(t *testing.T, tablet *vitesst.Tablet, timeout time.Duration) {
	t.Helper()
	sawRestore := false
	assert.Eventually(t, func() bool {
		tabletType := getTabletType(t, tablet)
		if tabletType == "restore" {
			sawRestore = true
		}
		return sawRestore && tabletType == "replica"
	}, timeout, 300*time.Millisecond)
	if sawRestore {
		require.Equal(t, "replica", getTabletType(t, tablet), "timed out waiting for tablet restore to complete")
	}
	// If we never observed "restore" type, the restore likely completed
	// before we started polling. Nothing to wait for.
}

// getTabletType returns the tablet type a vttablet currently reports, or an
// empty string while it is unreachable.
func getTabletType(t *testing.T, tablet *vitesst.Tablet) string {
	t.Helper()
	vars, err := tablet.GetVars(t.Context())
	if err != nil {
		return ""
	}
	tabletType, _ := vars["TabletType"].(string)
	return tabletType
}

// verifyRowsInTablet verifies the total number of rows in a tablet.
func verifyRowsInTablet(t *testing.T, tablet *vitesst.Tablet, expectedRows int) {
	t.Helper()
	lastNumRowsFound := 0
	require.Eventuallyf(t, func() bool {
		qr, err := tablet.QueryTablet(t.Context(), "select * from vt_insert_test")
		if err != nil {
			return false
		}
		lastNumRowsFound = len(qr.Rows)
		return lastNumRowsFound == expectedRows
	}, time.Minute, 300*time.Millisecond,
		"unexpected number of rows in %s (%s.vt_insert_test): found %d", tablet.Alias(), keyspaceName, lastNumRowsFound)
}

func verifyDisableEnableRedoLogs(ctx context.Context, t *testing.T, vtbackup *vitesst.Vtbackup) {
	params, err := vtbackup.DBAConnParams(ctx, "vt_"+keyspaceName)
	if !assert.NoError(t, err) {
		return
	}

	for {
		select {
		case <-time.After(100 * time.Millisecond):
			// Connect to vtbackup mysqld.
			conn, err := mysql.Connect(ctx, &params)
			if err != nil {
				// Keep trying, vtbackup mysqld may not be ready yet.
				continue
			}

			redoLogsVerified := func() bool {
				defer conn.Close()

				// Check if server supports disable/enable redo log.
				qr, err := conn.ExecuteFetch("SELECT 1 FROM performance_schema.global_status WHERE variable_name = 'innodb_redo_log_enabled'", 1, false)
				if !assert.NoError(t, err) {
					return true
				}
				// If not, there's nothing to test.
				if len(qr.Rows) == 0 {
					return true
				}

				// MY-013600
				// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_ib_wrn_redo_disabled
				qr, err = conn.ExecuteFetch("SELECT 1 FROM performance_schema.error_log WHERE data like '%InnoDB redo logging is disabled%'", 1, false)
				if !assert.NoError(t, err) {
					return true
				}
				if len(qr.Rows) != 1 {
					// Keep trying, possible we haven't disabled yet.
					return false
				}

				// MY-013601
				// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_ib_wrn_redo_enabled
				qr, err = conn.ExecuteFetch("SELECT 1 FROM performance_schema.error_log WHERE data like '%InnoDB redo logging is enabled%'", 1, false)
				if !assert.NoError(t, err) {
					return true
				}
				if len(qr.Rows) != 1 {
					// Keep trying, possible we haven't enabled yet.
					return false
				}

				// Success
				return true
			}()
			if redoLogsVerified {
				return
			}
		case <-ctx.Done():
			assert.Fail(t, "Failed to verify disable/enable redo log.")
			return
		}
	}
}

func verifyBackupStats(t *testing.T, dataPointReader *opentsdb.DataPointReader, initialBackup bool) {
	// During execution, the following phases will become active, in order.
	var expectActivePhases []string
	if initialBackup {
		expectActivePhases = []string{
			"initialbackup",
		}
	} else {
		expectActivePhases = []string{
			"restorelastbackup",
			"catchupreplication",
			"takenewbackup",
		}
	}

	// Sequence of phase activity.
	activePhases := make([]string, 0)

	// Last seen phase values.
	phaseValues := make(map[string]int64)

	// Scan for phase activity until all we're out of stats to scan.
	for dataPoint, err := dataPointReader.Read(); !errors.Is(err, io.EOF); dataPoint, err = dataPointReader.Read() {
		// We're only interested in "vtbackup.phase" metrics in this test.
		if dataPoint.Metric != "vtbackup.phase" {
			continue
		}

		phase := dataPoint.Tags["phase"]
		value := int64(dataPoint.Value)
		lastValue, ok := phaseValues[phase]

		// The value should always be 0 or 1.
		require.True(t, int64(0) == value || int64(1) == value)

		// The first time the phase is reported, it should be 0.
		if !ok {
			require.Equal(t, int64(0), value)
		}

		// Eventually the phase should go active. The next time it reports,
		// it should go inactive.
		if lastValue == 1 {
			require.Equal(t, int64(0), value)
		}

		// Record current value.
		phaseValues[phase] = value

		// Add phase to sequence once it goes from active to inactive.
		if lastValue == 1 && value == 0 {
			activePhases = append(activePhases, phase)
		}

		// Verify at most one phase is active.
		activeCount := 0
		for _, value := range phaseValues {
			if value == int64(0) {
				continue
			}

			activeCount++
			require.LessOrEqual(t, activeCount, 1)
		}
	}

	// Verify phase sequences.
	require.Equal(t, expectActivePhases, activePhases)
}
