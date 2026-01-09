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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/stats/opentsdb"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/utils"
)

var (
	vtInsertTest = `
		create table if not exists vt_insert_test (
		id bigint auto_increment,
		msg varchar(64),
		primary key (id)
		) Engine=InnoDB;`
)

func TestFailingReplication(t *testing.T) {
	prepareCluster(t)

	// Run the entire backup test
	firstBackupTest(t, false)

	// Insert one more row, the primary will be ahead of the last backup
	_, err := primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test_failure')", keyspaceName, true)
	require.NoError(t, err)

	// Disable replication from the primary by removing the grants to 'vt_repl'.
	_, err = primary.VttabletProcess.QueryTablet("REVOKE REPLICATION SLAVE ON *.* FROM 'vt_repl'@'%';", keyspaceName, true)
	require.NoError(t, err)
	_, err = primary.VttabletProcess.QueryTablet("FLUSH PRIVILEGES;", keyspaceName, true)
	require.NoError(t, err)

	// Take a backup with vtbackup: the process should fail entirely as it cannot replicate from the primary.
	_, err = startVtBackup(t, false, false, false)
	require.Error(t, err)

	// keep in mind how many backups we have right now
	backups, err := listBackups(shardKsName)
	require.NoError(t, err)

	// In 30 seconds, grant the replication permission again to 'vt_repl'.
	// This will mean that vtbackup should fail to replicate for ~30 seconds, until we grant the permission again.
	go func() {
		<-time.After(30 * time.Second)
		_, err = primary.VttabletProcess.QueryTablet("GRANT REPLICATION SLAVE ON *.* TO 'vt_repl'@'%';", keyspaceName, true)
		require.NoError(t, err)
		_, err = primary.VttabletProcess.QueryTablet("FLUSH PRIVILEGES;", keyspaceName, true)
		require.NoError(t, err)
	}()

	startTime := time.Now()
	// this will initially be stuck trying to replicate from the primary, and once we re-grant the permission in
	// the goroutine above, the process will work and complete successfully.
	_ = vtBackup(t, false, false, false)

	require.GreaterOrEqual(t, time.Since(startTime).Seconds(), float64(30))

	verifyBackupCount(t, shardKsName, len(backups)+1)

	removeBackups(t)
	verifyBackupCount(t, shardKsName, 0)

	tearDown(t, true)
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

	prepareCluster(t)

	// Run the entire backup test
	firstBackupTest(t, true)

	tearDown(t, true)
}

func prepareCluster(t *testing.T) {
	waitForReplicationToCatchup([]cluster.Vttablet{*replica1, *replica2})

	dataPointReader := vtBackup(t, true, false, false)
	verifyBackupCount(t, shardKsName, 1)
	verifyBackupStats(t, dataPointReader, true /* initialBackup */)

	// Initialize the tablets
	initTablets(t, false, false)

	err := primary.VttabletProcess.CreateDB("testDB")
	require.ErrorContains(t, err, "The MySQL server is running with the --super-read-only option so it cannot execute this statement")
	err = replica1.VttabletProcess.CreateDB("testDB")
	require.ErrorContains(t, err, "The MySQL server is running with the --super-read-only option so it cannot execute this statement")

	// Restore the Tablet
	restore(t, primary, "replica", "NOT_SERVING")
	// Vitess expects that the user has set the database into ReadWrite mode before calling
	// TabletExternallyReparented
	err = localCluster.VtctldClientProcess.ExecuteCommand(
		"SetWritable", primary.Alias, "true")
	require.NoError(t, err)
	err = localCluster.VtctldClientProcess.ExecuteCommand(
		"TabletExternallyReparented", primary.Alias)
	require.NoError(t, err)
	restore(t, replica1, "replica", "SERVING")
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

	// Reset the tablet object values in order on init tablet in the next step.
	primary.VttabletProcess.ServingStatus = "NOT_SERVING"
	replica1.VttabletProcess.ServingStatus = "NOT_SERVING"

	initTablets(t, true, true)
	firstBackupTest(t, true)

	tearDown(t, false)
}

func firstBackupTest(t *testing.T, removeBackup bool) {
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

	// Store initial backup counts
	backups, err := listBackups(shardKsName)
	require.NoError(t, err)

	// insert data on primary, wait for replica to get it
	_, err = primary.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	require.NoError(t, err)
	// Add a single row with value 'test1' to the primary tablet
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.NoError(t, err)

	// Check that the specified tablet has the expected number of rows
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 1)

	// backup the replica
	log.Infof("taking backup %s", time.Now())
	dataPointReader := vtBackup(t, false, true, true)
	log.Infof("done taking backup %s", time.Now())

	// check that the backup shows up in the listing
	verifyBackupCount(t, shardKsName, len(backups)+1)
	// check that backup stats are what we expect
	verifyBackupStats(t, dataPointReader, false /* initialBackup */)

	// insert more data on the primary
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	require.NoError(t, err)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 2)

	// even though we change the value of compression it won't affect
	// decompression since it gets its value from MANIFEST file, created
	// as part of backup.
	mysqlctl.CompressionEngineName = "lz4"
	defer func() { mysqlctl.CompressionEngineName = "pgzip" }()
	// now bring up the other replica, letting it restore from backup.
	err = localCluster.InitTablet(replica2, keyspaceName, shardName)
	require.NoError(t, err)
	restore(t, replica2, "replica", "SERVING")
	// Replica2 takes time to serve. Sleeping for 5 sec.
	time.Sleep(5 * time.Second)
	// check the new replica has the data
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 2)

	if removeBackup {
		removeBackups(t)
		verifyBackupCount(t, shardKsName, 0)
	}
}

func startVtBackup(t *testing.T, initialBackup bool, restartBeforeBackup, disableRedoLog bool) (*os.File, error) {
	mysqlSocket, err := os.CreateTemp("", "vtbackup_test_mysql.sock")
	require.NoError(t, err)
	defer os.Remove(mysqlSocket.Name())

	// Prepare opentsdb stats file path.
	statsPath := path.Join(t.TempDir(), fmt.Sprintf("opentsdb.%s.txt", t.Name()))

	// Take the back using vtbackup executable
	extraArgs := []string{
		"--allow_first_backup",
		"--db-credentials-file", dbCredentialFile,
		utils.GetFlagVariantForTests("--mysql-socket"), mysqlSocket.Name(),

		// Use opentsdb for stats.
		utils.GetFlagVariantForTests("--stats-backend"), "opentsdb",
		// Write stats to file for reading afterwards.
		utils.GetFlagVariantForTests("--opentsdb-uri"), "file://" + statsPath,
	}
	if restartBeforeBackup {
		extraArgs = append(extraArgs, "--restart_before_backup")
	}
	if disableRedoLog {
		extraArgs = append(extraArgs, "--disable-redo-log")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if !initialBackup && disableRedoLog {
		go verifyDisableEnableRedoLogs(ctx, t, mysqlSocket.Name())
	}

	log.Infof("starting backup tablet %s", time.Now())
	err = localCluster.StartVtbackup(newInitDBFile, initialBackup, keyspaceName, shardName, cell, extraArgs...)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(statsPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func vtBackup(t *testing.T, initialBackup bool, restartBeforeBackup, disableRedoLog bool) *opentsdb.DataPointReader {
	f, err := startVtBackup(t, initialBackup, restartBeforeBackup, disableRedoLog)
	require.NoError(t, err)
	return opentsdb.NewDataPointReader(f)
}

func verifyBackupCount(t *testing.T, shardKsName string, expected int) []string {
	backups, err := listBackups(shardKsName)
	require.NoError(t, err)
	assert.Equalf(t, expected, len(backups), "invalid number of backups")
	return backups
}

func listBackups(shardKsName string) ([]string, error) {
	backups, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput(
		"GetBackups", shardKsName,
	)
	if err != nil {
		return nil, err
	}
	result := strings.Split(backups, "\n")
	var returnResult []string
	for _, str := range result {
		if str != "" {
			returnResult = append(returnResult, str)
		}
	}
	return returnResult, nil
}

func removeBackups(t *testing.T) {
	// Remove all the backups from the shard
	backups, err := listBackups(shardKsName)
	require.NoError(t, err)
	for _, backup := range backups {
		_, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput(
			"RemoveBackup", shardKsName, backup,
		)
		require.NoError(t, err)
	}
}

func initTablets(t *testing.T, startTablet bool, initShardPrimary bool) {
	// Initialize tablets
	for _, tablet := range []cluster.Vttablet{*primary, *replica1} {
		err := localCluster.InitTablet(&tablet, keyspaceName, shardName)
		require.NoError(t, err)

		if startTablet {
			err = tablet.VttabletProcess.Setup()
			require.NoError(t, err)
		}
	}

	if initShardPrimary {
		// choose primary and start replication
		err := localCluster.VtctldClientProcess.InitShardPrimary(keyspaceName, shardName, cell, primary.TabletUID)
		require.NoError(t, err)
	}
}

func restore(t *testing.T, tablet *cluster.Vttablet, tabletType string, waitForState string) {
	// Erase mysql/tablet dir, then start tablet with restore enabled.
	log.Infof("restoring tablet %s", time.Now())
	resetTabletDirectory(t, *tablet, true)

	// Start tablets
	tablet.VttabletProcess.ExtraArgs = []string{"--db-credentials-file", dbCredentialFile}
	tablet.VttabletProcess.TabletType = tabletType
	tablet.VttabletProcess.ServingStatus = waitForState
	tablet.VttabletProcess.SupportsBackup = true
	err := tablet.VttabletProcess.Setup()
	require.NoError(t, err)
}

func resetTabletDirectory(t *testing.T, tablet cluster.Vttablet, initMysql bool) {
	extraArgs := []string{"--db-credentials-file", dbCredentialFile}
	tablet.MysqlctlProcess.ExtraArgs = extraArgs

	// Teardown Tablet
	err := tablet.VttabletProcess.TearDown()
	require.NoError(t, err)

	// Shutdown Mysql
	err = tablet.MysqlctlProcess.Stop()
	require.NoError(t, err)

	// Clear out the previous data
	tablet.MysqlctlProcess.CleanupFiles(tablet.TabletUID)

	if initMysql {
		// Init the Mysql
		tablet.MysqlctlProcess.InitDBFile = newInitDBFile
		err = tablet.MysqlctlProcess.Start()
		require.NoError(t, err)
	}
}

func tearDown(t *testing.T, initMysql bool) {
	// reset replication
	for _, db := range []string{"_vt", "vt_insert_test"} {
		_, err := primary.VttabletProcess.QueryTablet("drop database if exists "+db, keyspaceName, true)
		require.NoError(t, err)
	}
	caughtUp := waitForReplicationToCatchup([]cluster.Vttablet{*replica1, *replica2})
	require.True(t, caughtUp, "Timed out waiting for all replicas to catch up")

	promoteCommands := []string{"STOP REPLICA", "RESET REPLICA ALL"}

	disableSemiSyncCommandsSource := []string{"SET GLOBAL rpl_semi_sync_source_enabled = false", " SET GLOBAL rpl_semi_sync_replica_enabled = false"}
	disableSemiSyncCommandsMaster := []string{"SET GLOBAL rpl_semi_sync_master_enabled = false", " SET GLOBAL rpl_semi_sync_slave_enabled = false"}

	for _, tablet := range []cluster.Vttablet{*primary, *replica1, *replica2} {
		resetCmd, err := tablet.VttabletProcess.ResetBinaryLogsCommand()
		require.NoError(t, err)
		cmds := append(promoteCommands, resetCmd)
		err = tablet.VttabletProcess.QueryTabletMultiple(cmds, keyspaceName, true)
		require.NoError(t, err)
		semisyncType, err := tablet.VttabletProcess.SemiSyncExtensionLoaded()
		require.NoError(t, err)

		switch semisyncType {
		case mysql.SemiSyncTypeSource:
			err = tablet.VttabletProcess.QueryTabletMultiple(disableSemiSyncCommandsSource, keyspaceName, true)
			require.NoError(t, err)
		case mysql.SemiSyncTypeMaster:
			err = tablet.VttabletProcess.QueryTabletMultiple(disableSemiSyncCommandsMaster, keyspaceName, true)
			require.NoError(t, err)
		}
	}

	for _, tablet := range []cluster.Vttablet{*primary, *replica1, *replica2} {
		resetTabletDirectory(t, tablet, initMysql)
		// DeleteTablet on a primary will cause tablet to shutdown, so should only call it after tablet is already shut down
		err := localCluster.VtctldClientProcess.ExecuteCommand("DeleteTablets", "--allow-primary", tablet.Alias)
		require.NoError(t, err)
	}
}

func verifyDisableEnableRedoLogs(ctx context.Context, t *testing.T, mysqlSocket string) {
	params := cluster.NewConnParams(0, dbPassword, mysqlSocket, keyspaceName)

	for {
		select {
		case <-time.After(100 * time.Millisecond):
			// Connect to vtbackup mysqld.
			conn, err := mysql.Connect(ctx, &params)
			if err != nil {
				// Keep trying, vtbackup mysqld may not be ready yet.
				continue
			}

			// Check if server supports disable/enable redo log.
			qr, err := conn.ExecuteFetch("SELECT 1 FROM performance_schema.global_status WHERE variable_name = 'innodb_redo_log_enabled'", 1, false)
			require.NoError(t, err)
			// If not, there's nothing to test.
			if len(qr.Rows) == 0 {
				return
			}

			// MY-013600
			// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_ib_wrn_redo_disabled
			qr, err = conn.ExecuteFetch("SELECT 1 FROM performance_schema.error_log WHERE error_code = 'MY-013600'", 1, false)
			require.NoError(t, err)
			if len(qr.Rows) != 1 {
				// Keep trying, possible we haven't disabled yet.
				continue
			}

			// MY-013601
			// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_ib_wrn_redo_enabled
			qr, err = conn.ExecuteFetch("SELECT 1 FROM performance_schema.error_log WHERE error_code = 'MY-013601'", 1, false)
			require.NoError(t, err)
			if len(qr.Rows) != 1 {
				// Keep trying, possible we haven't disabled yet.
				continue
			}

			// Success
			return
		case <-ctx.Done():
			require.Fail(t, "Failed to verify disable/enable redo log.")
		}
	}
}

// This helper function wait for all replicas to catch-up the replication.
// It does this by querying the status detail url of each replica and find the lag.
func waitForReplicationToCatchup(tablets []cluster.Vttablet) bool {
	endTime := time.Now().Add(time.Second * 30)
	timeout := time.After(time.Until(endTime))
	// key-value structure returned by status url.
	type kv struct {
		Key   string
		Class string
		Value string
	}
	// defining a struct instance
	var statuslst []kv
	for {
		select {
		case <-timeout:
			return false
		default:
			var replicaCount = 0
			for _, tablet := range tablets {
				status := tablet.VttabletProcess.GetStatusDetails()
				json.Unmarshal([]byte(status), &statuslst)
				for _, obj := range statuslst {
					if obj.Key == "Replication Lag" && obj.Value == "0s" {
						replicaCount++
					}
				}
				if replicaCount == len(tablets) {
					return true
				}
			}
			time.Sleep(time.Second * 1)
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
