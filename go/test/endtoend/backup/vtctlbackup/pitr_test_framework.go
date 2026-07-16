/*
Copyright 2023 The Vitess Authors.

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

package vtctlbackup

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	gracefulPostBackupDuration = 10 * time.Millisecond
	backupTimeoutDuration      = 3 * time.Minute
)

const (
	postWriteSleepDuration = 2 * time.Second // Nice for debugging purposes: clearly distinguishes the timestamps of certain operations, and as results the names/timestamps of backups.
)

const (
	operationFullBackup = iota
	operationIncrementalBackup
	operationRestore
	operationFlushAndPurge
)

// backupStorageRoot is where the shard's backups are stored in every container of the cluster.
const backupStorageRoot = "/vt/backups"

// mysqlShellBackupLocation is where the MySQL Shell backup engine writes its dumps.
const mysqlShellBackupLocation = backupStorageRoot + "/mysqlshell"

// backupReadCnfPath is the mysqld configuration snippet that lets the tablets' mysqld read the
// backup storage, so that the tests can read a backup's MANIFEST.
const backupReadCnfPath = "/vt/files/backup-read.cnf"

const backupReadCnf = `
[mysqld]
secure-file-priv = ` + backupStorageRoot + `
`

type incrementalFromPosType int

const (
	incrementalFromPosPosition incrementalFromPosType = iota
	incrementalFromPosAuto
	incrementalFromPosBackupName
)

type PITRTestCase struct {
	Name           string
	SetupType      int
	ComprssDetails *CompressionDetails
}

type testedBackupTimestampInfo struct {
	rows          int
	postTimestamp time.Time
}

var (
	tmClient = tmc.NewClient()

	vitessCluster   *vitesst.Cluster
	clusterCleanup  func(context.Context) error
	primaryTablet   *vitesst.Tablet
	replicaTablets  []*vitesst.Tablet
	tabletExtraArgs []string
)

// launchPITRCluster starts the cluster the tests run against: a single shard with a primary, a
// replica, an rdonly tablet and a spare tablet, file backup storage, and the backup engine
// requested by the test case.
func launchPITRCluster(t *testing.T, ctx context.Context, setupType int, streamMode string, stripes int, cDetails *CompressionDetails) (int, error) {
	currentSetupType = setupType

	tabletExtraArgs = getDefaultCommonArgs()

	switch setupType {
	case XtraBackup:
		xtrabackupArgs := []string{
			"--backup-engine-implementation", "xtrabackup",
			fmt.Sprintf("%s=%s", "--xtrabackup-stream-mode", streamMode),
			"--xtrabackup-user" + "=vt_dba",
			fmt.Sprintf("%s=%d", "--xtrabackup-stripes", stripes),
		}

		// if streamMode is xbstream, add some additional args to test other xtrabackup flags
		if streamMode == "xbstream" {
			xtrabackupArgs = append(xtrabackupArgs, "--xtrabackup-prepare-flags", "--use-memory=100M")
		}

		tabletExtraArgs = append(tabletExtraArgs, xtrabackupArgs...)
	case MySQLShell:
		tabletExtraArgs = append(
			tabletExtraArgs,
			"--backup-engine-implementation", "mysqlshell",
			"--mysql-shell-backup-location", mysqlShellBackupLocation,
			"--mysql-shell-speedup-restore=true",
		)
	}

	tabletExtraArgs = append(tabletExtraArgs, getCompressorArgs(cDetails)...)

	keyspace := vitesst.WithKeyspace(keyspaceName).
		WithShardNames(shardName).
		WithReplicas(2).
		WithRDOnly(1).
		WithDurabilityPolicy("semi_sync").
		WithTabletArgs(tabletExtraArgs...).
		WithTabletSpec(func(spec *vitesst.TabletSpec) {
			spec.ExtraArgs = append(spec.ExtraArgs, mysqlShellTabletArgs(setupType, spec.UID)...)
		})

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithBackupStorage(),
		vitesst.WithVTOrc(),
		vitesst.WithTabletFiles(vitesst.ContainerFile{
			Content:       []byte(backupReadCnf),
			ContainerPath: backupReadCnfPath,
		}),
		vitesst.WithTabletEnv(map[string]string{"EXTRA_MY_CNF": backupReadCnfPath}),
		keyspace,
	)
	if err != nil {
		return 1, err
	}

	cleanup, err := cluster.Start(t, ctx)
	clusterCleanup = cleanup
	if err != nil {
		return 1, err
	}
	vitessCluster = cluster

	shard := cluster.Keyspace(keyspaceName).Shard(shardName)
	primaryTablet = shard.Primary()
	replicas := shard.Replicas()
	rdonly := shard.RDOnly()
	if primaryTablet == nil || len(replicas) != 2 || len(rdonly) != 1 {
		return 1, vterrors.Errorf(vtrpc.Code_INTERNAL,
			"expected a primary, two replicas and one rdonly tablet in %s, found %d replicas and %d rdonly tablets",
			shard.Ref(), len(replicas), len(rdonly))
	}

	// The tests address the shard's tablets by index: the replica they back up from, the rdonly
	// tablet the two-tablet tests back up from as well, and the spare tablet they bootstrap from
	// the backups.
	spare := replicas[1]
	replicaTablets = []*vitesst.Tablet{replicas[0], rdonly[0], spare}

	// The spare tablet leaves the shard with an empty mysqld and no tablet record, so that a test
	// can later start its vttablet and have it bootstrap itself from the backups.
	if err := spare.StopVttablet(ctx); err != nil {
		return 1, err
	}
	if err := cluster.Vtctld().ExecuteCommand(ctx, "DeleteTablets", spare.Alias()); err != nil {
		return 1, err
	}
	if err := emptyTabletDatabase(ctx, spare); err != nil {
		return 1, err
	}

	return 0, nil
}

// mysqlShellTabletArgs returns the vttablet arguments a tablet needs to run MySQL Shell against its
// own mysqld: each tablet has its own socket.
func mysqlShellTabletArgs(setupType int, tabletUID int) []string {
	if setupType != MySQLShell {
		return nil
	}
	socket := fmt.Sprintf("/vt/vtdataroot/vt_%010d/mysql.sock", tabletUID)
	return []string{"--mysql-shell-flags", "--js -u vt_dba -S " + socket}
}

// tearDownPITRCluster shuts the cluster down, dumping component logs when the test failed.
func tearDownPITRCluster(t *testing.T) {
	ctx := context.WithoutCancel(t.Context())

	if clusterCleanup != nil {
		if err := clusterCleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	}

	vitessCluster = nil
	clusterCleanup = nil
	primaryTablet = nil
	replicaTablets = nil
}

func getReplica(t *testing.T, replicaIndex int) *vitesst.Tablet {
	if replicaIndex < 0 || replicaIndex >= len(replicaTablets) {
		assert.Failf(t, "invalid replica index", "index=%d", replicaIndex)
		return nil
	}
	return replicaTablets[replicaIndex]
}

// SetupReplica3Tablet starts the vttablet of the shard's spare tablet with the given extra
// arguments. Its mysqld is empty, so the tablet bootstraps itself from the backups.
func SetupReplica3Tablet(t *testing.T, extraArgs []string) (*vitesst.Tablet, error) {
	ctx := t.Context()
	replica := getReplica(t, 2)

	args := slices.Clone(tabletExtraArgs)
	args = append(args, mysqlShellTabletArgs(currentSetupType, replica.UID)...)
	args = append(args, extraArgs...)
	if err := replica.StartVttablet(ctx, args...); err != nil {
		return replica, err
	}
	return replica, nil
}

func InitTestTable(t *testing.T) {
	_, err := primaryTablet.QueryTablet(t.Context(), "DROP TABLE IF EXISTS vt_insert_test")
	require.NoError(t, err)
	_, err = primaryTablet.QueryTablet(t.Context(), vtInsertTest)
	require.NoError(t, err)
}

func InsertRowOnPrimary(t *testing.T, hint string) {
	if hint == "" {
		hint = textutil.RandomHash()[:12]
	}
	query, err := sqlparser.ParseAndBind("insert into vt_insert_test (msg) values (%a)", sqltypes.StringBindVariable(hint))
	require.NoError(t, err)
	_, err = primaryTablet.QueryTablet(t.Context(), query)
	require.NoError(t, err)
}

func ReadRowsFromTablet(t *testing.T, tablet *vitesst.Tablet) (msgs []string) {
	query := "select msg from vt_insert_test"
	rs, err := tablet.QueryTablet(t.Context(), query)
	require.NoError(t, err)
	for _, row := range rs.Named().Rows {
		msg, err := row.ToString("msg")
		require.NoError(t, err)
		msgs = append(msgs, msg)
	}
	return msgs
}

func ReadRowsFromPrimary(t *testing.T) (msgs []string) {
	return ReadRowsFromTablet(t, primaryTablet)
}

func ReadRowsFromReplica(t *testing.T, replicaIndex int) (msgs []string) {
	return ReadRowsFromTablet(t, getReplica(t, replicaIndex))
}

func GetReplicaPosition(t *testing.T, replicaIndex int) string {
	replica := getReplica(t, replicaIndex)
	tablet, err := replica.TabletProto(t.Context())
	require.NoError(t, err)
	pos, err := tmClient.PrimaryPosition(t.Context(), tablet)
	require.NoError(t, err)
	return pos
}

func GetReplicaGtidPurged(t *testing.T, replicaIndex int) string {
	replica := getReplica(t, replicaIndex)
	query := "select @@global.gtid_purged as gtid_purged"
	rs, err := replica.QueryTablet(t.Context(), query)
	require.NoError(t, err)
	row := rs.Named().Row()
	require.NotNil(t, row)
	return row.AsString("gtid_purged", "")
}

// primaryMySQLPort returns the port the primary's mysqld listens on inside the cluster.
func primaryMySQLPort(t *testing.T) int64 {
	rs, err := primaryTablet.QueryTabletWithDB(t.Context(), "select @@port as port", "")
	require.NoError(t, err)
	row := rs.Named().Row()
	require.NotNil(t, row)
	return row.AsInt64("port", 0)
}

func ReconnectReplicaToPrimary(t *testing.T, replicaIndex int) {
	query := fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='vt_repl', GET_SOURCE_PUBLIC_KEY = 1, SOURCE_AUTO_POSITION = 1",
		primaryTablet.Name(), primaryMySQLPort(t))
	replica := getReplica(t, replicaIndex)
	_, err := replica.QueryTablet(t.Context(), "stop replica")
	require.NoError(t, err)
	_, err = replica.QueryTablet(t.Context(), query)
	require.NoError(t, err)
	_, err = replica.QueryTablet(t.Context(), "start replica")
	require.NoError(t, err)
}

// FlushBinaryLogsOnReplica issues `FLUSH BINARY LOGS` <count> times
func FlushBinaryLogsOnReplica(t *testing.T, replicaIndex int, count int) {
	replica := getReplica(t, replicaIndex)
	query := "flush binary logs"
	for range count {
		_, err := replica.QueryTablet(t.Context(), query)
		require.NoError(t, err)
	}
}

// FlushAndPurgeBinaryLogsOnReplica intentionally loses all existing binary logs. It flushes into a new binary log
// and immediately purges all previous logs.
// This is used to lose information.
func FlushAndPurgeBinaryLogsOnReplica(t *testing.T, replicaIndex int) (lastBinlog string) {
	FlushBinaryLogsOnReplica(t, replicaIndex, 1)

	replica := getReplica(t, replicaIndex)
	{
		query := "show binary logs"
		rs, err := replica.QueryTablet(t.Context(), query)
		require.NoError(t, err)
		require.NotEmpty(t, rs.Rows)
		for _, row := range rs.Rows {
			// binlog file name is first column
			lastBinlog = row[0].ToString()
		}
	}
	{
		query, err := sqlparser.ParseAndBind("purge binary logs to %a", sqltypes.StringBindVariable(lastBinlog))
		require.NoError(t, err)
		_, err = replica.QueryTablet(t.Context(), query)
		require.NoError(t, err)
	}
	return lastBinlog
}

// readBackupManifest reads a backup's MANIFEST from the shard's backup storage.
func readBackupManifest(t *testing.T, backupName string) (manifest *mysqlctl.BackupManifest) {
	fullPath := fmt.Sprintf("%s/%s/%s/%s/MANIFEST", backupStorageRoot, keyspaceName, shardName, backupName)

	// reading manifest
	query, err := sqlparser.ParseAndBind("select load_file(%a) as manifest", sqltypes.StringBindVariable(fullPath))
	require.NoError(t, err)
	rs, err := primaryTablet.QueryTabletWithDB(t.Context(), query, "")
	require.NoErrorf(t, err, "error while reading MANIFEST %v", fullPath)
	row := rs.Named().Row()
	require.NotNil(t, row)
	data := row.AsBytes("manifest", nil)
	require.NotEmptyf(t, data, "empty MANIFEST %v", fullPath)

	// parsing manifest
	err = json.Unmarshal(data, &manifest)
	require.NoErrorf(t, err, "error while parsing MANIFEST %v", err)
	require.NotNil(t, manifest)
	return manifest
}

// waitForNumBackups waits for GetBackups to list exactly the given expected number.
// If expectNumBackups < 0 then any response is considered valid
func waitForNumBackups(t *testing.T, expectNumBackups int) []string {
	ctx, cancel := context.WithTimeout(t.Context(), topoConsistencyTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		backups, err := vitessCluster.ListBackups(ctx, keyspaceName, shardName)
		require.NoError(t, err)
		if expectNumBackups < 0 {
			// any result is valid
			return backups
		}
		if len(backups) == expectNumBackups {
			// what we waited for
			return backups
		}
		assert.Less(t, len(backups), expectNumBackups)
		select {
		case <-ctx.Done():
			assert.Failf(t, ctx.Err().Error(), "expected %d backups, got %d", expectNumBackups, len(backups))
			return nil
		case <-ticker.C:
		}
	}
}

func removeBackup(t *testing.T, backupName string) {
	err := vitessCluster.Vtctld().ExecuteCommand(t.Context(), "RemoveBackup", shardKsName, backupName)
	require.NoError(t, err)
}

// waitForTabletType waits for a tablet's topology record to carry the expected type.
func waitForTabletType(t *testing.T, tablet *vitesst.Tablet, tabletType topodata.TabletType) {
	t.Helper()
	// for loop for 15 seconds to check if tablet type is correct
	for range 15 {
		output, err := vitessCluster.Vtctld().ExecuteCommandWithOutput(t.Context(), "GetTablet", tablet.Alias())
		if !assert.NoError(t, err) {
			return
		}
		var tabletPB topodata.Tablet
		err = json2.UnmarshalPB([]byte(output), &tabletPB)
		if !assert.NoError(t, err) {
			return
		}
		if tabletType == tabletPB.Type {
			return
		}
		time.Sleep(1 * time.Second)
	}
	assert.Failf(t, "waitForTabletType failed.", "Tablet type is not correct. Expected: %v", tabletType)
}

// getTabletVars returns a tablet's /debug/vars.
func getTabletVars(t *testing.T, tablet *vitesst.Tablet) map[string]any {
	vars, err := tablet.GetVars(t.Context())
	require.NoError(t, err)
	return vars
}

func vtctlBackupReplicaNoDestroyNoWrites(t *testing.T, replicaIndex int) (backups []string) {
	replica := getReplica(t, replicaIndex)
	numBackups := len(waitForNumBackups(t, -1))

	err := vitessCluster.Vtctld().ExecuteCommand(t.Context(), "Backup", replica.Alias())
	require.NoError(t, err)

	backups = waitForNumBackups(t, numBackups+1)
	require.NotEmpty(t, backups)

	verifyTabletBackupStats(t, getTabletVars(t, replica))

	return backups
}

func TestReplicaFullBackup(t *testing.T, replicaIndex int) (manifest *mysqlctl.BackupManifest) {
	backups := vtctlBackupReplicaNoDestroyNoWrites(t, replicaIndex)

	return readBackupManifest(t, backups[len(backups)-1])
}

func testReplicaIncrementalBackup(t *testing.T, replica *vitesst.Tablet, incrementalFromPos string, expectEmpty bool, expectError string) (manifest *mysqlctl.BackupManifest, backupName string) {
	numBackups := len(waitForNumBackups(t, -1))

	output, err := vitessCluster.Vtctld().ExecuteCommandWithOutput(t.Context(), "Backup", "--incremental-from-pos", incrementalFromPos, replica.Alias())
	if expectError != "" {
		require.Errorf(t, err, "expected: %v", expectError)
		require.Contains(t, output, expectError)
		return nil, ""
	}
	require.NoErrorf(t, err, "output: %v", output)

	if expectEmpty {
		require.Contains(t, output, mysqlctl.EmptyBackupMessage)
		return nil, ""
	}

	backups := waitForNumBackups(t, numBackups+1)
	require.NotEmptyf(t, backups, "output: %v", output)

	verifyTabletBackupStats(t, getTabletVars(t, replica))
	backupName = backups[len(backups)-1]

	return readBackupManifest(t, backupName), backupName
}

func TestReplicaIncrementalBackup(t *testing.T, replicaIndex int, incrementalFromPos string, expectEmpty bool, expectError string) (manifest *mysqlctl.BackupManifest, backupName string) {
	replica := getReplica(t, replicaIndex)
	return testReplicaIncrementalBackup(t, replica, incrementalFromPos, expectEmpty, expectError)
}

func TestReplicaFullRestore(t *testing.T, replicaIndex int, expectError string) {
	replica := getReplica(t, replicaIndex)

	output, err := vitessCluster.Vtctld().ExecuteCommandWithOutput(t.Context(), "RestoreFromBackup", replica.Alias())
	if expectError != "" {
		require.Errorf(t, err, "expected: %v", expectError)
		require.Contains(t, output, expectError)
		return
	}
	require.NoErrorf(t, err, "output: %v", output)
	verifyTabletRestoreStats(t, getTabletVars(t, replica))
}

func TestReplicaRestoreToPos(t *testing.T, replicaIndex int, restoreToPos replication.Position, expectError string) {
	replica := getReplica(t, replicaIndex)

	require.False(t, restoreToPos.IsZero())
	restoreToPosArg := replication.EncodePosition(restoreToPos)
	assert.Contains(t, restoreToPosArg, "MySQL56/")
	if rand.IntN(2) == 0 {
		// Verify that restore works whether or not the MySQL56/ prefix is present.
		restoreToPosArg = strings.Replace(restoreToPosArg, "MySQL56/", "", 1)
		assert.NotContains(t, restoreToPosArg, "MySQL56/")
	}

	output, err := vitessCluster.Vtctld().ExecuteCommandWithOutput(t.Context(), "RestoreFromBackup", "--restore-to-pos", restoreToPosArg, replica.Alias())
	if expectError != "" {
		require.Errorf(t, err, "expected: %v", expectError)
		require.Contains(t, output, expectError)
		return
	}
	require.NoErrorf(t, err, "output: %v", output)
	verifyTabletRestoreStats(t, getTabletVars(t, replica))
	waitForTabletType(t, getReplica(t, 0), topodata.TabletType_DRAINED)
}

func TestReplicaRestoreToTimestamp(t *testing.T, restoreToTimestamp time.Time, expectError string) {
	replica := getReplica(t, 0)

	require.False(t, restoreToTimestamp.IsZero())
	restoreToTimestampArg := mysqlctl.FormatRFC3339(restoreToTimestamp)
	output, err := vitessCluster.Vtctld().ExecuteCommandWithOutput(t.Context(), "RestoreFromBackup", "--restore-to-timestamp", restoreToTimestampArg, replica.Alias())
	if expectError != "" {
		require.Errorf(t, err, "expected: %v", expectError)
		require.Contains(t, output, expectError)
		return
	}
	require.NoErrorf(t, err, "output: %v", output)
	verifyTabletRestoreStats(t, getTabletVars(t, replica))
	waitForTabletType(t, replica, topodata.TabletType_DRAINED)
}

// waitForReplica waits for the replica to have same row set as on primary.
func waitForReplica(t *testing.T, replicaIndex int) int {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	pMsgs := ReadRowsFromPrimary(t)
	for {
		rMsgs := ReadRowsFromReplica(t, replicaIndex)
		if len(pMsgs) == len(rMsgs) {
			// success
			return len(pMsgs)
		}
		select {
		case <-ctx.Done():
			assert.FailNow(t, "timeout waiting for replica to catch up")
			return 0
		case <-ticker.C:
			//
		}
	}
}

// ExecTestIncrementalBackupAndRestoreToPos runs a series of backups: a full backup and multiple incremental backups.
// in between, it makes writes to the database, and takes notes: what data was available in what backup.
// It then restores each and every one of those backups, in random order, and expects to find the specific data associated with the backup.
func ExecTestIncrementalBackupAndRestoreToPos(t *testing.T, tcase *PITRTestCase) {
	t.Run(tcase.Name, func(t *testing.T) {
		// setup cluster for the testing
		code, err := launchPITRCluster(t, t.Context(), tcase.SetupType, "xbstream", 0, tcase.ComprssDetails)
		require.NoError(t, err, "setup failed with status code %d", code)
		defer tearDownPITRCluster(t)

		InitTestTable(t)

		rowsPerPosition := map[string]int{}
		backupPositions := []string{}

		recordRowsPerPosition := func(t *testing.T) {
			pos := GetReplicaPosition(t, 0)
			msgs := ReadRowsFromReplica(t, 0)
			if _, ok := rowsPerPosition[pos]; !ok {
				backupPositions = append(backupPositions, pos)
				rowsPerPosition[pos] = len(msgs)
			}
		}

		var fullBackupPos replication.Position
		var lastBackupName string
		t.Run("full backup", func(t *testing.T) {
			InsertRowOnPrimary(t, "before-full-backup")
			waitForReplica(t, 0)

			manifest := TestReplicaFullBackup(t, 0)
			fullBackupPos = manifest.Position
			require.False(t, fullBackupPos.IsZero())
			//
			msgs := ReadRowsFromReplica(t, 0)
			pos := replication.EncodePosition(fullBackupPos)
			backupPositions = append(backupPositions, pos)
			rowsPerPosition[pos] = len(msgs)

			lastBackupName = manifest.BackupName
		})

		lastBackupPos := fullBackupPos
		InsertRowOnPrimary(t, "before-incremental-backups")

		tt := []struct {
			name              string
			writeBeforeBackup bool
			fromFullPosition  bool
			expectEmpty       bool
			incrementalFrom   incrementalFromPosType
			expectError       string
		}{
			{
				name:            "first incremental backup",
				incrementalFrom: incrementalFromPosPosition,
			},
			{
				name:            "empty1",
				incrementalFrom: incrementalFromPosPosition,
				expectEmpty:     true,
			},
			{
				name:            "empty2",
				incrementalFrom: incrementalFromPosAuto,
				expectEmpty:     true,
			},
			{
				name:            "empty3",
				incrementalFrom: incrementalFromPosPosition,
				expectEmpty:     true,
			},
			{
				name:              "make writes, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosPosition,
			},
			{
				name:            "empty again",
				incrementalFrom: incrementalFromPosPosition,
				expectEmpty:     true,
			},
			{
				name:              "make writes again, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosBackupName,
			},
			{
				name:              "auto position, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosAuto,
			},
			{
				name:            "empty again, based on auto position",
				incrementalFrom: incrementalFromPosAuto,
				expectEmpty:     true,
			},
			{
				name:              "auto position, make writes again, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosAuto,
			},
			{
				name:             "from full backup position",
				fromFullPosition: true,
				incrementalFrom:  incrementalFromPosPosition,
			},
		}
		var fromFullPositionBackups []string
		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				if tc.writeBeforeBackup {
					InsertRowOnPrimary(t, "")
				}
				// we wait for >1 second because backups are written to a directory named after the current timestamp,
				// in 1 second resolution. We want to avoid two backups that have the same pathname. Realistically this
				// is only ever a problem in this end-to-end test, not in production.
				// Also, we give the replica a chance to catch up.
				time.Sleep(postWriteSleepDuration)
				// randomly flush binary logs 0, 1 or 2 times
				FlushBinaryLogsOnReplica(t, 0, rand.IntN(3))
				waitForReplica(t, 0)
				recordRowsPerPosition(t)
				// configure --incremental-from-pos to either:
				// - auto
				// - explicit last backup pos
				// - back in history to the original full backup
				var incrementalFromPos string
				switch tc.incrementalFrom {
				case incrementalFromPosAuto:
					incrementalFromPos = mysqlctl.AutoIncrementalFromPos
				case incrementalFromPosBackupName:
					incrementalFromPos = lastBackupName
				case incrementalFromPosPosition:
					incrementalFromPos = replication.EncodePosition(lastBackupPos)
					if tc.fromFullPosition {
						incrementalFromPos = replication.EncodePosition(fullBackupPos)
					}
					assert.Contains(t, incrementalFromPos, "MySQL56/")
				}
				incrementalFromPosArg := incrementalFromPos
				if tc.incrementalFrom == incrementalFromPosPosition && tc.fromFullPosition {
					// Verify that backup works whether or not the MySQL56/ prefix is present.
					// We arbitrarily decide to strip the prefix when "tc.fromFullPosition" is true, and keep it when false.
					incrementalFromPosArg = strings.Replace(incrementalFromPosArg, "MySQL56/", "", 1)
					assert.NotContains(t, incrementalFromPosArg, "MySQL56/")
				}
				// always use same 1st replica
				manifest, backupName := TestReplicaIncrementalBackup(t, 0, incrementalFromPosArg, tc.expectEmpty, tc.expectError)
				if tc.expectError != "" {
					return
				}
				if tc.expectEmpty {
					assert.Nil(t, manifest)
					return
				}
				require.NotNil(t, manifest)
				defer func() {
					lastBackupPos = manifest.Position
					lastBackupName = manifest.BackupName
				}()
				if tc.fromFullPosition {
					fromFullPositionBackups = append(fromFullPositionBackups, backupName)
				}
				require.False(t, manifest.FromPosition.IsZero())
				require.NotEqual(t, manifest.Position, manifest.FromPosition)
				require.True(t, manifest.Position.GTIDSet.Union(manifest.PurgedPosition.GTIDSet).Contains(manifest.FromPosition.GTIDSet))

				gtidPurgedPos, err := replication.ParsePosition(replication.Mysql56FlavorID, GetReplicaGtidPurged(t, 0))
				require.NoError(t, err)
				fromPositionIncludingPurged := manifest.FromPosition.GTIDSet.Union(gtidPurgedPos.GTIDSet)

				expectFromPosition := lastBackupPos.GTIDSet
				if tc.incrementalFrom == incrementalFromPosPosition {
					pos, err := replication.DecodePosition(incrementalFromPos)
					assert.NoError(t, err)
					expectFromPosition = pos.GTIDSet.Union(gtidPurgedPos.GTIDSet)
				}
				require.Equalf(t, expectFromPosition, fromPositionIncludingPurged, "expected: %v, found: %v, gtid_purged: %v,  manifest.Position: %v", expectFromPosition, fromPositionIncludingPurged, gtidPurgedPos, manifest.Position)
			})
		}

		sampleTestedBackupPos := ""
		testRestores := func(t *testing.T) {
			for _, r := range rand.Perm(len(backupPositions)) {
				pos := backupPositions[r]
				testName := fmt.Sprintf("%s, %d records", pos, rowsPerPosition[pos])
				t.Run(testName, func(t *testing.T) {
					restoreToPos, err := replication.DecodePosition(pos)
					require.NoError(t, err)
					require.False(t, restoreToPos.IsZero())
					TestReplicaRestoreToPos(t, 0, restoreToPos, "")
					msgs := ReadRowsFromReplica(t, 0)
					count, ok := rowsPerPosition[pos]
					require.True(t, ok)
					assert.Equalf(t, count, len(msgs), "messages: %v", msgs)
					if sampleTestedBackupPos == "" {
						sampleTestedBackupPos = pos
					}
					t.Run("post-pitr, wait for replica to catch up", func(t *testing.T) {
						// Replica is DRAINED and does not have replication configuration.
						// We now connect the replica to the primary and validate it's able to catch up.
						ReconnectReplicaToPrimary(t, 0)
						waitForReplica(t, 0)
					})
				})
			}
		}
		t.Run("PITR", func(t *testing.T) {
			testRestores(t)
		})
		t.Run("remove full position backups", func(t *testing.T) {
			// Delete the fromFullPosition backup(s), which leaves us with less restore options. Try again.
			for _, backupName := range fromFullPositionBackups {
				removeBackup(t, backupName)
			}
		})
		t.Run("PITR-2", func(t *testing.T) {
			testRestores(t)
		})
		// Test that we can create a new tablet with --restore-from-backup --restore-to-pos and that it bootstraps
		// via PITR and ends up in DRAINED type.
		t.Run("init tablet PITR", func(t *testing.T) {
			require.NotEmpty(t, sampleTestedBackupPos)

			var tablet *vitesst.Tablet

			t.Run("init from backup pos "+sampleTestedBackupPos, func(t *testing.T) {
				tablet, err = SetupReplica3Tablet(t, []string{"--restore-to-pos", sampleTestedBackupPos})
				assert.NoError(t, err)
			})
			t.Run("wait for drained", func(t *testing.T) {
				err = tablet.WaitForTabletType(t.Context(), backupTimeoutDuration, "drained")
				assert.NoError(t, err)
			})
			t.Run(fmt.Sprintf("validate %d rows", rowsPerPosition[sampleTestedBackupPos]), func(t *testing.T) {
				require.NotZero(t, rowsPerPosition[sampleTestedBackupPos])
				msgs := ReadRowsFromReplica(t, 2)
				assert.Equal(t, rowsPerPosition[sampleTestedBackupPos], len(msgs))
			})
		})
	})
}

// ExecTestIncrementalBackupAndRestoreToPos
func ExecTestIncrementalBackupAndRestoreToTimestamp(t *testing.T, tcase *PITRTestCase) {
	var lastInsertedRowTimestamp time.Time
	insertRowOnPrimary := func(t *testing.T, hint string) {
		InsertRowOnPrimary(t, hint)
		lastInsertedRowTimestamp = time.Now()
	}

	t.Run(tcase.Name, func(t *testing.T) {
		// setup cluster for the testing
		code, err := launchPITRCluster(t, t.Context(), tcase.SetupType, "xbstream", 0, &CompressionDetails{
			CompressorEngineName: "pgzip",
		})
		require.NoError(t, err, "setup failed with status code %d", code)
		defer tearDownPITRCluster(t)

		InitTestTable(t)

		testedBackups := []testedBackupTimestampInfo{}

		var fullBackupPos replication.Position
		var lastBackupName string
		t.Run("full backup", func(t *testing.T) {
			insertRowOnPrimary(t, "before-full-backup")
			waitForReplica(t, 0)

			manifest := TestReplicaFullBackup(t, 0)
			fullBackupPos = manifest.Position
			require.False(t, fullBackupPos.IsZero())
			//
			rows := ReadRowsFromReplica(t, 0)
			testedBackups = append(testedBackups, testedBackupTimestampInfo{len(rows), time.Now()})

			lastBackupName = manifest.BackupName
		})

		lastBackupPos := fullBackupPos
		insertRowOnPrimary(t, "before-incremental-backups")

		tt := []struct {
			name              string
			writeBeforeBackup bool
			fromFullPosition  bool
			expectEmpty       bool
			incrementalFrom   incrementalFromPosType
			expectError       string
		}{
			{
				name:            "first incremental backup",
				incrementalFrom: incrementalFromPosPosition,
			},
			{
				name:            "empty1",
				incrementalFrom: incrementalFromPosPosition,
				expectEmpty:     true,
			},
			{
				name:            "empty2",
				incrementalFrom: incrementalFromPosAuto,
				expectEmpty:     true,
			},
			{
				name:            "empty3",
				incrementalFrom: incrementalFromPosPosition,
				expectEmpty:     true,
			},
			{
				name:              "make writes, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosPosition,
			},
			{
				name:            "empty again",
				incrementalFrom: incrementalFromPosPosition,
				expectEmpty:     true,
			},
			{
				name:              "make writes again, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosBackupName,
			},
			{
				name:              "auto position, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosAuto,
			},
			{
				name:            "empty again, based on auto position",
				incrementalFrom: incrementalFromPosAuto,
				expectEmpty:     true,
			},
			{
				name:              "auto position, make writes again, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosAuto,
			},
			{
				name:             "from full backup position",
				fromFullPosition: true,
				incrementalFrom:  incrementalFromPosPosition,
			},
		}
		var fromFullPositionBackups []string
		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				if tc.writeBeforeBackup {
					insertRowOnPrimary(t, "")
				}
				// we wait for >1 second because backups are written to a directory named after the current timestamp,
				// in 1 second resolution. We want to avoid two backups that have the same pathname. Realistically this
				// is only ever a problem in this end-to-end test, not in production.
				// Also, we give the replica a chance to catch up.
				time.Sleep(postWriteSleepDuration)
				waitForReplica(t, 0)
				rowsBeforeBackup := ReadRowsFromReplica(t, 0)
				// configure --incremental-from-pos to either:
				// - auto
				// - explicit last backup pos
				// - back in history to the original full backup
				var incrementalFromPos string
				switch tc.incrementalFrom {
				case incrementalFromPosAuto:
					incrementalFromPos = mysqlctl.AutoIncrementalFromPos
				case incrementalFromPosBackupName:
					incrementalFromPos = lastBackupName
				case incrementalFromPosPosition:
					incrementalFromPos = replication.EncodePosition(lastBackupPos)
					if tc.fromFullPosition {
						incrementalFromPos = replication.EncodePosition(fullBackupPos)
					}
				}
				manifest, backupName := TestReplicaIncrementalBackup(t, 0, incrementalFromPos, tc.expectEmpty, tc.expectError)
				if tc.expectError != "" {
					return
				}
				if tc.expectEmpty {
					assert.Nil(t, manifest)
					return
				}
				require.NotNil(t, manifest)
				// We wish to mark the current post-backup timestamp. We will later on restore to this point in time.
				// However, the restore is up to and _exclusive_ of the timestamp. So for test's sake, we sleep
				// an extra few milliseconds just to ensure the timestamp we read is strictly after the backup time.
				// This is basicaly to avoid weird flakiness in CI.
				time.Sleep(gracefulPostBackupDuration)
				testedBackups = append(testedBackups, testedBackupTimestampInfo{len(rowsBeforeBackup), time.Now()})
				defer func() {
					lastBackupPos = manifest.Position
					lastBackupName = manifest.BackupName
				}()
				if tc.fromFullPosition {
					fromFullPositionBackups = append(fromFullPositionBackups, backupName)
				}
				require.False(t, manifest.FromPosition.IsZero())
				require.NotEqual(t, manifest.Position, manifest.FromPosition)
				require.True(t, manifest.Position.GTIDSet.Union(manifest.PurgedPosition.GTIDSet).Contains(manifest.FromPosition.GTIDSet))
				{
					incrDetails := manifest.IncrementalDetails
					require.NotNil(t, incrDetails)
					require.NotEmpty(t, incrDetails.FirstTimestamp)
					require.NotEmpty(t, incrDetails.FirstTimestampBinlog)
					require.NotEmpty(t, incrDetails.LastTimestamp)
					require.NotEmpty(t, incrDetails.LastTimestampBinlog)
					require.GreaterOrEqual(t, incrDetails.LastTimestamp, incrDetails.FirstTimestamp)

					if tc.fromFullPosition {
						require.Greater(t, incrDetails.LastTimestampBinlog, incrDetails.FirstTimestampBinlog)
					} else {
						// No binlog rotation
						require.Equal(t, incrDetails.LastTimestampBinlog, incrDetails.FirstTimestampBinlog)
					}
				}

				gtidPurgedPos, err := replication.ParsePosition(replication.Mysql56FlavorID, GetReplicaGtidPurged(t, 0))
				require.NoError(t, err)
				fromPositionIncludingPurged := manifest.FromPosition.GTIDSet.Union(gtidPurgedPos.GTIDSet)

				expectFromPosition := lastBackupPos.GTIDSet.Union(gtidPurgedPos.GTIDSet)
				if tc.incrementalFrom == incrementalFromPosPosition {
					pos, err := replication.DecodePosition(incrementalFromPos)
					assert.NoError(t, err)
					expectFromPosition = pos.GTIDSet.Union(gtidPurgedPos.GTIDSet)
				}
				require.Equalf(t, expectFromPosition, fromPositionIncludingPurged, "expected: %v, found: %v, gtid_purged: %v,  manifest.Position: %v", expectFromPosition, fromPositionIncludingPurged, gtidPurgedPos, manifest.Position)
			})
		}

		sampleTestedBackupIndex := -1
		testRestores := func(t *testing.T) {
			numFailedRestores := 0
			numSuccessfulRestores := 0
			for _, backupIndex := range rand.Perm(len(testedBackups)) {
				testedBackup := testedBackups[backupIndex]
				testName := fmt.Sprintf("backup num%v at %v, %v rows", backupIndex, mysqlctl.FormatRFC3339(testedBackup.postTimestamp), testedBackup.rows)
				t.Run(testName, func(t *testing.T) {
					expectError := ""
					if testedBackup.postTimestamp.After(lastInsertedRowTimestamp) {
						// The restore_to_timestamp value is beyond the last incremental
						// There is no path to restore to this timestamp.
						expectError = "no path found"
					}
					TestReplicaRestoreToTimestamp(t, testedBackup.postTimestamp, expectError)
					if expectError == "" {
						msgs := ReadRowsFromReplica(t, 0)
						assert.Equalf(t, testedBackup.rows, len(msgs), "messages: %v", msgs)
						numSuccessfulRestores++
						if sampleTestedBackupIndex < 0 {
							sampleTestedBackupIndex = backupIndex
						}
						t.Run("post-pitr, wait for replica to catch up", func(t *testing.T) {
							// Replica is DRAINED and does not have replication configuration.
							// We now connect the replica to the primary and validate it's able to catch up.
							ReconnectReplicaToPrimary(t, 0)
							waitForReplica(t, 0)
						})
					} else {
						numFailedRestores++
					}
				})
			}
			// Integrity check for the test itself: ensure we have both successful and failed restores.
			require.NotZero(t, numFailedRestores)
			require.NotZero(t, numSuccessfulRestores)
		}
		t.Run("PITR", func(t *testing.T) {
			testRestores(t)
		})
		t.Run("remove full position backups", func(t *testing.T) {
			// Delete the fromFullPosition backup(s), which leaves us with less restore options. Try again.
			for _, backupName := range fromFullPositionBackups {
				removeBackup(t, backupName)
			}
		})
		t.Run("PITR-2", func(t *testing.T) {
			testRestores(t)
		})
		// Test that we can create a new tablet with --restore-from-backup --restore-to-timestamp and that it bootstraps
		// via PITR and ends up in DRAINED type.
		t.Run("init tablet PITR", func(t *testing.T) {
			require.GreaterOrEqual(t, sampleTestedBackupIndex, 0)
			sampleTestedBackup := testedBackups[sampleTestedBackupIndex]
			restoreToTimestampArg := mysqlctl.FormatRFC3339(sampleTestedBackup.postTimestamp)

			var tablet *vitesst.Tablet

			t.Run(fmt.Sprintf("init from backup num %d", sampleTestedBackupIndex), func(t *testing.T) {
				tablet, err = SetupReplica3Tablet(t, []string{"--restore-to-timestamp", restoreToTimestampArg})
				assert.NoError(t, err)
			})
			t.Run("wait for drained", func(t *testing.T) {
				err = tablet.WaitForTabletType(t.Context(), backupTimeoutDuration, "drained")
				assert.NoError(t, err)
			})
			t.Run(fmt.Sprintf("validate %d rows", sampleTestedBackup.rows), func(t *testing.T) {
				require.NotZero(t, sampleTestedBackup.rows)
				msgs := ReadRowsFromReplica(t, 2)
				assert.Equal(t, sampleTestedBackup.rows, len(msgs))
			})
		})
	})
}

// ExecTestIncrementalBackupOnTwoTablets runs a series of interleaved backups on two different replicas: full and incremental.
// Specifically, it's designed to test how incremental backups are taken by interleaved replicas, so that they successfully build on
// one another.
func ExecTestIncrementalBackupOnTwoTablets(t *testing.T, tcase *PITRTestCase) {
	t.Run(tcase.Name, func(t *testing.T) {
		// setup cluster for the testing
		code, err := launchPITRCluster(t, t.Context(), tcase.SetupType, "xbstream", 0, tcase.ComprssDetails)
		require.NoError(t, err, "setup failed with status code %d", code)
		defer tearDownPITRCluster(t)

		InitTestTable(t)

		rowsPerPosition := map[string]int{}

		recordRowsPerPosition := func(t *testing.T, replicaIndex int) {
			pos := GetReplicaPosition(t, replicaIndex)
			msgs := ReadRowsFromReplica(t, replicaIndex)
			if _, ok := rowsPerPosition[pos]; !ok {
				rowsPerPosition[pos] = len(msgs)
			}
		}

		var lastBackupPos replication.Position
		InsertRowOnPrimary(t, "before-incremental-backups")
		waitForReplica(t, 0)
		waitForReplica(t, 1)

		tt := []struct {
			name          string
			operationType int
			replicaIndex  int
			expectError   string
		}{
			// The following tests run sequentially and build on top of previous results
			{
				name:          "full1",
				operationType: operationFullBackup,
			},
			{
				name:          "incremental1",
				operationType: operationIncrementalBackup,
			},
			{
				name:          "restore1",
				operationType: operationRestore,
			},
			{
				// Shows you can take an incremental restore when full & incremental backups were only ever executed on a different replica
				name:          "incremental2",
				operationType: operationIncrementalBackup,
				replicaIndex:  1,
			},
			{
				name:          "full2",
				operationType: operationFullBackup,
				replicaIndex:  1,
			},
			{
				// This incremental backup will use full2 as the base backup
				name:          "incremental2-after-full2",
				operationType: operationIncrementalBackup,
				replicaIndex:  1,
			},
			{
				name:          "restore2",
				operationType: operationRestore,
				replicaIndex:  1,
			},
			// Begin a series of interleaved incremental backups
			{
				name:          "incremental-replica1",
				operationType: operationIncrementalBackup,
			},
			{
				name:          "incremental-replica2",
				operationType: operationIncrementalBackup,
				replicaIndex:  1,
			},
			{
				name:          "incremental-replica1",
				operationType: operationIncrementalBackup,
			},
			{
				name:          "incremental-replica2",
				operationType: operationIncrementalBackup,
				replicaIndex:  1,
			},
			// Done interleaved backups.
			{
				// Lose binary log data
				name:          "flush and purge 1",
				operationType: operationFlushAndPurge,
				replicaIndex:  0,
			},
			{
				// Fail to run incremental backup due to lost data
				name:          "incremental-replica1 failure",
				operationType: operationIncrementalBackup,
				expectError:   "Required entries have been purged",
			},
			{
				// Lose binary log data
				name:          "flush and purge 2",
				operationType: operationFlushAndPurge,
				replicaIndex:  1,
			},
			{
				// Fail to run incremental backup due to lost data
				name:          "incremental-replica2 failure",
				operationType: operationIncrementalBackup,
				replicaIndex:  1,
				expectError:   "Required entries have been purged",
			},
			{
				// Since we've lost binlog data, incremental backups are no longer possible. The situation can be salvaged by running a full backup
				name:          "full1 after purge",
				operationType: operationFullBackup,
			},
			{
				// Show that replica2 incremental backup is able to work based on the above full backup
				name:          "incremental-replica2 after purge and backup",
				operationType: operationIncrementalBackup,
				replicaIndex:  1,
			},
		}
		insertRowAndWait := func(t *testing.T, replicaIndex int, data string) {
			t.Run("insert row and wait", func(t *testing.T) {
				InsertRowOnPrimary(t, data)
				time.Sleep(postWriteSleepDuration)
				waitForReplica(t, replicaIndex)
				recordRowsPerPosition(t, replicaIndex)
			})
		}
		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				insertRowAndWait(t, tc.replicaIndex, tc.name)
				t.Run("running operation", func(t *testing.T) {
					switch tc.operationType {
					case operationFlushAndPurge:
						FlushAndPurgeBinaryLogsOnReplica(t, tc.replicaIndex)
					case operationFullBackup:
						manifest := TestReplicaFullBackup(t, tc.replicaIndex)
						fullBackupPos := manifest.Position
						require.False(t, fullBackupPos.IsZero())
						//
						msgs := ReadRowsFromReplica(t, tc.replicaIndex)
						pos := replication.EncodePosition(fullBackupPos)
						rowsPerPosition[pos] = len(msgs)

						lastBackupPos = fullBackupPos
					case operationIncrementalBackup:
						manifest, _ := TestReplicaIncrementalBackup(t, tc.replicaIndex, "auto", false /* expectEmpty */, tc.expectError)
						if tc.expectError != "" {
							return
						}
						require.NotNil(t, manifest)
						defer func() {
							lastBackupPos = manifest.Position
						}()
						require.False(t, manifest.FromPosition.IsZero())
						require.NotEqual(t, manifest.Position, manifest.FromPosition)
						require.True(t, manifest.Position.GTIDSet.Union(manifest.PurgedPosition.GTIDSet).Contains(manifest.FromPosition.GTIDSet))

						gtidPurgedPos, err := replication.ParsePosition(replication.Mysql56FlavorID, GetReplicaGtidPurged(t, tc.replicaIndex))
						require.NoError(t, err)
						fromPositionIncludingPurged := manifest.FromPosition.GTIDSet.Union(gtidPurgedPos.GTIDSet)

						require.True(t, lastBackupPos.GTIDSet.Contains(fromPositionIncludingPurged), "expected: %v to contain %v", lastBackupPos.GTIDSet, fromPositionIncludingPurged)
					case operationRestore:
						TestReplicaFullRestore(t, tc.replicaIndex, "")
						// should return into replication stream
						insertRowAndWait(t, tc.replicaIndex, "post-restore-check")
					default:
						require.FailNowf(t, "unknown operation type", "operation: %d", tc.operationType)
					}
				})
			})
		}
	})
}
