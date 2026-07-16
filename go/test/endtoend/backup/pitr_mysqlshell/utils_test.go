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

package mysqlctld

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"path"
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
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

// constants for test variants
const (
	MySQLShell = iota

	topoConsistencyTimeout = 20 * time.Second
)

const (
	keyspaceName = "ks"
	shardName    = "0"
	shardKsName  = keyspaceName + "/" + shardName
	dbName       = "vt_" + keyspaceName
	dbaUser      = "vt_dba"

	// backupRoot is where the shared backup volume is mounted in every
	// container that reads or writes backups.
	backupRoot = "/vt/backups"

	// mysqlShellBackupLocation is where mysqlsh writes its dumps, on the
	// shared backup volume so that every tablet can read them.
	mysqlShellBackupLocation = backupRoot + "/backups-mysqlshell"

	// extraMyCnfPath carries the mysqld settings the tests need on top of the
	// generated my.cnf: reading a backup MANIFEST goes through LOAD_FILE, so
	// mysqld must be allowed to read the backup volume.
	extraMyCnfPath = "/vt/files/backup.cnf"
	extraMyCnf     = "[mysqld]\nsecure-file-priv = " + backupRoot + "\n"
)

// The tablets of the shard: a primary, the replica the tests back up and
// restore, an rdonly tablet, and a spare tablet whose vttablet only starts at
// the end of a test, to bootstrap itself from the backups.
const (
	primaryUID = 100 + iota
	replica1UID
	replica3UID
	replica2UID
)

var (
	primary  *vitesst.Tablet
	replica1 *vitesst.Tablet
	replica2 *vitesst.Tablet
	replica3 *vitesst.Tablet

	localCluster    *vitesst.Cluster
	commonTabletArg []string

	tmClient = tmc.NewClient()

	vtInsertTest = `
		create table vt_insert_test (
			id bigint auto_increment,
			msg varchar(64),
			primary key (id)
			) Engine=InnoDB
		`
)

type CompressionDetails struct {
	CompressorEngineName            string
	ExternalCompressorCmd           string
	ExternalCompressorExt           string
	ExternalDecompressorCmd         string
	ExternalDecompressorUseManifest bool
	ManifestExternalDecompressorCmd string
}

// LaunchCluster starts the cluster as per given params.
func LaunchCluster(t *testing.T, cDetails *CompressionDetails) {
	t.Helper()
	ctx := t.Context()

	commonTabletArg = getDefaultCommonArgs()
	commonTabletArg = append(
		commonTabletArg,
		"--backup-engine-implementation", "mysqlshell",
		"--mysql-shell-backup-location", mysqlShellBackupLocation,
		"--mysql-shell-speedup-restore=true",
	)
	commonTabletArg = append(commonTabletArg, getCompressorArgs(cDetails)...)

	uids := []int{primaryUID, replica1UID, replica3UID, replica2UID}
	idx := 0

	keyspace := vitesst.WithKeyspace(keyspaceName).
		WithShardNames(shardName).
		WithReplicas(2).
		WithRDOnly(1).
		WithDurabilityPolicy("semi_sync").
		WithTabletSpec(func(spec *vitesst.TabletSpec) {
			spec.UID = uids[idx]
			idx++
			spec.ExtraArgs = append(spec.ExtraArgs, tabletShellArgs(spec.UID)...)
			if spec.UID == replica3UID {
				// The spare tablet stays out of the shard until a test starts
				// its vttablet, so it must not restore at cluster startup.
				spec.ExtraArgs = append(spec.ExtraArgs, "--restore-from-backup=false")
			}
		})

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithBackupStorage(),
		vitesst.WithoutVTGate(),
		vitesst.WithVTOrc(),
		vitesst.WithVTTabletArgs(commonTabletArg...),
		vitesst.WithTabletFiles(vitesst.ContainerFile{Content: []byte(extraMyCnf), ContainerPath: extraMyCnfPath}),
		vitesst.WithTabletEnv(map[string]string{
			"EXTRA_MY_CNF": extraMyCnfPath,
			// mysqlsh writes its log and configuration under the home
			// directory, and refuses to run without a writable one.
			"HOME": "/vt/vtdataroot",
		}),
		keyspace,
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	localCluster = cluster

	shard := cluster.Keyspace(keyspaceName).Shard(shardName)
	primary = shard.Primary()
	require.NotNil(t, primary)

	replicas := shard.Replicas()
	require.Len(t, replicas, 2)
	replica1, replica3 = replicas[0], replicas[1]

	rdonly := shard.RDOnly()
	require.Len(t, rdonly, 1)
	replica2 = rdonly[0]

	detachReplica3(t)
}

// detachReplica3 leaves the spare tablet with a running, empty mysqld and no
// tablet record, so that a test can later start its vttablet and have it
// bootstrap itself from the backups.
func detachReplica3(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	require.NoError(t, replica3.StopVttablet(ctx))
	require.NoError(t, localCluster.Vtctld().ExecuteCommand(ctx, "DeleteTablets", replica3.Alias()))

	queries := []string{
		"set global super_read_only = off",
		"set global read_only = off",
		"stop replica",
		"reset replica all",
		"drop database if exists " + dbName,
		"drop database if exists _vt",
		"reset binary logs and gtids",
	}
	for _, query := range queries {
		_, err := replica3.QueryTabletWithDB(ctx, query, "")
		require.NoErrorf(t, err, "query: %s", query)
	}
}

// SetupReplica3Tablet starts the spare tablet's vttablet with the given extra
// arguments, letting it restore from the backups.
func SetupReplica3Tablet(t *testing.T, extraArgs []string) (*vitesst.Tablet, error) {
	args := append([]string{}, commonTabletArg...)
	args = append(args, tabletShellArgs(replica3UID)...)
	args = append(args, extraArgs...)

	if err := replica3.StartVttablet(t.Context(), args...); err != nil {
		return replica3, err
	}
	return replica3, nil
}

// tabletShellArgs returns the mysql-shell arguments of one tablet. Every
// tablet runs its own mysqld, so mysqlsh has to be pointed at the socket of
// that particular one when running dumps and loads.
func tabletShellArgs(uid int) []string {
	socket := fmt.Sprintf("%s/vt_%010d/mysql.sock", "/vt/vtdataroot", uid)
	return []string{"--mysql-shell-flags", fmt.Sprintf("--js -u %s --no-password -S %s", dbaUser, socket)}
}

func getCompressorArgs(cDetails *CompressionDetails) []string {
	var args []string

	if cDetails == nil {
		return args
	}

	if cDetails.CompressorEngineName != "" {
		args = append(args, "--compression-engine-name="+cDetails.CompressorEngineName)
	}
	if cDetails.ExternalCompressorCmd != "" {
		args = append(args, "--external-compressor="+cDetails.ExternalCompressorCmd)
	}
	if cDetails.ExternalCompressorExt != "" {
		args = append(args, "--external-compressor-extension="+cDetails.ExternalCompressorExt)
	}
	if cDetails.ExternalDecompressorCmd != "" {
		args = append(args, "--external-decompressor="+cDetails.ExternalDecompressorCmd)
	}
	if cDetails.ExternalDecompressorUseManifest {
		args = append(args, "--external-decompressor-use-manifest")
	}
	if cDetails.ManifestExternalDecompressorCmd != "" {
		args = append(args, "--manifest-external-decompressor="+cDetails.ManifestExternalDecompressorCmd)
	}

	return args
}

func getDefaultCommonArgs() []string {
	return []string{
		"--vreplication-retry-delay", "1s",
		"--degraded-threshold", "5s",
		"--lock-tables-timeout", "5s",
		"--enable-replication-reporter",
		"--serving-state-grace-period", "1s",
	}
}

func InitTestTable(t *testing.T) {
	ctx := t.Context()
	_, err := primary.QueryTablet(ctx, "DROP TABLE IF EXISTS vt_insert_test")
	require.NoError(t, err)
	_, err = primary.QueryTablet(ctx, vtInsertTest)
	require.NoError(t, err)
}

func checkTabletType(t *testing.T, alias string, tabletType topodata.TabletType) {
	t.Helper()
	ctx := t.Context()
	// for loop for 15 seconds to check if tablet type is correct
	for range 15 {
		output, err := localCluster.Vtctld().ExecuteCommandWithOutput(ctx, "GetTablet", alias)
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
	assert.Failf(t, "checkTabletType failed.", "Tablet type is not correct. Expected: %v", tabletType)
}

func GetReplicaPosition(t *testing.T, replicaIndex int) string {
	ctx := t.Context()
	replica := getReplica(t, replicaIndex)
	proto, err := replica.TabletProto(ctx)
	require.NoError(t, err)
	pos, err := tmClient.PrimaryPosition(ctx, proto)
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

func ReconnectReplicaToPrimary(t *testing.T, replicaIndex int) {
	ctx := t.Context()
	query := fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='vt_repl', GET_SOURCE_PUBLIC_KEY = 1, SOURCE_AUTO_POSITION = 1", primary.Name(), 3306)
	replica := getReplica(t, replicaIndex)
	_, err := replica.QueryTablet(ctx, "stop replica")
	require.NoError(t, err)
	_, err = replica.QueryTablet(ctx, query)
	require.NoError(t, err)
	_, err = replica.QueryTablet(ctx, "start replica")
	require.NoError(t, err)
}

func InsertRowOnPrimary(t *testing.T, hint string) {
	if hint == "" {
		hint = textutil.RandomHash()[:12]
	}
	query, err := sqlparser.ParseAndBind("insert into vt_insert_test (msg) values (%a)", sqltypes.StringBindVariable(hint))
	require.NoError(t, err)
	_, err = primary.QueryTablet(t.Context(), query)
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
	return ReadRowsFromTablet(t, primary)
}

func getReplica(t *testing.T, replicaIndex int) *vitesst.Tablet {
	switch replicaIndex {
	case 0:
		return replica1
	case 1:
		return replica2
	case 2:
		return replica3
	default:
		assert.Failf(t, "invalid replica index", "index=%d", replicaIndex)
		return nil
	}
}

func ReadRowsFromReplica(t *testing.T, replicaIndex int) (msgs []string) {
	return ReadRowsFromTablet(t, getReplica(t, replicaIndex))
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

// readManifestFile reads a backup's MANIFEST from the shared backup volume,
// through the mysqld of a tablet that has the volume mounted.
func readManifestFile(t *testing.T, backupName string) (manifest *mysqlctl.BackupManifest) {
	// reading manifest
	fullPath := path.Join(backupRoot, keyspaceName, shardName, backupName, "MANIFEST")
	query, err := sqlparser.ParseAndBind("select load_file(%a) as manifest", sqltypes.StringBindVariable(fullPath))
	require.NoError(t, err)
	rs, err := primary.QueryTabletWithDB(t.Context(), query, "")
	require.NoErrorf(t, err, "error while reading MANIFEST %v", err)
	row := rs.Named().Row()
	require.NotNil(t, row)
	data := row.AsBytes("manifest", nil)
	require.NotEmptyf(t, data, "empty MANIFEST at %v", fullPath)

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
		backups, err := localCluster.ListBackups(ctx, keyspaceName, shardName)
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

func RemoveBackup(t *testing.T, backupName string) {
	err := localCluster.Vtctld().ExecuteCommand(t.Context(), "RemoveBackup", shardKsName, backupName)
	require.NoError(t, err)
}

func vtctlBackupReplicaNoDestroyNoWrites(t *testing.T, replicaIndex int) (backups []string) {
	replica := getReplica(t, replicaIndex)
	numBackups := len(waitForNumBackups(t, -1))

	err := localCluster.Vtctld().ExecuteCommand(t.Context(), "Backup", replica.Alias())
	require.NoError(t, err)

	backups = waitForNumBackups(t, numBackups+1)
	require.NotEmpty(t, backups)

	verifyTabletBackupStats(t, tabletVars(t, replica))

	return backups
}

func replicaFullBackup(t *testing.T, replicaIndex int) (manifest *mysqlctl.BackupManifest) {
	backups := vtctlBackupReplicaNoDestroyNoWrites(t, replicaIndex)

	return readManifestFile(t, backups[len(backups)-1])
}

func testReplicaIncrementalBackup(t *testing.T, replica *vitesst.Tablet, incrementalFromPos string, expectEmpty bool, expectError string) (manifest *mysqlctl.BackupManifest, backupName string) {
	numBackups := len(waitForNumBackups(t, -1))

	output, err := localCluster.Vtctld().ExecuteCommandWithOutput(t.Context(), "Backup", "--incremental-from-pos", incrementalFromPos, replica.Alias())
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

	verifyTabletBackupStats(t, tabletVars(t, replica))
	backupName = backups[len(backups)-1]

	return readManifestFile(t, backupName), backupName
}

func replicaIncrementalBackup(t *testing.T, replicaIndex int, incrementalFromPos string, expectEmpty bool, expectError string) (manifest *mysqlctl.BackupManifest, backupName string) {
	replica := getReplica(t, replicaIndex)
	return testReplicaIncrementalBackup(t, replica, incrementalFromPos, expectEmpty, expectError)
}

func replicaRestoreToPos(t *testing.T, replicaIndex int, restoreToPos replication.Position, expectError string) {
	replica := getReplica(t, replicaIndex)

	require.False(t, restoreToPos.IsZero())
	restoreToPosArg := replication.EncodePosition(restoreToPos)
	assert.Contains(t, restoreToPosArg, "MySQL56/")
	if rand.IntN(2) == 0 {
		// Verify that restore works whether or not the MySQL56/ prefix is present.
		restoreToPosArg = strings.Replace(restoreToPosArg, "MySQL56/", "", 1)
		assert.NotContains(t, restoreToPosArg, "MySQL56/")
	}

	output, err := localCluster.Vtctld().ExecuteCommandWithOutput(t.Context(), "RestoreFromBackup", "--restore-to-pos", restoreToPosArg, replica.Alias())
	if expectError != "" {
		require.Errorf(t, err, "expected: %v", expectError)
		require.Contains(t, output, expectError)
		return
	}
	require.NoErrorf(t, err, "output: %v", output)
	verifyTabletRestoreStats(t, tabletVars(t, replica))
	checkTabletType(t, replica1.Alias(), topodata.TabletType_DRAINED)
}

func replicaRestoreToTimestamp(t *testing.T, restoreToTimestamp time.Time, expectError string) {
	require.False(t, restoreToTimestamp.IsZero())
	restoreToTimestampArg := mysqlctl.FormatRFC3339(restoreToTimestamp)
	output, err := localCluster.Vtctld().ExecuteCommandWithOutput(t.Context(), "RestoreFromBackup", "--restore-to-timestamp", restoreToTimestampArg, replica1.Alias())
	if expectError != "" {
		require.Errorf(t, err, "expected: %v", expectError)
		require.Contains(t, output, expectError)
		return
	}
	require.NoErrorf(t, err, "output: %v", output)
	verifyTabletRestoreStats(t, tabletVars(t, replica1))
	checkTabletType(t, replica1.Alias(), topodata.TabletType_DRAINED)
}

// tabletVars fetches a tablet's /debug/vars.
func tabletVars(t *testing.T, tablet *vitesst.Tablet) map[string]any {
	vars, err := tablet.GetVars(t.Context())
	require.NoError(t, err)
	return vars
}

func verifyTabletBackupStats(t *testing.T, vars map[string]any) {
	require.Contains(t, vars, "BackupCount")
	bc := vars["BackupCount"].(map[string]any)
	require.Contains(t, bc, "-.-.Backup")

	require.Contains(t, vars, "BackupDurationNanoseconds")
	bd := vars["BackupDurationNanoseconds"]

	require.Contains(t, bd, "-.-.Backup")

	if backupstorage.BackupStorageImplementation == "file" {
		require.Contains(t, bd, "BackupStorage.File.File:Write")
	}
}

func verifyRestorePositionAndTimeStats(t *testing.T, vars map[string]any) {
	backupPosition := vars["RestorePosition"].(string)
	backupTime := vars["RestoredBackupTime"].(string)
	require.Contains(t, vars, "RestoredBackupTime")
	require.Contains(t, vars, "RestorePosition")
	require.NotEqual(t, "", backupPosition)
	require.NotEqual(t, "", backupTime)
	rp, err := replication.DecodePosition(backupPosition)
	require.NoError(t, err)
	require.False(t, rp.IsZero())
}

func verifyTabletRestoreStats(t *testing.T, vars map[string]any) {
	verifyRestorePositionAndTimeStats(t, vars)

	require.Contains(t, vars, "RestoreCount")
	bc := vars["RestoreCount"].(map[string]any)
	require.Contains(t, bc, "-.-.Restore")

	require.Contains(t, vars, "RestoreDurationNanoseconds")
	bd := vars["RestoreDurationNanoseconds"]
	require.Contains(t, bd, "-.-.Restore")

	require.Contains(t, bd, "BackupStorage.File.File:Read")
}
