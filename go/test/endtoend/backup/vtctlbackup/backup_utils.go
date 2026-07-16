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

package vtctlbackup

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// constants for test variants
const (
	XtraBackup = iota
	BuiltinBackup
	Mysqlctld
	MySQLShell
	timeout                = time.Duration(60 * time.Second)
	topoConsistencyTimeout = 20 * time.Second
)

const (
	keyspaceName = "ks"
	shardName    = "0"
	shardKsName  = keyspaceName + "/" + shardName
	dbName       = "vt_" + keyspaceName
	dbaUser      = "vt_dba"

	// vtctldServer is the address of the cluster's vtctld inside its containers.
	vtctldServer = "vtctld:15999"

	// terminateOutputFile and terminatePIDFile carry the output and the process
	// id of the vtctldclient invocation that the terminated backup and restore
	// tests signal mid-flight.
	terminateOutputFile = "/vt/files/vtctldclient.out"
	terminatePIDFile    = "/vt/files/vtctldclient.pid"

	// terminateMessageTimeout bounds how long a terminated backup or restore
	// waits for the progress message it signals on.
	terminateMessageTimeout = 3 * time.Minute

	// hookDirectory is where vttablet looks for hooks inside its container, and
	// restoreDoneHook is the hook that runs when a restore completes or fails.
	// The hook writes its environment to hookOutputFile.
	hookDirectory   = "/vt/vthook"
	restoreDoneHook = "vttablet_restore_done"
	hookOutputFile  = "/vt/vtdataroot/hook_restore_done_output"

	// vttabletExitMessage is what a tablet container reports when its vttablet
	// process exits.
	vttabletExitMessage = "vttablet exited"

	// xtrabackupWrapperDir holds the xtrabackup and xbstream wrappers that
	// vttablet runs instead of the packaged binaries, and xtrabackupGateFile is
	// the file that holds a backup at its start. holdXtrabackup and
	// releaseXtrabackup drive the gate.
	xtrabackupWrapperDir  = "/vt/files/xtrabackup"
	xtrabackupGateFile    = "/vt/files/xtrabackup.gate"
	xtrabackupBinariesDir = "/usr/bin"

	// xtrabackupGateTimeout bounds how long a held backup waits for the gate
	// file to go away, so that a test which never releases the gate fails on its
	// own assertions instead of hanging.
	xtrabackupGateTimeout = 3 * time.Minute
)

var (
	primary  *vitesst.Tablet
	replica1 *vitesst.Tablet
	replica2 *vitesst.Tablet
	replica3 *vitesst.Tablet

	currentSetupType int

	// replica2Type is the tablet type replica2's vttablet last started with.
	replica2Type string

	commonTabletArg = getDefaultCommonArgs()

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

// LaunchCluster : starts the cluster as per given params.
func LaunchCluster(t *testing.T, setupType int, streamMode string, stripes int, cDetails *CompressionDetails) (int, error) {
	ctx := t.Context()
	currentSetupType = setupType
	replica2Type = ""

	commonTabletArg = getDefaultCommonArgs()

	// Update arguments for different backup engines
	switch setupType {
	case XtraBackup:
		xtrabackupArgs := []string{
			"--backup-engine-implementation", "xtrabackup",
			fmt.Sprintf("%s=%s", "--xtrabackup-stream-mode", streamMode),
			"--xtrabackup-user" + "=" + dbaUser,
			fmt.Sprintf("%s=%d", "--xtrabackup-stripes", stripes),
			"--xtrabackup-root-path=" + xtrabackupWrapperDir,
		}

		// if streamMode is xbstream, add some additional args to test other xtrabackup flags
		if streamMode == "xbstream" {
			xtrabackupArgs = append(xtrabackupArgs, "--xtrabackup-prepare-flags", "--use-memory=100M")
		}

		commonTabletArg = append(commonTabletArg, xtrabackupArgs...)
	case MySQLShell:
		commonTabletArg = append(
			commonTabletArg,
			"--backup-engine-implementation", "mysqlshell",
			"--mysql-shell-backup-location", mysqlShellBackupLocation,
			"--mysql-shell-speedup-restore=true",
		)
	}

	commonTabletArg = append(commonTabletArg, getCompressorArgs(cDetails)...)

	hookFile, err := restoreDoneHookFile(t)
	if err != nil {
		return 1, err
	}

	tabletFiles := []vitesst.ContainerFile{{
		Content:       []byte(backupReadCnf),
		ContainerPath: backupReadCnfPath,
	}}
	if setupType == XtraBackup {
		wrapperDir, err := xtrabackupWrapperFiles(t)
		if err != nil {
			return 1, err
		}
		tabletFiles = append(tabletFiles, wrapperDir)
	}

	// The shard's tablets, in start order: the primary, the replica the tests
	// back up from, the spare tablet whose vttablet only starts to bootstrap
	// itself from a backup, and the tablet the tests restore into.
	tabletIndex := 0
	keyspace := vitesst.WithKeyspace(keyspaceName).
		WithShardNames(shardName).
		WithReplicas(2).
		WithRDOnly(1).
		WithDurabilityPolicy("semi_sync").
		WithTabletArgs(commonTabletArg...).
		WithTabletSpec(func(spec *vitesst.TabletSpec) {
			spec.ExtraArgs = append(spec.ExtraArgs, mysqlShellTabletArgs(setupType, spec.UID)...)
			if tabletIndex == 2 {
				// Only the spare tablet ever restores from a corrupted backup,
				// so it is the only one that carries the restore-done hook.
				spec.Files = append(spec.Files, hookFile)
			}
			tabletIndex++
		})

	opts := []vitesst.ClusterOption{
		vitesst.WithBackupStorage(),
		vitesst.WithoutVTGate(),
		vitesst.WithVTOrc(),
		vitesst.WithTabletFiles(tabletFiles...),
		vitesst.WithTabletEnv(map[string]string{
			"EXTRA_MY_CNF": backupReadCnfPath,
			// mysqlsh writes its log and configuration under the home directory,
			// and refuses to run without a writable one.
			"HOME": "/vt/vtdataroot",
		}),
		keyspace,
	}
	if setupType == Mysqlctld {
		opts = append(opts, vitesst.WithMysqlctld())
	}

	cluster, err := vitesst.NewCluster(t, opts...)
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
	primary = shard.Primary()
	replicas := shard.Replicas()
	rdonly := shard.RDOnly()
	if primary == nil || len(replicas) != 2 || len(rdonly) != 1 {
		return 1, vterrors.Errorf(vtrpc.Code_INTERNAL,
			"expected a primary, two replicas and one rdonly tablet in %s, found %d replicas and %d rdonly tablets",
			shard.Ref(), len(replicas), len(rdonly))
	}
	replica1, replica3 = replicas[0], replicas[1]
	replica2 = rdonly[0]

	// The spare tablet leaves the shard with an empty mysqld, so that a test can
	// later start its vttablet and have it bootstrap itself from the backups.
	if err := replica3.StopVttablet(ctx); err != nil {
		return 1, err
	}
	if err := cluster.Vtctld().ExecuteCommand(ctx, "DeleteTablets", replica3.Alias()); err != nil {
		return 1, err
	}
	if err := emptyTabletDatabase(ctx, replica3); err != nil {
		return 1, err
	}

	return 0, nil
}

// restoreDoneHookFile builds the vttablet_restore_done hook the restore-done
// test reads back: a script that writes the hook's environment to a file.
func restoreDoneHookFile(t *testing.T) (vitesst.ContainerFile, error) {
	hostDir := filepath.Join(t.TempDir(), filepath.Base(hookDirectory))
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		return vitesst.ContainerFile{}, err
	}

	content := fmt.Sprintf("#!/bin/bash\nenv > %s\n", hookOutputFile)
	if err := os.WriteFile(filepath.Join(hostDir, restoreDoneHook), []byte(content), 0o755); err != nil {
		return vitesst.ContainerFile{}, err
	}

	return vitesst.ContainerFile{
		HostPath:      hostDir,
		ContainerPath: hookDirectory,
		Mode:          0o755,
	}, nil
}

// xtrabackupWrapperFiles builds the directory of executables that vttablet runs
// in place of the packaged xtrabackup and xbstream. The xtrabackup wrapper holds
// a backup at its start, before xtrabackup locks the instance for backup, for as
// long as the gate file exists. Everything else passes straight through.
func xtrabackupWrapperFiles(t *testing.T) (vitesst.ContainerFile, error) {
	hostDir := filepath.Join(t.TempDir(), filepath.Base(xtrabackupWrapperDir))
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		return vitesst.ContainerFile{}, err
	}

	xtrabackup := fmt.Sprintf(`#!/bin/bash
set -u

for arg in "$@"; do
  if [[ "$arg" == "--backup" ]]; then
    deadline=$((SECONDS + %d))
    while [[ -f %s && $SECONDS -lt $deadline ]]; do
      sleep 0.1
    done
    break
  fi
done

exec %s "$@"
`, int(xtrabackupGateTimeout.Seconds()), xtrabackupGateFile, path.Join(xtrabackupBinariesDir, "xtrabackup"))

	xbstream := fmt.Sprintf("#!/bin/bash\nexec %s \"$@\"\n", path.Join(xtrabackupBinariesDir, "xbstream"))

	for name, content := range map[string]string{"xtrabackup": xtrabackup, "xbstream": xbstream} {
		if err := os.WriteFile(filepath.Join(hostDir, name), []byte(content), 0o755); err != nil {
			return vitesst.ContainerFile{}, err
		}
	}

	return vitesst.ContainerFile{
		HostPath:      hostDir,
		ContainerPath: xtrabackupWrapperDir,
		Mode:          0o755,
	}, nil
}

// holdXtrabackup makes the tablet's next backup wait, before xtrabackup locks
// the instance for backup, until releaseXtrabackup runs.
func holdXtrabackup(ctx context.Context, tablet *vitesst.Tablet) error {
	return execTabletCommand(ctx, tablet, "touch", xtrabackupGateFile)
}

// releaseXtrabackup lets a held backup proceed.
func releaseXtrabackup(ctx context.Context, tablet *vitesst.Tablet) error {
	return execTabletCommand(ctx, tablet, "rm", "-f", xtrabackupGateFile)
}

// execTabletCommand runs a command in a tablet's container and fails when the
// command does.
func execTabletCommand(ctx context.Context, tablet *vitesst.Tablet, cmd ...string) error {
	exitCode, output, err := tablet.Exec(ctx, cmd...)
	if err != nil {
		return vterrors.Wrapf(err, "running %q on %s", strings.Join(cmd, " "), tablet.Alias())
	}
	if exitCode != 0 {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "%q on %s failed with exit code %d: %s",
			strings.Join(cmd, " "), tablet.Alias(), exitCode, output)
	}
	return nil
}

// emptyTabletDatabase leaves a tablet's mysqld running and empty, so that the
// next start of its vttablet restores from a backup instead of replicating.
func emptyTabletDatabase(ctx context.Context, tablet *vitesst.Tablet) error {
	// A tablet that last served as a semi-sync source keeps waiting for a
	// replica to acknowledge its writes, which would block the statements
	// below forever. The variable names differ between MySQL versions, so the
	// ones this mysqld does not know report an unknown-variable error, which
	// is exactly the case where there is nothing to turn off.
	for _, query := range []string{
		"set global rpl_semi_sync_source_enabled = 0",
		"set global rpl_semi_sync_master_enabled = 0",
		"set global rpl_semi_sync_replica_enabled = 0",
		"set global rpl_semi_sync_slave_enabled = 0",
	} {
		if _, err := tablet.QueryTabletWithDB(ctx, query, ""); err != nil {
			log.Info(
				"disabling semi-sync",
				slog.String("tablet", tablet.Alias()),
				slog.String("query", query),
				slog.Any("error", err),
			)
		}
	}

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
		if _, err := tablet.QueryTabletWithDB(ctx, query, ""); err != nil {
			return vterrors.Wrapf(err, "query %s on %s", query, tablet.Alias())
		}
	}
	return nil
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

// update arguments with new values of compressionDetail.
func updateCompressorArgs(commonArgs []string, cDetails *CompressionDetails) []string {
	if cDetails == nil {
		return commonArgs
	}

	// remove if any compression flag already exists
	for i, s := range commonArgs {
		if strings.Contains(s, "--compression-engine-name") || strings.Contains(s, "--external-compressor") ||
			strings.Contains(s, "--external-compressor-extension") || strings.Contains(s, "--external-decompressor") {
			commonArgs = append(commonArgs[:i], commonArgs[i+1:]...)
		}
	}

	// update it with new values
	commonArgs = append(commonArgs, getCompressorArgs(cDetails)...)
	return commonArgs
}

// TearDownCluster shuts down all cluster processes
func TearDownCluster(t *testing.T) {
	ctx := context.WithoutCancel(t.Context())

	if clusterCleanup != nil {
		if err := clusterCleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	}

	vitessCluster = nil
	clusterCleanup = nil
	primary = nil
	replica1 = nil
	replica2 = nil
	replica3 = nil
}

// TestBackup runs all the backup tests
func TestBackup(t *testing.T, setupType int, streamMode string, stripes int, cDetails *CompressionDetails, runSpecific []string) error {
	switch streamMode {
	case "xbstream":
	case "", "tar":
		// XtraBackup 8.0 must be used with MySQL 8.0 and it no longer supports tar as a stream method:
		//    https://docs.percona.com/percona-xtrabackup/8.0/xtrabackup_bin/backup.streaming.html
		t.Logf("Skipping xtrabackup tests with --xtrabackup-stream-mode=tar as tar is no longer a streaming option in XtraBackup 8.0")
		return nil
	default:
		require.FailNow(t, "Unsupported xtrabackup stream mode: "+streamMode)
	}

	testMethods := []struct {
		name   string
		method func(t *testing.T)
	}{
		{
			name: "TestReplicaBackup",
			method: func(t *testing.T) {
				vtctlBackup(t, "replica")
			},
		}, //
		{
			name: "TestRdonlyBackup",
			method: func(t *testing.T) {
				vtctlBackup(t, "rdonly")
			},
		}, //
		{
			name:   "TestPrimaryBackup",
			method: primaryBackup,
		},
		{
			name:   "TestPrimaryReplicaSameBackup",
			method: primaryReplicaSameBackup,
		}, //
		{
			name:   "primaryReplicaSameBackupModifiedCompressionEngine",
			method: primaryReplicaSameBackupModifiedCompressionEngine,
		}, //
		{
			name:   "TestRestoreOldPrimaryByRestart",
			method: restoreOldPrimaryByRestart,
		}, //
		{
			name:   "TestRestoreOldPrimaryInPlace",
			method: restoreOldPrimaryInPlace,
		}, //
		{
			name:   "TestTerminatedRestore",
			method: terminatedRestore,
		}, //
		{
			name:   "TestRestoreDoneHookOnFailure",
			method: testRestoreDoneHookOnFailure,
		}, //
		{
			name:   "DoNotDemoteNewlyPromotedPrimaryIfReparentingDuringBackup",
			method: doNotDemoteNewlyPromotedPrimaryIfReparentingDuringBackup,
		}, //
	}

	// setup cluster for the testing
	code, err := LaunchCluster(t, setupType, streamMode, stripes, cDetails)
	require.Nilf(t, err, "setup failed with status code %d", code)

	// Teardown the cluster
	defer TearDownCluster(t)

	// Run all the backup tests
	for _, test := range testMethods {
		if len(runSpecific) > 0 && !isRegistered(test.name, runSpecific) {
			continue
		}
		// don't run this one unless specified
		if len(runSpecific) == 0 && test.name == "DoNotDemoteNewlyPromotedPrimaryIfReparentingDuringBackup" {
			continue
		}
		if retVal := t.Run(test.name, test.method); !retVal {
			return vterrors.Errorf(vtrpc.Code_UNKNOWN, "test failure: %s", test.name)
		}
	}

	t.Run("check for files created with global permissions", func(t *testing.T) {
		t.Logf("Confirming that none of the MySQL data directories that we've created have files with global permissions")
		for _, tablet := range vitessCluster.Tablets() {
			confirmDataDirHasNoGlobalPerms(t, tablet)
		}
	})

	return nil
}

func isRegistered(name string, runlist []string) bool {
	return slices.Contains(runlist, name)
}

type restoreMethod func(t *testing.T, tablet *vitesst.Tablet)

//  1. create a shard with primary and replica1 only
//  2. run InitShardPrimary
//  3. insert some data
//  4. take a backup on primary and save the timestamp
//  5. bring up tablet_replica2 after the fact, let it restore the (latest/second) backup
//  6. check all data is right (before+after backup data)
//  7. insert more data on the primary
//  8. take another backup
//  9. verify that we now have 2 backups
//  10. do a PRS to make the original primary a replica so that we can do a restore there
//  11. Delete+teardown the new primary so that we can restore the first backup on the original
//     primary to confirm we don't have the data from #7
//  12. restore first backup on the original primary tablet using the first backup timstamp
//  13. verify that don't have the data added after the first backup
//  14. remove the backups
func primaryBackup(t *testing.T) {
	ctx := t.Context()

	// Having the VTOrc in this test causes a lot of flakiness. For example when we delete the tablet `replica2` which
	// is the current primary and then try to restore from backup the old primary (`primary.Alias`), but before that sometimes the VTOrc
	// promotes the `replica1` to primary right after we delete the replica2 (current primary).
	// This can result in unexpected behavior. Therefore, disabling the VTOrc in this test to remove flakiness.
	disableVTOrcRecoveries(t)
	defer func() {
		enableVTOrcRecoveries(t)
	}()
	verifyInitialReplication(t)

	output, err := vitessCluster.Vtctld().ExecuteCommandWithOutput(ctx, "Backup", primary.Alias())
	require.Error(t, err)
	assert.Contains(t, output, "type PRIMARY cannot take backup. if you really need to do this, rerun the backup command with --allow-primary")

	verifyBackupCount(t, 0)

	err = vitessCluster.Vtctld().ExecuteCommand(ctx, "Backup", "--allow-primary", primary.Alias())
	require.NoError(t, err)

	// We'll restore this on the primary later to test restores using a backup timestamp
	firstBackupTimestamp := time.Now().UTC().Format(mysqlctl.BackupTimestampFormat)

	backups := verifyBackupCount(t, 1)
	assert.Contains(t, backups[0], backupAlias(primary))

	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test2')")
	require.NoError(t, err)

	restoreWaitForBackup(t, "replica", nil, true)
	err = replica2.WaitForTabletStatus(ctx, timeout, "SERVING")
	require.NoError(t, err)

	// Verify that we have all the new data -- we should have 2 records now...
	// And only 1 record after we restore using the first backup timestamp
	verifyRowsInTablet(t, replica2, 2)

	sqlInitTestTable := "init_test"
	err = vitessCluster.Vtctld().ExecuteCommand(
		ctx, "Backup", "--allow-primary",
		// Test init SQL.
		"--init-backup-sql-queries", fmt.Sprintf("create table `%s`.%s (id int),optimize table `%s`.%s,insert into `%s`.%s (id) values (1)",
			dbName, sqlInitTestTable, dbName, sqlInitTestTable, dbName, sqlInitTestTable),
		"--init-backup-sql-timeout=10m",
		"--init-backup-tablet-types=primary",
		"--init-backup-sql-fail-on-error",
		primary.Alias(),
	)
	require.NoError(t, err)

	backups = verifyBackupCount(t, 2)
	assert.Contains(t, backups[1], backupAlias(primary))

	verifyTabletBackupStats(t, getTabletVars(t, primary))

	// Confirm that the init SQL quereies were run: the table was created and we inserted a row.
	res, err := primary.QueryTablet(ctx, "SELECT * FROM "+sqlInitTestTable)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	// Now get rid of the init_test table as its purpose has ended.
	_, err = primary.QueryTablet(ctx, "DROP TABLE "+sqlInitTestTable)
	require.NoError(t, err)

	// Perform PRS to demote the primary tablet (primary) so that we can do a restore there and verify we don't have the
	// data from after the older/first backup
	err = vitessCluster.Vtctld().ExecuteCommand(ctx, "PlannedReparentShard",
		"--new-primary", replica2.Alias(), shardKsName)
	require.NoError(t, err)

	// Delete the current primary tablet (replica2) so that the original primary tablet (primary) can be restored from the
	// older/first backup w/o it replicating the subsequent insert done after the first backup was taken
	err = vitessCluster.Vtctld().ExecuteCommand(ctx, "DeleteTablets", "--allow-primary", replica2.Alias())
	require.NoError(t, err)
	err = replica2.StopVttablet(ctx)
	require.NoError(t, err)

	// Restore the older/first backup -- using the timestamp we saved -- on the original primary tablet (primary)
	err = vitessCluster.Vtctld().ExecuteCommand(ctx, "RestoreFromBackup", "--backup-timestamp", firstBackupTimestamp, primary.Alias())
	require.NoError(t, err)

	verifyTabletRestoreStats(t, getTabletVars(t, primary))

	// Re-init the shard -- making the original primary tablet (primary) primary again -- for subsequent tests
	err = initShardPrimary(t)
	require.NoError(t, err)

	// Verify that we don't have the record created after the older/first backup
	verifyRowsInTablet(t, primary, 1)

	verifyAfterRemovingBackupNoBackupShouldBePresent(t, backups)
	require.NoError(t, err)

	_, err = primary.QueryTablet(ctx, "DROP TABLE vt_insert_test")
	require.NoError(t, err)

	restartPrimaryAndReplica(t)
}

// Check that a replica and primary both restored from the same backup
// can replicate successfully.
func primaryReplicaSameBackup(t *testing.T) {
	ctx := t.Context()

	// insert data on primary, wait for replica to get it
	verifyInitialReplication(t)

	// backup the replica
	err := vitessCluster.Vtctld().ExecuteCommand(ctx, "Backup", replica1.Alias())
	require.NoError(t, err)

	verifyTabletBackupStats(t, getTabletVars(t, replica1))

	//  insert more data on the primary
	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test2')")
	require.NoError(t, err)

	// now bring up the other replica, letting it restore from backup.
	restoreWaitForBackup(t, "replica", nil, true)
	err = replica2.WaitForTabletStatus(ctx, timeout, "SERVING")
	require.NoError(t, err)

	// check the new replica has the data
	verifyRowsInTablet(t, replica2, 2)

	// Promote replica2 to primary
	err = vitessCluster.Vtctld().ExecuteCommand(ctx, "PlannedReparentShard",
		"--new-primary", replica2.Alias(), shardKsName)
	require.NoError(t, err)

	// insert more data on replica2 (current primary)
	_, err = replica2.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test3')")
	require.NoError(t, err)

	// Force replica1 to restore from backup.
	verifyRestoreTablet(t, replica1, "SERVING")

	// wait for replica1 to catch up.
	verifyRowsInTablet(t, replica1, 3)

	// This is to test that replicationPosition is processed correctly
	// while doing backup/restore after a reparent.
	// It is written into the MANIFEST and read back from the MANIFEST.
	//
	// Take another backup on the replica.
	err = vitessCluster.Vtctld().ExecuteCommand(ctx, "Backup", replica1.Alias())
	require.NoError(t, err)

	// Insert more data on replica2 (current primary).
	_, err = replica2.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test4')")
	require.NoError(t, err)

	// Force replica1 to restore from backup.
	verifyRestoreTablet(t, replica1, "SERVING")

	verifyRowsInTablet(t, replica1, 4)
	err = replica2.StopVttablet(ctx)
	require.NoError(t, err)
	restartPrimaryAndReplica(t)
}

// Test a primary and replica from the same backup.
//
// Check that a replica and primary both restored from the same backup
// We change compression alogrithm in between but it should not break any restore functionality
func primaryReplicaSameBackupModifiedCompressionEngine(t *testing.T) {
	ctx := t.Context()

	// insert data on primary, wait for replica to get it
	verifyInitialReplication(t)

	// backup the replica
	err := vitessCluster.Vtctld().ExecuteCommand(ctx, "Backup", replica1.Alias())
	require.NoError(t, err)

	verifyTabletBackupStats(t, getTabletVars(t, replica1))

	//  insert more data on the primary
	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test2')")
	require.NoError(t, err)

	// now bring up the other replica, with change in compression engine
	// this is to verify that restore will read engine name from manifest instead of reading the new values
	cDetails := &CompressionDetails{
		CompressorEngineName:    "pgzip",
		ExternalCompressorCmd:   "gzip -c",
		ExternalCompressorExt:   ".gz",
		ExternalDecompressorCmd: "",
	}
	restoreWaitForBackup(t, "replica", cDetails, false)
	err = replica2.WaitForTabletStatus(ctx, timeout, "SERVING")
	require.NoError(t, err)

	// check the new replica has the data
	verifyRowsInTablet(t, replica2, 2)

	// Promote replica2 to primary
	err = vitessCluster.Vtctld().ExecuteCommand(ctx, "PlannedReparentShard",
		"--new-primary", replica2.Alias(), shardKsName)
	require.NoError(t, err)

	// insert more data on replica2 (current primary)
	_, err = replica2.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test3')")
	require.NoError(t, err)

	// Force replica1 to restore from backup.
	verifyRestoreTablet(t, replica1, "SERVING")

	// wait for replica1 to catch up.
	verifyRowsInTablet(t, replica1, 3)

	// Promote replica1 to primary
	err = vitessCluster.Vtctld().ExecuteCommand(ctx, "PlannedReparentShard",
		"--new-primary", replica1.Alias(), shardKsName)
	require.NoError(t, err)

	// Insert more data on replica1 (current primary).
	_, err = replica1.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test4')")
	require.NoError(t, err)

	// wait for replica2 to catch up.
	verifyRowsInTablet(t, replica2, 4)

	// Now take replica2 backup with gzip (new compressor)
	err = vitessCluster.Vtctld().ExecuteCommand(ctx, "Backup", replica2.Alias())
	require.NoError(t, err)

	verifyTabletBackupStats(t, getTabletVars(t, replica2))

	// Force replica2 to restore from backup.
	verifyRestoreTablet(t, replica2, "SERVING")
	verifyRowsInTablet(t, replica2, 4)
	err = replica2.StopVttablet(ctx)
	require.NoError(t, err)
	restartPrimaryAndReplica(t)
}

func restoreOldPrimaryByRestart(t *testing.T) {
	testRestoreOldPrimary(t, restoreUsingRestart)
}

func restoreOldPrimaryInPlace(t *testing.T) {
	testRestoreOldPrimary(t, restoreInPlace)
}

// Test that a former primary replicates correctly after being restored.
//
// - Take a backup.
// - Reparent from old primary to new primary.
// - Force old primary to restore from a previous backup using restore_method.
//
// Args:
// restore_method: function accepting one parameter of type tablet.Tablet,
// this function is called to force a restore on the provided tablet
func testRestoreOldPrimary(t *testing.T, method restoreMethod) {
	ctx := t.Context()

	// insert data on primary, wait for replica to get it
	verifyInitialReplication(t)

	// backup the replica
	err := vitessCluster.Vtctld().ExecuteCommand(ctx, "Backup", replica1.Alias())
	require.NoError(t, err)

	verifyTabletBackupStats(t, getTabletVars(t, replica1))

	//  insert more data on the primary
	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test2')")
	require.NoError(t, err)

	// reparent to replica1
	err = vitessCluster.Vtctld().ExecuteCommand(ctx, "PlannedReparentShard",
		"--new-primary", replica1.Alias(), shardKsName)
	require.NoError(t, err)

	// insert more data to new primary
	_, err = replica1.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test3')")
	require.NoError(t, err)

	// force the old primary to restore at the latest backup.
	method(t, primary)

	// wait for it to catch up.
	verifyRowsInTablet(t, primary, 3)

	verifyTabletRestoreStats(t, getTabletVars(t, primary))

	// teardown
	restartPrimaryAndReplica(t)
}

func restoreUsingRestart(t *testing.T, tablet *vitesst.Tablet) {
	err := tablet.StopVttablet(t.Context())
	require.NoError(t, err)
	verifyRestoreTablet(t, tablet, "SERVING")
}

func restoreInPlace(t *testing.T, tablet *vitesst.Tablet) {
	err := vitessCluster.Vtctld().ExecuteCommand(t.Context(), "RestoreFromBackup", tablet.Alias())
	require.NoError(t, err)
}

func restartPrimaryAndReplica(t *testing.T) {
	ctx := t.Context()

	// Stop all primary, replica tablet and mysql instance
	stopAllTablets(t)

	// remove all backups
	require.NoError(t, vitessCluster.RemoveAllBackups(ctx, keyspaceName, shardName))

	// Start all tablet and mysql instances again. The data directories live on a
	// tmpfs, so restarting a container gives it a freshly initialized mysqld and
	// a vttablet that registers itself in the topology again.
	for _, tablet := range []*vitesst.Tablet{primary, replica1} {
		require.NoError(t, tablet.KillContainer(ctx))
		require.NoError(t, tablet.StartContainer(ctx))
	}
	err := initShardPrimary(t)
	require.NoError(t, err)
}

// initShardPrimary elects the shard's original primary tablet again.
func initShardPrimary(t *testing.T) error {
	return vitessCluster.Vtctld().ExecuteCommand(t.Context(),
		"InitShardPrimary", "--force", "--wait-replicas-timeout", "31s", shardKsName, primary.Alias())
}

func stopAllTablets(t *testing.T) {
	ctx := t.Context()
	for _, tablet := range []*vitesst.Tablet{primary, replica1, replica2} {
		if err := tablet.StopVttablet(ctx); err != nil {
			t.Logf("stopping vttablet %s: %v", tablet.Alias(), err)
		}
		if err := vitessCluster.Vtctld().ExecuteCommand(ctx, "DeleteTablets", "--allow-primary", tablet.Alias()); err != nil {
			t.Logf("deleting tablet %s: %v", tablet.Alias(), err)
		}
	}
}

func terminatedRestore(t *testing.T) {
	ctx := t.Context()

	// insert data on primary, wait for replica to get it
	verifyInitialReplication(t)

	waitForTabletType(t, replica1, topodata.TabletType_REPLICA)
	terminateBackup(t, replica1.Alias())
	// If backup fails then the tablet type goes back to original type.
	waitForTabletType(t, replica1, topodata.TabletType_REPLICA)

	// backup the replica
	err := vitessCluster.Vtctld().ExecuteCommand(ctx, "Backup", replica1.Alias())
	require.NoError(t, err)
	waitForTabletType(t, replica1, topodata.TabletType_REPLICA)

	verifyTabletBackupStats(t, getTabletVars(t, replica1))

	//  insert more data on the primary
	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test2')")
	require.NoError(t, err)

	// reparent to replica1
	err = vitessCluster.Vtctld().ExecuteCommand(ctx, "PlannedReparentShard",
		"--new-primary", replica1.Alias(), shardKsName)
	require.NoError(t, err)

	// insert more data to new primary
	_, err = replica1.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test3')")
	require.NoError(t, err)

	waitForTabletType(t, primary, topodata.TabletType_REPLICA)
	terminateRestore(t)
	// If restore fails then the tablet type goes back to original type.
	waitForTabletType(t, primary, topodata.TabletType_REPLICA)

	err = vitessCluster.Vtctld().ExecuteCommand(ctx, "RestoreFromBackup", primary.Alias())
	require.NoError(t, err)
	waitForTabletType(t, primary, topodata.TabletType_REPLICA)

	assert.False(t, restoreInProgress(t, primary))

	verifyRowsInTablet(t, primary, 3)

	verifyTabletRestoreStats(t, getTabletVars(t, primary))

	stopAllTablets(t)
}

// restoreInProgress reports whether a tablet carries the restore_in_progress
// marker file in its tablet directory.
func restoreInProgress(t *testing.T, tablet *vitesst.Tablet) bool {
	exitCode, _, err := tablet.Exec(t.Context(), "test", "-e", path.Join(tablet.TabletDir(), "restore_in_progress"))
	require.NoError(t, err)
	return exitCode == 0
}

func doNotDemoteNewlyPromotedPrimaryIfReparentingDuringBackup(t *testing.T) {
	ctx := t.Context()

	// The backup and the reparent both land on replica1. xtrabackup locks the
	// instance for backup as its first step, and MySQL refuses the STOP REPLICA
	// that the promotion runs while that lock is held. Holding the backup at its
	// start keeps the reparent ahead of the lock: the backup is in flight for the
	// whole reparent and completes on the tablet the reparent has promoted, which
	// is the sequence this test is about.
	require.NoError(t, holdXtrabackup(ctx, replica1))

	var wg sync.WaitGroup
	wg.Add(2)

	// Start the backup on a replica
	go func() {
		defer wg.Done()
		// ensure this is a primary first
		waitForTabletType(t, primary, topodata.TabletType_PRIMARY)

		// now backup
		err := vitessCluster.Vtctld().ExecuteCommand(ctx, "Backup", replica1.Alias())
		if !assert.NoError(t, err) {
			return
		}
	}()

	// Perform a graceful reparent operation
	go func() {
		defer wg.Done()
		// the backup runs once the reparent is done, whether it succeeded or not
		defer func() {
			assert.NoError(t, releaseXtrabackup(ctx, replica1))
		}()

		// ensure this is a primary first
		waitForTabletType(t, primary, topodata.TabletType_PRIMARY)

		// now reparent
		_, err := vitessCluster.Vtctld().ExecuteCommandWithOutput(
			ctx,
			"PlannedReparentShard",
			"--new-primary", replica1.Alias(),
			fmt.Sprintf("%s/%s", keyspaceName, shardName),
		)
		if !assert.NoError(t, err) {
			return
		}

		// check that we reparented
		waitForTabletType(t, replica1, topodata.TabletType_PRIMARY)
	}()

	wg.Wait()

	// check that this is still a primary
	waitForTabletType(t, replica1, topodata.TabletType_PRIMARY)
}

// test_backup will:
// - create a shard with primary and replica1 only
// - run InitShardPrimary
// - bring up tablet_replica2 concurrently, telling it to wait for a backup
// - insert some data
// - take a backup
// - insert more data on the primary
// - wait for tablet_replica2 to become SERVING
// - check all data is right (before+after backup data)
// - list the backup, remove it
//
// Args:
// tablet_type: 'replica' or 'rdonly'.
func vtctlBackup(t *testing.T, tabletType string) {
	ctx := t.Context()

	// StopReplication on replica1. We verify that the replication works fine later in
	// verifyInitialReplication. So this will also check that VTOrc is running.
	err := vitessCluster.Vtctld().ExecuteCommand(ctx, "StopReplication", replica1.Alias())
	require.NoError(t, err)

	verifyInitialReplication(t)
	restoreWaitForBackup(t, tabletType, nil, true)

	err = vitessCluster.Vtctld().ExecuteCommand(ctx, "Backup", replica1.Alias())
	require.NoError(t, err)

	backups := verifyBackupCount(t, 1)

	verifyTabletBackupStats(t, getTabletVars(t, replica1))

	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test2')")
	require.NoError(t, err)

	err = replica2.WaitForTabletStatus(ctx, 25*time.Second, "SERVING")
	require.NoError(t, err)
	verifyRowsInTablet(t, replica2, 2)

	verifyAfterRemovingBackupNoBackupShouldBePresent(t, backups)
	err = replica2.StopVttablet(ctx)
	require.NoError(t, err)

	err = vitessCluster.Vtctld().ExecuteCommand(ctx, "DeleteTablets", replica2.Alias())
	require.NoError(t, err)
	_, err = primary.QueryTablet(ctx, "DROP TABLE vt_insert_test")
	require.NoError(t, err)
}

func initTestTableOnPrimary(t *testing.T) {
	ctx := t.Context()
	_, err := primary.QueryTablet(ctx, "DROP TABLE IF EXISTS vt_insert_test")
	require.NoError(t, err)
	_, err = primary.QueryTablet(ctx, vtInsertTest)
	require.NoError(t, err)
}

// This will create schema in primary, insert some data to primary and verify the same data in replica
func verifyInitialReplication(t *testing.T) {
	initTestTableOnPrimary(t)
	_, err := primary.QueryTablet(t.Context(), "insert into vt_insert_test (msg) values ('test1')")
	require.NoError(t, err)
	verifyRowsInTablet(t, replica1, 1)
}

// Bring up another replica concurrently, telling it to wait until a backup
// is available instead of starting up empty.
//
// Override the backup engine implementation to a non-existent one for restore.
// This setting should only matter for taking new backups. We should be able
// to restore a previous backup successfully regardless of this setting.
func restoreWaitForBackup(t *testing.T, tabletType string, cDetails *CompressionDetails, fakeImpl bool) {
	ctx := t.Context()

	replica2Type = tabletType
	require.NoError(t, replica2.StopVttablet(ctx))
	require.NoError(t, emptyTabletDatabase(ctx, replica2))

	replicaTabletArgs := slices.Clone(commonTabletArg)
	if cDetails != nil {
		replicaTabletArgs = updateCompressorArgs(replicaTabletArgs, cDetails)
	}
	if fakeImpl {
		replicaTabletArgs = append(replicaTabletArgs, "--backup-engine-implementation", "fake_implementation")
	}
	replicaTabletArgs = append(replicaTabletArgs, "--wait-for-backup-interval", "1s")
	replicaTabletArgs = append(replicaTabletArgs, "--init-tablet-type", tabletType)
	replicaTabletArgs = append(replicaTabletArgs, mysqlShellTabletArgs(currentSetupType, replica2.UID)...)

	err := replica2.StartVttablet(ctx, replicaTabletArgs...)
	require.NoError(t, err)
}

func RemoveBackup(t *testing.T, backupName string) {
	err := vitessCluster.Vtctld().ExecuteCommand(t.Context(), "RemoveBackup", shardKsName, backupName)
	require.NoError(t, err)
}

func verifyAfterRemovingBackupNoBackupShouldBePresent(t *testing.T, backups []string) {
	// Remove the backup
	for _, backup := range backups {
		RemoveBackup(t, backup)
	}

	// Now, there should not be no backup
	verifyBackupCount(t, 0)
}

// verifyBackupCount asserts the number of backups stored for the shard.
func verifyBackupCount(t *testing.T, expected int) []string {
	backups, err := vitessCluster.ListBackups(t.Context(), keyspaceName, shardName)
	require.NoError(t, err)
	assert.Equalf(t, expected, len(backups), "invalid number of backups")
	return backups
}

// verifyRowsInTablet verifies the total number of rows in the test table. This
// is used to check that replication has caught up with the changes on primary.
func verifyRowsInTablet(t *testing.T, tablet *vitesst.Tablet, expectedRows int) {
	ctx := t.Context()

	// The loop holds on to its connection: reconnecting on every poll burns
	// through the host's ephemeral ports. A tablet whose mysqld restarts under
	// the connection reports an error, which drops the connection and reopens
	// it on the next round.
	var conn *mysql.Conn
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	deadline := time.Now().Add(1 * time.Minute)
	lastNumRowsFound := 0
	for time.Now().Before(deadline) {
		// ignoring the error check, if the newly created table is not replicated, then there might be error and we should ignore it
		// but eventually it will catch up and if not caught up in required time, testcase will fail
		if conn == nil {
			conn, _ = vitesst.GetMySQLConn(ctx, tablet, dbName)
		}
		if conn != nil {
			qr, err := conn.ExecuteFetch("select * from vt_insert_test", 10000, true)
			switch {
			case err != nil:
				conn.Close()
				conn = nil
			case len(qr.Rows) == expectedRows:
				return
			default:
				lastNumRowsFound = len(qr.Rows)
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.Equalf(t, expectedRows, lastNumRowsFound, "unexpected number of rows in %s (%s.vt_insert_test)", tablet.Alias(), keyspaceName)
}

func verifyRestoreTablet(t *testing.T, tablet *vitesst.Tablet, status string) {
	ctx := t.Context()

	require.NoError(t, tablet.StopVttablet(ctx))
	require.NoError(t, emptyTabletDatabase(ctx, tablet))
	require.NoError(t, tablet.StartVttablet(ctx))
	if status != "" {
		err := tablet.WaitForTabletStatus(ctx, 25*time.Second, status)
		require.NoError(t, err)
	}
	// We restart replication here because semi-sync will not be set correctly on tablet startup since
	// we deprecated enable_semi_sync. StartReplication RPC fixes the semi-sync settings by consulting the
	// durability policies set.
	err := vitessCluster.Vtctld().ExecuteCommand(ctx, "StopReplication", tablet.Alias())
	require.NoError(t, err)
	err = vitessCluster.Vtctld().ExecuteCommand(ctx, "StartReplication", tablet.Alias())
	require.NoError(t, err)

	switch tabletType(tablet) {
	case "replica":
		verifySemiSyncStatus(t, tablet, "ON")
	case "rdonly":
		verifySemiSyncStatus(t, tablet, "OFF")
	}
}

// backupAlias returns the tablet alias in the zero-padded form that backup
// names carry, e.g. "zone1-0000000100".
func backupAlias(tablet *vitesst.Tablet) string {
	return fmt.Sprintf("%s-%010d", tablet.Cell, tablet.UID)
}

// tabletType returns the tablet type a tablet's vttablet last started with.
func tabletType(tablet *vitesst.Tablet) string {
	if tablet == replica2 && replica2Type != "" {
		return replica2Type
	}
	return tablet.Type()
}

func verifySemiSyncStatus(t *testing.T, tablet *vitesst.Tablet, expectedStatus string) {
	ctx := t.Context()

	conn, err := vitesst.GetMySQLConn(ctx, tablet, dbName)
	require.NoError(t, err)
	defer conn.Close()

	semisyncType, err := conn.SemiSyncExtensionLoaded()
	require.NoError(t, err)
	switch semisyncType {
	case mysql.SemiSyncTypeSource:
		status, err := getDBSystemValue(t, tablet, "variables", "rpl_semi_sync_replica_enabled")
		require.NoError(t, err)
		assert.Equal(t, status, expectedStatus)
		status, err = getDBSystemValue(t, tablet, "status", "rpl_semi_sync_replica_status")
		require.NoError(t, err)
		assert.Equal(t, status, expectedStatus)
	case mysql.SemiSyncTypeMaster:
		status, err := getDBSystemValue(t, tablet, "variables", "rpl_semi_sync_slave_enabled")
		require.NoError(t, err)
		assert.Equal(t, status, expectedStatus)
		status, err = getDBSystemValue(t, tablet, "status", "rpl_semi_sync_slave_status")
		require.NoError(t, err)
		assert.Equal(t, status, expectedStatus)
	}
}

// getDBSystemValue returns the first matching database variable's or status'
// value on a tablet's mysqld.
func getDBSystemValue(t *testing.T, tablet *vitesst.Tablet, placeholder, value string) (string, error) {
	output, err := tablet.QueryTablet(t.Context(), fmt.Sprintf("show %s like '%s'", placeholder, value))
	if err != nil || output.Rows == nil {
		return "", err
	}
	if len(output.Rows) > 0 {
		return output.Rows[0][1].ToString(), nil
	}
	return "", nil
}

// confirmDataDirHasNoGlobalPerms fails the test when a tablet's data directory
// carries files that anyone on the system may read, write or execute.
func confirmDataDirHasNoGlobalPerms(t *testing.T, tablet *vitesst.Tablet) {
	ctx := t.Context()
	datadir := tablet.TabletDir()

	allowedFiles := []string{
		// These are intentionally created with the world/other read bit set by mysqld itself
		// during the --initialize[-insecure] step.
		// See: https://dev.mysql.com/doc/mysql-security-excerpt/en/creating-ssl-rsa-files-using-mysql.html
		// "On Unix and Unix-like systems, the file access mode is 644 for certificate files
		// (that is, world readable) and 600 for key files (that is, accessible only by the
		// account that runs the server)."
		path.Join("data", "ca.pem"),
		path.Join("data", "client-cert.pem"),
		path.Join("data", "public_key.pem"),
		path.Join("data", "server-cert.pem"),
		// The domain socket must have global perms for anyone to use it.
		"mysql.sock",
		// These files are created by xtrabackup.
		path.Join("tmp", "xtrabackup_checkpoints"),
		path.Join("tmp", "xtrabackup_info"),
	}

	exitCode, _, err := tablet.Exec(ctx, "test", "-d", datadir)
	require.NoError(t, err)
	if exitCode != 0 {
		t.Logf("Data directory %s no longer exists, skipping permissions check", datadir)
		return
	}

	exitCode, output, err := tablet.Exec(ctx, "find", datadir, "-perm", "/o+rwx", "-printf", "%p (%M)\n")
	require.NoError(t, err)
	require.Zerof(t, exitCode, "Error walking directory: %s", output)

	var matches []string
	for line := range strings.SplitSeq(strings.TrimSpace(output), "\n") {
		if line == "" {
			continue
		}
		file, _, _ := strings.Cut(line, " (")
		if slices.ContainsFunc(allowedFiles, func(name string) bool { return strings.HasSuffix(file, name) }) {
			continue
		}
		matches = append(matches, line)
	}

	require.Empty(t, matches, "Found files with global permissions: %s\n", strings.Join(matches, "\n"))
}

// disableVTOrcRecoveries stops VTOrc from running any recoveries.
func disableVTOrcRecoveries(t *testing.T) {
	setVTOrcRecoveries(t, "/api/disable-global-recoveries")
}

// enableVTOrcRecoveries allows VTOrc to run any recoveries.
func enableVTOrcRecoveries(t *testing.T) {
	setVTOrcRecoveries(t, "/api/enable-global-recoveries")
}

func setVTOrcRecoveries(t *testing.T, apiPath string) {
	vtorc := vitessCluster.VTOrc()
	require.NotNil(t, vtorc)

	status, _, err := vtorc.MakeAPICallRetry(t.Context(), apiPath, topoConsistencyTimeout, func(status int, body string) bool {
		return status == 200
	})
	require.NoError(t, err)
	require.Equal(t, 200, status)
}

func terminateBackup(t *testing.T, alias string) {
	stopBackupMsg := "Completed backing up"
	if currentSetupType == XtraBackup {
		stopBackupMsg = "Starting backup with"
	}

	found := signalVtctldClient(t, stopBackupMsg, nil, "Backup", alias)
	assert.True(t, found, "backup message not found")
}

func terminateRestore(t *testing.T) {
	stopRestoreMsg := "Copying file 10"
	if currentSetupType == XtraBackup {
		stopRestoreMsg = "Restore: Preparing"
	}

	onMessage := func(t *testing.T) {
		if !restoreInProgress(t, primary) {
			assert.Fail(t, "restore in progress file missing")
		}
	}

	found := signalVtctldClient(t, stopRestoreMsg, onMessage, "RestoreFromBackup", primary.Alias())
	assert.True(t, found, "Restore message not found")
}

// signalVtctldClient runs a vtctldclient command in the background inside the
// vtctld container, waits for the given message in its output, and then
// terminates it. It reports whether the message showed up.
func signalVtctldClient(t *testing.T, message string, onMessage func(t *testing.T), args ...string) bool {
	ctx := t.Context()
	vtctld := vitessCluster.Vtctld()

	start := fmt.Sprintf("rm -f %[1]s %[2]s; vtctldclient --server %[3]s %[4]s > %[1]s 2>&1 & echo $! > %[2]s",
		terminateOutputFile, terminatePIDFile, vtctldServer, strings.Join(args, " "))
	_, output, err := vtctld.Exec(ctx, "bash", "-c", start)
	require.NoErrorf(t, err, "starting vtctldclient: %s", output)

	deadline := time.Now().Add(terminateMessageTimeout)
	for time.Now().Before(deadline) {
		content, err := vtctld.ReadFile(ctx, terminateOutputFile)
		if err == nil && strings.Contains(content, message) {
			if onMessage != nil {
				onMessage(t)
			}
			_, output, err := vtctld.Exec(ctx, "bash", "-c", "kill -TERM $(cat "+terminatePIDFile+")")
			require.NoErrorf(t, err, "terminating vtctldclient: %s", output)
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// readManifestFile reads a backup's MANIFEST from the shared backup storage.
func readManifestFile(t *testing.T, backupName string) (manifest *mysqlctl.BackupManifest) {
	// reading manifest
	fullPath := path.Join(backupStorageRoot, keyspaceName, shardName, backupName, "MANIFEST")
	data, err := vitessCluster.Vtctld().ReadFile(t.Context(), fullPath)
	require.NoErrorf(t, err, "error while reading MANIFEST %v", err)

	// parsing manifest
	err = json.Unmarshal([]byte(data), &manifest)
	require.NoErrorf(t, err, "error while parsing MANIFEST %v", err)
	require.NotNil(t, manifest)
	return manifest
}

func verifyTabletBackupStats(t *testing.T, vars map[string]any) {
	switch currentSetupType {
	// Currently only the builtin backup engine instruments bytes-processed counts.
	case BuiltinBackup:
		require.Contains(t, vars, "BackupBytes")
		bb := vars["BackupBytes"].(map[string]any)
		require.Contains(t, bb, "BackupEngine.Builtin.Compressor:Write")
		require.Contains(t, bb, "BackupEngine.Builtin.Destination:Write")
		require.Contains(t, bb, "BackupEngine.Builtin.Source:Read")
		if backupstorage.BackupStorageImplementation == "file" {
			require.Contains(t, bb, "BackupStorage.File.File:Write")
		}
	}

	require.Contains(t, vars, "BackupCount")
	bc := vars["BackupCount"].(map[string]any)
	require.Contains(t, bc, "-.-.Backup")

	switch currentSetupType {
	// Currently only the builtin backup engine instruments bytes-processed counts.
	case BuiltinBackup:
		require.Contains(t, bc, "BackupEngine.Builtin.Compressor:Close")
		require.Contains(t, bc, "BackupEngine.Builtin.Destination:Close")
		require.Contains(t, bc, "BackupEngine.Builtin.Destination:Open")
		require.Contains(t, bc, "BackupEngine.Builtin.Source:Close")
		require.Contains(t, bc, "BackupEngine.Builtin.Source:Open")
	}

	require.Contains(t, vars, "BackupDurationNanoseconds")
	bd := vars["BackupDurationNanoseconds"]
	require.Contains(t, bd, "-.-.Backup")

	switch currentSetupType {
	// Currently only the builtin backup engine emits timings.
	case BuiltinBackup:
		require.Contains(t, bd, "BackupEngine.Builtin.Compressor:Close")
		require.Contains(t, bd, "BackupEngine.Builtin.Compressor:Write")
		require.Contains(t, bd, "BackupEngine.Builtin.Destination:Close")
		require.Contains(t, bd, "BackupEngine.Builtin.Destination:Open")
		require.Contains(t, bd, "BackupEngine.Builtin.Destination:Write")
		require.Contains(t, bd, "BackupEngine.Builtin.Source:Close")
		require.Contains(t, bd, "BackupEngine.Builtin.Source:Open")
		require.Contains(t, bd, "BackupEngine.Builtin.Source:Read")
	}

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
	// Currently only the builtin backup engine instruments bytes-processed
	// counts.

	verifyRestorePositionAndTimeStats(t, vars)

	switch currentSetupType {
	case BuiltinBackup:
		require.Contains(t, vars, "RestoreBytes")
		bb := vars["RestoreBytes"].(map[string]any)
		require.Contains(t, bb, "BackupEngine.Builtin.Decompressor:Read")
		require.Contains(t, bb, "BackupEngine.Builtin.Destination:Write")
		require.Contains(t, bb, "BackupEngine.Builtin.Source:Read")
		require.Contains(t, bb, "BackupStorage.File.File:Read")
	}

	require.Contains(t, vars, "RestoreCount")
	bc := vars["RestoreCount"].(map[string]any)
	require.Contains(t, bc, "-.-.Restore")

	switch currentSetupType {
	// Currently only the builtin backup engine emits operation counts.
	case BuiltinBackup:
		require.Contains(t, bc, "BackupEngine.Builtin.Decompressor:Close")
		require.Contains(t, bc, "BackupEngine.Builtin.Destination:Close")
		require.Contains(t, bc, "BackupEngine.Builtin.Destination:Open")
		require.Contains(t, bc, "BackupEngine.Builtin.Source:Close")
		require.Contains(t, bc, "BackupEngine.Builtin.Source:Open")
	}

	require.Contains(t, vars, "RestoreDurationNanoseconds")
	bd := vars["RestoreDurationNanoseconds"]
	require.Contains(t, bd, "-.-.Restore")

	switch currentSetupType {
	// Currently only the builtin backup engine emits timings.
	case BuiltinBackup:
		require.Contains(t, bd, "BackupEngine.Builtin.Decompressor:Close")
		require.Contains(t, bd, "BackupEngine.Builtin.Decompressor:Read")
		require.Contains(t, bd, "BackupEngine.Builtin.Destination:Close")
		require.Contains(t, bd, "BackupEngine.Builtin.Destination:Open")
		require.Contains(t, bd, "BackupEngine.Builtin.Destination:Write")
		require.Contains(t, bd, "BackupEngine.Builtin.Source:Close")
		require.Contains(t, bd, "BackupEngine.Builtin.Source:Open")
		require.Contains(t, bd, "BackupEngine.Builtin.Source:Read")
	}

	require.Contains(t, bd, "BackupStorage.File.File:Read")
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

func setDefaultCommonArgs() { commonTabletArg = getDefaultCommonArgs() }

// fetch the backup engine used on the last backup triggered by the end-to-end tests.
func getBackupEngineOfLastBackup(t *testing.T) string {
	lastBackup := getLastBackup(t)

	manifest := readManifestFile(t, lastBackup)

	return manifest.BackupMethod
}

func getLastBackup(t *testing.T) string {
	backups, err := vitessCluster.ListBackups(t.Context(), keyspaceName, shardName)
	require.NoError(t, err)

	return backups[len(backups)-1]
}

func TestBackupEngineSelector(t *testing.T) {
	ctx := t.Context()

	defer setDefaultCommonArgs()

	// launch the custer with xtrabackup as the default engine
	code, err := LaunchCluster(t, XtraBackup, "xbstream", 0, &CompressionDetails{CompressorEngineName: "pgzip"})
	require.Nilf(t, err, "setup failed with status code %d", code)

	defer TearDownCluster(t)

	disableVTOrcRecoveries(t)
	defer func() {
		enableVTOrcRecoveries(t)
	}()
	verifyInitialReplication(t)

	t.Run("backup with backup-engine=builtin", func(t *testing.T) {
		// first try to backup with an alternative engine (builtin)
		err = vitessCluster.Vtctld().ExecuteCommand(ctx, "Backup", "--allow-primary", "--backup-engine=builtin", primary.Alias())
		require.NoError(t, err)
		engineUsed := getBackupEngineOfLastBackup(t)
		require.Equal(t, "builtin", engineUsed)
	})

	t.Run("backup with backup-engine=xtrabackup", func(t *testing.T) {
		// then try to backup specifying the xtrabackup engine
		err = vitessCluster.Vtctld().ExecuteCommand(ctx, "Backup", "--allow-primary", "--backup-engine=xtrabackup", primary.Alias())
		require.NoError(t, err)
		engineUsed := getBackupEngineOfLastBackup(t)
		require.Equal(t, "xtrabackup", engineUsed)
	})

	t.Run("backup without specifying backup-engine", func(t *testing.T) {
		// check that by default we still use the xtrabackup engine if not specified
		err = vitessCluster.Vtctld().ExecuteCommand(ctx, "Backup", "--allow-primary", primary.Alias())
		require.NoError(t, err)
		engineUsed := getBackupEngineOfLastBackup(t)
		require.Equal(t, "xtrabackup", engineUsed)
	})
}

func TestRestoreAllowedBackupEngines(t *testing.T) {
	ctx := t.Context()

	defer setDefaultCommonArgs()

	backupMsg := "right after xtrabackup backup"

	cDetails := &CompressionDetails{CompressorEngineName: "pgzip"}

	// launch the custer with xtrabackup as the default engine
	code, err := LaunchCluster(t, XtraBackup, "xbstream", 0, cDetails)
	require.Nilf(t, err, "setup failed with status code %d", code)

	defer TearDownCluster(t)

	disableVTOrcRecoveries(t)
	defer func() {
		enableVTOrcRecoveries(t)
	}()
	verifyInitialReplication(t)

	t.Run("generate backups", func(t *testing.T) {
		// lets take two backups, each using a different backup engine
		err = vitessCluster.Vtctld().ExecuteCommand(ctx, "Backup", "--allow-primary", "--backup-engine=builtin", primary.Alias())
		require.NoError(t, err)

		err = vitessCluster.Vtctld().ExecuteCommand(ctx, "Backup", "--allow-primary", "--backup-engine=xtrabackup", primary.Alias())
		require.NoError(t, err)
	})

	//  insert more data on the primary
	_, err = primary.QueryTablet(ctx, fmt.Sprintf("insert into vt_insert_test (msg) values ('%s')", backupMsg))
	require.NoError(t, err)

	t.Run("restore replica and verify data", func(t *testing.T) {
		// now bring up another replica, letting it restore from backup.
		restoreWaitForBackup(t, "replica", cDetails, true)
		err = replica2.WaitForTabletStatus(ctx, timeout, "SERVING")
		require.NoError(t, err)

		// check the new replica has the data
		verifyRowsInTablet(t, replica2, 2)
		result, err := replica2.QueryTablet(ctx,
			fmt.Sprintf("select msg from vt_insert_test where msg='%s'", backupMsg))
		require.NoError(t, err)
		require.Equal(t, backupMsg, result.Named().Row().AsString("msg", ""))
	})

	t.Run("test broken restore", func(t *testing.T) {
		// now lets break the last backup in the shard
		err = vitessCluster.Vtctld().RemoveFile(ctx, path.Join(backupStorageRoot,
			keyspaceName, shardName,
			getLastBackup(t), "backup.xbstream.gz"))
		require.NoError(t, err)

		// and try to restore from it
		err = vitessCluster.Vtctld().ExecuteCommand(ctx, "RestoreFromBackup", replica2.Alias())
		require.Error(t, err) // this should fail
	})

	t.Run("test older working backup", func(t *testing.T) {
		// now we retry but with the first backup
		err = vitessCluster.Vtctld().ExecuteCommand(ctx, "RestoreFromBackup", "--allowed-backup-engines=builtin", replica2.Alias())
		require.NoError(t, err) // this should succeed

		// make sure we are replicating after the restore is done
		err = replica2.WaitForTabletStatus(ctx, timeout, "SERVING")
		require.NoError(t, err)
		verifyRowsInTablet(t, replica2, 2)

		result, err := replica2.QueryTablet(ctx,
			fmt.Sprintf("select msg from vt_insert_test where msg='%s'", backupMsg))
		require.NoError(t, err)
		require.Equal(t, backupMsg, result.Named().Row().AsString("msg", ""))
	})
}

// testRestoreDoneHookOnFailure verifies that the vttablet_restore_done hook
// fires synchronously even when the restore fails and the process exits.
// This is a regression test for a bug where the hook was launched in a
// goroutine that was killed by os.Exit(1) before it could complete.
func testRestoreDoneHookOnFailure(t *testing.T) {
	ctx := t.Context()

	// Ensure a clean cluster state — preceding tests (e.g., terminatedRestore)
	// may have torn down all tablets.
	restartPrimaryAndReplica(t)

	// 1. The spare tablet carries a vttablet_restore_done hook that writes its
	//    environment to a file.

	// 2. Take a backup (using replica1 which is already running).
	err := vitessCluster.Vtctld().ExecuteCommand(ctx, "Backup", replica1.Alias())
	require.NoError(t, err)
	backups := verifyBackupCount(t, 1)
	require.Len(t, backups, 1)

	// 3. Corrupt the backup by overwriting one of its data files with garbage.
	vtctld := vitessCluster.Vtctld()
	backupDir := path.Join(backupStorageRoot, keyspaceName, shardName, backups[0])
	exitCode, output, err := vtctld.Exec(ctx, "bash", "-c",
		fmt.Sprintf("find %s -maxdepth 1 -type f ! -name MANIFEST | head -1", backupDir))
	require.NoError(t, err)
	require.Zerof(t, exitCode, "listing the backup's files: %s", output)

	dataFile := strings.TrimSpace(output)
	require.NotEmpty(t, dataFile, "expected at least one data file to corrupt in backup")
	// Overwrite with deterministic garbage to cause a checksum/read error.
	require.NoError(t, vtctld.WriteFile(ctx, dataFile, "CORRUPTED_FOR_TEST_DO_NOT_USE"))

	// 4. Start replica3 — it will attempt to restore from the corrupted backup.
	//    The restore fails and the process exits with os.Exit(1). The hook
	//    writes its output afresh, so anything an earlier restore left behind
	//    goes first.
	require.NoError(t, replica3.RemoveFile(ctx, hookOutputFile))

	containerLog, err := replica3.Logs(ctx)
	require.NoError(t, err)
	exitsBefore := strings.Count(containerLog, vttabletExitMessage)

	replica3Args := slices.Clone(commonTabletArg)
	replica3Args = append(replica3Args, mysqlShellTabletArgs(currentSetupType, replica3.UID)...)
	// A tablet reports NOT_SERVING while it restores, so the start returns
	// before the restore fails; the exit below is what the test is after.
	if err := replica3.StartVttablet(ctx, replica3Args...); err != nil {
		t.Logf("vttablet on %s did not report a serving state: %v", replica3.Alias(), err)
	}
	require.Eventually(t, func() bool {
		containerLog, err := replica3.Logs(ctx)
		return err == nil && strings.Count(containerLog, vttabletExitMessage) > exitsBefore
	}, 2*time.Minute, time.Second, "expected vttablet to exit due to failed restore")
	require.NoError(t, replica3.StopVttablet(ctx))

	// 5. Verify the hook output file exists and contains the expected env vars.
	//    Before the fix, this file would not exist because the hook goroutine was
	//    killed by os.Exit(1) before it could complete.
	hookOutput, err := replica3.ReadFile(ctx, hookOutputFile)
	require.NoError(t, err, "hook output file must exist — hook should fire synchronously before os.Exit")

	assert.Contains(t, hookOutput, "TM_RESTORE_DATA_ERROR=")
	assert.Contains(t, hookOutput, "TM_RESTORE_DATA_START_TS=")
	assert.Contains(t, hookOutput, "TM_RESTORE_DATA_STOP_TS=")
	assert.Contains(t, hookOutput, "TM_RESTORE_DATA_DURATION=")
	assert.Contains(t, hookOutput, "TABLET_ALIAS=")
	assert.Contains(t, hookOutput, "KEYSPACE="+keyspaceName)
	assert.Contains(t, hookOutput, "SHARD="+shardName)

	// With the backup engine fix, the engine should be reported even on failure.
	switch currentSetupType {
	case BuiltinBackup:
		assert.Contains(t, hookOutput, "TM_RESTORE_DATA_BACKUP_ENGINE=builtin")
	case XtraBackup:
		assert.Contains(t, hookOutput, "TM_RESTORE_DATA_BACKUP_ENGINE=xtrabackup")
	case MySQLShell:
		assert.Contains(t, hookOutput, "TM_RESTORE_DATA_BACKUP_ENGINE=mysqlshell")
	}

	// Clean up: remove all backups so other tests aren't affected.
	require.NoError(t, vitessCluster.RemoveAllBackups(ctx, keyspaceName, shardName))
}
