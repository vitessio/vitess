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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// constants for test variants
const (
	XtraBackup = iota
	BuiltinBackup
	Mysqlctld
	timeout                = time.Duration(60 * time.Second)
	topoConsistencyTimeout = 20 * time.Second
)

var (
	primary       *cluster.Vttablet
	replica1      *cluster.Vttablet
	replica2      *cluster.Vttablet
	replica3      *cluster.Vttablet
	localCluster  *cluster.LocalProcessCluster
	newInitDBFile string
	useXtrabackup bool
	cell          = cluster.DefaultCell

	hostname         = "localhost"
	keyspaceName     = "ks"
	dbPassword       = "VtDbaPass"
	shardKsName      = fmt.Sprintf("%s/%s", keyspaceName, shardName)
	dbCredentialFile string
	shardName        = "0"
	commonTabletArg  = []string{
		"--vreplication_retry_delay", "1s",
		"--degraded_threshold", "5s",
		"--lock_tables_timeout", "5s",
		"--watch_replication_stream",
		"--enable_replication_reporter",
		"--serving_state_grace_period", "1s",
	}

	vtInsertTest = `
		create table vt_insert_test (
			id bigint auto_increment,
			msg varchar(64),
			primary key (id)
			) Engine=InnoDB
		`
	SetupReplica3Tablet func(extraArgs []string) (*cluster.Vttablet, error)
)

type CompressionDetails struct {
	CompressorEngineName            string
	ExternalCompressorCmd           string
	ExternalCompressorExt           string
	ExternalDecompressorCmd         string
	ManifestExternalDecompressorCmd string
}

// LaunchCluster : starts the cluster as per given params.
func LaunchCluster(setupType int, streamMode string, stripes int, cDetails *CompressionDetails) (int, error) {
	localCluster = cluster.NewCluster(cell, hostname)

	// Start topo server
	err := localCluster.StartTopo()
	if err != nil {
		return 1, err
	}

	// Start keyspace
	localCluster.Keyspaces = []cluster.Keyspace{
		{
			Name: keyspaceName,
			Shards: []cluster.Shard{
				{
					Name: shardName,
				},
			},
		},
	}
	shard := &localCluster.Keyspaces[0].Shards[0]

	// Create a new init_db.sql file that sets up passwords for all users.
	// Then we use a db-credentials-file with the passwords.
	// TODO: We could have operated with empty password here. Create a separate test for --db-credentials-file functionality (@rsajwani)
	dbCredentialFile = cluster.WriteDbCredentialToTmp(localCluster.TmpDirectory)
	initDb, _ := os.ReadFile(path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"))
	sql := string(initDb)
	// The original init_db.sql does not have any passwords. Here we update the init file with passwords
	sql, err = utils.GetInitDBSQL(sql, cluster.GetPasswordUpdateSQL(localCluster), "")
	if err != nil {
		return 1, err
	}
	newInitDBFile = path.Join(localCluster.TmpDirectory, "init_db_with_passwords.sql")
	err = os.WriteFile(newInitDBFile, []byte(sql), 0666)
	if err != nil {
		return 1, err
	}

	extraArgs := []string{"--db-credentials-file", dbCredentialFile}
	commonTabletArg = append(commonTabletArg, "--db-credentials-file", dbCredentialFile)

	// Update arguments for xtrabackup
	if setupType == XtraBackup {
		useXtrabackup = true

		xtrabackupArgs := []string{
			"--backup_engine_implementation", "xtrabackup",
			fmt.Sprintf("--xtrabackup_stream_mode=%s", streamMode),
			"--xtrabackup_user=vt_dba",
			fmt.Sprintf("--xtrabackup_stripes=%d", stripes),
			"--xtrabackup_backup_flags", fmt.Sprintf("--password=%s", dbPassword),
		}

		// if streamMode is xbstream, add some additional args to test other xtrabackup flags
		if streamMode == "xbstream" {
			xtrabackupArgs = append(xtrabackupArgs, "--xtrabackup_prepare_flags", "--use-memory=100M")
		}

		commonTabletArg = append(commonTabletArg, xtrabackupArgs...)
	}

	commonTabletArg = append(commonTabletArg, getCompressorArgs(cDetails)...)

	var mysqlProcs []*exec.Cmd
	tabletTypes := map[int]string{
		0: "primary",
		1: "replica",
		2: "rdonly",
		3: "spare",
	}

	createTablet := func(tabletType string) error {
		tablet := localCluster.NewVttabletInstance(tabletType, 0, cell)
		tablet.VttabletProcess = localCluster.VtprocessInstanceFromVttablet(tablet, shard.Name, keyspaceName)
		tablet.VttabletProcess.DbPassword = dbPassword
		tablet.VttabletProcess.ExtraArgs = commonTabletArg
		tablet.VttabletProcess.SupportsBackup = true

		if setupType == Mysqlctld {
			mysqlctldProcess, err := cluster.MysqlCtldProcessInstance(tablet.TabletUID, tablet.MySQLPort, localCluster.TmpDirectory)
			if err != nil {
				return err
			}
			tablet.MysqlctldProcess = *mysqlctldProcess
			tablet.MysqlctldProcess.InitDBFile = newInitDBFile
			tablet.MysqlctldProcess.ExtraArgs = extraArgs
			tablet.MysqlctldProcess.Password = tablet.VttabletProcess.DbPassword
			if err := tablet.MysqlctldProcess.Start(); err != nil {
				return err
			}
			shard.Vttablets = append(shard.Vttablets, tablet)
			return nil
		}

		mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, localCluster.TmpDirectory)
		if err != nil {
			return err
		}
		tablet.MysqlctlProcess = *mysqlctlProcess
		tablet.MysqlctlProcess.InitDBFile = newInitDBFile
		tablet.MysqlctlProcess.ExtraArgs = extraArgs
		proc, err := tablet.MysqlctlProcess.StartProcess()
		if err != nil {
			return err
		}
		mysqlProcs = append(mysqlProcs, proc)

		shard.Vttablets = append(shard.Vttablets, tablet)
		return nil
	}
	for i := 0; i < 4; i++ {
		tabletType := tabletTypes[i]
		if err := createTablet(tabletType); err != nil {
			return 1, err
		}
	}
	for _, proc := range mysqlProcs {
		if err := proc.Wait(); err != nil {
			return 1, err
		}
	}
	primary = shard.Vttablets[0]
	replica1 = shard.Vttablets[1]
	replica2 = shard.Vttablets[2]
	replica3 = shard.Vttablets[3]

	if err := localCluster.VtctlclientProcess.InitTablet(primary, cell, keyspaceName, hostname, shard.Name); err != nil {
		return 1, err
	}
	if err := localCluster.VtctlclientProcess.InitTablet(replica1, cell, keyspaceName, hostname, shard.Name); err != nil {
		return 1, err
	}
	if err := localCluster.VtctlclientProcess.InitTablet(replica2, cell, keyspaceName, hostname, shard.Name); err != nil {
		return 1, err
	}
	vtctldClientProcess := cluster.VtctldClientProcessInstance("localhost", localCluster.VtctldProcess.GrpcPort, localCluster.TmpDirectory)
	_, err = vtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", keyspaceName, "--durability-policy=semi_sync")
	if err != nil {
		return 1, err
	}

	for _, tablet := range []*cluster.Vttablet{primary, replica1, replica2} { // we don't start replica3 yet
		if err := tablet.VttabletProcess.Setup(); err != nil {
			return 1, err
		}
	}

	SetupReplica3Tablet = func(extraArgs []string) (*cluster.Vttablet, error) {
		replica3.VttabletProcess.ExtraArgs = append(replica3.VttabletProcess.ExtraArgs, extraArgs...)
		if err := replica3.VttabletProcess.Setup(); err != nil {
			return replica3, err
		}
		return replica3, nil
	}

	if err := localCluster.VtctldClientProcess.InitShardPrimary(keyspaceName, shard.Name, cell, primary.TabletUID); err != nil {
		return 1, err
	}

	if err := localCluster.StartVTOrc(keyspaceName); err != nil {
		return 1, err
	}

	return 0, nil
}

func getCompressorArgs(cDetails *CompressionDetails) []string {
	var args []string

	if cDetails == nil {
		return args
	}

	if cDetails.CompressorEngineName != "" {
		args = append(args, fmt.Sprintf("--compression-engine-name=%s", cDetails.CompressorEngineName))
	}
	if cDetails.ExternalCompressorCmd != "" {
		args = append(args, fmt.Sprintf("--external-compressor=%s", cDetails.ExternalCompressorCmd))
	}
	if cDetails.ExternalCompressorExt != "" {
		args = append(args, fmt.Sprintf("--external-compressor-extension=%s", cDetails.ExternalCompressorExt))
	}
	if cDetails.ExternalDecompressorCmd != "" {
		args = append(args, fmt.Sprintf("--external-decompressor=%s", cDetails.ExternalDecompressorCmd))
	}
	if cDetails.ManifestExternalDecompressorCmd != "" {
		args = append(args, fmt.Sprintf("--manifest-external-decompressor=%s", cDetails.ManifestExternalDecompressorCmd))
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
func TearDownCluster() {
	localCluster.Teardown()
}

// TestBackup runs all the backup tests
func TestBackup(t *testing.T, setupType int, streamMode string, stripes int, cDetails *CompressionDetails, runSpecific []string) error {
	verStr, err := mysqlctl.GetVersionString()
	require.NoError(t, err)
	_, vers, err := mysqlctl.ParseVersionString(verStr)
	require.NoError(t, err)
	switch streamMode {
	case "xbstream":
		if vers.Major < 8 {
			t.Logf("Skipping xtrabackup tests with --xtrabackup_stream_mode=xbstream as those are only tested on XtraBackup/MySQL 8.0+")
			return nil
		}
	case "", "tar": // streaming method of tar is the default for the vttablet --xtrabackup_stream_mode flag
		// XtraBackup 8.0 must be used with MySQL 8.0 and it no longer supports tar as a stream method:
		//    https://docs.percona.com/percona-xtrabackup/2.4/innobackupex/streaming_backups_innobackupex.html
		//    https://docs.percona.com/percona-xtrabackup/8.0/xtrabackup_bin/backup.streaming.html
		if vers.Major > 5 {
			t.Logf("Skipping xtrabackup tests with --xtrabackup_stream_mode=tar as tar is no longer a streaming option in XtraBackup 8.0")
			return nil
		}
	default:
		require.FailNow(t, fmt.Sprintf("Unsupported xtrabackup stream mode: %s", streamMode))
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
			name:   "DoNotDemoteNewlyPromotedPrimaryIfReparentingDuringBackup",
			method: doNotDemoteNewlyPromotedPrimaryIfReparentingDuringBackup,
		}, //
	}

	defer cluster.PanicHandler(t)
	// setup cluster for the testing
	code, err := LaunchCluster(setupType, streamMode, stripes, cDetails)
	require.Nilf(t, err, "setup failed with status code %d", code)

	// Teardown the cluster
	defer TearDownCluster()

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
	return nil
}

func isRegistered(name string, runlist []string) bool {
	for _, f := range runlist {
		if f == name {
			return true
		}
	}
	return false
}

type restoreMethod func(t *testing.T, tablet *cluster.Vttablet)

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
	// Having the VTOrc in this test causes a lot of flakiness. For example when we delete the tablet `replica2` which
	// is the current primary and then try to restore from backup the old primary (`primary.Alias`), but before that sometimes the VTOrc
	// promotes the `replica1` to primary right after we delete the replica2 (current primary).
	// This can result in unexpected behavior. Therefore, disabling the VTOrc in this test to remove flakiness.
	localCluster.DisableVTOrcRecoveries(t)
	defer func() {
		localCluster.EnableVTOrcRecoveries(t)
	}()
	verifyInitialReplication(t)

	output, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("Backup", primary.Alias)
	require.Error(t, err)
	assert.Contains(t, output, "type PRIMARY cannot take backup. if you really need to do this, rerun the backup command with --allow_primary")

	localCluster.VerifyBackupCount(t, shardKsName, 0)

	err = localCluster.VtctldClientProcess.ExecuteCommand("Backup", "--allow-primary", primary.Alias)
	require.Nil(t, err)

	// We'll restore this on the primary later to test restores using a backup timestamp
	firstBackupTimestamp := time.Now().UTC().Format(mysqlctl.BackupTimestampFormat)

	backups := localCluster.VerifyBackupCount(t, shardKsName, 1)
	assert.Contains(t, backups[0], primary.Alias)

	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	require.Nil(t, err)

	restoreWaitForBackup(t, "replica", nil, true)
	err = replica2.VttabletProcess.WaitForTabletStatusesForTimeout([]string{"SERVING"}, timeout)
	require.Nil(t, err)

	// Verify that we have all the new data -- we should have 2 records now...
	// And only 1 record after we restore using the first backup timestamp
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 2)

	err = localCluster.VtctldClientProcess.ExecuteCommand("Backup", "--allow-primary", primary.Alias)
	require.Nil(t, err)

	backups = localCluster.VerifyBackupCount(t, shardKsName, 2)
	assert.Contains(t, backups[1], primary.Alias)

	verifyTabletBackupStats(t, primary.VttabletProcess.GetVars())

	// Perform PRS to demote the primary tablet (primary) so that we can do a restore there and verify we don't have the
	// data from after the older/first backup
	err = localCluster.VtctldClientProcess.ExecuteCommand("PlannedReparentShard",
		"--new-primary", replica2.Alias, shardKsName)
	require.Nil(t, err)

	// Delete the current primary tablet (replica2) so that the original primary tablet (primary) can be restored from the
	// older/first backup w/o it replicating the subsequent insert done after the first backup was taken
	err = localCluster.VtctldClientProcess.ExecuteCommand("DeleteTablets", "--allow-primary", replica2.Alias)
	require.Nil(t, err)
	err = replica2.VttabletProcess.TearDown()
	require.Nil(t, err)

	// Restore the older/first backup -- using the timestamp we saved -- on the original primary tablet (primary)
	err = localCluster.VtctldClientProcess.ExecuteCommand("RestoreFromBackup", "--backup-timestamp", firstBackupTimestamp, primary.Alias)
	require.Nil(t, err)

	verifyTabletRestoreStats(t, primary.VttabletProcess.GetVars())

	// Re-init the shard -- making the original primary tablet (primary) primary again -- for subsequent tests
	err = localCluster.VtctldClientProcess.InitShardPrimary(keyspaceName, shardName, cell, primary.TabletUID)
	require.Nil(t, err)

	// Verify that we don't have the record created after the older/first backup
	cluster.VerifyRowsInTablet(t, primary, keyspaceName, 1)

	verifyAfterRemovingBackupNoBackupShouldBePresent(t, backups)
	require.Nil(t, err)

	_, err = primary.VttabletProcess.QueryTablet("DROP TABLE vt_insert_test", keyspaceName, true)
	require.Nil(t, err)
}

// Test a primary and replica from the same backup.
//
// Check that a replica and primary both restored from the same backup
// can replicate successfully.
func primaryReplicaSameBackup(t *testing.T) {
	// insert data on primary, wait for replica to get it
	verifyInitialReplication(t)

	// backup the replica
	err := localCluster.VtctldClientProcess.ExecuteCommand("Backup", replica1.Alias)
	require.Nil(t, err)

	verifyTabletBackupStats(t, replica1.VttabletProcess.GetVars())

	//  insert more data on the primary
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	require.Nil(t, err)

	// now bring up the other replica, letting it restore from backup.
	restoreWaitForBackup(t, "replica", nil, true)
	err = replica2.VttabletProcess.WaitForTabletStatusesForTimeout([]string{"SERVING"}, timeout)
	require.Nil(t, err)

	// check the new replica has the data
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 2)

	// Promote replica2 to primary
	err = localCluster.VtctldClientProcess.ExecuteCommand("PlannedReparentShard",
		"--new-primary", replica2.Alias, shardKsName)
	require.Nil(t, err)

	// insert more data on replica2 (current primary)
	_, err = replica2.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test3')", keyspaceName, true)
	require.Nil(t, err)

	// Force replica1 to restore from backup.
	verifyRestoreTablet(t, replica1, "SERVING")

	// wait for replica1 to catch up.
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 3)

	// This is to test that replicationPosition is processed correctly
	// while doing backup/restore after a reparent.
	// It is written into the MANIFEST and read back from the MANIFEST.
	//
	// Take another backup on the replica.
	err = localCluster.VtctldClientProcess.ExecuteCommand("Backup", replica1.Alias)
	require.Nil(t, err)

	// Insert more data on replica2 (current primary).
	_, err = replica2.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test4')", keyspaceName, true)
	require.Nil(t, err)

	// Force replica1 to restore from backup.
	verifyRestoreTablet(t, replica1, "SERVING")

	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 4)
	err = replica2.VttabletProcess.TearDown()
	require.Nil(t, err)
	restartPrimaryAndReplica(t)
}

// Test a primary and replica from the same backup.
//
// Check that a replica and primary both restored from the same backup
// We change compression alogrithm in between but it should not break any restore functionality
func primaryReplicaSameBackupModifiedCompressionEngine(t *testing.T) {
	// insert data on primary, wait for replica to get it
	verifyInitialReplication(t)

	// TODO: The following Sleep in introduced as it seems like the previous step doesn't fully complete, causing
	// this test to be flaky. Sleep seems to solve the problem. Need to fix this in a better way and Wait for
	// previous test to complete (suspicion: MySQL does not fully start)
	time.Sleep(5 * time.Second)

	// backup the replica
	err := localCluster.VtctldClientProcess.ExecuteCommand("Backup", replica1.Alias)
	require.Nil(t, err)

	verifyTabletBackupStats(t, replica1.VttabletProcess.GetVars())

	//  insert more data on the primary
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	require.Nil(t, err)

	// now bring up the other replica, with change in compression engine
	// this is to verify that restore will read engine name from manifest instead of reading the new values
	cDetails := &CompressionDetails{
		CompressorEngineName:    "pgzip",
		ExternalCompressorCmd:   "gzip -c",
		ExternalCompressorExt:   ".gz",
		ExternalDecompressorCmd: "",
	}
	restoreWaitForBackup(t, "replica", cDetails, false)
	err = replica2.VttabletProcess.WaitForTabletStatusesForTimeout([]string{"SERVING"}, timeout)
	require.Nil(t, err)

	// check the new replica has the data
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 2)

	// Promote replica2 to primary
	err = localCluster.VtctldClientProcess.ExecuteCommand("PlannedReparentShard",
		"--new-primary", replica2.Alias, shardKsName)
	require.Nil(t, err)

	// insert more data on replica2 (current primary)
	_, err = replica2.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test3')", keyspaceName, true)
	require.Nil(t, err)

	// Force replica1 to restore from backup.
	verifyRestoreTablet(t, replica1, "SERVING")

	// wait for replica1 to catch up.
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 3)

	// Promote replica1 to primary
	err = localCluster.VtctldClientProcess.ExecuteCommand("PlannedReparentShard",
		"--new-primary", replica1.Alias, shardKsName)
	require.Nil(t, err)

	// Insert more data on replica1 (current primary).
	_, err = replica1.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test4')", keyspaceName, true)
	require.Nil(t, err)

	// wait for replica2 to catch up.
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 4)

	// Now take replica2 backup with gzip (new compressor)
	err = localCluster.VtctldClientProcess.ExecuteCommand("Backup", replica2.Alias)
	require.Nil(t, err)

	verifyTabletBackupStats(t, replica2.VttabletProcess.GetVars())

	// Force replica2 to restore from backup.
	verifyRestoreTablet(t, replica2, "SERVING")
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 4)
	err = replica2.VttabletProcess.TearDown()
	require.Nil(t, err)
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
	// insert data on primary, wait for replica to get it
	verifyInitialReplication(t)

	// TODO: The following Sleep in introduced as it seems like the previous step doesn't fully complete, causing
	// this test to be flaky. Sleep seems to solve the problem. Need to fix this in a better way and Wait for
	// previous test to complete (suspicion: MySQL does not fully start)
	time.Sleep(5 * time.Second)

	// backup the replica
	err := localCluster.VtctldClientProcess.ExecuteCommand("Backup", replica1.Alias)
	require.Nil(t, err)

	verifyTabletBackupStats(t, replica1.VttabletProcess.GetVars())

	//  insert more data on the primary
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	require.Nil(t, err)

	// reparent to replica1
	err = localCluster.VtctldClientProcess.ExecuteCommand("PlannedReparentShard",
		"--new-primary", replica1.Alias, shardKsName)
	require.Nil(t, err)

	// insert more data to new primary
	_, err = replica1.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test3')", keyspaceName, true)
	require.Nil(t, err)

	// force the old primary to restore at the latest backup.
	method(t, primary)

	// wait for it to catch up.
	cluster.VerifyRowsInTablet(t, primary, keyspaceName, 3)

	verifyTabletRestoreStats(t, primary.VttabletProcess.GetVars())

	// teardown
	restartPrimaryAndReplica(t)
}

func restoreUsingRestart(t *testing.T, tablet *cluster.Vttablet) {
	err := tablet.VttabletProcess.TearDown()
	require.Nil(t, err)
	verifyRestoreTablet(t, tablet, "SERVING")
}

func restoreInPlace(t *testing.T, tablet *cluster.Vttablet) {
	err := localCluster.VtctldClientProcess.ExecuteCommand("RestoreFromBackup", tablet.Alias)
	require.Nil(t, err)
}

func restartPrimaryAndReplica(t *testing.T) {
	// Stop all primary, replica tablet and mysql instance
	stopAllTablets()

	// remove all backups
	localCluster.RemoveAllBackups(t, shardKsName)
	// start all tablet and mysql instances
	var mysqlProcs []*exec.Cmd
	for _, tablet := range []*cluster.Vttablet{primary, replica1, replica2} {
		if tablet.MysqlctldProcess.TabletUID > 0 {
			err := tablet.MysqlctldProcess.Start()
			require.Nilf(t, err, "error while starting mysqlctld, tabletUID %v", tablet.TabletUID)
			continue
		}
		proc, _ := tablet.MysqlctlProcess.StartProcess()
		mysqlProcs = append(mysqlProcs, proc)
	}
	for _, proc := range mysqlProcs {
		proc.Wait()
	}
	for _, tablet := range []*cluster.Vttablet{primary, replica1} {
		err := localCluster.VtctlclientProcess.InitTablet(tablet, cell, keyspaceName, hostname, shardName)
		require.Nil(t, err)
		err = tablet.VttabletProcess.Setup()
		require.Nil(t, err)
	}
	err := localCluster.VtctldClientProcess.InitShardPrimary(keyspaceName, shardName, cell, primary.TabletUID)
	require.Nil(t, err)
}

func stopAllTablets() {
	var mysqlProcs []*exec.Cmd
	for _, tablet := range []*cluster.Vttablet{primary, replica1, replica2} {
		tablet.VttabletProcess.TearDown()
		if tablet.MysqlctldProcess.TabletUID > 0 {
			tablet.MysqlctldProcess.Stop()
			localCluster.VtctldClientProcess.ExecuteCommand("DeleteTablets", "--allow-primary", tablet.Alias)
			continue
		}
		proc, _ := tablet.MysqlctlProcess.StopProcess()
		mysqlProcs = append(mysqlProcs, proc)
		localCluster.VtctldClientProcess.ExecuteCommand("DeleteTablets", "--allow-primary", tablet.Alias)
	}
	for _, proc := range mysqlProcs {
		proc.Wait()
	}
	for _, tablet := range []*cluster.Vttablet{primary, replica1} {
		os.RemoveAll(tablet.VttabletProcess.Directory)
	}
}

func terminatedRestore(t *testing.T) {
	// insert data on primary, wait for replica to get it
	verifyInitialReplication(t)

	// TODO: The following Sleep in introduced as it seems like the previous step doesn't fully complete, causing
	// this test to be flaky. Sleep seems to solve the problem. Need to fix this in a better way and Wait for
	// previous test to complete (suspicion: MySQL does not fully start)
	time.Sleep(5 * time.Second)

	checkTabletType(t, replica1.Alias, topodata.TabletType_REPLICA)
	terminateBackup(t, replica1.Alias)
	// If backup fails then the tablet type goes back to original type.
	checkTabletType(t, replica1.Alias, topodata.TabletType_REPLICA)

	// backup the replica
	err := localCluster.VtctldClientProcess.ExecuteCommand("Backup", replica1.Alias)
	require.Nil(t, err)
	checkTabletType(t, replica1.Alias, topodata.TabletType_REPLICA)

	verifyTabletBackupStats(t, replica1.VttabletProcess.GetVars())

	//  insert more data on the primary
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	require.Nil(t, err)

	// reparent to replica1
	err = localCluster.VtctldClientProcess.ExecuteCommand("PlannedReparentShard",
		"--new-primary", replica1.Alias, shardKsName)
	require.Nil(t, err)

	// insert more data to new primary
	_, err = replica1.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test3')", keyspaceName, true)
	require.Nil(t, err)

	checkTabletType(t, primary.Alias, topodata.TabletType_REPLICA)
	terminateRestore(t)
	// If restore fails then the tablet type goes back to original type.
	checkTabletType(t, primary.Alias, topodata.TabletType_REPLICA)

	err = localCluster.VtctldClientProcess.ExecuteCommand("RestoreFromBackup", primary.Alias)
	require.Nil(t, err)
	checkTabletType(t, primary.Alias, topodata.TabletType_REPLICA)

	_, err = os.Stat(path.Join(primary.VttabletProcess.Directory, "restore_in_progress"))
	assert.True(t, os.IsNotExist(err))

	cluster.VerifyRowsInTablet(t, primary, keyspaceName, 3)

	verifyTabletRestoreStats(t, primary.VttabletProcess.GetVars())

	stopAllTablets()
}

func checkTabletType(t *testing.T, alias string, tabletType topodata.TabletType) {
	t.Helper()
	// for loop for 15 seconds to check if tablet type is correct
	for i := 0; i < 15; i++ {
		output, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("GetTablet", alias)
		require.Nil(t, err)
		var tabletPB topodata.Tablet
		err = json2.Unmarshal([]byte(output), &tabletPB)
		require.NoError(t, err)
		if tabletType == tabletPB.Type {
			return
		}
		time.Sleep(1 * time.Second)
	}
	require.Failf(t, "checkTabletType failed.", "Tablet type is not correct. Expected: %v", tabletType)
}

func doNotDemoteNewlyPromotedPrimaryIfReparentingDuringBackup(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)

	// Start the backup on a replica
	go func() {
		defer wg.Done()
		// ensure this is a primary first
		checkTabletType(t, primary.Alias, topodata.TabletType_PRIMARY)

		// now backup
		err := localCluster.VtctldClientProcess.ExecuteCommand("Backup", replica1.Alias)
		require.Nil(t, err)
	}()

	// Perform a graceful reparent operation
	go func() {
		defer wg.Done()
		// ensure this is a primary first
		checkTabletType(t, primary.Alias, topodata.TabletType_PRIMARY)

		// now reparent
		_, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput(
			"PlannedReparentShard",
			"--new-primary", replica1.Alias,
			fmt.Sprintf("%s/%s", keyspaceName, shardName),
		)
		require.Nil(t, err)

		// check that we reparented
		checkTabletType(t, replica1.Alias, topodata.TabletType_PRIMARY)
	}()

	wg.Wait()

	// check that this is still a primary
	checkTabletType(t, replica1.Alias, topodata.TabletType_PRIMARY)
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
	// StopReplication on replica1. We verify that the replication works fine later in
	// verifyInitialReplication. So this will also check that VTOrc is running.
	err := localCluster.VtctldClientProcess.ExecuteCommand("StopReplication", replica1.Alias)
	require.Nil(t, err)

	verifyInitialReplication(t)
	restoreWaitForBackup(t, tabletType, nil, true)

	err = localCluster.VtctldClientProcess.ExecuteCommand("Backup", replica1.Alias)
	require.Nil(t, err)

	backups := localCluster.VerifyBackupCount(t, shardKsName, 1)

	verifyTabletBackupStats(t, replica1.VttabletProcess.GetVars())

	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	require.Nil(t, err)

	err = replica2.VttabletProcess.WaitForTabletStatusesForTimeout([]string{"SERVING"}, 25*time.Second)
	require.Nil(t, err)
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 2)

	verifyAfterRemovingBackupNoBackupShouldBePresent(t, backups)
	err = replica2.VttabletProcess.TearDown()
	require.Nil(t, err)

	err = localCluster.VtctldClientProcess.ExecuteCommand("DeleteTablets", replica2.Alias)
	require.Nil(t, err)
	_, err = primary.VttabletProcess.QueryTablet("DROP TABLE vt_insert_test", keyspaceName, true)
	require.Nil(t, err)
}

func InitTestTable(t *testing.T) {
	_, err := primary.VttabletProcess.QueryTablet("DROP TABLE IF EXISTS vt_insert_test", keyspaceName, true)
	require.Nil(t, err)
	_, err = primary.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	require.Nil(t, err)
}

// This will create schema in primary, insert some data to primary and verify the same data in replica
func verifyInitialReplication(t *testing.T) {
	InitTestTable(t)
	_, err := primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.Nil(t, err)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 1)
}

// Bring up another replica concurrently, telling it to wait until a backup
// is available instead of starting up empty.
//
// Override the backup engine implementation to a non-existent one for restore.
// This setting should only matter for taking new backups. We should be able
// to restore a previous backup successfully regardless of this setting.
func restoreWaitForBackup(t *testing.T, tabletType string, cDetails *CompressionDetails, fakeImpl bool) {
	replica2.Type = tabletType
	replica2.ValidateTabletRestart(t)
	replicaTabletArgs := commonTabletArg
	if cDetails != nil {
		replicaTabletArgs = updateCompressorArgs(replicaTabletArgs, cDetails)
	}
	if fakeImpl {
		replicaTabletArgs = append(replicaTabletArgs, "--backup_engine_implementation", "fake_implementation")
	}
	replicaTabletArgs = append(replicaTabletArgs, "--wait_for_backup_interval", "1s")
	replicaTabletArgs = append(replicaTabletArgs, "--init_tablet_type", tabletType)
	replica2.VttabletProcess.ExtraArgs = replicaTabletArgs
	replica2.VttabletProcess.ServingStatus = ""
	err := replica2.VttabletProcess.Setup()
	require.Nil(t, err)
}

func RemoveBackup(t *testing.T, backupName string) {
	err := localCluster.VtctldClientProcess.ExecuteCommand("RemoveBackup", shardKsName, backupName)
	require.Nil(t, err)
}

func verifyAfterRemovingBackupNoBackupShouldBePresent(t *testing.T, backups []string) {
	// Remove the backup
	for _, backup := range backups {
		RemoveBackup(t, backup)
	}

	// Now, there should not be no backup
	localCluster.VerifyBackupCount(t, shardKsName, 0)
}

func verifyRestoreTablet(t *testing.T, tablet *cluster.Vttablet, status string) {
	tablet.ValidateTabletRestart(t)
	tablet.VttabletProcess.ServingStatus = ""
	err := tablet.VttabletProcess.Setup()
	require.Nil(t, err)
	if status != "" {
		err = tablet.VttabletProcess.WaitForTabletStatusesForTimeout([]string{status}, 25*time.Second)
		require.Nil(t, err)
	}
	// We restart replication here because semi-sync will not be set correctly on tablet startup since
	// we deprecated enable_semi_sync. StartReplication RPC fixes the semi-sync settings by consulting the
	// durability policies set.
	err = localCluster.VtctldClientProcess.ExecuteCommand("StopReplication", tablet.Alias)
	require.NoError(t, err)
	err = localCluster.VtctldClientProcess.ExecuteCommand("StartReplication", tablet.Alias)
	require.NoError(t, err)

	if tablet.Type == "replica" {
		verifySemiSyncStatus(t, tablet, "ON")
	} else if tablet.Type == "rdonly" {
		verifySemiSyncStatus(t, tablet, "OFF")
	}
}

func verifySemiSyncStatus(t *testing.T, vttablet *cluster.Vttablet, expectedStatus string) {
	status, err := vttablet.VttabletProcess.GetDBVar("rpl_semi_sync_slave_enabled", keyspaceName)
	require.Nil(t, err)
	assert.Equal(t, status, expectedStatus)
	status, err = vttablet.VttabletProcess.GetDBStatus("rpl_semi_sync_slave_status", keyspaceName)
	require.Nil(t, err)
	assert.Equal(t, status, expectedStatus)
}

func terminateBackup(t *testing.T, alias string) {
	stopBackupMsg := "Done taking Backup"
	if useXtrabackup {
		stopBackupMsg = "Starting backup with"
		useXtrabackup = false
		defer func() {
			useXtrabackup = true
		}()
	}

	args := append([]string{"--server", localCluster.VtctldClientProcess.Server, "--alsologtostderr"}, "Backup", alias)
	tmpProcess := exec.Command(
		"vtctldclient",
		args...,
	)

	reader, _ := tmpProcess.StdoutPipe()
	err := tmpProcess.Start()
	require.Nil(t, err)
	found := false

	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		text := scanner.Text()
		if strings.Contains(text, stopBackupMsg) {
			tmpProcess.Process.Signal(syscall.SIGTERM)
			found = true //nolint
			return
		}
	}
	assert.True(t, found, "backup message not found")
}

func terminateRestore(t *testing.T) {
	stopRestoreMsg := "Copying file 10"
	if useXtrabackup {
		stopRestoreMsg = "Restore: Preparing"
		useXtrabackup = false
		defer func() {
			useXtrabackup = true
		}()
	}

	args := append([]string{"--server", localCluster.VtctldClientProcess.Server, "--alsologtostderr"}, "RestoreFromBackup", primary.Alias)
	tmpProcess := exec.Command(
		"vtctldclient",
		args...,
	)

	reader, _ := tmpProcess.StdoutPipe()
	err := tmpProcess.Start()
	require.Nil(t, err)
	found := false

	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		text := scanner.Text()
		if strings.Contains(text, stopRestoreMsg) {
			if _, err := os.Stat(path.Join(primary.VttabletProcess.Directory, "restore_in_progress")); os.IsNotExist(err) {
				assert.Fail(t, "restore in progress file missing")
			}
			tmpProcess.Process.Signal(syscall.SIGTERM)
			found = true
			break
		}
	}
	assert.True(t, found, "Restore message not found")
}

func vtctlBackupReplicaNoDestroyNoWrites(t *testing.T, replicaIndex int) (backups []string) {
	replica := getReplica(t, replicaIndex)
	numBackups := len(waitForNumBackups(t, -1))

	err := localCluster.VtctldClientProcess.ExecuteCommand("Backup", replica.Alias)
	require.Nil(t, err)

	backups = waitForNumBackups(t, numBackups+1)
	require.NotEmpty(t, backups)

	verifyTabletBackupStats(t, replica.VttabletProcess.GetVars())

	return backups
}

func GetReplicaPosition(t *testing.T, replicaIndex int) string {
	replica := getReplica(t, replicaIndex)
	pos, _ := cluster.GetPrimaryPosition(t, *replica, hostname)
	return pos
}

func GetReplicaGtidPurged(t *testing.T, replicaIndex int) string {
	replica := getReplica(t, replicaIndex)
	query := "select @@global.gtid_purged as gtid_purged"
	rs, err := replica.VttabletProcess.QueryTablet(query, keyspaceName, true)
	require.NoError(t, err)
	row := rs.Named().Row()
	require.NotNil(t, row)
	return row.AsString("gtid_purged", "")
}

func InsertRowOnPrimary(t *testing.T, hint string) {
	if hint == "" {
		hint = textutil.RandomHash()[:12]
	}
	query, err := sqlparser.ParseAndBind("insert into vt_insert_test (msg) values (%a)", sqltypes.StringBindVariable(hint))
	require.NoError(t, err)
	_, err = primary.VttabletProcess.QueryTablet(query, keyspaceName, true)
	require.NoError(t, err)
}

func ReadRowsFromTablet(t *testing.T, tablet *cluster.Vttablet) (msgs []string) {
	query := "select msg from vt_insert_test"
	rs, err := tablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
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

func getReplica(t *testing.T, replicaIndex int) *cluster.Vttablet {
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
	for i := 0; i < count; i++ {
		_, err := replica.VttabletProcess.QueryTablet(query, keyspaceName, true)
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
		rs, err := replica.VttabletProcess.QueryTablet(query, keyspaceName, true)
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
		_, err = replica.VttabletProcess.QueryTablet(query, keyspaceName, true)
		require.NoError(t, err)
	}
	return lastBinlog
}

func readManifestFile(t *testing.T, backupLocation string) (manifest *mysqlctl.BackupManifest) {
	// reading manifest
	fullPath := backupLocation + "/MANIFEST"
	data, err := os.ReadFile(fullPath)
	require.NoErrorf(t, err, "error while reading MANIFEST %v", err)

	// parsing manifest
	err = json.Unmarshal(data, &manifest)
	require.NoErrorf(t, err, "error while parsing MANIFEST %v", err)
	require.NotNil(t, manifest)
	return manifest
}

func TestReplicaFullBackup(t *testing.T, replicaIndex int) (manifest *mysqlctl.BackupManifest) {
	backups := vtctlBackupReplicaNoDestroyNoWrites(t, replicaIndex)

	backupLocation := localCluster.CurrentVTDATAROOT + "/backups/" + shardKsName + "/" + backups[len(backups)-1]
	return readManifestFile(t, backupLocation)
}

// waitForNumBackups waits for GetBackups to list exactly the given expected number.
// If expectNumBackups < 0 then any response is considered valid
func waitForNumBackups(t *testing.T, expectNumBackups int) []string {
	ctx, cancel := context.WithTimeout(context.Background(), topoConsistencyTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		backups, err := localCluster.ListBackups(shardKsName)
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

func testReplicaIncrementalBackup(t *testing.T, replica *cluster.Vttablet, incrementalFromPos string, expectEmpty bool, expectError string) (manifest *mysqlctl.BackupManifest, backupName string) {
	numBackups := len(waitForNumBackups(t, -1))

	output, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("Backup", "--incremental-from-pos", incrementalFromPos, replica.Alias)
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

	verifyTabletBackupStats(t, replica.VttabletProcess.GetVars())
	backupName = backups[len(backups)-1]

	backupLocation := localCluster.CurrentVTDATAROOT + "/backups/" + shardKsName + "/" + backupName
	return readManifestFile(t, backupLocation), backupName
}

func TestReplicaIncrementalBackup(t *testing.T, replicaIndex int, incrementalFromPos string, expectEmpty bool, expectError string) (manifest *mysqlctl.BackupManifest, backupName string) {
	replica := getReplica(t, replicaIndex)
	return testReplicaIncrementalBackup(t, replica, incrementalFromPos, expectEmpty, expectError)
}

func TestReplicaFullRestore(t *testing.T, replicaIndex int, expectError string) {
	replica := getReplica(t, replicaIndex)

	output, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("RestoreFromBackup", replica.Alias)
	if expectError != "" {
		require.Errorf(t, err, "expected: %v", expectError)
		require.Contains(t, output, expectError)
		return
	}
	require.NoErrorf(t, err, "output: %v", output)
	verifyTabletRestoreStats(t, replica.VttabletProcess.GetVars())
}

func TestReplicaRestoreToPos(t *testing.T, replicaIndex int, restoreToPos replication.Position, expectError string) {
	replica := getReplica(t, replicaIndex)

	require.False(t, restoreToPos.IsZero())
	restoreToPosArg := replication.EncodePosition(restoreToPos)
	output, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("RestoreFromBackup", "--restore-to-pos", restoreToPosArg, replica.Alias)
	if expectError != "" {
		require.Errorf(t, err, "expected: %v", expectError)
		require.Contains(t, output, expectError)
		return
	}
	require.NoErrorf(t, err, "output: %v", output)
	verifyTabletRestoreStats(t, replica.VttabletProcess.GetVars())
	checkTabletType(t, replica1.Alias, topodata.TabletType_DRAINED)
}

func TestReplicaRestoreToTimestamp(t *testing.T, restoreToTimestamp time.Time, expectError string) {
	require.False(t, restoreToTimestamp.IsZero())
	restoreToTimestampArg := mysqlctl.FormatRFC3339(restoreToTimestamp)
	output, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("RestoreFromBackup", "--restore-to-timestamp", restoreToTimestampArg, replica1.Alias)
	if expectError != "" {
		require.Errorf(t, err, "expected: %v", expectError)
		require.Contains(t, output, expectError)
		return
	}
	require.NoErrorf(t, err, "output: %v", output)
	verifyTabletRestoreStats(t, replica1.VttabletProcess.GetVars())
	checkTabletType(t, replica1.Alias, topodata.TabletType_DRAINED)
}

func verifyTabletBackupStats(t *testing.T, vars map[string]any) {
	// Currently only the builtin backup engine instruments bytes-processed
	// counts.
	if !useXtrabackup {
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
	// Currently only the builtin backup engine implements operation counts.
	if !useXtrabackup {
		require.Contains(t, bc, "BackupEngine.Builtin.Compressor:Close")
		require.Contains(t, bc, "BackupEngine.Builtin.Destination:Close")
		require.Contains(t, bc, "BackupEngine.Builtin.Destination:Open")
		require.Contains(t, bc, "BackupEngine.Builtin.Source:Close")
		require.Contains(t, bc, "BackupEngine.Builtin.Source:Open")
	}

	require.Contains(t, vars, "BackupDurationNanoseconds")
	bd := vars["BackupDurationNanoseconds"]
	require.Contains(t, bd, "-.-.Backup")
	// Currently only the builtin backup engine emits timings.
	if !useXtrabackup {
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

	if !useXtrabackup {
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
	// Currently only the builtin backup engine emits operation counts.
	if !useXtrabackup {
		require.Contains(t, bc, "BackupEngine.Builtin.Decompressor:Close")
		require.Contains(t, bc, "BackupEngine.Builtin.Destination:Close")
		require.Contains(t, bc, "BackupEngine.Builtin.Destination:Open")
		require.Contains(t, bc, "BackupEngine.Builtin.Source:Close")
		require.Contains(t, bc, "BackupEngine.Builtin.Source:Open")
	}

	require.Contains(t, vars, "RestoreDurationNanoseconds")
	bd := vars["RestoreDurationNanoseconds"]
	require.Contains(t, bd, "-.-.Restore")
	// Currently only the builtin backup engine emits timings.
	if !useXtrabackup {
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
