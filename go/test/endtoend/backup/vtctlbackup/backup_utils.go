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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/sharding/initialsharding"

	"vitess.io/vitess/go/vt/proto/topodata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

// constants for test variants
const (
	XtraBackup = iota
	Backup
	Mysqlctld
)

var (
	master        *cluster.Vttablet
	replica1      *cluster.Vttablet
	replica2      *cluster.Vttablet
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
		"-vreplication_healthcheck_topology_refresh", "1s",
		"-vreplication_healthcheck_retry_delay", "1s",
		"-vreplication_retry_delay", "1s",
		"-degraded_threshold", "5s",
		"-lock_tables_timeout", "5s",
		"-watch_replication_stream",
		"-enable_replication_reporter",
		"-serving_state_grace_period", "1s",
	}

	vtInsertTest = `
					create table vt_insert_test (
					  id bigint auto_increment,
					  msg varchar(64),
					  primary key (id)
					  ) Engine=InnoDB`
)

// LaunchCluster : starts the cluster as per given params.
func LaunchCluster(setupType int, streamMode string, stripes int) (int, error) {
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

	dbCredentialFile = initialsharding.WriteDbCredentialToTmp(localCluster.TmpDirectory)
	initDb, _ := ioutil.ReadFile(path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"))
	sql := string(initDb)
	newInitDBFile = path.Join(localCluster.TmpDirectory, "init_db_with_passwords.sql")
	sql = sql + initialsharding.GetPasswordUpdateSQL(localCluster)
	err = ioutil.WriteFile(newInitDBFile, []byte(sql), 0666)
	if err != nil {
		return 1, err
	}

	extraArgs := []string{"-db-credentials-file", dbCredentialFile}
	commonTabletArg = append(commonTabletArg, "-db-credentials-file", dbCredentialFile)

	// Update arguments for xtrabackup
	if setupType == XtraBackup {
		useXtrabackup = true

		xtrabackupArgs := []string{
			"-backup_engine_implementation", "xtrabackup",
			fmt.Sprintf("-xtrabackup_stream_mode=%s", streamMode),
			"-xtrabackup_user=vt_dba",
			fmt.Sprintf("-xtrabackup_stripes=%d", stripes),
			"-xtrabackup_backup_flags", fmt.Sprintf("--password=%s", dbPassword),
		}

		// if streamMode is xbstream, add some additional args to test other xtrabackup flags
		if streamMode == "xbstream" {
			xtrabackupArgs = append(xtrabackupArgs, "-xtrabackup_prepare_flags", fmt.Sprintf("--use-memory=100M")) //nolint
		}

		commonTabletArg = append(commonTabletArg, xtrabackupArgs...)
	}

	var mysqlProcs []*exec.Cmd
	for i := 0; i < 3; i++ {
		tabletType := "replica"
		if i == 0 {
			tabletType = "master"
		}
		tablet := localCluster.NewVttabletInstance(tabletType, 0, cell)
		tablet.VttabletProcess = localCluster.VtprocessInstanceFromVttablet(tablet, shard.Name, keyspaceName)
		tablet.VttabletProcess.DbPassword = dbPassword
		tablet.VttabletProcess.ExtraArgs = commonTabletArg
		tablet.VttabletProcess.SupportsBackup = true
		tablet.VttabletProcess.EnableSemiSync = true

		if setupType == Mysqlctld {
			tablet.MysqlctldProcess = *cluster.MysqlCtldProcessInstance(tablet.TabletUID, tablet.MySQLPort, localCluster.TmpDirectory)
			tablet.MysqlctldProcess.InitDBFile = newInitDBFile
			tablet.MysqlctldProcess.ExtraArgs = extraArgs
			tablet.MysqlctldProcess.Password = tablet.VttabletProcess.DbPassword
			if err := tablet.MysqlctldProcess.Start(); err != nil {
				return 1, err
			}
			shard.Vttablets = append(shard.Vttablets, tablet)
			continue
		}

		tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, localCluster.TmpDirectory)
		tablet.MysqlctlProcess.InitDBFile = newInitDBFile
		tablet.MysqlctlProcess.ExtraArgs = extraArgs
		proc, err := tablet.MysqlctlProcess.StartProcess()
		if err != nil {
			return 1, err
		}
		mysqlProcs = append(mysqlProcs, proc)

		shard.Vttablets = append(shard.Vttablets, tablet)
	}
	for _, proc := range mysqlProcs {
		if err := proc.Wait(); err != nil {
			return 1, err
		}
	}
	master = shard.Vttablets[0]
	replica1 = shard.Vttablets[1]
	replica2 = shard.Vttablets[2]

	if err := localCluster.VtctlclientProcess.InitTablet(master, cell, keyspaceName, hostname, shard.Name); err != nil {
		return 1, err
	}
	if err := localCluster.VtctlclientProcess.InitTablet(replica1, cell, keyspaceName, hostname, shard.Name); err != nil {
		return 1, err
	}

	for _, tablet := range []cluster.Vttablet{*master, *replica1} {
		if err := tablet.VttabletProcess.CreateDB(keyspaceName); err != nil {
			return 1, err
		}
		if err := tablet.VttabletProcess.Setup(); err != nil {
			return 1, err
		}
	}

	if err := localCluster.VtctlclientProcess.InitShardMaster(keyspaceName, shard.Name, cell, master.TabletUID); err != nil {
		return 1, err
	}
	return 0, nil
}

// TearDownCluster shuts down all cluster processes
func TearDownCluster() {
	localCluster.Teardown()
}

// TestBackup runs all the backup tests
func TestBackup(t *testing.T, setupType int, streamMode string, stripes int) {

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
			name:   "TestMasterBackup",
			method: masterBackup,
		}, //
		{
			name:   "TestMasterReplicaSameBackup",
			method: masterReplicaSameBackup,
		}, //
		{
			name:   "TestRestoreOldMasterByRestart",
			method: restoreOldMasterByRestart,
		}, //
		{
			name:   "TestRestoreOldMasterInPlace",
			method: restoreOldMasterInPlace,
		}, //
		{
			name:   "TestTerminatedRestore",
			method: terminatedRestore,
		}, //
	}

	defer cluster.PanicHandler(t)

	// setup cluster for the testing
	code, err := LaunchCluster(setupType, streamMode, stripes)
	require.Nilf(t, err, "setup failed with status code %d", code)

	// Teardown the cluster
	defer TearDownCluster()

	// Run all the backup tests

	for _, test := range testMethods {
		t.Run(test.name, test.method)
	}

}

type restoreMethod func(t *testing.T, tablet *cluster.Vttablet)

//- create a shard with master and replica1 only
//- run InitShardMaster
//- insert some data
//- take a backup on master
//- insert more data on the master
//- bring up tablet_replica2 after the fact, let it restore the backup
//- check all data is right (before+after backup data)
//- list the backup, remove it
func masterBackup(t *testing.T) {
	verifyInitialReplication(t)

	output, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("Backup", master.Alias)
	require.Error(t, err)
	assert.Contains(t, output, "type MASTER cannot take backup. if you really need to do this, rerun the backup command with -allow_master")

	localCluster.VerifyBackupCount(t, shardKsName, 0)

	err = localCluster.VtctlclientProcess.ExecuteCommand("Backup", "-allow_master=true", master.Alias)
	require.Nil(t, err)

	backups := localCluster.VerifyBackupCount(t, shardKsName, 1)
	assert.Contains(t, backups[0], master.Alias)

	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	require.Nil(t, err)

	restoreWaitForBackup(t, "replica")
	err = replica2.VttabletProcess.WaitForTabletTypesForTimeout([]string{"SERVING"}, 25*time.Second)
	require.Nil(t, err)

	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 2)
	cluster.VerifyLocalMetadata(t, replica2, keyspaceName, shardName, cell)
	verifyAfterRemovingBackupNoBackupShouldBePresent(t, backups)

	err = replica2.VttabletProcess.TearDown()
	require.Nil(t, err)

	_, err = master.VttabletProcess.QueryTablet("DROP TABLE vt_insert_test", keyspaceName, true)
	require.Nil(t, err)
}

//    Test a master and replica from the same backup.
//
//    Check that a replica and master both restored from the same backup
//    can replicate successfully.
func masterReplicaSameBackup(t *testing.T) {
	// insert data on master, wait for replica to get it
	verifyInitialReplication(t)

	// backup the replica
	err := localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	require.Nil(t, err)

	//  insert more data on the master
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	require.Nil(t, err)

	// now bring up the other replica, letting it restore from backup.
	restoreWaitForBackup(t, "replica")
	err = replica2.VttabletProcess.WaitForTabletTypesForTimeout([]string{"SERVING"}, 25*time.Second)
	require.Nil(t, err)

	// check the new replica has the data
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 2)

	// Promote replica2 to master
	err = localCluster.VtctlclientProcess.ExecuteCommand("PlannedReparentShard",
		"-keyspace_shard", shardKsName,
		"-new_master", replica2.Alias)
	require.Nil(t, err)

	// insert more data on replica2 (current master)
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
	err = localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	require.Nil(t, err)

	// Insert more data on replica2 (current master).
	_, err = replica2.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test4')", keyspaceName, true)
	require.Nil(t, err)

	// Force replica1 to restore from backup.
	verifyRestoreTablet(t, replica1, "SERVING")

	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 4)
	err = replica2.VttabletProcess.TearDown()
	require.Nil(t, err)
	restartMasterReplica(t)
}

func restoreOldMasterByRestart(t *testing.T) {
	testRestoreOldMaster(t, restoreUsingRestart)
}

func restoreOldMasterInPlace(t *testing.T) {
	testRestoreOldMaster(t, restoreInPlace)
}

//Test that a former master replicates correctly after being restored.
//
//- Take a backup.
//- Reparent from old master to new master.
//- Force old master to restore from a previous backup using restore_method.
//
//Args:
//restore_method: function accepting one parameter of type tablet.Tablet,
//this function is called to force a restore on the provided tablet
//
func testRestoreOldMaster(t *testing.T, method restoreMethod) {
	// insert data on master, wait for replica to get it
	verifyInitialReplication(t)

	// backup the replica
	err := localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	require.Nil(t, err)

	//  insert more data on the master
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	require.Nil(t, err)

	// reparent to replica1
	err = localCluster.VtctlclientProcess.ExecuteCommand("PlannedReparentShard",
		"-keyspace_shard", shardKsName,
		"-new_master", replica1.Alias)
	require.Nil(t, err)

	// insert more data to new master
	_, err = replica1.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test3')", keyspaceName, true)
	require.Nil(t, err)

	// force the old master to restore at the latest backup.
	method(t, master)

	// wait for it to catch up.
	cluster.VerifyRowsInTablet(t, master, keyspaceName, 3)

	// teardown
	restartMasterReplica(t)
}

func restoreUsingRestart(t *testing.T, tablet *cluster.Vttablet) {
	err := tablet.VttabletProcess.TearDown()
	require.Nil(t, err)
	verifyRestoreTablet(t, tablet, "SERVING")
}

func restoreInPlace(t *testing.T, tablet *cluster.Vttablet) {
	err := localCluster.VtctlclientProcess.ExecuteCommand("RestoreFromBackup", tablet.Alias)
	require.Nil(t, err)
}

func restartMasterReplica(t *testing.T) {
	// Stop all master, replica tablet and mysql instance
	stopAllTablets()

	// remove all backups
	localCluster.RemoveAllBackups(t, shardKsName)
	// start all tablet and mysql instances
	var mysqlProcs []*exec.Cmd
	for _, tablet := range []*cluster.Vttablet{master, replica1, replica2} {
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
	for _, tablet := range []*cluster.Vttablet{master, replica1} {
		err := localCluster.VtctlclientProcess.InitTablet(tablet, cell, keyspaceName, hostname, shardName)
		require.Nil(t, err)
		err = tablet.VttabletProcess.CreateDB(keyspaceName)
		require.Nil(t, err)
		err = tablet.VttabletProcess.Setup()
		require.Nil(t, err)
	}
	err := localCluster.VtctlclientProcess.InitShardMaster(keyspaceName, shardName, cell, master.TabletUID)
	require.Nil(t, err)
}

func stopAllTablets() {
	var mysqlProcs []*exec.Cmd
	for _, tablet := range []*cluster.Vttablet{master, replica1, replica2} {
		tablet.VttabletProcess.TearDown()
		if tablet.MysqlctldProcess.TabletUID > 0 {
			tablet.MysqlctldProcess.Stop()
			localCluster.VtctlclientProcess.ExecuteCommand("DeleteTablet", "-allow_master", tablet.Alias)
			continue
		}
		proc, _ := tablet.MysqlctlProcess.StopProcess()
		mysqlProcs = append(mysqlProcs, proc)
		localCluster.VtctlclientProcess.ExecuteCommand("DeleteTablet", "-allow_master", tablet.Alias)
	}
	for _, proc := range mysqlProcs {
		proc.Wait()
	}
	for _, tablet := range []*cluster.Vttablet{master, replica1} {
		os.RemoveAll(tablet.VttabletProcess.Directory)
	}
}

func terminatedRestore(t *testing.T) {
	// insert data on master, wait for replica to get it
	verifyInitialReplication(t)

	// backup the replica
	err := localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	require.Nil(t, err)

	//  insert more data on the master
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	require.Nil(t, err)

	// reparent to replica1
	err = localCluster.VtctlclientProcess.ExecuteCommand("PlannedReparentShard",
		"-keyspace_shard", shardKsName,
		"-new_master", replica1.Alias)
	require.Nil(t, err)

	// insert more data to new master
	_, err = replica1.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test3')", keyspaceName, true)
	require.Nil(t, err)

	terminateRestore(t)

	err = localCluster.VtctlclientProcess.ExecuteCommand("RestoreFromBackup", master.Alias)
	require.Nil(t, err)

	output, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", master.Alias)
	require.Nil(t, err)

	var tabletPB topodata.Tablet
	err = json.Unmarshal([]byte(output), &tabletPB)
	require.Nil(t, err)
	assert.Equal(t, tabletPB.Type, topodata.TabletType_REPLICA)

	_, err = os.Stat(path.Join(master.VttabletProcess.Directory, "restore_in_progress"))
	assert.True(t, os.IsNotExist(err))

	cluster.VerifyRowsInTablet(t, master, keyspaceName, 3)
	stopAllTablets()
}

//test_backup will:
//- create a shard with master and replica1 only
//- run InitShardMaster
//- bring up tablet_replica2 concurrently, telling it to wait for a backup
//- insert some data
//- take a backup
//- insert more data on the master
//- wait for tablet_replica2 to become SERVING
//- check all data is right (before+after backup data)
//- list the backup, remove it
//
//Args:
//tablet_type: 'replica' or 'rdonly'.
//
//
func vtctlBackup(t *testing.T, tabletType string) {
	restoreWaitForBackup(t, tabletType)
	verifyInitialReplication(t)

	err := localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	require.Nil(t, err)

	backups := localCluster.VerifyBackupCount(t, shardKsName, 1)

	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	require.Nil(t, err)

	err = replica2.VttabletProcess.WaitForTabletTypesForTimeout([]string{"SERVING"}, 25*time.Second)
	require.Nil(t, err)
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 2)

	cluster.VerifyLocalMetadata(t, replica2, keyspaceName, shardName, cell)
	verifyAfterRemovingBackupNoBackupShouldBePresent(t, backups)

	err = replica2.VttabletProcess.TearDown()
	require.Nil(t, err)

	err = localCluster.VtctlclientProcess.ExecuteCommand("DeleteTablet", replica2.Alias)
	require.Nil(t, err)
	_, err = master.VttabletProcess.QueryTablet("DROP TABLE vt_insert_test", keyspaceName, true)
	require.Nil(t, err)

}

// This will create schema in master, insert some data to master and verify the same data in replica
func verifyInitialReplication(t *testing.T) {
	_, err := master.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	require.Nil(t, err)
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.Nil(t, err)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 1)
}

// Bring up another replica concurrently, telling it to wait until a backup
// is available instead of starting up empty.
//
// Override the backup engine implementation to a non-existent one for restore.
// This setting should only matter for taking new backups. We should be able
// to restore a previous backup successfully regardless of this setting.
func restoreWaitForBackup(t *testing.T, tabletType string) {
	replica2.Type = tabletType
	replica2.ValidateTabletRestart(t)
	replicaTabletArgs := commonTabletArg
	replicaTabletArgs = append(replicaTabletArgs, "-backup_engine_implementation", "fake_implementation")
	replicaTabletArgs = append(replicaTabletArgs, "-wait_for_backup_interval", "1s")
	replicaTabletArgs = append(replicaTabletArgs, "-init_tablet_type", tabletType)
	replica2.VttabletProcess.ExtraArgs = replicaTabletArgs
	replica2.VttabletProcess.ServingStatus = ""
	err := replica2.VttabletProcess.Setup()
	require.Nil(t, err)
}

func verifyAfterRemovingBackupNoBackupShouldBePresent(t *testing.T, backups []string) {
	// Remove the backup
	for _, backup := range backups {
		err := localCluster.VtctlclientProcess.ExecuteCommand("RemoveBackup", shardKsName, backup)
		require.Nil(t, err)
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
		err = tablet.VttabletProcess.WaitForTabletTypesForTimeout([]string{status}, 25*time.Second)
		require.Nil(t, err)
	}

	if tablet.Type == "replica" {
		verifyReplicationStatus(t, tablet, "ON")
	} else if tablet.Type == "rdonly" {
		verifyReplicationStatus(t, tablet, "OFF")
	}
}

func verifyReplicationStatus(t *testing.T, vttablet *cluster.Vttablet, expectedStatus string) {
	status, err := vttablet.VttabletProcess.GetDBVar("rpl_semi_sync_slave_enabled", keyspaceName)
	require.Nil(t, err)
	assert.Equal(t, status, expectedStatus)
	status, err = vttablet.VttabletProcess.GetDBStatus("rpl_semi_sync_slave_status", keyspaceName)
	require.Nil(t, err)
	assert.Equal(t, status, expectedStatus)
}

func terminateRestore(t *testing.T) {
	stopRestoreMsg := "Copying file 10"
	if useXtrabackup {
		stopRestoreMsg = "Restore: Preparing"
		useXtrabackup = false
	}

	args := append([]string{"-server", localCluster.VtctlclientProcess.Server, "-alsologtostderr"}, "RestoreFromBackup", master.Alias)
	tmpProcess := exec.Command(
		"vtctlclient",
		args...,
	)

	reader, _ := tmpProcess.StderrPipe()
	err := tmpProcess.Start()
	require.Nil(t, err)
	found := false

	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		text := scanner.Text()
		if strings.Contains(text, stopRestoreMsg) {
			if _, err := os.Stat(path.Join(master.VttabletProcess.Directory, "restore_in_progress")); os.IsNotExist(err) {
				assert.Fail(t, "restore in progress file missing")
			}
			tmpProcess.Process.Signal(syscall.SIGTERM)
			found = true //nolint
			return
		}
	}
	assert.True(t, found, "Restore message not found")
}
