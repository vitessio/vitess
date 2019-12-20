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

package backup

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/proto/topodata"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	vtInsertTest = `
					create table vt_insert_test (
					  id bigint auto_increment,
					  msg varchar(64),
					  primary key (id)
					  ) Engine=InnoDB`
)

type restoreMethod func(t *testing.T, tablet *cluster.Vttablet)

func TestReplicaBackup(t *testing.T) {
	testBackup(t, "replica")
}

func TestRdonlyBackup(t *testing.T) {
	testBackup(t, "rdonly")
}

//test_backup will:
//- create a shard with master and replica1 only
//- run InitShardMaster
//- insert some data
//- take a backup on master
//- insert more data on the master
//- bring up tablet_replica2 after the fact, let it restore the backup
//- check all data is right (before+after backup data)
//- list the backup, remove it
func TestMasterBackup(t *testing.T) {
	verifyInitialReplication(t)

	output, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("Backup", master.Alias)
	assert.NotNil(t, err)
	assert.Contains(t, output, "type MASTER cannot take backup. if you really need to do this, rerun the backup command with -allow_master")

	backups := listBackups(t)
	assert.Equal(t, len(backups), 0)

	err = localCluster.VtctlclientProcess.ExecuteCommand("Backup", "-allow_master=true", master.Alias)
	assert.Nil(t, err)

	backups = listBackups(t)
	assert.Equal(t, len(backups), 1)
	assert.Contains(t, backups[0], master.Alias)

	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	assert.Nil(t, err)

	restoreWaitForBackup(t, "replica")
	err = replica2.VttabletProcess.WaitForTabletTypeForTimeout("SERVING", 15*time.Second)
	assert.Nil(t, err)

	verifyRowsInTablet(t, replica2, 2)
	verifyLocalMetadata(t, "replica")
	verifyAfterRemovingBackupNoBackupShouldBePresent(t, backups)

	replica2.VttabletProcess.TearDown()
	master.VttabletProcess.QueryTablet("DROP TABLE vt_insert_test", keyspaceName, true)
}

//    Test a master and slave from the same backup.
//
//    Check that a slave and master both restored from the same backup
//    can replicate successfully.
func TestMasterSlaveSameBackup(t *testing.T) {
	// insert data on master, wait for slave to get it
	verifyInitialReplication(t)

	// backup the slave
	err := localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	assert.Nil(t, err)

	//  insert more data on the master
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	assert.Nil(t, err)

	// now bring up the other slave, letting it restore from backup.
	restoreWaitForBackup(t, "replica")
	err = replica2.VttabletProcess.WaitForTabletTypeForTimeout("SERVING", 15*time.Second)
	assert.Nil(t, err)

	// check the new slave has the data
	verifyRowsInTablet(t, replica2, 2)

	// Promote replica2 to master
	err = localCluster.VtctlclientProcess.ExecuteCommand("PlannedReparentShard",
		"-keyspace_shard", shardKsName,
		"-new_master", replica2.Alias)
	assert.Nil(t, err)

	// insert more data on replica2 (current master)
	_, err = replica2.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test3')", keyspaceName, true)
	assert.Nil(t, err)

	// Force replica1 to restore from backup.
	verifyRestoreTablet(t, replica1, "SERVING")

	// wait for replica1 to catch up.
	verifyRowsInTablet(t, replica1, 3)

	// This is to test that replicationPosition is processed correctly
	// while doing backup/restore after a reparent.
	// It is written into the MANIFEST and read back from the MANIFEST.
	//
	// Take another backup on the slave.
	err = localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	assert.Nil(t, err)

	// Insert more data on replica2 (current master).
	_, err = replica2.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test4')", keyspaceName, true)
	assert.Nil(t, err)

	// Force replica1 to restore from backup.
	verifyRestoreTablet(t, replica1, "SERVING")

	verifyRowsInTablet(t, replica1, 4)
	replica2.VttabletProcess.TearDown()
	master.VttabletProcess.QueryTablet("DROP TABLE vt_insert_test", keyspaceName, true)
}

func TestRestoreOldMasterByRestart(t *testing.T) {
	testRestoreOldMaster(t, restoreUsingRestart)
}

func TestRestoreOldMasterInPlace(t *testing.T) {
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
	// insert data on master, wait for slave to get it
	verifyInitialReplication(t)

	// backup the slave
	err := localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	assert.Nil(t, err)

	//  insert more data on the master
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	assert.Nil(t, err)

	// reparent to replica1
	err = localCluster.VtctlclientProcess.ExecuteCommand("PlannedReparentShard",
		"-keyspace_shard", shardKsName,
		"-new_master", replica1.Alias)
	assert.Nil(t, err)

	// insert more data to new master
	_, err = replica1.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test3')", keyspaceName, true)
	assert.Nil(t, err)

	// force the old master to restore at the latest backup.
	method(t, master)

	// wait for it to catch up.
	verifyRowsInTablet(t, master, 3)
}

func restoreUsingRestart(t *testing.T, tablet *cluster.Vttablet) {
	tablet.VttabletProcess.TearDown()
	verifyRestoreTablet(t, tablet, "SERVING")
}

func restoreInPlace(t *testing.T, tablet *cluster.Vttablet) {
	err := localCluster.VtctlclientProcess.ExecuteCommand("RestoreFromBackup", tablet.Alias)
	assert.Nil(t, err)
}

func TestTerminatedRestore(t *testing.T) {

	// insert data on master, wait for slave to get it
	verifyInitialReplication(t)

	// backup the slave
	err := localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	assert.Nil(t, err)

	//  insert more data on the master
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	assert.Nil(t, err)

	// reparent to replica1
	err = localCluster.VtctlclientProcess.ExecuteCommand("PlannedReparentShard",
		"-keyspace_shard", shardKsName,
		"-new_master", replica1.Alias)
	assert.Nil(t, err)

	// insert more data to new master
	_, err = replica1.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test3')", keyspaceName, true)
	assert.Nil(t, err)

	terminateRestore(t)

	err = localCluster.VtctlclientProcess.ExecuteCommand("RestoreFromBackup", master.Alias)
	assert.Nil(t, err)

	output, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", master.Alias)
	assert.Nil(t, err)

	var tabletPB topodata.Tablet
	err = json.Unmarshal([]byte(output), &tabletPB)
	assert.Nil(t, err)
	assert.Equal(t, tabletPB.Type, topodata.TabletType_REPLICA)

	_, err = os.Stat(path.Join(master.VttabletProcess.Directory, "restore_in_progress"))
	assert.True(t, os.IsNotExist(err))

	verifyRowsInTablet(t, master, 3)
}

//
//Test backup flow.
//
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
func testBackup(t *testing.T, tabletType string) {
	restoreWaitForBackup(t, tabletType)
	verifyInitialReplication(t)

	err := localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	assert.Nil(t, err)

	backups := listBackups(t)
	assert.Equal(t, len(backups), 1)

	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	assert.Nil(t, err)

	err = replica2.VttabletProcess.WaitForTabletTypeForTimeout("SERVING", 15*time.Second)
	assert.Nil(t, err)

	verifyRowsInTablet(t, replica2, 2)

	verifyLocalMetadata(t, tabletType)
	verifyAfterRemovingBackupNoBackupShouldBePresent(t, backups)

	replica2.VttabletProcess.TearDown()
	localCluster.VtctlclientProcess.ExecuteCommand("DeleteTablet", replica2.Alias)
	master.VttabletProcess.QueryTablet("DROP TABLE vt_insert_test", keyspaceName, true)

}

// This will create schema in master, insert some data to master and verify the same data in replica
func verifyInitialReplication(t *testing.T) {
	_, err := master.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	assert.Nil(t, err)
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	assert.Nil(t, err)
	verifyRowsInTablet(t, replica1, 1)
}

// Bring up another replica concurrently, telling it to wait until a backup
// is available instead of starting up empty.
//
// Override the backup engine implementation to a non-existent one for restore.
// This setting should only matter for taking new backups. We should be able
// to restore a previous backup successfully regardless of this setting.
func restoreWaitForBackup(t *testing.T, tabletType string) {
	if tabletType == "rdonly" {
		replica2.Type = "rdonly"
	}
	resetTabletDir(t, replica2)
	replicaTabletArgs := commonTabletArg
	replicaTabletArgs = append(replicaTabletArgs, "-backup_engine_implementation", "fake_implementation")
	replicaTabletArgs = append(replicaTabletArgs, "-wait_for_backup_interval", "1s")
	replicaTabletArgs = append(replicaTabletArgs, "-init_tablet_type", tabletType)
	replica2.VttabletProcess.ExtraArgs = replicaTabletArgs
	replica2.VttabletProcess.ServingStatus = ""
	err := replica2.VttabletProcess.Setup()
	assert.Nil(t, err)
}

func resetTabletDir(t *testing.T, tablet *cluster.Vttablet) {
	tablet.MysqlctlProcess.Stop()
	tablet.VttabletProcess.TearDown()
	os.RemoveAll(tablet.VttabletProcess.Directory)

	err := tablet.MysqlctlProcess.Start()
	assert.Nil(t, err)
}

func listBackups(t *testing.T) []string {
	output, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("ListBackups", shardKsName)
	assert.Nil(t, err)
	result := strings.Split(output, "\n")
	var returnResult []string
	for _, str := range result {
		if str != "" {
			returnResult = append(returnResult, str)
		}
	}
	return returnResult
}

func verifyRowsInTablet(t *testing.T, vttablet *cluster.Vttablet, totalRows int) {
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		qr, err := vttablet.VttabletProcess.QueryTablet("select * from vt_insert_test", keyspaceName, true)
		assert.Nil(t, err)
		if len(qr.Rows) != totalRows {
			time.Sleep(300 * time.Millisecond)
		} else {
			return
		}
	}
	assert.Fail(t, "expected rows not found.")
}

func verifyLocalMetadata(t *testing.T, tabletType string) {
	qr, err := replica2.VttabletProcess.QueryTablet("select * from _vt.local_metadata", keyspaceName, false)
	assert.Nil(t, err)
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows[0]), fmt.Sprintf(`[VARCHAR("Alias") BLOB("%s") VARBINARY("vt_%s")]`, replica2.Alias, keyspaceName))
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows[1]), fmt.Sprintf(`[VARCHAR("ClusterAlias") BLOB("%s.%s") VARBINARY("vt_%s")]`, keyspaceName, shardName, keyspaceName))
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows[2]), fmt.Sprintf(`[VARCHAR("DataCenter") BLOB("%s") VARBINARY("vt_%s")]`, cell, keyspaceName))
	if tabletType == "replica" {
		assert.Equal(t, fmt.Sprintf("%v", qr.Rows[3]), fmt.Sprintf(`[VARCHAR("PromotionRule") BLOB("neutral") VARBINARY("vt_%s")]`, keyspaceName))
	} else if tabletType == "rdonly" {
		assert.Equal(t, fmt.Sprintf("%v", qr.Rows[3]), fmt.Sprintf(`[VARCHAR("PromotionRule") BLOB("must_not") VARBINARY("vt_%s")]`, keyspaceName))
	}
}

func verifyAfterRemovingBackupNoBackupShouldBePresent(t *testing.T, backups []string) {
	// Remove the backup
	for _, backup := range backups {
		err := localCluster.VtctlclientProcess.ExecuteCommand("RemoveBackup", shardKsName, backup)
		assert.Nil(t, err)
	}

	// Now, there should not be no backup
	backups = listBackups(t)
	assert.Equal(t, len(backups), 0)
}

func verifyRestoreTablet(t *testing.T, tablet *cluster.Vttablet, status string) {
	tablet.VttabletProcess.TearDown()
	resetTabletDir(t, tablet)
	tablet.VttabletProcess.ServingStatus = ""
	err := tablet.VttabletProcess.Setup()
	assert.Nil(t, err)
	if status != "" {
		err = tablet.VttabletProcess.WaitForTabletTypeForTimeout(status, 15*time.Second)
		assert.Nil(t, err)
	}

	// TODO: Check db vars and status rpl_semi_sync_slave_enabled, rpl_semi_sync_slave_status

}

func terminateRestore(t *testing.T) {
	stopRestoreMsg := "Copying file 10"
	args := append([]string{"-server", localCluster.VtctlclientProcess.Server, "-alsologtostderr"}, "RestoreFromBackup", master.Alias)
	tmpProcess := exec.Command(
		"vtctlclient",
		args...,
	)

	reader, _ := tmpProcess.StderrPipe()
	err := tmpProcess.Start()
	assert.Nil(t, err)
	found := false

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		text := scanner.Text()
		if strings.Contains(text, stopRestoreMsg) {
			if _, err := os.Stat(path.Join(master.VttabletProcess.Directory, "restore_in_progress")); os.IsNotExist(err) {
				assert.Fail(t, "restore in progress file missing")
			}
			tmpProcess.Process.Signal(syscall.SIGTERM)
			found = true
			return
		}
	}
	assert.True(t, found, "Restore message not found")
}
