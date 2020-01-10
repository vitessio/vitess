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

package backuptransform

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

// create query for test table creation
var (
	vtInsertTest = `
					create table vt_insert_test (
					  id bigint auto_increment,
					  msg varchar(64),
					  primary key (id)
					  ) Engine=InnoDB`
)

func TestBackupTransform(t *testing.T) {
	// insert data in master, validate in slave
	verifyInitialReplication(t)

	// restart the replica with transform parameter
	replica1.VttabletProcess.TearDown()

	replica1.VttabletProcess.ExtraArgs = []string{
		"-db-credentials-file", dbCredentialFile,
		"-backup_storage_hook", "test_backup_transform",
		"-backup_storage_compress=false",
		"-restore_from_backup",
		"-backup_storage_implementation", "file",
		"-file_backup_storage_root", localCluster.VtctldProcess.FileBackupStorageRoot}
	replica1.VttabletProcess.ServingStatus = ""
	err := replica1.VttabletProcess.Setup()
	assert.Nil(t, err)
	err = replica1.VttabletProcess.WaitForTabletTypesForTimeout([]string{"SERVING"}, 25*time.Second)
	assert.Nil(t, err)

	// take backup, it should not give any error
	err = localCluster.VtctlclientProcess.ExecuteCommand("Backup", "-allow_master=true", replica1.Alias)
	assert.Nil(t, err)

	// insert data in master
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	assert.Nil(t, err)

	// validate that MANIFEST is having TransformHook
	// every file is starting with 'header'
	backups := listBackups(t)
	assert.Equalf(t, 1, len(backups), "invalid backups: %v", backups)

	// reading from the manifest is pending

	// restore replica2 from backup, should not give any error
	// Note: we don't need to pass in the backup_storage_transform parameter,
	// as it is read from the MANIFEST.
	replica2.MysqlctlProcess.ExtraArgs = []string{
		"-db-credentials-file", dbCredentialFile}
	// clear replica2
	proc, _ := replica2.MysqlctlProcess.StopProcess()
	proc.Wait()
	os.RemoveAll(replica2.VttabletProcess.Directory)

	// start replica2 from backup
	proc, _ = replica2.MysqlctlProcess.StartProcess()
	proc.Wait()
	err = localCluster.VtctlclientProcess.InitTablet(replica2, cell, keyspaceName, hostname, shardName)
	assert.Nil(t, err)
	replica2.VttabletProcess.CreateDB(keyspaceName)
	replica2.VttabletProcess.ExtraArgs = []string{
		"-db-credentials-file", dbCredentialFile,
		"-restore_from_backup",
		"-backup_storage_implementation", "file",
		"-file_backup_storage_root", localCluster.VtctldProcess.FileBackupStorageRoot}
	replica2.VttabletProcess.Setup()
	err = replica2.VttabletProcess.WaitForTabletTypesForTimeout([]string{"SERVING"}, 25*time.Second)
	assert.Nil(t, err)
	defer replica2.VttabletProcess.TearDown()

	// validate that semi-sync is enabled for replica, desable for rdOnly
	if replica2.Type == "replica" {
		verifyReplicationStatus(t, replica2, "ON")
	} else if replica2.Type == "rdonly" {
		verifyReplicationStatus(t, replica2, "OFF")
	}

	// validate new slave has all the data
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 2)

	// Remove the backup
	for _, backup := range listBackups(t) {
		err := localCluster.VtctlclientProcess.ExecuteCommand("RemoveBackup", shardKsName, backup)
		assert.Nil(t, err)
	}

	// restart the replica with transform param
	err = replica1.VttabletProcess.TearDown()
	require.Nil(t, err)

	replica1.VttabletProcess.ServingStatus = ""
	replica1.VttabletProcess.ExtraArgs = []string{
		"-db-credentials-file", dbCredentialFile,
		"-backup_storage_hook", "test_backup_error",
		"-restore_from_backup",
		"-backup_storage_implementation", "file",
		"-file_backup_storage_root", localCluster.VtctldProcess.FileBackupStorageRoot}
	err = replica1.VttabletProcess.Setup()
	assert.Nil(t, err)
	err = replica1.VttabletProcess.WaitForTabletTypesForTimeout([]string{"SERVING"}, 25*time.Second)
	assert.Nil(t, err)

	// create backup, it should fail
	err = localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	assert.NotNil(t, err)

	// validate there is no backup left
	backups = listBackups(t)
	assert.Equalf(t, 0, len(backups), "invalid backups: %v", backups)
}

// listBackups fetches the list of backup
func listBackups(t *testing.T) []string {
	output, err := localCluster.ListBackups(shardKsName)
	assert.Nil(t, err)
	return output
}

// verifyReplicationStatus validate the replication status in tablet.
func verifyReplicationStatus(t *testing.T, vttablet *cluster.Vttablet, expectedStatus string) {
	status, err := vttablet.VttabletProcess.GetDBVar("rpl_semi_sync_slave_enabled", keyspaceName)
	assert.Nil(t, err)
	assert.Equal(t, status, expectedStatus)
	status, err = vttablet.VttabletProcess.GetDBStatus("rpl_semi_sync_slave_status", keyspaceName)
	assert.Nil(t, err)
	assert.Equal(t, status, expectedStatus)
}

// verifyInitialReplication creates schema in master, insert some data to master and verify the same data in replica
func verifyInitialReplication(t *testing.T) {
	_, err := master.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	assert.Nil(t, err)
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	assert.Nil(t, err)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 1)
}
