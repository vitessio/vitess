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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

// create query for test table creation
var vtInsertTest = `create table vt_insert_test (
		id bigint auto_increment,
		msg varchar(64),
		primary key (id)
		) Engine=InnoDB`

func TestBackupTransform(t *testing.T) {
	// insert data in master, validate same in slave
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
	replica1.VttabletProcess.ServingStatus = "SERVING"
	err := replica1.VttabletProcess.Setup()
	assert.Nil(t, err)

	// take backup, it should not give any error
	err = localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	assert.Nil(t, err)

	// insert data in master
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	assert.Nil(t, err)

	// validate backup_list, expecting 1 backup available
	backups := listBackups(t)
	require.Equalf(t, 1, len(backups), "invalid backups: %v", backups)

	backupLocation := localCluster.CurrentVTDATAROOT + "/backups/" + shardKsName + "/" + backups[0]

	// validate that MANIFEST is having TransformHook
	// every file is starting with 'header'
	validateManifestFile(t, backupLocation)

	// restore replica2 from backup, should not give any error
	// Note: we don't need to pass in the backup_storage_transform parameter,
	// as it is read from the MANIFEST.
	replica2.MysqlctlProcess.ExtraArgs = []string{
		"-db-credentials-file", dbCredentialFile}
	// clear replica2
	replica2.MysqlctlProcess.Stop()
	os.RemoveAll(replica2.VttabletProcess.Directory)

	// start replica2 from backup
	err = replica2.MysqlctlProcess.Start()
	require.Nil(t, err)
	err = localCluster.VtctlclientProcess.InitTablet(replica2, cell, keyspaceName, hostname, shardName)
	assert.Nil(t, err)
	replica2.VttabletProcess.CreateDB(keyspaceName)
	replica2.VttabletProcess.ExtraArgs = []string{
		"-db-credentials-file", dbCredentialFile,
		"-restore_from_backup",
		"-backup_storage_implementation", "file",
		"-file_backup_storage_root", localCluster.VtctldProcess.FileBackupStorageRoot}
	replica2.VttabletProcess.ServingStatus = ""
	err = replica2.VttabletProcess.Setup()
	require.Nil(t, err)
	err = replica2.VttabletProcess.WaitForTabletTypesForTimeout([]string{"SERVING"}, 25*time.Second)
	require.Nil(t, err)
	defer replica2.VttabletProcess.TearDown()

	// validate that semi-sync is enabled for replica, disable for rdOnly
	if replica2.Type == "replica" {
		verifyReplicationStatus(t, replica2, "ON")
	} else if replica2.Type == "rdonly" {
		verifyReplicationStatus(t, replica2, "OFF")
	}

	// validate that new slave has all the data
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 2)

	// Remove all backups
	for _, backup := range listBackups(t) {
		err := localCluster.VtctlclientProcess.ExecuteCommand("RemoveBackup", shardKsName, backup)
		assert.Nil(t, err)
	}

}

// TestBackupTransformErr validate backup with test_backup_error
// backup_storage_hook, which should fail.
func TestBackupTransformErr(t *testing.T) {
	// restart the replica with transform param
	err := replica1.VttabletProcess.TearDown()
	require.Nil(t, err)

	replica1.VttabletProcess.ExtraArgs = []string{
		"-db-credentials-file", dbCredentialFile,
		"-backup_storage_hook", "test_backup_error",
		"-restore_from_backup",
		"-backup_storage_implementation", "file",
		"-file_backup_storage_root", localCluster.VtctldProcess.FileBackupStorageRoot}
	replica1.VttabletProcess.ServingStatus = "SERVING"
	err = replica1.VttabletProcess.Setup()
	assert.Nil(t, err)

	// create backup, it should fail
	out, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("Backup", replica1.Alias)
	require.NotNil(t, err)
	assert.Containsf(t, out, "backup is not usable, aborting it", "unexpected error received %v", err)

	// validate there is no backup left
	backups := listBackups(t)
	assert.Equalf(t, 0, len(backups), "invalid backups: %v", backups)
}

// listBackups fetches the list of backup
func listBackups(t *testing.T) []string {
	output, err := localCluster.ListBackups(shardKsName)
	assert.Nil(t, err)
	return output
}

// validateManifestFile reads manifest and validates that it is
// having a TransformHook, SkipCompress and FileEntries. It also
// validates that backup_files avalable in FileEntries are having
// 'header' at it's first line.
func validateManifestFile(t *testing.T, backupLocation string) {

	// reading manifest
	data, err := ioutil.ReadFile(backupLocation + "/MANIFEST")
	require.Nilf(t, err, "error while reading MANIFEST %v", err)
	manifest := make(map[string]interface{})

	// parsing manifest
	err = json.Unmarshal(data, &manifest)
	require.Nilf(t, err, "error while parsing MANIFEST %v", err)

	// validate manifest
	transformHook, _ := manifest["TransformHook"]
	require.Equalf(t, "test_backup_transform", transformHook, "invalid transformHook in MANIFEST")
	skipCompress, _ := manifest["SkipCompress"]
	assert.Equalf(t, skipCompress, true, "invalid value of skipCompress")

	// validate backup files
	for i := range manifest["FileEntries"].([]interface{}) {
		f, err := os.Open(fmt.Sprintf("%s/%d", backupLocation, i))
		require.Nilf(t, err, "error while opening backup_file %d: %v", i, err)
		var fileHeader string
		_, err = fmt.Fscanln(f, &fileHeader)
		f.Close()

		require.Nilf(t, err, "error while reading backup_file %d: %v", i, err)
		require.Equalf(t, "header", fileHeader, "wrong file contents for %d", i)
	}

}

// verifyReplicationStatus validate the replication status in tablet.
func verifyReplicationStatus(t *testing.T, vttablet *cluster.Vttablet, expectedStatus string) {
	status, err := vttablet.VttabletProcess.GetDBVar("rpl_semi_sync_slave_enabled", keyspaceName)
	assert.Nil(t, err)
	assert.Equal(t, expectedStatus, status)
	status, err = vttablet.VttabletProcess.GetDBStatus("rpl_semi_sync_slave_status", keyspaceName)
	assert.Nil(t, err)
	assert.Equal(t, expectedStatus, status)
}

// verifyInitialReplication creates schema in master, insert some data to master and verify the same data in replica
func verifyInitialReplication(t *testing.T) {
	_, err := master.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	assert.Nil(t, err)
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	assert.Nil(t, err)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 1)
}
