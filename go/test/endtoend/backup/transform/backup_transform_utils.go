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

package transform

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/sharding/initialsharding"
	"vitess.io/vitess/go/vt/log"
)

// test main part of the testcase
var (
	master           *cluster.Vttablet
	replica1         *cluster.Vttablet
	replica2         *cluster.Vttablet
	localCluster     *cluster.LocalProcessCluster
	newInitDBFile    string
	cell             = cluster.DefaultCell
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
		"-serving_state_grace_period", "1s"}
)

// TestMainSetup sets up the basic test cluster
func TestMainSetup(m *testing.M, useMysqlctld bool) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode, err := func() (int, error) {
		localCluster = cluster.NewCluster(cell, hostname)
		defer localCluster.Teardown()

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
		// changing password for mysql user
		dbCredentialFile = initialsharding.WriteDbCredentialToTmp(localCluster.TmpDirectory)
		initDb, _ := ioutil.ReadFile(path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"))
		sql := string(initDb)
		newInitDBFile = path.Join(localCluster.TmpDirectory, "init_db_with_passwords.sql")
		sql = sql + initialsharding.GetPasswordUpdateSQL(localCluster)
		ioutil.WriteFile(newInitDBFile, []byte(sql), 0666)

		extraArgs := []string{"-db-credentials-file", dbCredentialFile}
		commonTabletArg = append(commonTabletArg, "-db-credentials-file", dbCredentialFile)

		// start mysql process for all replicas and master
		var mysqlProcs []*exec.Cmd
		for i := 0; i < 3; i++ {
			tabletType := "replica"
			tablet := localCluster.NewVttabletInstance(tabletType, 0, cell)
			tablet.VttabletProcess = localCluster.VtprocessInstanceFromVttablet(tablet, shard.Name, keyspaceName)
			tablet.VttabletProcess.DbPassword = dbPassword
			tablet.VttabletProcess.ExtraArgs = commonTabletArg
			tablet.VttabletProcess.SupportsBackup = true
			tablet.VttabletProcess.EnableSemiSync = true

			if useMysqlctld {
				tablet.MysqlctldProcess = *cluster.MysqlCtldProcessInstance(tablet.TabletUID, tablet.MySQLPort, localCluster.TmpDirectory)
				tablet.MysqlctldProcess.InitDBFile = newInitDBFile
				tablet.MysqlctldProcess.ExtraArgs = extraArgs
				tablet.MysqlctldProcess.Password = tablet.VttabletProcess.DbPassword
				err := tablet.MysqlctldProcess.Start()
				if err != nil {
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

		// initialize tablets
		master = shard.Vttablets[0]
		replica1 = shard.Vttablets[1]
		replica2 = shard.Vttablets[2]

		for _, tablet := range []*cluster.Vttablet{master, replica1} {
			if err := localCluster.VtctlclientProcess.InitTablet(tablet, cell, keyspaceName, hostname, shard.Name); err != nil {
				return 1, err
			}
		}

		// create database for master and replica
		for _, tablet := range []cluster.Vttablet{*master, *replica1} {
			if err := tablet.VttabletProcess.CreateDB(keyspaceName); err != nil {
				return 1, err
			}
			if err := tablet.VttabletProcess.Setup(); err != nil {
				return 1, err
			}
		}

		// initialize master and start replication
		if err := localCluster.VtctlclientProcess.InitShardMaster(keyspaceName, shard.Name, cell, master.TabletUID); err != nil {
			return 1, err
		}
		return m.Run(), nil
	}()

	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	} else {
		os.Exit(exitCode)
	}

}

// create query for test table creation
var vtInsertTest = `create table vt_insert_test (
	id bigint auto_increment,
	msg varchar(64),
	primary key (id)
	) Engine=InnoDB`

// TestBackupTransformImpl tests backups with transform hooks
func TestBackupTransformImpl(t *testing.T) {
	// insert data in master, validate same in replica
	defer cluster.PanicHandler(t)
	verifyInitialReplication(t)

	// restart the replica with transform hook parameter
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
	require.Nil(t, err)

	// take backup, it should not give any error
	err = localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	require.Nil(t, err)

	// insert data in master
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	require.Nil(t, err)

	// validate backup_list, expecting 1 backup available
	backups := localCluster.VerifyBackupCount(t, shardKsName, 1)

	backupLocation := localCluster.CurrentVTDATAROOT + "/backups/" + shardKsName + "/" + backups[0]

	// validate that MANIFEST has TransformHook
	// every file should start with 'header'
	validateManifestFile(t, backupLocation)

	// restore replica2 from backup, should not give any error
	// Note: we don't need to pass in the backup_storage_transform parameter,
	// as it is read from the MANIFEST.
	// clear replica2

	if replica2.MysqlctlProcess.TabletUID > 0 {
		replica2.MysqlctlProcess.Stop()
		os.RemoveAll(replica2.VttabletProcess.Directory)
		// start replica2 from backup
		err = replica2.MysqlctlProcess.Start()
		require.Nil(t, err)
	} else {
		replica2.MysqlctldProcess.Stop()
		os.RemoveAll(replica2.VttabletProcess.Directory)
		// start replica2 from backup
		err = replica2.MysqlctldProcess.Start()
		require.Nil(t, err)
	}

	err = localCluster.VtctlclientProcess.InitTablet(replica2, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err)
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

	// validate that new replica has all the data
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 2)

	// Remove all backups
	localCluster.RemoveAllBackups(t, shardKsName)

}

// TestBackupTransformErrorImpl validates backup behavior with transform hook
// when the hook encounters an error
func TestBackupTransformErrorImpl(t *testing.T) {
	// restart the replica with transform hook parameter
	defer cluster.PanicHandler(t)
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
	require.Nil(t, err)

	// create backup, it should fail
	out, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("Backup", replica1.Alias)
	require.NotNil(t, err)
	require.Containsf(t, out, "backup is not usable, aborting it", "unexpected error received %v", err)

	// validate there is no backup left
	localCluster.VerifyBackupCount(t, shardKsName, 0)
}

// validateManifestFile reads manifest and validates that it
// has a TransformHook, SkipCompress and FileEntries. It also
// validates that backup_files available in FileEntries have
// 'header' as their first line.
func validateManifestFile(t *testing.T, backupLocation string) {

	// reading manifest
	data, err := ioutil.ReadFile(backupLocation + "/MANIFEST")
	require.Nilf(t, err, "error while reading MANIFEST %v", err)
	manifest := make(map[string]interface{})

	// parsing manifest
	err = json.Unmarshal(data, &manifest)
	require.Nilf(t, err, "error while parsing MANIFEST %v", err)

	// validate manifest
	transformHook := manifest["TransformHook"]
	require.Equalf(t, "test_backup_transform", transformHook, "invalid transformHook in MANIFEST")
	skipCompress := manifest["SkipCompress"]
	assert.Equalf(t, skipCompress, true, "invalid value of skipCompress")

	// validate backup files
	fielEntries := manifest["FileEntries"]
	fileArr, ok := fielEntries.([]interface{})
	require.True(t, ok)
	for i := range fileArr {
		f, err := os.Open(fmt.Sprintf("%s/%d", backupLocation, i))
		require.Nilf(t, err, "error while opening backup_file %d: %v", i, err)
		var fileHeader string
		_, err = fmt.Fscanln(f, &fileHeader)
		f.Close()

		require.Nilf(t, err, "error while reading backup_file %d: %v", i, err)
		require.Equalf(t, "header", fileHeader, "wrong file contents for %d", i)
	}

}

// verifyReplicationStatus validates the replication status in tablet.
func verifyReplicationStatus(t *testing.T, vttablet *cluster.Vttablet, expectedStatus string) {
	status, err := vttablet.VttabletProcess.GetDBVar("rpl_semi_sync_slave_enabled", keyspaceName)
	require.Nil(t, err)
	assert.Equal(t, expectedStatus, status)
	status, err = vttablet.VttabletProcess.GetDBStatus("rpl_semi_sync_slave_status", keyspaceName)
	require.Nil(t, err)
	assert.Equal(t, expectedStatus, status)
}

// verifyInitialReplication creates schema in master, insert some data to master and verify the same data in replica
func verifyInitialReplication(t *testing.T) {
	_, err := master.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	require.Nil(t, err)
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.Nil(t, err)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 1)
}
