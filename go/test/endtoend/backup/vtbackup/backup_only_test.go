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
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/log"
)

var (
	vtInsertTest = `
		create table vt_insert_test (
		id bigint auto_increment,
		msg varchar(64),
		primary key (id)
		) Engine=InnoDB;`
)

func TestTabletInitialBackup(t *testing.T) {
	// Test Initial Backup Flow
	//    TestTabletInitialBackup will:
	//    - Create a shard using vtbackup and --initial-backup
	//    - Create the rest of the cluster restoring from backup
	//    - Externally Reparenting to a master tablet
	//    - Insert Some data
	//    - Verify that the cluster is working
	//    - Take a Second Backup
	//    - Bring up a second replica, and restore from the second backup
	//    - list the backups, remove them
	defer cluster.PanicHandler(t)

	vtBackup(t, true)
	verifyBackupCount(t, shardKsName, 1)

	// Initialize the tablets
	initTablets(t, false, false)

	// Restore the Tablets
	restore(t, master, "replica", "NOT_SERVING")
	err := localCluster.VtctlclientProcess.ExecuteCommand(
		"TabletExternallyReparented", master.Alias)
	require.Nil(t, err)
	restore(t, replica1, "replica", "SERVING")

	// Run the entire backup test
	firstBackupTest(t, "replica")

	tearDown(t, true)
}
func TestTabletBackupOnly(t *testing.T) {
	// Test Backup Flow
	//    TestTabletBackupOnly will:
	//    - Create a shard using regular init & start tablet
	//    - Run initShardMaster to start replication
	//    - Insert Some data
	//    - Verify that the cluster is working
	//    - Take a Second Backup
	//    - Bring up a second replica, and restore from the second backup
	//    - list the backups, remove them
	defer cluster.PanicHandler(t)

	// Reset the tablet object values in order on init tablet in the next step.
	master.VttabletProcess.ServingStatus = "NOT_SERVING"
	replica1.VttabletProcess.ServingStatus = "NOT_SERVING"

	initTablets(t, true, true)
	firstBackupTest(t, "replica")

	tearDown(t, false)
}

func firstBackupTest(t *testing.T, tabletType string) {
	// Test First Backup flow.
	//
	//    firstBackupTest will:
	//    - create a shard with master and replica1 only
	//    - run InitShardMaster
	//    - insert some data
	//    - take a backup
	//    - insert more data on the master
	//    - bring up replica2 after the fact, let it restore the backup
	//    - check all data is right (before+after backup data)
	//    - list the backup, remove it

	// Store initial backup counts
	backups, err := listBackups(shardKsName)
	require.Nil(t, err)

	// insert data on master, wait for replica to get it
	_, err = master.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	require.Nil(t, err)
	// Add a single row with value 'test1' to the master tablet
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.Nil(t, err)

	// Check that the specified tablet has the expected number of rows
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 1)

	// backup the replica
	log.Infof("taking backup %s", time.Now())
	vtBackup(t, false)
	log.Infof("done taking backup %s", time.Now())

	// check that the backup shows up in the listing
	verifyBackupCount(t, shardKsName, len(backups)+1)

	// insert more data on the master
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	require.Nil(t, err)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 2)

	// now bring up the other replica, letting it restore from backup.
	err = localCluster.VtctlclientProcess.InitTablet(replica2, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err)
	restore(t, replica2, "replica", "SERVING")
	// Replica2 takes time to serve. Sleeping for 5 sec.
	time.Sleep(5 * time.Second)
	//check the new replica has the data
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 2)

	// check that the restored replica has the right local_metadata
	result, err := replica2.VttabletProcess.QueryTabletWithDB("select * from local_metadata", "_vt")
	require.Nil(t, err)
	require.NotNil(t, result)
	require.NotEmpty(t, result.Rows)
	assert.Equal(t, replica2.Alias, result.Rows[0][1].ToString(), "Alias")
	assert.Equal(t, "ks.0", result.Rows[1][1].ToString(), "ClusterAlias")
	assert.Equal(t, cell, result.Rows[2][1].ToString(), "DataCenter")
	if tabletType == "replica" {
		assert.Equal(t, "neutral", result.Rows[3][1].ToString(), "PromotionRule")
	} else {
		assert.Equal(t, "must_not", result.Rows[3][1].ToString(), "PromotionRule")
	}

	removeBackups(t)
	verifyBackupCount(t, shardKsName, 0)

}

func vtBackup(t *testing.T, initialBackup bool) {
	// Take the back using vtbackup executable
	extraArgs := []string{"-allow_first_backup", "-db-credentials-file", dbCredentialFile}
	log.Infof("starting backup tablet %s", time.Now())
	err := localCluster.StartVtbackup(newInitDBFile, initialBackup, keyspaceName, shardName, cell, extraArgs...)
	require.Nil(t, err)
}

func verifyBackupCount(t *testing.T, shardKsName string, expected int) []string {
	backups, err := listBackups(shardKsName)
	require.Nil(t, err)
	assert.Equalf(t, expected, len(backups), "invalid number of backups")
	return backups
}

func listBackups(shardKsName string) ([]string, error) {
	backups, err := localCluster.VtctlProcess.ExecuteCommandWithOutput(
		"-backup_storage_implementation", "file",
		"-file_backup_storage_root",
		path.Join(os.Getenv("VTDATAROOT"), "tmp", "backupstorage"),
		"ListBackups", shardKsName,
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
	require.Nil(t, err)
	for _, backup := range backups {
		_, err := localCluster.VtctlProcess.ExecuteCommandWithOutput(
			"-backup_storage_implementation", "file",
			"-file_backup_storage_root",
			path.Join(os.Getenv("VTDATAROOT"), "tmp", "backupstorage"),
			"RemoveBackup", shardKsName, backup,
		)
		require.Nil(t, err)
	}
}

func initTablets(t *testing.T, startTablet bool, initShardMaster bool) {
	// Initialize tablets
	for _, tablet := range []cluster.Vttablet{*master, *replica1} {
		err := localCluster.VtctlclientProcess.InitTablet(&tablet, cell, keyspaceName, hostname, shardName)
		require.Nil(t, err)

		if startTablet {
			err = tablet.VttabletProcess.Setup()
			require.Nil(t, err)
		}
	}

	if initShardMaster {
		// choose master and start replication
		err := localCluster.VtctlclientProcess.InitShardMaster(keyspaceName, shardName, cell, master.TabletUID)
		require.Nil(t, err)
	}
}

func restore(t *testing.T, tablet *cluster.Vttablet, tabletType string, waitForState string) {
	// Erase mysql/tablet dir, then start tablet with restore enabled.

	log.Infof("restoring tablet %s", time.Now())
	resetTabletDirectory(t, *tablet, true)

	err := tablet.VttabletProcess.CreateDB(keyspaceName)
	require.Nil(t, err)

	// Start tablets
	tablet.VttabletProcess.ExtraArgs = []string{"-db-credentials-file", dbCredentialFile}
	tablet.VttabletProcess.TabletType = tabletType
	tablet.VttabletProcess.ServingStatus = waitForState
	tablet.VttabletProcess.SupportsBackup = true
	err = tablet.VttabletProcess.Setup()
	require.Nil(t, err)
}

func resetTabletDirectory(t *testing.T, tablet cluster.Vttablet, initMysql bool) {

	extraArgs := []string{"-db-credentials-file", dbCredentialFile}
	tablet.MysqlctlProcess.ExtraArgs = extraArgs

	// Shutdown Mysql
	err := tablet.MysqlctlProcess.Stop()
	require.Nil(t, err)
	// Teardown Tablet
	err = tablet.VttabletProcess.TearDown()
	require.Nil(t, err)

	// Empty the dir
	err = os.RemoveAll(tablet.VttabletProcess.Directory)
	require.Nil(t, err)

	if initMysql {
		// Init the Mysql
		tablet.MysqlctlProcess.InitDBFile = newInitDBFile
		err = tablet.MysqlctlProcess.Start()
		require.Nil(t, err)
	}

}

func tearDown(t *testing.T, initMysql bool) {
	// reset replication
	promoteCommands := "STOP SLAVE; RESET SLAVE ALL; RESET MASTER;"
	disableSemiSyncCommands := "SET GLOBAL rpl_semi_sync_master_enabled = false; SET GLOBAL rpl_semi_sync_slave_enabled = false"
	for _, tablet := range []cluster.Vttablet{*master, *replica1, *replica2} {
		_, err := tablet.VttabletProcess.QueryTablet(promoteCommands, keyspaceName, true)
		require.Nil(t, err)
		_, err = tablet.VttabletProcess.QueryTablet(disableSemiSyncCommands, keyspaceName, true)
		require.Nil(t, err)
		for _, db := range []string{"_vt", "vt_insert_test"} {
			_, err = tablet.VttabletProcess.QueryTablet(fmt.Sprintf("drop database if exists %s", db), keyspaceName, true)
			require.Nil(t, err)
		}
	}

	// TODO: Ideally we should not be resetting the mysql.
	// So in below code we will have to uncomment the commented code and remove resetTabletDirectory
	for _, tablet := range []cluster.Vttablet{*master, *replica1, *replica2} {
		//Tear down Tablet
		//err := tablet.VttabletProcess.TearDown()
		//require.Nil(t, err)

		resetTabletDirectory(t, tablet, initMysql)
		// DeleteTablet on a primary will cause tablet to shutdown, so should only call it after tablet is already shut down
		err := localCluster.VtctlclientProcess.ExecuteCommand("DeleteTablet", "-allow_master", tablet.Alias)
		require.Nil(t, err)
	}
}
