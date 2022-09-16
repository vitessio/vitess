/*
Copyright 2022 The Vitess Authors.

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
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"vitess.io/vitess/go/vt/log"
)

var (
	vtInsertTest = `
		create table if not exists vt_insert_test (
		id bigint auto_increment,
		msg varchar(64),
		primary key (id)
		) Engine=InnoDB;`
)

func TestTabletRestartAfterInitPrimary(t *testing.T) {
	//    - Initialize tablets with one primary and two replica
	//    - Insert some data and verify replication is working
	//    - Take a replica backup. This will stop and start mysql
	//    - Now tear down primary, and create new replica
	//    - This will start producing errors with super-read-only permission for primary (new replica)
	//    - Make this new replica primary via InitPrimary. This will stop super-read-only errors.
	//    - Teardown all tablets
	defer cluster.PanicHandler(t)

	// Initialize the tablets
	initTablets(t, true, true)

	currentCount := 0
	qr, _ := primary.VttabletProcess.QueryTablet("select * from vt_insert_test", keyspaceName, true)
	if qr != nil {
		currentCount = len(qr.Rows)
	}

	// create vt_insert_test table
	_, err := primary.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	require.Nil(t, err)
	// Add a single row with value 'test1' to the primary tablet
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.Nil(t, err)
	currentCount++
	// Check that the specified tablet has the expected number of rows
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, currentCount)

	// backup the replica
	log.Infof("taking backup %s", time.Now())
	vtBackup(t, false, true)
	log.Infof("done taking backup %s", time.Now())

	// teardown primary but don't teardown mysql
	stopOrRestartTablet(t, primary, false, true, true, false)

	time.Sleep(5 * time.Second)

	// teardown will bring down primary and bring up a new replica. It will start giving super-read-only
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.ErrorContains(t, err, "The MySQL server is running with the --super-read-only option so it cannot execute this statement")

	// teardown primary but don't teardown mysql, this time make new replica primary
	stopOrRestartTablet(t, primary, false, true, true, true)

	// Add a single row with value 'test1' to the primary tablet
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.Nil(t, err)
	currentCount++
	// Check that the specified tablet has the expected number of rows
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, currentCount)

	tearDown(t)
}

func TestTabletRestartAfterPRS(t *testing.T) {
	//    - Initialize tablets with one primary and two replica
	//    - Insert some data and verify replication is working
	//    - Take a replica backup. This will stop and start mysql
	//    - Make replica1 primarry via PlannedReparentShard
	//    - Any insert in primary will now fail as it has super-read-only enable now (due to demotion)
	//    - replica1 is newly promoted primary so no inserts error there.
	//    - If you teardown replica1 and create new replica , it will start giving super-read-only errors
	//      until you make it primary again.
	defer cluster.PanicHandler(t)

	// Initialize the tablets
	initTablets(t, true, true)

	currentCount := 0
	qr, _ := primary.VttabletProcess.QueryTablet("select * from vt_insert_test", keyspaceName, true)
	if qr != nil {
		currentCount = len(qr.Rows)
	}

	// insert data on primary, wait for replica to get it
	_, err := primary.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	require.Nil(t, err)
	// Add a single row with value 'test1' to the primary tablet
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.Nil(t, err)
	currentCount++

	// Check that the specified tablet has the expected number of rows
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, currentCount)

	// backup the replica
	log.Infof("taking backup %s", time.Now())
	vtBackup(t, false, true)
	log.Infof("done taking backup %s", time.Now())

	// reparent to replica1
	err = localCluster.VtctlclientProcess.ExecuteCommand("PlannedReparentShard", "--",
		"--keyspace_shard", shardKsName,
		"--new_primary", replica1.Alias)
	require.Nil(t, err)

	time.Sleep(5 * time.Second)

	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.ErrorContains(t, err, "The MySQL server is running with the --super-read-only option so it cannot execute this statement")

	_, err = replica1.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.Nil(t, err)
	currentCount++

	// Check that the specified tablet has the expected number of rows
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, currentCount)

	// teardown replica1 but don't teardown mysql
	stopOrRestartTablet(t, replica1, false, true, true, false)

	time.Sleep(5 * time.Second)

	// Add a single row with value 'test1' to the primary tablet
	_, err = replica1.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.ErrorContains(t, err, "The MySQL server is running with the --super-read-only option so it cannot execute this statement")

	stopOrRestartTablet(t, replica1, false, true, true, true)

	time.Sleep(5 * time.Second)
	// Add a single row with value 'test1' to the primary tablet
	_, err = replica1.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.Nil(t, err)
	currentCount++

	stopOrRestartTablet(t, primary, false, true, true, false)
	time.Sleep(5 * time.Second)
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.ErrorContains(t, err, "The MySQL server is running with the --super-read-only option so it cannot execute this statement")

	cluster.VerifyRowsInTablet(t, primary, keyspaceName, currentCount)
	tearDown(t)
}

func TestTabletRestartAfterERS(t *testing.T) {
	//    - Initialize tablets with one primary and two replica
	//    - Insert some data and verify replication is working
	//    - Take a replica backup. This will stop and start mysql
	//    - Make replica1 primary via ERS
	//    - Since ERS does not set super-read-only, therefore both replica1 and primar
	//       still fail during inserts with super-read-only errors
	//    - replica1 will start passing inserts only when we call InitShardPrimary explicitly.
	defer cluster.PanicHandler(t)

	// Initialize the tablets
	initTablets(t, true, true)

	currentCount := 0
	qr, _ := primary.VttabletProcess.QueryTablet("select * from vt_insert_test", keyspaceName, true)
	if qr != nil {
		currentCount = len(qr.Rows)
	}

	// insert data on primary, wait for replica to get it
	_, err := primary.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	require.Nil(t, err)
	// Add a single row with value 'test1' to the primary tablet
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.Nil(t, err)
	currentCount++
	// Check that the specified tablet has the expected number of rows
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, currentCount)

	// backup the replica
	log.Infof("taking backup %s", time.Now())
	vtBackup(t, false, true)
	log.Infof("done taking backup %s", time.Now())

	// external reparent to replica1
	err = localCluster.VtctlclientProcess.ExecuteCommand("TabletExternallyReparented", replica1.Alias)
	require.Nil(t, err)

	/*err = localCluster.VtctlclientProcess.InitShardPrimary(keyspaceName, shardName, cell, replica1.TabletUID)
	require.Nil(t, err)*/

	time.Sleep(5 * time.Second)

	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.ErrorContains(t, err, "The MySQL server is running with the --super-read-only option so it cannot execute this statement")

	_, err = replica1.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.ErrorContains(t, err, "The MySQL server is running with the --super-read-only option so it cannot execute this statement")

	// Check that the specified tablet has the expected number of rows
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, currentCount)

	// teardown primary but don't teardown mysql
	stopOrRestartTablet(t, replica1, false, true, true, false)

	time.Sleep(5 * time.Second)

	// Add a single row with value 'test1' to the primary tablet
	_, err = replica1.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.ErrorContains(t, err, "The MySQL server is running with the --super-read-only option so it cannot execute this statement")

	err = localCluster.VtctlclientProcess.InitShardPrimary(keyspaceName, shardName, cell, replica1.TabletUID)
	require.Nil(t, err)

	time.Sleep(5 * time.Second)
	// Add a single row with value 'test1' to the primary tablet
	_, err = replica1.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.Nil(t, err)
	currentCount++

	stopOrRestartTablet(t, replica1, false, true, true, false)

	_, err = replica1.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.Nil(t, err)
	currentCount++

	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, currentCount)
	tearDown(t)
}

func TestTabletRestartAfterMySqlRestart(t *testing.T) {
	//    - Initialize tablets with one primary and two replica
	//    - Insert some data and verify replication is working
	//    - Take a replica backup. This will stop and start mysql
	//    - Restart only mysql instance of primary tablet
	//    - Mysql will now start giving super-read-only errors.
	defer cluster.PanicHandler(t)

	// Initialize the tablets
	initTablets(t, true, true)

	currentCount := 0
	qr, _ := primary.VttabletProcess.QueryTablet("select * from vt_insert_test", keyspaceName, true)
	if qr != nil {
		currentCount = len(qr.Rows)
	}

	// insert data on primary, wait for replica to get it
	_, err := primary.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	require.Nil(t, err)
	// Add a single row with value 'test1' to the primary tablet
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.Nil(t, err)
	currentCount++
	// Check that the specified tablet has the expected number of rows
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, currentCount)

	// backup the replica
	log.Infof("taking backup %s", time.Now())
	vtBackup(t, false, true)
	log.Infof("done taking backup %s", time.Now())

	// just restart mysql process
	err = primary.MysqlctlProcess.Stop()
	require.Nil(t, err)
	primary.MysqlctlProcess.InitMysql = false
	err = primary.MysqlctlProcess.Start()
	require.Nil(t, err)

	time.Sleep(5 * time.Second)

	// Add a single row with value 'test1' to the primary tablet
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	require.ErrorContains(t, err, "The MySQL server is running with the --super-read-only option so it cannot execute this statement")
}

func vtBackup(t *testing.T, initialBackup bool, restartBeforeBackup bool) {
	// Take the backup using vtbackup executable
	extraArgs := []string{"--allow_first_backup", "--db-credentials-file", dbCredentialFile}
	if restartBeforeBackup {
		extraArgs = append(extraArgs, "--restart_before_backup")
	}
	log.Infof("starting backup tablet %s", time.Now())
	err := localCluster.StartVtbackup(newInitDBFile, initialBackup, keyspaceName, shardName, cell, extraArgs...)
	require.Nil(t, err)
}

func initTablets(t *testing.T, startTablet bool, initShardPrimary bool) {
	// Initialize tablets
	for _, tablet := range []cluster.Vttablet{*primary, *replica1, *replica2} {
		if initShardPrimary {
			tablet.VttabletProcess.ExtraArgs = removeFromArgs(tablet.VttabletProcess.ExtraArgs, "--disable_active_reparents=true")
			if !existsInArgs(tablet.VttabletProcess.ExtraArgs, "--use_super_read_only=true") {
				tablet.VttabletProcess.ExtraArgs = append(tablet.VttabletProcess.ExtraArgs, fmt.Sprintf("--use_super_read_only=%t", true))
			}
		}
		err := localCluster.VtctlclientProcess.InitTablet(&tablet, cell, keyspaceName, hostname, shardName)
		require.Nil(t, err)

		if startTablet {
			tablet.VttabletProcess.ServingStatus = "NOT_SERVING"
			err = tablet.VttabletProcess.Setup()
			require.Nil(t, err)
		}
	}

	// wait for both tablet to get into replica state in topo
	waitUntil := time.Now().Add(10 * time.Second)
	for time.Now().Before(waitUntil) {
		result, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("ListAllTablets", cell)
		require.Nil(t, err)

		tabletsFromCMD := strings.Split(result, "\n")
		tabletCountFromCMD := 0

		for _, line := range tabletsFromCMD {
			if len(line) > 0 {
				if strings.Contains(line, "replica") {
					tabletCountFromCMD = tabletCountFromCMD + 1
				}
			}
		}

		if tabletCountFromCMD == 2 {
			break
		}

		time.Sleep(1 * time.Second)
	}

	if initShardPrimary {
		// choose primary and start replication
		err := localCluster.VtctlclientProcess.InitShardPrimary(keyspaceName, shardName, cell, primary.TabletUID)
		require.Nil(t, err)
	}
}

func removeFromArgs(extraArgs []string, argToRemove string) []string {
	for index, val := range extraArgs {
		if val == argToRemove {
			return append(extraArgs[:index], extraArgs[index+1:]...)
		}
	}
	return extraArgs
}

func existsInArgs(extraArgs []string, arg string) bool {
	for _, val := range extraArgs {
		if val == arg {
			return true
		}
	}
	return false
}

func tearDown(t *testing.T) {
	for _, tablet := range []cluster.Vttablet{*primary, *replica1, *replica2} {
		stopOrRestartTablet(t, &tablet, false, false, true, false)
	}
}

func stopOrRestartTablet(t *testing.T, tablet *cluster.Vttablet, tearMySQL bool, startNewTablet bool, tearOldTablet bool, makePrimary bool) {
	// Stop all primary, replica tablet and mysql instance
	stopTablet(t, tablet, tearOldTablet, tearMySQL, true)

	// start all tablet and mysql instances
	var mysqlProcs []*exec.Cmd

	if tearMySQL {
		tablet.MysqlctlProcess.InitDBFile = newInitDBFile
		extraArgs := []string{"--db-credentials-file", dbCredentialFile}
		tablet.MysqlctlProcess.ExtraArgs = extraArgs
		err := tablet.MysqlctlProcess.Start()
		require.Nilf(t, err, "error while starting mysqlctld, tabletUID %v", tablet.TabletUID)
	}

	for _, proc := range mysqlProcs {
		proc.Wait()
	}

	if startNewTablet {
		err := tablet.VttabletProcess.Setup()
		require.Nil(t, err)

		// wait for both tablet to get into replica state in topo
		waitUntil := time.Now().Add(10 * time.Second)
		for time.Now().Before(waitUntil) {
			result, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("ListAllTablets", cell)
			require.Nil(t, err)

			tabletsFromCMD := strings.Split(result, "\n")
			tabletCountFromCMD := 0

			for _, line := range tabletsFromCMD {
				if len(line) > 0 {
					if strings.Contains(line, "replica") && strings.Contains(line, tablet.Alias) {
						tabletCountFromCMD = tabletCountFromCMD + 1
					}
				}
			}

			if tabletCountFromCMD == 1 {
				break
			}

			time.Sleep(1 * time.Second)
		}
	}

	if makePrimary {
		err := localCluster.VtctlclientProcess.InitShardPrimary(keyspaceName, shardName, cell, tablet.TabletUID)
		require.Nil(t, err)
	}
}

func stopTablet(t *testing.T, tablet *cluster.Vttablet, tearOldTablet bool, tearMySQL bool, removeDirectory bool) {
	if tearOldTablet {
		// reset replication
		promoteCommands := "STOP SLAVE; RESET SLAVE ALL; RESET MASTER;"
		disableSemiSyncCommands := "SET GLOBAL rpl_semi_sync_master_enabled = false; SET GLOBAL rpl_semi_sync_slave_enabled = false"

		_, err := tablet.VttabletProcess.QueryTablet(promoteCommands, keyspaceName, true)
		require.Nil(t, err)
		_, err = tablet.VttabletProcess.QueryTablet(disableSemiSyncCommands, keyspaceName, true)
		require.Nil(t, err)
		for _, db := range []string{"_vt", "vt_insert_test"} {
			_, err = tablet.VttabletProcess.QueryTabletWithSuperReadOnlyHandling(fmt.Sprintf("drop database if exists %s", db), keyspaceName, true)
			require.Nil(t, err)
		}
		tablet.VttabletProcess.TearDown()
	}

	var mysqlProcs []*exec.Cmd

	if tearMySQL {
		err := tablet.MysqlctlProcess.Stop()
		require.Nil(t, err)
		if removeDirectory {
			err = os.RemoveAll(tablet.VttabletProcess.Directory)
			require.Nil(t, err)
		}

	}

	if tearOldTablet {
		localCluster.VtctlclientProcess.ExecuteCommand("DeleteTablet", "--", "--allow_primary", tablet.Alias)
	}

	for _, proc := range mysqlProcs {
		proc.Wait()
	}
}
