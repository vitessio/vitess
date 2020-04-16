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

package reparent

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/mysql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/test/endtoend/cluster"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestMasterToSpareStateChangeImpossible(t *testing.T) {
	defer cluster.PanicHandler(t)

	args := []string{"InitTablet", "-hostname", hostname,
		"-port", fmt.Sprintf("%d", tablet62344.HTTPPort), "-allow_update", "-parent",
		"-keyspace", keyspaceName,
		"-shard", shardName,
		"-mysql_port", fmt.Sprintf("%d", tablet62344.MySQLPort),
		"-grpc_port", fmt.Sprintf("%d", tablet62344.GrpcPort)}
	args = append(args, fmt.Sprintf("%s-%010d", tablet62344.Cell, tablet62344.TabletUID), "master")
	err := clusterInstance.VtctlclientProcess.ExecuteCommand(args...)
	require.Nil(t, err)

	// Start the tablet
	err = tablet62344.VttabletProcess.Setup()
	require.Nil(t, err)

	// Create Database
	err = tablet62344.VttabletProcess.CreateDB(keyspaceName)
	require.Nil(t, err)

	// We cannot change a master to spare
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", tablet62344.Alias, "spare")
	require.Error(t, err)

	//kill Tablet
	err = tablet62344.VttabletProcess.TearDown()
	require.Nil(t, err)
}

func TestReparentDownMaster(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		// Create Database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		require.Nil(t, err)

		// Reset status, don't wait for the tablet status. We will check it later
		tablet.VttabletProcess.ServingStatus = ""
		// Init Tablet
		err = clusterInstance.VtctlclientProcess.InitTablet(&tablet, tablet.Cell, keyspaceName, hostname, shardName)
		require.Nil(t, err)

		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.Nil(t, err)
	}

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"SERVING", "NOT_SERVING"})
		require.Nil(t, err)
	}

	// Init Shard Master
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shardName), tablet62344.Alias)
	require.Nil(t, err)

	validateTopology(t, true)

	// create Tables
	runSQL(ctx, t, sqlSchema, tablet62344)

	// Make the current master agent and database unavailable.
	err = tablet62344.VttabletProcess.TearDown()
	require.Nil(t, err)
	err = tablet62344.MysqlctlProcess.Stop()
	require.Nil(t, err)

	// Perform a planned reparent operation, will try to contact
	// the current master and fail somewhat quickly
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"PlannedReparentShard",
		"-wait-time", "5s",
		"-keyspace_shard", keyspaceShard,
		"-new_master", tablet62044.Alias)
	require.Error(t, err)

	// Run forced reparent operation, this should now proceed unimpeded.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"EmergencyReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-new_master", tablet62044.Alias)
	require.Nil(t, err)

	validateTopology(t, false)

	checkMasterTablet(t, tablet62044)

	// insert data into the new master, check the connected replica work
	insertSQL := fmt.Sprintf(insertSQL, 2, 2)
	runSQL(ctx, t, insertSQL, tablet62044)
	err = checkInsertedValues(ctx, t, tablet41983, 2)
	require.Nil(t, err)
	err = checkInsertedValues(ctx, t, tablet31981, 2)
	require.Nil(t, err)

	// bring back the old master as a replica, check that it catches up
	tablet62344.MysqlctlProcess.InitMysql = false
	err = tablet62344.MysqlctlProcess.Start()
	require.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.InitTablet(tablet62344, tablet62344.Cell, keyspaceName, hostname, shardName)
	require.Nil(t, err)

	// As there is already a master the new replica will come directly in SERVING state
	tablet62344.VttabletProcess.ServingStatus = "SERVING"
	// Start the tablet
	err = tablet62344.VttabletProcess.Setup()
	require.Nil(t, err)

	err = checkInsertedValues(ctx, t, tablet62344, 2)
	require.Nil(t, err)

	// Kill tablets
	killTablets(t)
}

func TestReparentCrossCell(t *testing.T) {

	defer cluster.PanicHandler(t)
	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		// create database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		require.Nil(t, err)

		// Init Tablet
		err = clusterInstance.VtctlclientProcess.InitTablet(&tablet, tablet.Cell, keyspaceName, hostname, shardName)
		require.Nil(t, err)

		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.Nil(t, err)
	}

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"SERVING", "NOT_SERVING"})
		require.Nil(t, err)
	}

	// Force the replica to reparent assuming that all the datasets are identical.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shardName), tablet62344.Alias)
	require.Nil(t, err)

	validateTopology(t, true)

	checkMasterTablet(t, tablet62344)

	// Perform a graceful reparent operation to another cell.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"PlannedReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-new_master", tablet31981.Alias)
	require.Nil(t, err)

	validateTopology(t, false)

	checkMasterTablet(t, tablet31981)

	// Kill tablets
	killTablets(t)

}

func TestReparentGraceful(t *testing.T) {
	reparentGraceful(t, false)
}

func TestReparentGracefulRecovery(t *testing.T) {
	reparentGraceful(t, true)
}

func reparentGraceful(t *testing.T, confusedMaster bool) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		// create database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		require.Nil(t, err)

		// Init Tablet
		err = clusterInstance.VtctlclientProcess.InitTablet(&tablet, tablet.Cell, keyspaceName, hostname, shardName)
		require.Nil(t, err)

		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.Nil(t, err)
	}

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"SERVING", "NOT_SERVING"})
		require.Nil(t, err)
	}

	// Force the replica to reparent assuming that all the datasets are identical.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shardName), tablet62344.Alias)
	require.Nil(t, err)

	validateTopology(t, true)

	// create Tables
	runSQL(ctx, t, sqlSchema, tablet62344)

	checkMasterTablet(t, tablet62344)

	validateTopology(t, false)

	// Run this to make sure it succeeds.
	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"ShardReplicationPositions", fmt.Sprintf("%s/%s", keyspaceName, shardName))
	require.Nil(t, err)
	strArray := strings.Split(output, "\n")
	if strArray[len(strArray)-1] == "" {
		strArray = strArray[:len(strArray)-1] // Truncate slice, remove empty line
	}
	assert.Equal(t, 4, len(strArray))         // one master, three replicas
	assert.Contains(t, strArray[0], "master") // master first

	// Perform a graceful reparent operation
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"PlannedReparentShard",
		"-keyspace_shard", fmt.Sprintf("%s/%s", keyspaceName, shardName),
		"-new_master", tablet62044.Alias)
	require.Nil(t, err)

	validateTopology(t, false)

	checkMasterTablet(t, tablet62044)

	// Simulate a master that forgets it's master and becomes replica.
	// PlannedReparentShard should be able to recover by reparenting to the same master again,
	// as long as all tablets are available to check that it's safe.
	if confusedMaster {
		tablet62044.Type = "replica"
		err = clusterInstance.VtctlclientProcess.InitTablet(tablet62044, tablet62044.Cell, keyspaceName, hostname, shardName)
		require.Nil(t, err)

		err = clusterInstance.VtctlclientProcess.ExecuteCommand("RefreshState", tablet62044.Alias)
		require.Nil(t, err)
	}

	// Perform a graceful reparent to the same master.
	// It should be idempotent, and should fix any inconsistencies if necessary
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"PlannedReparentShard",
		"-keyspace_shard", fmt.Sprintf("%s/%s", keyspaceName, shardName),
		"-new_master", tablet62044.Alias)
	require.Nil(t, err)

	validateTopology(t, false)

	checkMasterTablet(t, tablet62044)

	// insert data into the new master, check the connected replica work
	insertSQL := fmt.Sprintf(insertSQL, 1, 1)
	runSQL(ctx, t, insertSQL, tablet62044)
	err = checkInsertedValues(ctx, t, tablet41983, 1)
	require.Nil(t, err)
	err = checkInsertedValues(ctx, t, tablet62344, 1)
	require.Nil(t, err)

	// Kill tablets
	killTablets(t)
}

func TestReparentSlaveOffline(t *testing.T) {
	defer cluster.PanicHandler(t)

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		// create database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		require.Nil(t, err)

		// Init Tablet
		err = clusterInstance.VtctlclientProcess.InitTablet(&tablet, tablet.Cell, keyspaceName, hostname, shardName)
		require.Nil(t, err)

		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.Nil(t, err)
	}

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"SERVING", "NOT_SERVING"})
		require.Nil(t, err)
	}

	// Force the replica to reparent assuming that all the datasets are identical.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", keyspaceShard, tablet62344.Alias)
	require.Nil(t, err)

	validateTopology(t, true)

	checkMasterTablet(t, tablet62344)

	// Kill one tablet so we seem offline
	err = tablet31981.VttabletProcess.TearDown()
	require.Nil(t, err)

	// Perform a graceful reparent operation.
	out, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"PlannedReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-new_master", tablet62044.Alias)
	require.Error(t, err)
	assert.Contains(t, out, "tablet zone2-0000031981 SetMaster failed")

	checkMasterTablet(t, tablet62044)

	killTablets(t)
}

func TestReparentAvoid(t *testing.T) {
	defer cluster.PanicHandler(t)
	// Remove tablet41983 from topology as that tablet is not required for this test
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", tablet41983.Alias)
	require.Nil(t, err)

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet31981} {
		// create database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		require.Nil(t, err)

		// Init Tablet
		err = clusterInstance.VtctlclientProcess.InitTablet(&tablet, tablet.Cell, keyspaceName, hostname, shardName)
		require.Nil(t, err)

		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.Nil(t, err)
	}

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet31981} {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"SERVING", "NOT_SERVING"})
		require.Nil(t, err)
	}

	// Force the replica to reparent assuming that all the dataset's are identical.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", keyspaceShard, tablet62344.Alias)
	require.Nil(t, err)

	validateTopology(t, true)

	checkMasterTablet(t, tablet62344)

	// Perform a reparent operation with avoid_master pointing to non-master. It
	// should succeed without doing anything.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"PlannedReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-avoid_master", tablet62044.Alias)
	require.Nil(t, err)

	validateTopology(t, false)

	checkMasterTablet(t, tablet62344)

	// Perform a reparent operation with avoid_master pointing to master.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"PlannedReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-avoid_master", tablet62344.Alias)
	require.Nil(t, err)

	validateTopology(t, false)

	// 62044 is in the same cell and 31981 is in a different cell, so we must land on 62044
	checkMasterTablet(t, tablet62044)

	// If we kill the tablet in the same cell as master then reparent -avoid_master will fail.
	err = tablet62344.VttabletProcess.TearDown()
	require.Nil(t, err)

	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"PlannedReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-avoid_master", tablet62044.Alias)
	require.Error(t, err)
	assert.Contains(t, output, "cannot find a tablet to reparent to")

	validateTopology(t, false)

	checkMasterTablet(t, tablet62044)

	killTablets(t)
}

func TestReparentFromOutside(t *testing.T) {
	reparentFromOutside(t, false)
}

func TestReparentFromOutsideWithNoMaster(t *testing.T) {
	defer cluster.PanicHandler(t)
	reparentFromOutside(t, true)

	// We will have to restart mysql to avoid hanging/locks due to external Reparent
	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		log.Infof("Restarting MySql for tablet %v", tablet.Alias)
		err := tablet.MysqlctlProcess.Stop()
		require.Nil(t, err)
		tablet.MysqlctlProcess.InitMysql = false
		err = tablet.MysqlctlProcess.Start()
		require.Nil(t, err)
	}
}

func reparentFromOutside(t *testing.T, downMaster bool) {
	//This test will start a master and 3 replicas.
	//Then:
	//- one replica will be the new master
	//- one replica will be reparented to that new master
	//- one replica will be busted and dead in the water and we'll call TabletExternallyReparented.
	//Args:
	//downMaster: kills the old master first
	defer cluster.PanicHandler(t)

	ctx := context.Background()

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		// create database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		require.Nil(t, err)

		// Init Tablet
		err = clusterInstance.VtctlclientProcess.InitTablet(&tablet, tablet.Cell, keyspaceName, hostname, shardName)
		require.Nil(t, err)

		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.Nil(t, err)
	}

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"SERVING", "NOT_SERVING"})
		require.Nil(t, err)
	}

	// Reparent as a starting point
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shardName), tablet62344.Alias)
	require.Nil(t, err)

	validateTopology(t, true)

	checkMasterTablet(t, tablet62344)

	// now manually reparent 1 out of 2 tablets
	// 62044 will be the new master
	// 31981 won't be re-parented, so it will be busted

	if !downMaster {
		// commands to stop the current master
		demoteMasterCommands := "SET GLOBAL read_only = ON; FLUSH TABLES WITH READ LOCK; UNLOCK TABLES"
		runSQL(ctx, t, demoteMasterCommands, tablet62344)

		//Get the position of the old master and wait for the new one to catch up.
		err = waitForReplicationPosition(t, tablet62344, tablet62044)
		require.Nil(t, err)
	}

	// commands to convert a replica to a master
	promoteSlaveCommands := "STOP SLAVE; RESET SLAVE ALL; SET GLOBAL read_only = OFF;"
	runSQL(ctx, t, promoteSlaveCommands, tablet62044)

	// Get master position
	_, gtID := cluster.GetMasterPosition(t, *tablet62044, hostname)

	// 62344 will now be a slave of 62044
	changeMasterCommands := fmt.Sprintf("RESET MASTER; RESET SLAVE; SET GLOBAL gtid_purged = '%s';"+
		"CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='vt_repl', MASTER_AUTO_POSITION = 1;"+
		"START SLAVE;", gtID, hostname, tablet62044.MySQLPort)
	runSQL(ctx, t, changeMasterCommands, tablet62344)

	// Capture time when we made tablet62044 master
	baseTime := time.Now().UnixNano() / 1000000000

	// 41983 will be a slave of 62044
	changeMasterCommands = fmt.Sprintf("STOP SLAVE; RESET MASTER; SET GLOBAL gtid_purged = '%s';"+
		"CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='vt_repl', MASTER_AUTO_POSITION = 1;"+
		"START SLAVE;", gtID, hostname, tablet62044.MySQLPort)
	runSQL(ctx, t, changeMasterCommands, tablet41983)

	// To test the downMaster, we kill the old master first and delete its tablet record
	if downMaster {
		err := tablet62344.VttabletProcess.TearDown()
		require.Nil(t, err)
		err = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet",
			"-allow_master", tablet62344.Alias)
		require.Nil(t, err)
	}

	// update topology with the new server
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("TabletExternallyReparented",
		tablet62044.Alias)
	require.Nil(t, err)

	checkReparentFromOutside(t, tablet62044, downMaster, baseTime)

	if !downMaster {
		err := tablet62344.VttabletProcess.TearDown()
		require.Nil(t, err)
	}

	killTablets(t)
}

func TestReparentWithDownSlave(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		// Create Database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		require.Nil(t, err)

		// Init Tablet
		err = clusterInstance.VtctlclientProcess.InitTablet(&tablet, tablet.Cell, keyspaceName, hostname, shardName)
		require.Nil(t, err)

		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.Nil(t, err)
	}

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"SERVING", "NOT_SERVING"})
		require.Nil(t, err)
	}

	// Init Shard Master
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shardName), tablet62344.Alias)
	require.Nil(t, err)

	validateTopology(t, true)

	// create Tables
	runSQL(ctx, t, sqlSchema, tablet62344)

	// Stop replica mysql Process
	err = tablet41983.MysqlctlProcess.Stop()
	require.Nil(t, err)

	// Perform a graceful reparent operation. It will fail as one tablet is down.
	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"PlannedReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-new_master", tablet62044.Alias)
	require.Error(t, err)
	assert.Contains(t, output, "TabletManager.SetMaster on zone1-0000041983 error")

	// insert data into the new master, check the connected replica work
	insertSQL := fmt.Sprintf(insertSQL, 3, 3)
	runSQL(ctx, t, insertSQL, tablet62044)
	err = checkInsertedValues(ctx, t, tablet31981, 3)
	require.Nil(t, err)
	err = checkInsertedValues(ctx, t, tablet62344, 3)
	require.Nil(t, err)

	// restart mysql on the old replica, should still be connecting to the old master
	tablet41983.MysqlctlProcess.InitMysql = false
	err = tablet41983.MysqlctlProcess.Start()
	require.Nil(t, err)

	// Use the same PlannedReparentShard command to fix up the tablet.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"PlannedReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-new_master", tablet62044.Alias)
	require.Nil(t, err)

	// wait until it gets the data
	err = checkInsertedValues(ctx, t, tablet41983, 3)
	require.Nil(t, err)

	killTablets(t)
}

func TestChangeTypeSemiSync(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	// Create new names for tablets, so this test is less confusing.
	master := tablet62344
	replica := tablet62044
	rdonly1 := tablet41983
	rdonly2 := tablet31981

	for _, tablet := range []cluster.Vttablet{*master, *replica, *rdonly1, *rdonly2} {
		// Create Database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		require.Nil(t, err)

		// Init Tablet
		err = clusterInstance.VtctlclientProcess.InitTablet(&tablet, tablet.Cell, keyspaceName, hostname, shardName)
		require.Nil(t, err)

		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.Nil(t, err)
	}

	// Init Shard Master
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shardName), master.Alias)
	require.Nil(t, err)

	for _, tablet := range []cluster.Vttablet{*master, *replica, *rdonly1, *rdonly2} {
		err := tablet.VttabletProcess.WaitForTabletType("SERVING")
		require.Nil(t, err)
	}

	// Updated rdonly tablet and set tablet type to rdonly
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", rdonly1.Alias, "rdonly")
	require.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", rdonly2.Alias, "rdonly")
	require.Nil(t, err)

	validateTopology(t, true)

	checkMasterTablet(t, master)

	// Stop replication on rdonly1, to make sure when we make it replica it doesn't start again.
	// Note we do a similar test for replica -> rdonly below.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopSlave", rdonly1.Alias)
	require.Nil(t, err)

	// Check semi-sync on replicas.
	// The flag is only an indication of the value to use next time
	// we turn replication on, so also check the status.
	// rdonly1 is not replicating, so its status is off.
	checkDBvar(ctx, t, replica, "rpl_semi_sync_slave_enabled", "ON")
	checkDBvar(ctx, t, rdonly1, "rpl_semi_sync_slave_enabled", "OFF")
	checkDBvar(ctx, t, rdonly2, "rpl_semi_sync_slave_enabled", "OFF")
	checkDBstatus(ctx, t, replica, "Rpl_semi_sync_slave_status", "ON")
	checkDBstatus(ctx, t, rdonly1, "Rpl_semi_sync_slave_status", "OFF")
	checkDBstatus(ctx, t, rdonly2, "Rpl_semi_sync_slave_status", "OFF")

	// Change replica to rdonly while replicating, should turn off semi-sync, and restart replication.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", replica.Alias, "rdonly")
	require.Nil(t, err)
	checkDBvar(ctx, t, replica, "rpl_semi_sync_slave_enabled", "OFF")
	checkDBstatus(ctx, t, replica, "Rpl_semi_sync_slave_status", "OFF")

	// Change rdonly1 to replica, should turn on semi-sync, and not start replication.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", rdonly1.Alias, "replica")
	require.Nil(t, err)
	checkDBvar(ctx, t, rdonly1, "rpl_semi_sync_slave_enabled", "ON")
	checkDBstatus(ctx, t, rdonly1, "Rpl_semi_sync_slave_status", "OFF")
	checkSlaveStatus(ctx, t, rdonly1)

	// Now change from replica back to rdonly, make sure replication is still not enabled.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", rdonly1.Alias, "rdonly")
	require.Nil(t, err)
	checkDBvar(ctx, t, rdonly1, "rpl_semi_sync_slave_enabled", "OFF")
	checkDBstatus(ctx, t, rdonly1, "Rpl_semi_sync_slave_status", "OFF")
	checkSlaveStatus(ctx, t, rdonly1)

	// Change rdonly2 to replica, should turn on semi-sync, and restart replication.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", rdonly2.Alias, "replica")
	require.Nil(t, err)
	checkDBvar(ctx, t, rdonly2, "rpl_semi_sync_slave_enabled", "ON")
	checkDBstatus(ctx, t, rdonly2, "Rpl_semi_sync_slave_status", "ON")

	killTablets(t)
}

func TestReparentDoesntHangIfMasterFails(t *testing.T) {
	defer cluster.PanicHandler(t)
	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		// Create Database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		require.Nil(t, err)

		// Init Tablet
		err = clusterInstance.VtctlclientProcess.InitTablet(&tablet, tablet.Cell, keyspaceName, hostname, shardName)
		require.Nil(t, err)

		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.Nil(t, err)
	}

	// Init Shard Master
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shardName), tablet62344.Alias)
	require.Nil(t, err)

	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		err := tablet.VttabletProcess.WaitForTabletType("SERVING")
		require.Nil(t, err)
	}

	validateTopology(t, true)

	// Change the schema of the _vt.reparent_journal table, so that
	// inserts into it will fail. That will make the master fail.
	_, err = tablet62344.VttabletProcess.QueryTabletWithDB(
		"ALTER TABLE reparent_journal DROP COLUMN replication_position", "_vt")
	require.Nil(t, err)

	// Perform a planned reparent operation, the master will fail the
	// insert.  The slaves should then abort right away.
	out, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"PlannedReparentShard",
		"-keyspace_shard", keyspaceShard,
		"-new_master", tablet62044.Alias)
	require.Error(t, err)
	assert.Contains(t, out, "master failed to PopulateReparentJournal")

	killTablets(t)
}

//	Waits for tablet B to catch up to the replication position of tablet A.
func waitForReplicationPosition(t *testing.T, tabletA *cluster.Vttablet, tabletB *cluster.Vttablet) error {
	posA, _ := cluster.GetMasterPosition(t, *tabletA, hostname)
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		posB, _ := cluster.GetMasterPosition(t, *tabletB, hostname)
		if positionAtLeast(t, tabletB, posA, posB) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("failed to catch up on replication position")
}

func positionAtLeast(t *testing.T, tablet *cluster.Vttablet, a string, b string) bool {
	isAtleast := false
	val, err := tablet.MysqlctlProcess.ExecuteCommandWithOutput("position", "at_least", a, b)
	require.Nil(t, err)
	if strings.Contains(val, "true") {
		isAtleast = true
	}
	return isAtleast
}

func checkReparentFromOutside(t *testing.T, tablet *cluster.Vttablet, downMaster bool, baseTime int64) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShardReplication", cell1, keyspaceShard)
	require.Nil(t, err, "error should be Nil")
	if !downMaster {
		assertNodeCount(t, result, int(3))
	} else {
		assertNodeCount(t, result, int(2))
	}

	// make sure the master status page says it's the master
	status := tablet.VttabletProcess.GetStatus()
	assert.Contains(t, status, "Tablet Type: MASTER")

	// make sure the master health stream says it's the master too
	// (health check is disabled on these servers, force it first)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet.Alias)
	require.Nil(t, err)

	streamHealth, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"VtTabletStreamHealth",
		"-count", "1", tablet.Alias)
	require.Nil(t, err)

	var streamHealthResponse querypb.StreamHealthResponse
	err = json.Unmarshal([]byte(streamHealth), &streamHealthResponse)
	require.Nil(t, err)
	assert.Equal(t, streamHealthResponse.Target.TabletType, topodatapb.TabletType_MASTER)
	assert.True(t, streamHealthResponse.TabletExternallyReparentedTimestamp >= baseTime)

}

func assertNodeCount(t *testing.T, result string, want int) {
	resultMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(result), &resultMap)
	require.Nil(t, err)

	nodes := reflect.ValueOf(resultMap["nodes"])
	got := nodes.Len()
	assert.Equal(t, want, got)
}

func checkDBvar(ctx context.Context, t *testing.T, tablet *cluster.Vttablet, variable string, status string) {
	tabletParams := getMysqlConnParam(tablet)
	conn, err := mysql.Connect(ctx, &tabletParams)
	require.Nil(t, err)
	defer conn.Close()

	qr := execute(t, conn, fmt.Sprintf("show variables like '%s'", variable))
	got := fmt.Sprintf("%v", qr.Rows)
	want := fmt.Sprintf("[[VARCHAR(\"%s\") VARCHAR(\"%s\")]]", variable, status)
	assert.Equal(t, want, got)
}

func checkDBstatus(ctx context.Context, t *testing.T, tablet *cluster.Vttablet, variable string, status string) {
	tabletParams := getMysqlConnParam(tablet)
	conn, err := mysql.Connect(ctx, &tabletParams)
	require.Nil(t, err)
	defer conn.Close()

	qr := execute(t, conn, fmt.Sprintf("show status like '%s'", variable))
	got := fmt.Sprintf("%v", qr.Rows)
	want := fmt.Sprintf("[[VARCHAR(\"%s\") VARCHAR(\"%s\")]]", variable, status)
	assert.Equal(t, want, got)
}

func checkSlaveStatus(ctx context.Context, t *testing.T, tablet *cluster.Vttablet) {
	qr := runSQL(ctx, t, "show slave status", tablet)
	SlaveIORunning := fmt.Sprintf("%v", qr.Rows[0][10])  // Slave_IO_Running
	SlaveSQLRunning := fmt.Sprintf("%v", qr.Rows[0][10]) // Slave_SQL_Running
	assert.Equal(t, SlaveIORunning, "VARCHAR(\"No\")")
	assert.Equal(t, SlaveSQLRunning, "VARCHAR(\"No\")")
}

// Makes sure the tablet type is master, and its health check agrees.
func checkMasterTablet(t *testing.T, tablet *cluster.Vttablet) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", tablet.Alias)
	require.Nil(t, err)
	var tabletInfo topodatapb.Tablet
	err = json2.Unmarshal([]byte(result), &tabletInfo)
	require.Nil(t, err)
	assert.Equal(t, topodatapb.TabletType_MASTER, tabletInfo.GetType())

	// make sure the health stream is updated
	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", tablet.Alias)
	require.Nil(t, err)
	var streamHealthResponse querypb.StreamHealthResponse

	err = json2.Unmarshal([]byte(result), &streamHealthResponse)
	require.Nil(t, err)

	assert.True(t, streamHealthResponse.GetServing())
	tabletType := streamHealthResponse.GetTarget().GetTabletType()
	assert.Equal(t, topodatapb.TabletType_MASTER, tabletType)

}

func checkInsertedValues(ctx context.Context, t *testing.T, tablet *cluster.Vttablet, index int) error {
	// wait until it gets the data
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		selectSQL := fmt.Sprintf("select msg from vt_insert_test where id=%d", index)
		qr := runSQL(ctx, t, selectSQL, tablet)
		if len(qr.Rows) == 1 {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("data is not yet replicated")
}

func validateTopology(t *testing.T, pingTablets bool) {
	if pingTablets {
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("Validate", "-ping-tablets=true")
		require.Nil(t, err)
	} else {
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("Validate")
		require.Nil(t, err)
	}
}

func killTablets(t *testing.T) {
	for _, tablet := range []cluster.Vttablet{*tablet62344, *tablet62044, *tablet41983, *tablet31981} {
		log.Infof("Calling TearDown on tablet %v", tablet.Alias)
		err := tablet.VttabletProcess.TearDown()
		require.Nil(t, err)

		// Reset status and type
		tablet.VttabletProcess.ServingStatus = ""
		tablet.Type = "replica"
	}
}
