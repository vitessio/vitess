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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

var (
	cell1         = "zone1"
	cell2         = "zone2"
	shardName     = "0"
	keyspaceShard = keyspaceName + "/" + shardName

	tab1, tab2, tab3, tab4 *cluster.Vttablet
)

func TestMasterToSpareStateChangeImpossible(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()

	// We cannot change a master to spare
	out, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ChangeTabletType", tab1.Alias, "spare")
	require.Error(t, err, out)
	require.Contains(t, out, "type change MASTER -> SPARE is not an allowed transition for ChangeTabletType")
}

func TestReparentDownMaster(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()

	ctx := context.Background()

	// Make the current master agent and database unavailable.
	stopTablet(t, tab1, true)

	// Perform a planned reparent operation, will try to contact
	// the current master and fail somewhat quickly
	_, err := prsWithTimeout(t, tab2, false, "1s", "5s")
	require.Error(t, err)

	validateTopology(t, false)

	// Run forced reparent operation, this should now proceed unimpeded.
	out, err := ers(t, tab2, "30s")
	log.Infof("EmergencyReparentShard Output: %v", out)
	require.NoError(t, err)

	// Check that old master tablet is left around for human intervention.
	confirmOldMasterIsHangingAround(t)

	// Now we'll manually remove it, simulating a human cleaning up a dead master.
	deleteTablet(t, tab1)

	// Now validate topo is correct.
	validateTopology(t, false)
	checkMasterTablet(t, tab2)
	confirmReplication(t, tab2, []*cluster.Vttablet{tab3, tab4})
	resurrectTablet(ctx, t, tab1)
}

func TestReparentNoChoiceDownMaster(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()
	var err error
	ctx := context.Background()

	confirmReplication(t, tab1, []*cluster.Vttablet{tab2, tab3, tab4})

	// Make the current master agent and database unavailable.
	stopTablet(t, tab1, true)

	// Run forced reparent operation, this should now proceed unimpeded.
	out, err := ers(t, nil, "61s")
	require.NoError(t, err, out)

	// Check that old master tablet is left around for human intervention.
	confirmOldMasterIsHangingAround(t)
	// Now we'll manually remove the old master, simulating a human cleaning up a dead master.
	deleteTablet(t, tab1)
	validateTopology(t, false)
	newMaster := getNewMaster(t)
	// Validate new master is not old master.
	require.NotEqual(t, newMaster.Alias, tab1.Alias)

	// Check new master has latest transaction.
	err = checkInsertedValues(ctx, t, newMaster, 2)
	require.NoError(t, err)

	// bring back the old master as a replica, check that it catches up
	resurrectTablet(ctx, t, tab1)
}

func TestReparentIgnoreReplicas(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()
	var err error
	ctx := context.Background()

	confirmReplication(t, tab1, []*cluster.Vttablet{tab2, tab3, tab4})

	// Make the current master agent and database unavailable.
	stopTablet(t, tab1, true)

	// Take down a replica - this should cause the emergency reparent to fail.
	stopTablet(t, tab3, true)

	// We expect this one to fail because we have an unreachable replica
	out, err := ers(t, nil, "30s")
	require.NotNil(t, err, out)

	// Now let's run it again, but set the command to ignore the unreachable replica.
	out, err = ersIgnoreTablet(t, nil, "30s", tab3)
	require.Nil(t, err, out)

	// We'll bring back the replica we took down.
	restartTablet(t, tab3)

	// Check that old master tablet is left around for human intervention.
	confirmOldMasterIsHangingAround(t)
	deleteTablet(t, tab1)
	validateTopology(t, false)

	newMaster := getNewMaster(t)
	// Check new master has latest transaction.
	err = checkInsertedValues(ctx, t, newMaster, 2)
	require.Nil(t, err)

	// bring back the old master as a replica, check that it catches up
	resurrectTablet(ctx, t, tab1)
}

func TestReparentCrossCell(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()

	// Perform a graceful reparent operation to another cell.
	_, err := prs(t, tab4)
	require.NoError(t, err)

	validateTopology(t, false)
	checkMasterTablet(t, tab4)
}

func TestReparentGraceful(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()

	// Run this to make sure it succeeds.
	strArray := getShardReplicationPositions(t, keyspaceName, shardName, false)
	assert.Equal(t, 4, len(strArray))         // one master, three replicas
	assert.Contains(t, strArray[0], "master") // master first

	// Perform a graceful reparent operation
	prs(t, tab2)
	validateTopology(t, false)
	checkMasterTablet(t, tab2)

	// A graceful reparent to the same master should be idempotent.
	prs(t, tab2)
	validateTopology(t, false)
	checkMasterTablet(t, tab2)

	confirmReplication(t, tab2, []*cluster.Vttablet{tab1, tab3, tab4})
}

func TestReparentReplicaOffline(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()

	// Kill one tablet so we seem offline
	stopTablet(t, tab4, true)

	// Perform a graceful reparent operation.
	out, err := prsWithTimeout(t, tab2, false, "", "31s")
	require.Error(t, err)
	assert.Contains(t, out, fmt.Sprintf("tablet %s failed to SetMaster", tab4.Alias))
	checkMasterTablet(t, tab2)
}

func TestReparentAvoid(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()
	deleteTablet(t, tab3)

	// Perform a reparent operation with avoid_master pointing to non-master. It
	// should succeed without doing anything.
	_, err := prsAvoid(t, tab2)
	require.NoError(t, err)

	validateTopology(t, false)
	checkMasterTablet(t, tab1)

	// Perform a reparent operation with avoid_master pointing to master.
	_, err = prsAvoid(t, tab1)
	require.NoError(t, err)
	validateTopology(t, false)

	// tab2 is in the same cell and tab4 is in a different cell, so we must land on tab2
	checkMasterTablet(t, tab2)

	// If we kill the tablet in the same cell as master then reparent -avoid_master will fail.
	stopTablet(t, tab1, true)
	out, err := prsAvoid(t, tab2)
	require.Error(t, err)
	assert.Contains(t, out, "cannot find a tablet to reparent to")
	validateTopology(t, false)
	checkMasterTablet(t, tab2)
}

func TestReparentFromOutside(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()
	reparentFromOutside(t, false)
}

func TestReparentFromOutsideWithNoMaster(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()
	reparentFromOutside(t, true)

	// FIXME: @Deepthi: is this needed, since we teardown the cluster, does this achieve any additional test coverage?
	// We will have to restart mysql to avoid hanging/locks due to external Reparent
	for _, tablet := range []cluster.Vttablet{*tab1, *tab2, *tab3, *tab4} {
		log.Infof("Restarting MySql for tablet %v", tablet.Alias)
		err := tablet.MysqlctlProcess.Stop()
		require.NoError(t, err)
		tablet.MysqlctlProcess.InitMysql = false
		err = tablet.MysqlctlProcess.Start()
		require.NoError(t, err)
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
	ctx := context.Background()

	// now manually reparent 1 out of 2 tablets
	// tab2 will be the new master
	// tab3 won't be re-parented, so it will be busted

	if !downMaster {
		// commands to stop the current master
		demoteMasterCommands := "SET GLOBAL read_only = ON; FLUSH TABLES WITH READ LOCK; UNLOCK TABLES"
		runSQL(ctx, t, demoteMasterCommands, tab1)

		//Get the position of the old master and wait for the new one to catch up.
		err := waitForReplicationPosition(t, tab1, tab2)
		require.NoError(t, err)
	}

	// commands to convert a replica to a master
	promoteReplicaCommands := "STOP SLAVE; RESET SLAVE ALL; SET GLOBAL read_only = OFF;"
	runSQL(ctx, t, promoteReplicaCommands, tab2)

	// Get master position
	_, gtID := cluster.GetMasterPosition(t, *tab2, hostname)

	// tab1 will now be a replica of tab2
	changeMasterCommands := fmt.Sprintf("RESET MASTER; RESET SLAVE; SET GLOBAL gtid_purged = '%s';"+
		"CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='vt_repl', MASTER_AUTO_POSITION = 1;"+
		"START SLAVE;", gtID, hostname, tab2.MySQLPort)
	runSQL(ctx, t, changeMasterCommands, tab1)

	// Capture time when we made tab2 master
	baseTime := time.Now().UnixNano() / 1000000000

	// tab3 will be a replica of tab2
	changeMasterCommands = fmt.Sprintf("STOP SLAVE; RESET MASTER; SET GLOBAL gtid_purged = '%s';"+
		"CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='vt_repl', MASTER_AUTO_POSITION = 1;"+
		"START SLAVE;", gtID, hostname, tab2.MySQLPort)
	runSQL(ctx, t, changeMasterCommands, tab3)

	// To test the downMaster, we kill the old master first and delete its tablet record
	if downMaster {
		err := tab1.VttabletProcess.TearDown()
		require.NoError(t, err)
		err = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet",
			"-allow_master", tab1.Alias)
		require.NoError(t, err)
	}

	// update topology with the new server
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("TabletExternallyReparented",
		tab2.Alias)
	require.NoError(t, err)

	checkReparentFromOutside(t, tab2, downMaster, baseTime)

	if !downMaster {
		err := tab1.VttabletProcess.TearDown()
		require.NoError(t, err)
	}
}

func TestReparentWithDownReplica(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()
	ctx := context.Background()

	// Stop replica mysql Process
	err := tab3.MysqlctlProcess.Stop()
	require.NoError(t, err)

	// Perform a graceful reparent operation. It will fail as one tablet is down.
	out, err := prs(t, tab2)
	require.Error(t, err)
	assert.Contains(t, out, fmt.Sprintf("tablet %s failed to SetMaster", tab3.Alias))

	// insert data into the new master, check the connected replica work
	confirmReplication(t, tab2, []*cluster.Vttablet{tab1, tab4})

	// restart mysql on the old replica, should still be connecting to the old master
	tab3.MysqlctlProcess.InitMysql = false
	err = tab3.MysqlctlProcess.Start()
	require.NoError(t, err)

	// Use the same PlannedReparentShard command to fix up the tablet.
	_, err = prs(t, tab2)
	require.NoError(t, err)

	// wait until it gets the data
	err = checkInsertedValues(ctx, t, tab3, 2)
	require.NoError(t, err)
}

func TestChangeTypeSemiSync(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()
	ctx := context.Background()

	// Create new names for tablets, so this test is less confusing.
	master, replica, rdonly1, rdonly2 := tab1, tab2, tab3, tab4

	// Updated rdonly tablet and set tablet type to rdonly
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonly1.Alias, "rdonly")
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonly2.Alias, "rdonly")
	require.NoError(t, err)

	validateTopology(t, true)

	checkMasterTablet(t, master)

	// Stop replication on rdonly1, to make sure when we make it replica it doesn't start again.
	// Note we do a similar test for replica -> rdonly below.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", rdonly1.Alias)
	require.NoError(t, err)

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
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", replica.Alias, "rdonly")
	require.NoError(t, err)
	checkDBvar(ctx, t, replica, "rpl_semi_sync_slave_enabled", "OFF")
	checkDBstatus(ctx, t, replica, "Rpl_semi_sync_slave_status", "OFF")

	// Change rdonly1 to replica, should turn on semi-sync, and not start replication.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonly1.Alias, "replica")
	require.NoError(t, err)
	checkDBvar(ctx, t, rdonly1, "rpl_semi_sync_slave_enabled", "ON")
	checkDBstatus(ctx, t, rdonly1, "Rpl_semi_sync_slave_status", "OFF")
	checkReplicaStatus(ctx, t, rdonly1)

	// Now change from replica back to rdonly, make sure replication is still not enabled.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonly1.Alias, "rdonly")
	require.NoError(t, err)
	checkDBvar(ctx, t, rdonly1, "rpl_semi_sync_slave_enabled", "OFF")
	checkDBstatus(ctx, t, rdonly1, "Rpl_semi_sync_slave_status", "OFF")
	checkReplicaStatus(ctx, t, rdonly1)

	// Change rdonly2 to replica, should turn on semi-sync, and restart replication.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonly2.Alias, "replica")
	require.NoError(t, err)
	checkDBvar(ctx, t, rdonly2, "rpl_semi_sync_slave_enabled", "ON")
	checkDBstatus(ctx, t, rdonly2, "Rpl_semi_sync_slave_status", "ON")
}

func TestReparentDoesntHangIfMasterFails(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()

	// Change the schema of the _vt.reparent_journal table, so that
	// inserts into it will fail. That will make the master fail.
	_, err := tab1.VttabletProcess.QueryTabletWithDB(
		"ALTER TABLE reparent_journal DROP COLUMN replication_position", "_vt")
	require.NoError(t, err)

	// Perform a planned reparent operation, the master will fail the
	// insert.  The replicas should then abort right away.
	out, err := prs(t, tab2)
	require.Error(t, err)
	assert.Contains(t, out, "primary failed to PopulateReparentJournal")
}
