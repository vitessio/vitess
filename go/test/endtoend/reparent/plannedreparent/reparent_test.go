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

package plannedreparent

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	"vitess.io/vitess/go/vt/log"
)

func TestPrimaryToSpareStateChangeImpossible(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	// We cannot change a primary to spare
	out, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ChangeTabletType", tablets[0].Alias, "spare")
	require.Error(t, err, out)
	require.Contains(t, out, "type change PRIMARY -> SPARE is not an allowed transition for ChangeTabletType")
}

func TestReparentCrossCell(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	// Perform a graceful reparent operation to another cell.
	_, err := utils.Prs(t, clusterInstance, tablets[3])
	require.NoError(t, err)

	utils.ValidateTopology(t, clusterInstance, false)
	utils.CheckPrimaryTablet(t, clusterInstance, tablets[3])
}

func TestReparentGraceful(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	// Run this to make sure it succeeds.
	utils.WaitForReplicationToStart(t, clusterInstance, utils.KeyspaceName, utils.ShardName, len(tablets), true)

	// Perform a graceful reparent operation
	utils.Prs(t, clusterInstance, tablets[1])
	utils.ValidateTopology(t, clusterInstance, false)
	utils.CheckPrimaryTablet(t, clusterInstance, tablets[1])

	// A graceful reparent to the same primary should be idempotent.
	utils.Prs(t, clusterInstance, tablets[1])
	utils.ValidateTopology(t, clusterInstance, false)
	utils.CheckPrimaryTablet(t, clusterInstance, tablets[1])

	utils.ConfirmReplication(t, tablets[1], []*cluster.Vttablet{tablets[0], tablets[2], tablets[3]})
}

// TestPRSWithDrainedLaggingTablet tests that PRS succeeds even if we have a lagging drained tablet
func TestPRSWithDrainedLaggingTablet(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	err := clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", tablets[1].Alias, "drained")
	require.NoError(t, err)

	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// make tablets[1 lag from the other tablets by setting the delay to a large number
	utils.RunSQL(context.Background(), t, `stop slave;CHANGE MASTER TO MASTER_DELAY = 1999;start slave;`, tablets[1])

	// insert another row in tablets[1
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[2], tablets[3]})

	// assert that there is indeed only 1 row in tablets[1
	res := utils.RunSQL(context.Background(), t, `select msg from vt_insert_test;`, tablets[1])
	assert.Equal(t, 1, len(res.Rows))

	// Perform a graceful reparent operation
	utils.Prs(t, clusterInstance, tablets[2])
	utils.ValidateTopology(t, clusterInstance, false)
	utils.CheckPrimaryTablet(t, clusterInstance, tablets[2])
}

func TestReparentReplicaOffline(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	// Kill one tablet so we seem offline
	utils.StopTablet(t, tablets[3], true)

	// Perform a graceful reparent operation.
	out, err := utils.PrsWithTimeout(t, clusterInstance, tablets[1], false, "", "31s")
	require.Error(t, err)
	assert.True(t, utils.SetReplicationSourceFailed(tablets[3], out))

	utils.CheckPrimaryTablet(t, clusterInstance, tablets[1])
}

func TestReparentAvoid(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.DeleteTablet(t, clusterInstance, tablets[2])

	// Perform a reparent operation with avoid_tablet pointing to non-primary. It
	// should succeed without doing anything.
	_, err := utils.PrsAvoid(t, clusterInstance, tablets[1])
	require.NoError(t, err)

	utils.ValidateTopology(t, clusterInstance, false)
	utils.CheckPrimaryTablet(t, clusterInstance, tablets[0])

	// Perform a reparent operation with avoid_tablet pointing to primary.
	_, err = utils.PrsAvoid(t, clusterInstance, tablets[0])
	require.NoError(t, err)
	utils.ValidateTopology(t, clusterInstance, false)

	// tablets[1 is in the same cell and tablets[3] is in a different cell, so we must land on tablets[1
	utils.CheckPrimaryTablet(t, clusterInstance, tablets[1])

	// If we kill the tablet in the same cell as primary then reparent --avoid_tablet will fail.
	utils.StopTablet(t, tablets[0], true)
	out, err := utils.PrsAvoid(t, clusterInstance, tablets[1])
	require.Error(t, err)
	assert.Contains(t, out, "cannot find a tablet to reparent to in the same cell as the current primary")
	utils.ValidateTopology(t, clusterInstance, false)
	utils.CheckPrimaryTablet(t, clusterInstance, tablets[1])
}

func TestReparentFromOutside(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
	reparentFromOutside(t, clusterInstance, false)
}

func TestReparentFromOutsideWithNoPrimary(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	reparentFromOutside(t, clusterInstance, true)

	// FIXME: @Deepthi: is this needed, since we teardown the cluster, does this achieve any additional test coverage?
	// We will have to restart mysql to avoid hanging/locks due to external Reparent
	for _, tablet := range tablets {
		log.Infof("Restarting MySql for tablet %v", tablet.Alias)
		err := tablet.MysqlctlProcess.Stop()
		require.NoError(t, err)
		tablet.MysqlctlProcess.InitMysql = false
		err = tablet.MysqlctlProcess.Start()
		require.NoError(t, err)
	}
}

func reparentFromOutside(t *testing.T, clusterInstance *cluster.LocalProcessCluster, downPrimary bool) {
	//This test will start a primary and 3 replicas.
	//Then:
	//- one replica will be the new primary
	//- one replica will be reparented to that new primary
	//- one replica will be busted and dead in the water and we'll call TabletExternallyReparented.
	//Args:
	//downPrimary: kills the old primary first
	ctx := context.Background()
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	// now manually reparent 1 out of 2 tablets
	// tablets[1 will be the new primary
	// tablets[2 won't be re-parented, so it will be busted

	if !downPrimary {
		// commands to stop the current primary
		demoteCommands := "SET GLOBAL read_only = ON; FLUSH TABLES WITH READ LOCK; UNLOCK TABLES"
		utils.RunSQL(ctx, t, demoteCommands, tablets[0])

		//Get the position of the old primary and wait for the new one to catch up.
		err := utils.WaitForReplicationPosition(t, tablets[0], tablets[1])
		require.NoError(t, err)
	}

	// commands to convert a replica to be writable
	promoteReplicaCommands := "STOP SLAVE; RESET SLAVE ALL; SET GLOBAL read_only = OFF;"
	utils.RunSQL(ctx, t, promoteReplicaCommands, tablets[1])

	// Get primary position
	_, gtID := cluster.GetPrimaryPosition(t, *tablets[1], utils.Hostname)

	// tablets[0] will now be a replica of tablets[1
	changeReplicationSourceCommands := fmt.Sprintf("RESET MASTER; RESET SLAVE; SET GLOBAL gtid_purged = '%s';"+
		"CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='vt_repl', MASTER_AUTO_POSITION = 1;"+
		"START SLAVE;", gtID, utils.Hostname, tablets[1].MySQLPort)
	utils.RunSQL(ctx, t, changeReplicationSourceCommands, tablets[0])

	// Capture time when we made tablets[1 writable
	baseTime := time.Now().UnixNano() / 1000000000

	// tablets[2 will be a replica of tablets[1
	changeReplicationSourceCommands = fmt.Sprintf("STOP SLAVE; RESET MASTER; SET GLOBAL gtid_purged = '%s';"+
		"CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='vt_repl', MASTER_AUTO_POSITION = 1;"+
		"START SLAVE;", gtID, utils.Hostname, tablets[1].MySQLPort)
	utils.RunSQL(ctx, t, changeReplicationSourceCommands, tablets[2])

	// To test the downPrimary, we kill the old primary first and delete its tablet record
	if downPrimary {
		err := tablets[0].VttabletProcess.TearDownWithTimeout(30 * time.Second)
		require.NoError(t, err)
		err = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", "--",
			"--allow_primary", tablets[0].Alias)
		require.NoError(t, err)
	}

	// update topology with the new server
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("TabletExternallyReparented",
		tablets[1].Alias)
	require.NoError(t, err)

	utils.CheckReparentFromOutside(t, clusterInstance, tablets[1], downPrimary, baseTime)

	if !downPrimary {
		err := tablets[0].VttabletProcess.TearDownWithTimeout(30 * time.Second)
		require.NoError(t, err)
	}
}

func TestReparentWithDownReplica(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	ctx := context.Background()

	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// Stop replica mysql Process
	err := tablets[2].MysqlctlProcess.Stop()
	require.NoError(t, err)

	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[3]})

	// Perform a graceful reparent operation. It will fail as one tablet is down.
	out, err := utils.Prs(t, clusterInstance, tablets[1])
	require.Error(t, err)
	assert.True(t, utils.SetReplicationSourceFailed(tablets[2], out))

	// insert data into the new primary, check the connected replica work
	insertVal := utils.ConfirmReplication(t, tablets[1], []*cluster.Vttablet{tablets[0], tablets[3]})

	// restart mysql on the old replica, should still be connecting to the old primary
	tablets[2].MysqlctlProcess.InitMysql = false
	err = tablets[2].MysqlctlProcess.Start()
	require.NoError(t, err)

	// Use the same PlannedReparentShard command to fix up the tablet.
	_, err = utils.Prs(t, clusterInstance, tablets[1])
	require.NoError(t, err)

	// We have to StartReplication on tablets[2] since the MySQL instance is restarted and does not have replication running
	// We earlier used to rely on replicationManager to fix this but we have disabled it in our testing environment for latest versions of vttablet and vtctl.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StartReplication", tablets[2].Alias)
	require.NoError(t, err)

	// wait until it gets the data
	err = utils.CheckInsertedValues(ctx, t, tablets[2], insertVal)
	require.NoError(t, err)
}

func TestChangeTypeSemiSync(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	ctx := context.Background()

	// Create new names for tablets, so this test is less confusing.
	primary, replica, rdonly1, rdonly2 := tablets[0], tablets[1], tablets[2], tablets[3]

	// Updated rdonly tablet and set tablet type to rdonly
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonly1.Alias, "rdonly")
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonly2.Alias, "rdonly")
	require.NoError(t, err)

	utils.ValidateTopology(t, clusterInstance, true)

	utils.CheckPrimaryTablet(t, clusterInstance, primary)

	// Stop replication on rdonly1, to make sure when we make it replica it doesn't start again.
	// Note we do a similar test for replica -> rdonly below.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", rdonly1.Alias)
	require.NoError(t, err)

	// Check semi-sync on replicas.
	// The flag is only an indication of the value to use next time
	// we turn replication on, so also check the status.
	// rdonly1 is not replicating, so its status is off.
	utils.CheckDBvar(ctx, t, replica, "rpl_semi_sync_slave_enabled", "ON")
	utils.CheckDBvar(ctx, t, rdonly1, "rpl_semi_sync_slave_enabled", "OFF")
	utils.CheckDBvar(ctx, t, rdonly2, "rpl_semi_sync_slave_enabled", "OFF")
	utils.CheckDBstatus(ctx, t, replica, "Rpl_semi_sync_slave_status", "ON")
	utils.CheckDBstatus(ctx, t, rdonly1, "Rpl_semi_sync_slave_status", "OFF")
	utils.CheckDBstatus(ctx, t, rdonly2, "Rpl_semi_sync_slave_status", "OFF")

	// Change replica to rdonly while replicating, should turn off semi-sync, and restart replication.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", replica.Alias, "rdonly")
	require.NoError(t, err)
	utils.CheckDBvar(ctx, t, replica, "rpl_semi_sync_slave_enabled", "OFF")
	utils.CheckDBstatus(ctx, t, replica, "Rpl_semi_sync_slave_status", "OFF")

	// Change rdonly1 to replica, should turn on semi-sync, and not start replication.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonly1.Alias, "replica")
	require.NoError(t, err)
	utils.CheckDBvar(ctx, t, rdonly1, "rpl_semi_sync_slave_enabled", "ON")
	utils.CheckDBstatus(ctx, t, rdonly1, "Rpl_semi_sync_slave_status", "OFF")
	utils.CheckReplicaStatus(ctx, t, rdonly1)

	// Now change from replica back to rdonly, make sure replication is still not enabled.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonly1.Alias, "rdonly")
	require.NoError(t, err)
	utils.CheckDBvar(ctx, t, rdonly1, "rpl_semi_sync_slave_enabled", "OFF")
	utils.CheckDBstatus(ctx, t, rdonly1, "Rpl_semi_sync_slave_status", "OFF")
	utils.CheckReplicaStatus(ctx, t, rdonly1)

	// Change rdonly2 to replica, should turn on semi-sync, and restart replication.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonly2.Alias, "replica")
	require.NoError(t, err)
	utils.CheckDBvar(ctx, t, rdonly2, "rpl_semi_sync_slave_enabled", "ON")
	utils.CheckDBstatus(ctx, t, rdonly2, "Rpl_semi_sync_slave_status", "ON")
}

func TestReparentDoesntHangIfPrimaryFails(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)

	// This test is no longer valid post v16
	if clusterInstance.VtTabletMajorVersion >= 16 {
		t.Skip("Skipping TestReparentDoesntHangIfPrimaryFails in CI environment for v16")
	}
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	// Change the schema of the _vt.reparent_journal table, so that
	// inserts into it will fail. That will make the primary fail.
	_, err := tablets[0].VttabletProcess.QueryTabletWithDB(
		"ALTER TABLE reparent_journal DROP COLUMN replication_position", "_vt")
	require.NoError(t, err)
	// Perform a planned reparent operation, the primary will fail the
	// insert.  The replicas should then abort right away.
	out, err := utils.Prs(t, clusterInstance, tablets[1])
	require.Error(t, err)
	assert.Contains(t, out, "primary failed to PopulateReparentJournal")
}
