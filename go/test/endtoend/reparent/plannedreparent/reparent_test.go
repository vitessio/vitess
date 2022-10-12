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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	"vitess.io/vitess/go/vt/log"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
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
	//FIXME: need to rewrite this test: how?
	t.Skip("since the new schema init approach will automatically heal mismatched schemas, the approach in this test doesn't work now")
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
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

// TestCrossCellDurability tests 2 things -
// 1. When PRS is run with the cross_cell durability policy setup, then the semi-sync settings on all the tablets are as expected
// 2. Bringing up a new vttablet should have its replication and semi-sync setup correctly without any manual intervention
func TestCrossCellDurability(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "cross_cell")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// When tablets[0] is the primary, the only tablet in a different cell is tablets[3].
	// So the other two should have semi-sync turned off
	utils.CheckSemiSyncSetupCorrectly(t, tablets[0], "ON")
	utils.CheckSemiSyncSetupCorrectly(t, tablets[3], "ON")
	utils.CheckSemiSyncSetupCorrectly(t, tablets[1], "OFF")
	utils.CheckSemiSyncSetupCorrectly(t, tablets[2], "OFF")

	// Run forced reparent operation, this should proceed unimpeded.
	out, err := utils.Prs(t, clusterInstance, tablets[3])
	require.NoError(t, err, out)

	utils.ConfirmReplication(t, tablets[3], []*cluster.Vttablet{tablets[0], tablets[1], tablets[2]})

	// All the tablets will have semi-sync setup since tablets[3] is in Cell2 and all
	// others are in Cell1, so all of them are eligible to send semi-sync ACKs
	for _, tablet := range tablets {
		utils.CheckSemiSyncSetupCorrectly(t, tablet, "ON")
	}

	for i, supportsBackup := range []bool{false, true} {
		// Bring up a new replica tablet
		// In this new tablet, we do not disable active reparents, otherwise replication will not be started.
		newReplica := utils.StartNewVTTablet(t, clusterInstance, 300+i, supportsBackup)
		// Add the tablet to the list of tablets in this shard
		clusterInstance.Keyspaces[0].Shards[0].Vttablets = append(clusterInstance.Keyspaces[0].Shards[0].Vttablets, newReplica)
		// Check that we can replicate to it and semi-sync is setup correctly on it
		utils.ConfirmReplication(t, tablets[3], []*cluster.Vttablet{tablets[0], tablets[1], tablets[2], newReplica})
		utils.CheckSemiSyncSetupCorrectly(t, newReplica, "ON")
	}
}

// TestFullStatus tests that the RPC FullStatus works as intended.
func TestFullStatus(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// Check that full status gives the correct result for a primary tablet
	primaryTablet := tablets[0]
	primaryStatusString, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("GetFullStatus", primaryTablet.Alias)
	require.NoError(t, err)
	primaryStatus := &replicationdatapb.FullStatus{}
	err = protojson.Unmarshal([]byte(primaryStatusString), primaryStatus)
	require.NoError(t, err)
	assert.NotEmpty(t, primaryStatus.ServerUuid)
	assert.NotEmpty(t, primaryStatus.ServerId)
	// For a primary tablet there is no replication status
	assert.Nil(t, primaryStatus.ReplicationStatus)
	assert.Contains(t, primaryStatus.PrimaryStatus.String(), "vt-0000000101-bin")
	assert.Equal(t, primaryStatus.GtidPurged, "MySQL56/")
	assert.False(t, primaryStatus.ReadOnly)
	assert.True(t, primaryStatus.SemiSyncPrimaryEnabled)
	assert.True(t, primaryStatus.SemiSyncReplicaEnabled)
	assert.True(t, primaryStatus.SemiSyncPrimaryStatus)
	assert.False(t, primaryStatus.SemiSyncReplicaStatus)
	assert.EqualValues(t, 3, primaryStatus.SemiSyncPrimaryClients)
	assert.EqualValues(t, 1000000000000000000, primaryStatus.SemiSyncPrimaryTimeout)
	assert.EqualValues(t, 1, primaryStatus.SemiSyncWaitForReplicaCount)
	assert.Equal(t, "ROW", primaryStatus.BinlogFormat)
	assert.Equal(t, "FULL", primaryStatus.BinlogRowImage)
	assert.Equal(t, "ON", primaryStatus.GtidMode)
	assert.True(t, primaryStatus.LogReplicaUpdates)
	assert.True(t, primaryStatus.LogBinEnabled)
	assert.Regexp(t, `[58]\.[07].*`, primaryStatus.Version)
	assert.NotEmpty(t, primaryStatus.VersionComment)

	replicaTablet := tablets[1]

	waitForFilePosition(t, clusterInstance, primaryTablet, replicaTablet, 5*time.Second)

	// Check that full status gives the correct result for a replica tablet
	replicaStatusString, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("GetFullStatus", replicaTablet.Alias)
	require.NoError(t, err)
	replicaStatus := &replicationdatapb.FullStatus{}
	err = protojson.Unmarshal([]byte(replicaStatusString), replicaStatus)
	require.NoError(t, err)
	assert.NotEmpty(t, replicaStatus.ServerUuid)
	assert.NotEmpty(t, replicaStatus.ServerId)
	assert.Contains(t, replicaStatus.ReplicationStatus.Position, "MySQL56/"+replicaStatus.ReplicationStatus.SourceUuid)
	assert.EqualValues(t, mysql.ReplicationStateRunning, replicaStatus.ReplicationStatus.IoState)
	assert.EqualValues(t, mysql.ReplicationStateRunning, replicaStatus.ReplicationStatus.SqlState)
	assert.Equal(t, fileNameFromPosition(replicaStatus.ReplicationStatus.FilePosition), fileNameFromPosition(primaryStatus.PrimaryStatus.FilePosition))
	assert.LessOrEqual(t, rowNumberFromPosition(replicaStatus.ReplicationStatus.FilePosition), rowNumberFromPosition(primaryStatus.PrimaryStatus.FilePosition))
	assert.Equal(t, replicaStatus.ReplicationStatus.RelayLogSourceBinlogEquivalentPosition, primaryStatus.PrimaryStatus.FilePosition)
	assert.Contains(t, replicaStatus.ReplicationStatus.RelayLogFilePosition, "vt-0000000102-relay")
	assert.Equal(t, replicaStatus.ReplicationStatus.Position, primaryStatus.PrimaryStatus.Position)
	assert.Equal(t, replicaStatus.ReplicationStatus.RelayLogPosition, primaryStatus.PrimaryStatus.Position)
	assert.Empty(t, replicaStatus.ReplicationStatus.LastIoError)
	assert.Empty(t, replicaStatus.ReplicationStatus.LastSqlError)
	assert.Equal(t, replicaStatus.ReplicationStatus.SourceUuid, primaryStatus.ServerUuid)
	assert.LessOrEqual(t, int(replicaStatus.ReplicationStatus.ReplicationLagSeconds), 1)
	assert.False(t, replicaStatus.ReplicationStatus.ReplicationLagUnknown)
	assert.EqualValues(t, 0, replicaStatus.ReplicationStatus.SqlDelay)
	assert.False(t, replicaStatus.ReplicationStatus.SslAllowed)
	assert.False(t, replicaStatus.ReplicationStatus.HasReplicationFilters)
	assert.False(t, replicaStatus.ReplicationStatus.UsingGtid)
	assert.True(t, replicaStatus.ReplicationStatus.AutoPosition)
	assert.Equal(t, replicaStatus.ReplicationStatus.SourceHost, utils.Hostname)
	assert.EqualValues(t, replicaStatus.ReplicationStatus.SourcePort, tablets[0].MySQLPort)
	assert.Equal(t, replicaStatus.ReplicationStatus.SourceUser, "vt_repl")
	assert.Contains(t, replicaStatus.PrimaryStatus.String(), "vt-0000000102-bin")
	assert.Equal(t, replicaStatus.GtidPurged, "MySQL56/")
	assert.True(t, replicaStatus.ReadOnly)
	assert.False(t, replicaStatus.SemiSyncPrimaryEnabled)
	assert.True(t, replicaStatus.SemiSyncReplicaEnabled)
	assert.False(t, replicaStatus.SemiSyncPrimaryStatus)
	assert.True(t, replicaStatus.SemiSyncReplicaStatus)
	assert.EqualValues(t, 0, replicaStatus.SemiSyncPrimaryClients)
	assert.EqualValues(t, 1000000000000000000, replicaStatus.SemiSyncPrimaryTimeout)
	assert.EqualValues(t, 1, replicaStatus.SemiSyncWaitForReplicaCount)
	assert.Equal(t, "ROW", replicaStatus.BinlogFormat)
	assert.Equal(t, "FULL", replicaStatus.BinlogRowImage)
	assert.Equal(t, "ON", replicaStatus.GtidMode)
	assert.True(t, replicaStatus.LogReplicaUpdates)
	assert.True(t, replicaStatus.LogBinEnabled)
	assert.Regexp(t, `[58]\.[07].*`, replicaStatus.Version)
	assert.NotEmpty(t, replicaStatus.VersionComment)
}

func getFullStatus(t *testing.T, clusterInstance *cluster.LocalProcessCluster, tablet *cluster.Vttablet) *replicationdatapb.FullStatus {
	statusString, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("GetFullStatus", tablet.Alias)
	require.NoError(t, err)
	status := &replicationdatapb.FullStatus{}
	err = protojson.Unmarshal([]byte(statusString), status)
	require.NoError(t, err)
	return status
}

// waitForFilePosition waits for timeout to see if FilePositions align b/w primary and replica, to fix flakiness in tests due to race conditions where replica is still catching up
func waitForFilePosition(t *testing.T, clusterInstance *cluster.LocalProcessCluster, primary *cluster.Vttablet, replica *cluster.Vttablet, timeout time.Duration) {
	start := time.Now()
	for {
		primaryStatus := getFullStatus(t, clusterInstance, primary)
		replicaStatus := getFullStatus(t, clusterInstance, replica)
		if primaryStatus.PrimaryStatus.FilePosition == replicaStatus.ReplicationStatus.FilePosition {
			return
		}
		if d := time.Since(start); d > timeout {
			log.Infof("waitForFilePosition timed out, primary %s, replica %s",
				primaryStatus.PrimaryStatus.FilePosition, replicaStatus.ReplicationStatus.FilePosition)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// fileNameFromPosition gets the file name from the position
func fileNameFromPosition(pos string) string {
	return pos[0 : len(pos)-4]
}

// rowNumberFromPosition gets the row number from the position
func rowNumberFromPosition(pos string) int {
	rowNumStr := pos[len(pos)-4:]
	rowNum, _ := strconv.Atoi(rowNumStr)
	return rowNum
}
