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
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vitesst"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

func TestPrimaryToSpareStateChangeImpossible(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)

	// We cannot change a primary to spare
	out, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "ChangeTabletType", tablets[0].Alias(), "spare")
	require.Error(t, err, out)
	require.Contains(t, out, "type change PRIMARY -> SPARE is not an allowed transition for ChangeTabletType")
}

func TestReparentCrossCell(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)

	// Perform a graceful reparent operation to another cell.
	_, err := Prs(t, clusterInstance, tablets[3])
	require.NoError(t, err)

	ValidateTopology(t, clusterInstance, false)
	CheckPrimaryTablet(t, clusterInstance, tablets[3])
}

func TestReparentGraceful(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)

	// Run this to make sure it succeeds.
	WaitForReplicationToStart(t, clusterInstance, KeyspaceName, ShardName, len(tablets), true)

	// Perform a graceful reparent operation
	Prs(t, clusterInstance, tablets[1])
	ValidateTopology(t, clusterInstance, false)
	CheckPrimaryTablet(t, clusterInstance, tablets[1])

	// A graceful reparent to the same primary should be idempotent.
	Prs(t, clusterInstance, tablets[1])
	ValidateTopology(t, clusterInstance, false)
	CheckPrimaryTablet(t, clusterInstance, tablets[1])

	ConfirmReplication(t, tablets[1], []*vitesst.Tablet{tablets[0], tablets[2], tablets[3]})
}

// TestPRSWithDrainedLaggingTablet tests that PRS succeeds even if we have a lagging drained tablet
func TestPRSWithDrainedLaggingTablet(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)

	err := clusterInstance.Vtctld().ExecuteCommand(t.Context(), "ChangeTabletType", tablets[1].Alias(), "drained")
	require.NoError(t, err)

	ConfirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

	// make tablets[1 lag from the other tablets by setting the delay to a large number
	RunSQLs(t.Context(), t, []string{`stop replica`, `CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 1999`, `start replica;`}, tablets[1])

	// insert another row in tablets[1
	ConfirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[2], tablets[3]})

	// assert that there is indeed only 1 row in tablets[1
	res := RunSQL(t.Context(), t, `select msg from vt_insert_test`, tablets[1])
	assert.Equal(t, 1, len(res.Rows))

	// Perform a graceful reparent operation
	Prs(t, clusterInstance, tablets[2])
	ValidateTopology(t, clusterInstance, false)
	CheckPrimaryTablet(t, clusterInstance, tablets[2])
}

func TestReparentReplicaOffline(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)
	killTablet := tablets[3]

	tabletInfo, err := getTablet(t.Context(), clusterInstance, killTablet.Alias())
	require.NoError(t, err)
	require.NotNil(t, tabletInfo.TabletStartTime)
	require.Nil(t, tabletInfo.TabletShutdownTime)

	// Gracefully kill one tablet so we seem offline.
	startKillTime := time.Now()
	err = killTablet.StopVttablet(t.Context())
	require.NoError(t, err)

	// Confirm the tablet shutdown via the topo.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		tabletInfo, err := getTablet(t.Context(), clusterInstance, killTablet.Alias())
		require.NoError(c, err)

		require.Nil(c, tabletInfo.TabletStartTime)
		require.NotNil(c, tabletInfo.TabletShutdownTime)
		shutdownTime := protoutil.TimeFromProto(tabletInfo.TabletShutdownTime)
		require.WithinRange(c, shutdownTime, startKillTime, time.Now())
	}, time.Second, time.Second*31)

	// Perform a graceful reparent operation.
	out, err := PrsWithTimeout(t, clusterInstance, tablets[1], false, "", "31s")
	require.Error(t, err)

	// Assert that PRS failed
	assert.Contains(t, out, "rpc error: code = Unknown desc = tablet is shutdown")
	CheckPrimaryTablet(t, clusterInstance, tablets[0])
}

func TestReparentAvoid(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)
	DeleteTablet(t, clusterInstance, tablets[2])

	// Perform a reparent operation with avoid_tablet pointing to non-primary. It
	// should succeed without doing anything.
	_, err := PrsAvoid(t, clusterInstance, tablets[1])
	require.NoError(t, err)

	ValidateTopology(t, clusterInstance, false)
	CheckPrimaryTablet(t, clusterInstance, tablets[0])

	// Perform a reparent operation with avoid_tablet pointing to primary.
	_, err = PrsAvoid(t, clusterInstance, tablets[0])
	require.NoError(t, err)
	ValidateTopology(t, clusterInstance, false)

	// tablets[1] is in the same cell and tablets[3] is in a different cell, so we must land on tablets[1]
	CheckPrimaryTablet(t, clusterInstance, tablets[1])

	// If we kill the tablet in the same cell as primary then reparent --avoid-tablet will fail.
	StopTablet(t, tablets[0], true)
	out, err := PrsAvoid(t, clusterInstance, tablets[1])
	require.Error(t, err)
	assert.Contains(t, out, "rpc error: code = Unknown desc = tablet is shutdown")

	ValidateTopology(t, clusterInstance, false)
	CheckPrimaryTablet(t, clusterInstance, tablets[1])

	t.Run("Allow cross cell promotion", func(t *testing.T) {
		DeleteTablet(t, clusterInstance, tablets[0])
		// Perform a graceful reparent operation and verify it fails because we have no replicas in the same cell as the primary.
		out, err = PrsAvoid(t, clusterInstance, tablets[1])
		require.Error(t, err)
		assert.Contains(t, out, "is not in the same cell as the previous primary")

		// If we run PRS with allow cross cell promotion then it should succeed and should promote the replica in another cell.
		_, err = PrsAvoid(t, clusterInstance, tablets[1], "--allow-cross-cell-promotion")
		require.NoError(t, err)
		CheckPrimaryTablet(t, clusterInstance, tablets[3])
	})
}

func TestReparentFromOutside(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer TeardownCluster(t, clusterInstance)
	reparentFromOutside(t, clusterInstance, false)
}

func TestReparentFromOutsideWithNoPrimary(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)

	reparentFromOutside(t, clusterInstance, true)

	// We will have to restart mysql to avoid hanging/locks due to external Reparent
	for _, tablet := range tablets {
		t.Logf("Restarting MySql for tablet %v", tablet.Alias())
		err := tablet.StopMySQL(t.Context())
		require.NoError(t, err)
		err = tablet.StartMySQL(t.Context())
		require.NoError(t, err)
	}
}

func reparentFromOutside(t *testing.T, clusterInstance *vitesst.Cluster, downPrimary bool) {
	// This test will start a primary and 3 replicas.
	// Then:
	// - one replica will be the new primary
	// - one replica will be reparented to that new primary
	// - one replica will be busted and dead in the water and we'll call TabletExternallyReparented.
	// Args:
	// downPrimary: kills the old primary first
	ctx := t.Context()
	tablets := shardTablets(clusterInstance)

	// now manually reparent 1 out of 2 tablets
	// tablets[1 will be the new primary
	// tablets[2 won't be re-parented, so it will be busted

	if !downPrimary {
		// commands to stop the current primary
		demoteCommands := []string{"SET GLOBAL read_only = ON", "FLUSH TABLES WITH READ LOCK", "UNLOCK TABLES"}
		RunSQLs(ctx, t, demoteCommands, tablets[0])

		// Get the position of the old primary and wait for the new one to catch up.
		err := WaitForReplicationPosition(t, tablets[0], tablets[1])
		require.NoError(t, err)
	}

	// commands to convert a replica to be writable
	promoteReplicaCommands := []string{"STOP REPLICA", "RESET REPLICA ALL", "SET GLOBAL read_only = OFF"}
	RunSQLs(ctx, t, promoteReplicaCommands, tablets[1])

	// Get primary position
	_, gtID := primaryPosition(t, tablets[1])

	// tablets[0] will now be a replica of tablets[1
	resetCmd, err := resetBinaryLogsCommand(ctx, tablets[0])
	require.NoError(t, err)
	changeReplicationSourceCommands := []string{
		resetCmd,
		"RESET REPLICA",
		fmt.Sprintf("SET GLOBAL gtid_purged = '%s'", gtID),
		fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='vt_repl', GET_SOURCE_PUBLIC_KEY = 1, SOURCE_AUTO_POSITION = 1", tablets[1].Name(), tabletMySQLPort),
	}
	RunSQLs(ctx, t, changeReplicationSourceCommands, tablets[0])

	// Capture time when we made tablets[1 writable
	baseTime := time.Now().UnixNano() / 1000000000

	// tablets[2 will be a replica of tablets[1
	resetCmd, err = resetBinaryLogsCommand(ctx, tablets[2])
	require.NoError(t, err)
	changeReplicationSourceCommands = []string{
		"STOP REPLICA",
		resetCmd,
		fmt.Sprintf("SET GLOBAL gtid_purged = '%s'", gtID),
		fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='vt_repl', GET_SOURCE_PUBLIC_KEY = 1, SOURCE_AUTO_POSITION = 1", tablets[1].Name(), tabletMySQLPort),
		"START REPLICA",
	}
	RunSQLs(ctx, t, changeReplicationSourceCommands, tablets[2])

	// To test the downPrimary, we kill the old primary first and delete its tablet record
	if downPrimary {
		err := tablets[0].StopVttablet(ctx)
		require.NoError(t, err)
		err = clusterInstance.Vtctld().ExecuteCommand(ctx, "DeleteTablets",
			"--allow-primary", tablets[0].Alias())
		require.NoError(t, err)
	}

	// update topology with the new server
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "TabletExternallyReparented",
		tablets[1].Alias())
	require.NoError(t, err)

	CheckReparentFromOutside(t, clusterInstance, tablets[1], downPrimary, baseTime)

	if !downPrimary {
		err := tablets[0].StopVttablet(ctx)
		require.NoError(t, err)
	}
}

func TestReparentWithDownReplica(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)

	ctx := t.Context()

	ConfirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

	// Stop replica mysql Process
	err := tablets[2].StopMySQL(ctx)
	require.NoError(t, err)

	ConfirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[3]})

	// Perform a graceful reparent operation. It will fail as one tablet is down.
	out, err := Prs(t, clusterInstance, tablets[1])
	require.Error(t, err)
	// Assert that PRS failed
	assert.Contains(t, out, "TabletManager.GetGlobalStatusVars on "+canonicalAlias(tablets[2]))
	// insert data into the old primary, check the connected replica works. The primary tablet shouldn't have changed.
	insertVal := ConfirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[3]})

	// restart mysql on the old replica, should still be connecting to the old primary
	err = tablets[2].StartMySQL(ctx)
	require.NoError(t, err)

	// Use the same PlannedReparentShard command to promote the new primary.
	_, err = Prs(t, clusterInstance, tablets[1])
	require.NoError(t, err)

	// We have to StartReplication on tablets[2] since the MySQL instance is restarted and does not have replication running
	// We earlier used to rely on replicationManager to fix this but we have disabled it in our testing environment for latest versions of vttablet and vtctl.
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "StartReplication", tablets[2].Alias())
	require.NoError(t, err)

	// wait until it gets the data
	err = CheckInsertedValues(ctx, t, tablets[2], insertVal)
	require.NoError(t, err)
}

func TestChangeTypeSemiSync(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)

	ctx := t.Context()

	// Create new names for tablets, so this test is less confusing.
	primary, replica, rdonly1, rdonly2 := tablets[0], tablets[1], tablets[2], tablets[3]

	// Updated rdonly tablet and set tablet type to rdonly
	err := clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", rdonly1.Alias(), "rdonly")
	require.NoError(t, err)
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", rdonly2.Alias(), "rdonly")
	require.NoError(t, err)

	ValidateTopology(t, clusterInstance, true)

	CheckPrimaryTablet(t, clusterInstance, primary)

	// Stop replication on rdonly1, to make sure when we make it replica it doesn't start again.
	// Note we do a similar test for replica -> rdonly below.
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "StopReplication", rdonly1.Alias())
	require.NoError(t, err)

	// Check semi-sync on replicas.
	// The flag is only an indication of the value to use next time
	// we turn replication on, so also check the status.
	// rdonly1 is not replicating, so its status is off.
	CheckSemisyncEnabled(ctx, t, replica, true)
	CheckSemisyncEnabled(ctx, t, rdonly1, false)
	CheckSemisyncEnabled(ctx, t, rdonly2, false)
	CheckSemisyncStatus(ctx, t, replica, true)
	CheckSemisyncStatus(ctx, t, rdonly1, false)
	CheckSemisyncStatus(ctx, t, rdonly2, false)

	// Change replica to rdonly while replicating, should turn off semi-sync, and restart replication.
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", replica.Alias(), "rdonly")
	require.NoError(t, err)
	CheckSemisyncEnabled(ctx, t, replica, false)
	CheckSemisyncStatus(ctx, t, replica, false)

	// Change rdonly1 to replica, should turn on semi-sync, and not start replication.
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", rdonly1.Alias(), "replica")
	require.NoError(t, err)
	CheckSemisyncEnabled(ctx, t, rdonly1, true)
	CheckSemisyncStatus(ctx, t, rdonly1, false)
	CheckReplicaStatus(ctx, t, rdonly1)

	// Now change from replica back to rdonly, make sure replication is still not enabled.
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", rdonly1.Alias(), "rdonly")
	require.NoError(t, err)
	CheckSemisyncEnabled(ctx, t, rdonly1, false)
	CheckSemisyncStatus(ctx, t, rdonly1, false)
	CheckReplicaStatus(ctx, t, rdonly1)

	// Change rdonly2 to replica, should turn on semi-sync, and restart replication.
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", rdonly2.Alias(), "replica")
	require.NoError(t, err)
	CheckSemisyncEnabled(ctx, t, rdonly2, true)
	CheckSemisyncStatus(ctx, t, rdonly2, true)
}

// TestCrossCellDurability tests 2 things -
// 1. When PRS is run with the cross_cell durability policy setup, then the semi-sync settings on all the tablets are as expected
// 2. Bringing up a new vttablet should have its replication and semi-sync setup correctly without any manual intervention
func TestCrossCellDurability(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilityCrossCell)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)

	ConfirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

	// When tablets[0] is the primary, the only tablet in a different cell is tablets[3].
	// So the other two should have semi-sync turned off
	CheckSemiSyncSetupCorrectly(t, tablets[0], "ON")
	CheckSemiSyncSetupCorrectly(t, tablets[3], "ON")
	CheckSemiSyncSetupCorrectly(t, tablets[1], "OFF")
	CheckSemiSyncSetupCorrectly(t, tablets[2], "OFF")

	// Run forced reparent operation, this should proceed unimpeded.
	out, err := Prs(t, clusterInstance, tablets[3])
	require.NoError(t, err, out)

	ConfirmReplication(t, tablets[3], []*vitesst.Tablet{tablets[0], tablets[1], tablets[2]})

	// All the tablets will have semi-sync setup since tablets[3] is in Cell2 and all
	// others are in Cell1, so all of them are eligible to send semi-sync ACKs
	for _, tablet := range tablets {
		CheckSemiSyncSetupCorrectly(t, tablet, "ON")
	}

	for range 2 {
		// Bring up a new replica tablet
		// In this new tablet, we do not disable active reparents, otherwise replication will not be started.
		newReplica := StartNewVTTablet(t, clusterInstance)
		// Check that we can replicate to it and semi-sync is setup correctly on it
		ConfirmReplication(t, tablets[3], []*vitesst.Tablet{tablets[0], tablets[1], tablets[2], newReplica})
		CheckSemiSyncSetupCorrectly(t, newReplica, "ON")
	}
}

// TestFullStatus tests that the RPC FullStatus works as intended.
func TestFullStatus(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)
	ConfirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

	// Check that full status gives the correct result for a primary tablet
	primaryTablet := tablets[0]
	primaryStatusString, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "GetFullStatus", primaryTablet.Alias())
	require.NoError(t, err)
	primaryStatus := &replicationdatapb.FullStatus{}
	opt := protojson.UnmarshalOptions{DiscardUnknown: true}
	err = opt.Unmarshal([]byte(primaryStatusString), primaryStatus)
	require.NoError(t, err)
	assert.NotEmpty(t, primaryStatus.ServerUuid)
	assert.NotEmpty(t, primaryStatus.ServerId)
	// For a primary tablet there is no replication status
	assert.Nil(t, primaryStatus.ReplicationStatus)
	assert.Contains(t, primaryStatus.PrimaryStatus.String(), "vt-0000000101-bin")
	assert.Equal(t, primaryStatus.GtidPurged, "MySQL56/")
	assert.False(t, primaryStatus.ReadOnly)
	assert.False(t, primaryStatus.SuperReadOnly)
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
	assert.Regexp(t, `[58]\.[074].*`, primaryStatus.Version)
	assert.NotEmpty(t, primaryStatus.VersionComment)

	replicaTablet := tablets[1]

	waitForFilePosition(t, clusterInstance, primaryTablet, replicaTablet, 5*time.Second)

	// Check that full status gives the correct result for a replica tablet
	replicaStatusString, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "GetFullStatus", replicaTablet.Alias())
	require.NoError(t, err)
	replicaStatus := &replicationdatapb.FullStatus{}
	opt = protojson.UnmarshalOptions{DiscardUnknown: true}
	err = opt.Unmarshal([]byte(replicaStatusString), replicaStatus)
	require.NoError(t, err)
	assert.NotEmpty(t, replicaStatus.ServerUuid)
	assert.NotEmpty(t, replicaStatus.ServerId)
	assert.Contains(t, replicaStatus.ReplicationStatus.Position, "MySQL56/"+replicaStatus.ReplicationStatus.SourceUuid)
	assert.EqualValues(t, replication.ReplicationStateRunning, replicaStatus.ReplicationStatus.IoState)
	assert.EqualValues(t, replication.ReplicationStateRunning, replicaStatus.ReplicationStatus.SqlState)
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
	assert.Equal(t, replicaStatus.ReplicationStatus.SourceHost, tablets[0].Name())
	assert.EqualValues(t, replicaStatus.ReplicationStatus.SourcePort, tabletMySQLPort)
	assert.Equal(t, replicaStatus.ReplicationStatus.SourceUser, "vt_repl")
	assert.Contains(t, replicaStatus.PrimaryStatus.String(), "vt-0000000102-bin")
	assert.Equal(t, replicaStatus.GtidPurged, "MySQL56/")
	assert.True(t, replicaStatus.ReadOnly)
	assert.True(t, replicaStatus.SuperReadOnly)
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
	assert.Regexp(t, `[58]\.[074].*`, replicaStatus.Version)
	assert.NotEmpty(t, replicaStatus.VersionComment)
}

func getFullStatus(t *testing.T, clusterInstance *vitesst.Cluster, tablet *vitesst.Tablet) *replicationdatapb.FullStatus {
	statusString, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "GetFullStatus", tablet.Alias())
	require.NoError(t, err)
	status := &replicationdatapb.FullStatus{}
	opt := protojson.UnmarshalOptions{DiscardUnknown: true}
	err = opt.Unmarshal([]byte(statusString), status)
	require.NoError(t, err)
	return status
}

// waitForFilePosition waits for timeout to see if FilePositions align b/w primary and replica, to fix flakiness in tests due to race conditions where replica is still catching up
func waitForFilePosition(t *testing.T, clusterInstance *vitesst.Cluster, primary *vitesst.Tablet, replica *vitesst.Tablet, timeout time.Duration) {
	start := time.Now()
	for {
		primaryStatus := getFullStatus(t, clusterInstance, primary)
		replicaStatus := getFullStatus(t, clusterInstance, replica)
		if primaryStatus.PrimaryStatus.FilePosition == replicaStatus.ReplicationStatus.FilePosition {
			return
		}
		if d := time.Since(start); d > timeout {
			require.FailNowf(t, "waitForFilePosition timed out, primary %s, replica %s",
				primaryStatus.PrimaryStatus.FilePosition, replicaStatus.ReplicationStatus.FilePosition)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// fileNameFromPosition gets the file name from the position
func fileNameFromPosition(pos string) string {
	s := strings.SplitN(pos, ":", 2)
	if len(s) != 2 {
		return ""
	}
	return s[0]
}

func TestFileNameFromPosition(t *testing.T) {
	assert.Equal(t, "", fileNameFromPosition("shouldfail"))
	assert.Equal(t, "FilePos/vt-0000000101-bin.000001", fileNameFromPosition("FilePos/vt-0000000101-bin.000001:123456789"))
}

// rowNumberFromPosition gets the row number from the position
func rowNumberFromPosition(pos string) int {
	rowNumStr := pos[len(pos)-4:]
	rowNum, _ := strconv.Atoi(rowNumStr)
	return rowNum
}
