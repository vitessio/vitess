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

package newfeaturetest

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
)

// TestCrossCellDurability tests 2 things -
// 1. When PRS is run with the cross_cell durability policy setup, then the semi-sync settings on all the tablets are as expected
// 2. Bringing up a new vttablet should have its replication and semi-sync setup correctly without any external interference
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
	primaryStatusString, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("GetFullStatus", tablets[0].Alias)
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

	// Check that full status gives the correct result for a replica tablet
	replicaStatusString, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("GetFullStatus", tablets[1].Alias)
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

// TestTabletRestart tests that a running tablet can be  restarted and everything is still fine
func TestTabletRestart(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	utils.StopTablet(t, tablets[1], false)
	tablets[1].VttabletProcess.ServingStatus = "SERVING"
	err := tablets[1].VttabletProcess.Setup()
	require.NoError(t, err)
}
