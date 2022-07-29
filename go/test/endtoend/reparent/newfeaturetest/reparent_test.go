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
	"context"
	"strconv"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ERS TESTS

func TestRecoverWithMultipleFailures(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, true)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// make tablets[1] a rdonly tablet.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", tablets[1].Alias, "rdonly")
	require.NoError(t, err)

	// Confirm that replication is still working as intended
	utils.ConfirmReplication(t, tablets[0], tablets[1:])

	// Make the rdonly and primary tablets and databases unavailable.
	utils.StopTablet(t, tablets[1], true)
	utils.StopTablet(t, tablets[0], true)

	// We expect this to succeed since we only have 1 primary eligible tablet which is down
	out, err := utils.Ers(clusterInstance, nil, "30s", "10s")
	require.NoError(t, err, out)

	newPrimary := utils.GetNewPrimary(t, clusterInstance)
	utils.ConfirmReplication(t, newPrimary, []*cluster.Vttablet{tablets[2], tablets[3]})
}

// TestERSFailFast tests that ERS will fail fast if it cannot find any tablet which can be safely promoted instead of promoting
// a tablet and hanging while inserting a row in the reparent journal on getting semi-sync ACKs
func TestERSFailFast(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, true)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// make tablets[1] a rdonly tablet.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", tablets[1].Alias, "rdonly")
	require.NoError(t, err)

	// Confirm that replication is still working as intended
	utils.ConfirmReplication(t, tablets[0], tablets[1:])

	strChan := make(chan string)
	go func() {
		// We expect this to fail since we have ignored all replica tablets and only the rdonly is left, which is not capable of sending semi-sync ACKs
		out, err := utils.ErsIgnoreTablet(clusterInstance, tablets[2], "240s", "90s", []*cluster.Vttablet{tablets[0], tablets[3]}, false)
		require.Error(t, err)
		strChan <- out
	}()

	select {
	case out := <-strChan:
		require.Contains(t, out, "proposed primary zone1-0000000103 will not be able to make forward progress on being promoted")
	case <-time.After(60 * time.Second):
		require.Fail(t, "Emergency Reparent Shard did not fail in 60 seconds")
	}
}

// TestReplicationStopped checks that ERS ignores the tablets that have sql thread stopped.
// If there are more than 1, we also fail.
func TestReplicationStopped(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, true)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	err := clusterInstance.VtctlclientProcess.ExecuteCommand("ExecuteFetchAsDba", tablets[1].Alias, `STOP SLAVE SQL_THREAD;`)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ExecuteFetchAsDba", tablets[2].Alias, `STOP SLAVE;`)
	require.NoError(t, err)
	// Run an additional command in the current primary which will only be acked by tablets[3] and be in its relay log.
	insertedVal := utils.ConfirmReplication(t, tablets[0], nil)
	// Failover to tablets[3]
	_, err = utils.Ers(clusterInstance, tablets[3], "60s", "30s")
	require.Error(t, err, "ERS should fail with 2 replicas having replication stopped")

	// Start replication back on tablet[1]
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ExecuteFetchAsDba", tablets[1].Alias, `START SLAVE;`)
	require.NoError(t, err)
	// Failover to tablets[3] again. This time it should succeed
	out, err := utils.Ers(clusterInstance, tablets[3], "60s", "30s")
	require.NoError(t, err, out)
	// Verify that the tablet has the inserted value
	err = utils.CheckInsertedValues(context.Background(), t, tablets[3], insertedVal)
	require.NoError(t, err)
	// Confirm that replication is setup correctly from tablets[3] to tablets[0]
	utils.ConfirmReplication(t, tablets[3], tablets[:1])
	// Confirm that tablets[2] which had replication stopped initially still has its replication stopped
	utils.CheckReplicationStatus(context.Background(), t, tablets[2], false, false)
}

// TestFullStatus tests that the RPC FullStatus works as intended.
func TestFullStatus(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, true)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// Check that full status gives the correct result for a primary tablet
	primaryStatus, err := utils.TmcFullStatus(context.Background(), tablets[0])
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
	replicaStatus, err := utils.TmcFullStatus(context.Background(), tablets[1])
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
