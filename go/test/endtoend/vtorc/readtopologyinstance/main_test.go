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

package readtopologyinstance

import (
	"fmt"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/logic"
	"vitess.io/vitess/go/vt/vtorc/server"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestReadTopologyInstanceBufferable(t *testing.T) {
	clusterInfo := utils.SetupNewClusterSemiSync(t)
	defer func() {
		clusterInfo.ClusterInstance.Teardown()
	}()
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	oldArgs := os.Args
	defer func() {
		// Restore the old args after the test
		os.Args = oldArgs
	}()

	// Change the args such that they match how we would invoke VTOrc
	os.Args = []string{"vtorc",
		"--topo_global_server_address", clusterInfo.ClusterInstance.VtctlProcess.TopoGlobalAddress,
		"--topo_implementation", clusterInfo.ClusterInstance.VtctlProcess.TopoImplementation,
		"--topo_global_root", clusterInfo.ClusterInstance.VtctlProcess.TopoGlobalRoot,
	}
	servenv.ParseFlags("vtorc")
	config.Config.RecoveryPeriodBlockSeconds = 1
	config.Config.InstancePollSeconds = 1
	config.MarkConfigurationLoaded()
	server.StartVTOrcDiscovery()

	primary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, primary, "should have elected a primary")
	utils.CheckReplication(t, clusterInfo, primary, shard0.Vttablets, 10*time.Second)
	var replica *cluster.Vttablet
	for _, vttablet := range shard0.Vttablets {
		if vttablet.Type == "replica" && vttablet.Alias != primary.Alias {
			replica = vttablet
		}
	}

	primaryInstance, err := inst.ReadTopologyInstanceBufferable(primary.Alias, nil)
	require.NoError(t, err)
	require.NotNil(t, primaryInstance)
	assert.Equal(t, utils.Hostname, primaryInstance.Hostname)
	assert.Equal(t, primary.MySQLPort, primaryInstance.Port)
	assert.Contains(t, primaryInstance.InstanceAlias, "zone1")
	assert.NotEqual(t, 0, primaryInstance.ServerID)
	assert.Greater(t, len(primaryInstance.ServerUUID), 10)
	assert.Regexp(t, "[58].[70].*", primaryInstance.Version)
	assert.NotEmpty(t, primaryInstance.VersionComment)
	assert.False(t, primaryInstance.ReadOnly)
	assert.True(t, primaryInstance.LogBinEnabled)
	assert.True(t, primaryInstance.LogReplicationUpdatesEnabled)
	assert.Equal(t, "ROW", primaryInstance.BinlogFormat)
	assert.Equal(t, "ON", primaryInstance.GTIDMode)
	assert.Equal(t, "FULL", primaryInstance.BinlogRowImage)
	assert.Contains(t, primaryInstance.SelfBinlogCoordinates.LogFile, fmt.Sprintf("vt-0000000%d-bin", primary.TabletUID))
	assert.Greater(t, primaryInstance.SelfBinlogCoordinates.LogPos, uint32(0))
	assert.True(t, primaryInstance.SemiSyncPrimaryEnabled)
	assert.True(t, primaryInstance.SemiSyncReplicaEnabled)
	assert.True(t, primaryInstance.SemiSyncPrimaryStatus)
	assert.False(t, primaryInstance.SemiSyncReplicaStatus)
	assert.EqualValues(t, 2, primaryInstance.SemiSyncPrimaryClients)
	assert.EqualValues(t, 1, primaryInstance.SemiSyncPrimaryWaitForReplicaCount)
	assert.EqualValues(t, 1000000000000000000, primaryInstance.SemiSyncPrimaryTimeout)
	assert.NotEmpty(t, primaryInstance.ExecutedGtidSet)
	assert.Contains(t, primaryInstance.ExecutedGtidSet, primaryInstance.ServerUUID)
	assert.Empty(t, primaryInstance.GtidPurged)
	assert.Empty(t, primaryInstance.GtidErrant)
	assert.False(t, primaryInstance.HasReplicationCredentials)
	assert.Equal(t, primaryInstance.ReplicationIOThreadState, inst.ReplicationThreadStateNoThread)
	assert.Equal(t, primaryInstance.ReplicationSQLThreadState, inst.ReplicationThreadStateNoThread)

	// Insert an errant GTID in the replica.
	// The way to do this is to disable global recoveries, stop replication and inject an errant GTID.
	// After this we restart the replication and enable the recoveries again.
	err = logic.DisableRecovery()
	require.NoError(t, err)
	err = utils.RunSQLs(t, []string{`STOP SLAVE;`,
		`SET GTID_NEXT="12345678-1234-1234-1234-123456789012:1";`,
		`BEGIN;`, `COMMIT;`,
		`SET GTID_NEXT="AUTOMATIC";`,
		`START SLAVE;`,
	}, replica, "")
	require.NoError(t, err)
	err = logic.EnableRecovery()
	require.NoError(t, err)

	replicaInstance, err := inst.ReadTopologyInstanceBufferable(replica.Alias, nil)
	require.NoError(t, err)
	require.NotNil(t, replicaInstance)
	assert.Equal(t, utils.Hostname, replicaInstance.Hostname)
	assert.Equal(t, replica.MySQLPort, replicaInstance.Port)
	assert.Contains(t, replicaInstance.InstanceAlias, "zone1")
	assert.NotEqual(t, 0, replicaInstance.ServerID)
	assert.Greater(t, len(replicaInstance.ServerUUID), 10)
	assert.Regexp(t, "[58].[70].*", replicaInstance.Version)
	assert.NotEmpty(t, replicaInstance.VersionComment)
	assert.True(t, replicaInstance.ReadOnly)
	assert.True(t, replicaInstance.LogBinEnabled)
	assert.True(t, replicaInstance.LogReplicationUpdatesEnabled)
	assert.Equal(t, "ROW", replicaInstance.BinlogFormat)
	assert.Equal(t, "ON", replicaInstance.GTIDMode)
	assert.Equal(t, "FULL", replicaInstance.BinlogRowImage)
	assert.Equal(t, utils.Hostname, replicaInstance.SourceHost)
	assert.Equal(t, primary.MySQLPort, replicaInstance.SourcePort)
	assert.Contains(t, replicaInstance.SelfBinlogCoordinates.LogFile, fmt.Sprintf("vt-0000000%d-bin", replica.TabletUID))
	assert.Greater(t, replicaInstance.SelfBinlogCoordinates.LogPos, uint32(0))
	assert.False(t, replicaInstance.SemiSyncPrimaryEnabled)
	assert.True(t, replicaInstance.SemiSyncReplicaEnabled)
	assert.False(t, replicaInstance.SemiSyncPrimaryStatus)
	assert.True(t, replicaInstance.SemiSyncReplicaStatus)
	assert.EqualValues(t, 0, replicaInstance.SemiSyncPrimaryClients)
	assert.EqualValues(t, 1, replicaInstance.SemiSyncPrimaryWaitForReplicaCount)
	assert.EqualValues(t, 1000000000000000000, replicaInstance.SemiSyncPrimaryTimeout)
	assert.NotEmpty(t, replicaInstance.ExecutedGtidSet)
	assert.Contains(t, replicaInstance.ExecutedGtidSet, primaryInstance.ServerUUID)
	assert.Empty(t, replicaInstance.GtidPurged)
	assert.Regexp(t, ".{8}-.{4}-.{4}-.{4}-.{12}:.*", replicaInstance.GtidErrant)
	assert.True(t, replicaInstance.HasReplicationCredentials)
	assert.Equal(t, replicaInstance.ReplicationIOThreadState, inst.ReplicationThreadStateRunning)
	assert.Equal(t, replicaInstance.ReplicationSQLThreadState, inst.ReplicationThreadStateRunning)
	assert.True(t, replicaInstance.ReplicationIOThreadRuning)
	assert.True(t, replicaInstance.ReplicationSQLThreadRuning)
	assert.Equal(t, replicaInstance.ReadBinlogCoordinates.LogFile, primaryInstance.SelfBinlogCoordinates.LogFile)
	assert.Greater(t, replicaInstance.ReadBinlogCoordinates.LogPos, uint32(0))
	assert.Equal(t, replicaInstance.ExecBinlogCoordinates.LogFile, primaryInstance.SelfBinlogCoordinates.LogFile)
	assert.Greater(t, replicaInstance.ExecBinlogCoordinates.LogPos, uint32(0))
	assert.Contains(t, replicaInstance.RelaylogCoordinates.LogFile, fmt.Sprintf("vt-0000000%d-relay", replica.TabletUID))
	assert.Greater(t, replicaInstance.RelaylogCoordinates.LogPos, uint32(0))
	assert.Empty(t, replicaInstance.LastIOError)
	assert.Empty(t, replicaInstance.LastSQLError)
	assert.EqualValues(t, 0, replicaInstance.SQLDelay)
	assert.True(t, replicaInstance.UsingOracleGTID)
	assert.False(t, replicaInstance.UsingMariaDBGTID)
	assert.Equal(t, replicaInstance.SourceUUID, primaryInstance.ServerUUID)
	assert.False(t, replicaInstance.HasReplicationFilters)
	assert.LessOrEqual(t, int(replicaInstance.SecondsBehindPrimary.Int64), 1)
	assert.False(t, replicaInstance.AllowTLS)
}
