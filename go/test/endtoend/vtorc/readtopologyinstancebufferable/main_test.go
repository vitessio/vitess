package readtopologyinstancebufferable

import (
	"flag"
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
	"vitess.io/vitess/go/vt/orchestrator/app"
	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/inst"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadTopologyInstanceBufferable(t *testing.T) {
	clusterInfo := utils.SetupNewClusterSemiSync(t)
	defer func() {
		clusterInfo.ClusterInstance.Teardown()
	}()
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	err := flag.Set("topo_global_server_address", clusterInfo.ClusterInstance.VtctlProcess.TopoGlobalAddress)
	require.NoError(t, err)
	err = flag.Set("topo_implementation", clusterInfo.ClusterInstance.VtctlProcess.TopoImplementation)
	require.NoError(t, err)
	err = flag.Set("topo_global_root", clusterInfo.ClusterInstance.VtctlProcess.TopoGlobalRoot)
	require.NoError(t, err)
	falseVal := false
	emptyVal := ""
	config.Config.Debug = true
	config.Config.MySQLTopologyUser = "orc_client_user"
	config.Config.MySQLTopologyPassword = "orc_client_user_password"
	config.Config.MySQLReplicaUser = "vt_repl"
	config.Config.MySQLReplicaPassword = ""
	config.Config.RecoveryPeriodBlockSeconds = 1
	config.Config.InstancePollSeconds = 1
	config.RuntimeCLIFlags.SkipUnresolve = &falseVal
	config.RuntimeCLIFlags.SkipUnresolveCheck = &falseVal
	config.RuntimeCLIFlags.Noop = &falseVal
	config.RuntimeCLIFlags.BinlogFile = &emptyVal
	config.RuntimeCLIFlags.Statement = &emptyVal
	config.RuntimeCLIFlags.GrabElection = &falseVal
	config.RuntimeCLIFlags.SkipContinuousRegistration = &falseVal
	config.RuntimeCLIFlags.EnableDatabaseUpdate = &falseVal
	config.RuntimeCLIFlags.IgnoreRaftSetup = &falseVal
	config.RuntimeCLIFlags.Tag = &emptyVal
	config.MarkConfigurationLoaded()

	go func() {
		app.HTTP(true)
	}()

	primary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, primary, "should have elected a primary")
	utils.CheckReplication(t, clusterInfo, primary, shard0.Vttablets, 10*time.Second)
	var replica *cluster.Vttablet
	for _, vttablet := range shard0.Vttablets {
		if vttablet.Type == "replica" && vttablet.Alias != primary.Alias {
			replica = vttablet
		}
	}

	primaryInstance, err := inst.ReadTopologyInstanceBufferable(&inst.InstanceKey{
		Hostname: utils.Hostname,
		Port:     primary.MySQLPort,
	}, false, nil)
	require.NoError(t, err)
	require.NotNil(t, primaryInstance)
	assert.Contains(t, primaryInstance.InstanceAlias, "zone1")
	assert.NotEqual(t, 0, primaryInstance.ServerID)
	assert.Greater(t, len(primaryInstance.ServerUUID), 10)
	assert.Contains(t, primaryInstance.Version, "5.7")
	assert.NotEmpty(t, primaryInstance.VersionComment)
	assert.False(t, primaryInstance.ReadOnly)
	assert.True(t, primaryInstance.LogBinEnabled)
	assert.True(t, primaryInstance.LogReplicationUpdatesEnabled)
	assert.Equal(t, "ROW", primaryInstance.BinlogFormat)
	assert.Equal(t, "ON", primaryInstance.GTIDMode)
	assert.Equal(t, "FULL", primaryInstance.BinlogRowImage)
	assert.Contains(t, primaryInstance.SelfBinlogCoordinates.LogFile, fmt.Sprintf("vt-0000000%d", primary.TabletUID))
	assert.Greater(t, primaryInstance.SelfBinlogCoordinates.LogPos, int64(0))
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

	replicaInstance, err := inst.ReadTopologyInstanceBufferable(&inst.InstanceKey{
		Hostname: utils.Hostname,
		Port:     replica.MySQLPort,
	}, false, nil)
	require.NoError(t, err)
	require.NotNil(t, replicaInstance)
	assert.Contains(t, replicaInstance.InstanceAlias, "zone1")
	assert.NotEqual(t, 0, replicaInstance.ServerID)
	assert.Greater(t, len(replicaInstance.ServerUUID), 10)
	assert.Contains(t, replicaInstance.Version, "5.7")
	assert.NotEmpty(t, replicaInstance.VersionComment)
	assert.True(t, replicaInstance.ReadOnly)
	assert.True(t, replicaInstance.LogBinEnabled)
	assert.True(t, replicaInstance.LogReplicationUpdatesEnabled)
	assert.Equal(t, "ROW", replicaInstance.BinlogFormat)
	assert.Equal(t, "ON", replicaInstance.GTIDMode)
	assert.Equal(t, "FULL", replicaInstance.BinlogRowImage)
	assert.Contains(t, replicaInstance.SelfBinlogCoordinates.LogFile, fmt.Sprintf("vt-0000000%d", replica.TabletUID))
	assert.Greater(t, replicaInstance.SelfBinlogCoordinates.LogPos, int64(0))
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
	assert.Empty(t, replicaInstance.GtidErrant)
}
