/*
Copyright 2020 The Vitess Authors.

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

package general

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtutils "vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/logic"
)

// Cases to test:
// 1. create cluster with 2 replicas and 1 rdonly, let orc choose primary
// verify rdonly is not elected, only replica
// verify replication is setup
// verify that with multiple vtorc instances, we still only have 1 PlannedReparentShard call
func TestPrimaryElection(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, nil, cluster.VTOrcConfiguration{
		PreventCrossCellFailover: true,
	}, 2, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	primary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, primary, "should have elected a primary")
	utils.CheckReplication(t, clusterInfo, primary, shard0.Vttablets, 10*time.Second)

	for _, vttablet := range shard0.Vttablets {
		if vttablet.Type == "rdonly" && primary.Alias == vttablet.Alias {
			t.Errorf("Rdonly tablet promoted as primary - %v", primary.Alias)
		}
	}

	res, err := utils.RunSQL(t, "select * from reparent_journal", primary, "_vt")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1, "There should only be 1 primary tablet which was elected")
}

// TestErrantGTIDOnPreviousPrimary tests that VTOrc is able to detect errant GTIDs on a previously demoted primary
// if it has an errant GTID.
func TestErrantGTIDOnPreviousPrimary(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 3, 0, []string{"--change-tablets-with-errant-gtid-to-drained"}, cluster.VTOrcConfiguration{}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	vtOrcProcess := clusterInfo.ClusterInstance.VTOrcProcesses[0]
	utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	utils.WaitForSuccessfulPRSCount(t, vtOrcProcess, keyspace.Name, shard0.Name, 1)

	var replica, otherReplica *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias != curPrimary.Alias {
			if replica == nil {
				replica = tablet
			} else {
				otherReplica = tablet
			}
		}
	}
	require.NotNil(t, replica, "should be able to find a replica")
	require.NotNil(t, otherReplica, "should be able to find 2nd replica")
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica, otherReplica}, 15*time.Second)

	// Disable global recoveries for the cluster.
	vtOrcProcess.DisableGlobalRecoveries(t)

	// Run PRS to promote a different replica.
	output, err := clusterInfo.ClusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(
		"PlannedReparentShard",
		fmt.Sprintf("%s/%s", keyspace.Name, shard0.Name),
		"--new-primary", replica.Alias)
	require.NoError(t, err, "error in PlannedReparentShard output - %s", output)

	// Stop replicatin on the previous primary to simulate it not reparenting properly.
	// Also insert an errant GTID on the previous primary.
	err = utils.RunSQLs(t, []string{
		"STOP REPLICA",
		"RESET REPLICA ALL",
		"set global read_only=OFF",
		"insert into vt_ks.vt_insert_test(id, msg) values (10173, 'test 178342')",
	}, curPrimary, "")
	require.NoError(t, err)

	// Wait for VTOrc to detect the errant GTID and change the tablet to a drained type.
	vtOrcProcess.EnableGlobalRecoveries(t)

	// Wait for the tablet to be drained.
	utils.WaitForTabletType(t, curPrimary, "drained")
}

// Cases to test:
// 1. create cluster with 1 replica and 1 rdonly, let orc choose primary
// verify rdonly is not elected, only replica
// verify replication is setup
func TestSingleKeyspace(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 1, 1, []string{"--clusters_to_watch", "ks"}, cluster.VTOrcConfiguration{
		PreventCrossCellFailover: true,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	utils.CheckPrimaryTablet(t, clusterInfo, shard0.Vttablets[0], true)
	utils.CheckReplication(t, clusterInfo, shard0.Vttablets[0], shard0.Vttablets[1:], 10*time.Second)
	utils.WaitForSuccessfulRecoveryCount(t, clusterInfo.ClusterInstance.VTOrcProcesses[0], logic.ElectNewPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	utils.WaitForSuccessfulPRSCount(t, clusterInfo.ClusterInstance.VTOrcProcesses[0], keyspace.Name, shard0.Name, 1)
}

// Cases to test:
// 1. create cluster with 1 replica and 1 rdonly, let orc choose primary
// verify rdonly is not elected, only replica
// verify replication is setup
func TestKeyspaceShard(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 1, 1, []string{"--clusters_to_watch", "ks/0"}, cluster.VTOrcConfiguration{
		PreventCrossCellFailover: true,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	utils.CheckPrimaryTablet(t, clusterInfo, shard0.Vttablets[0], true)
	utils.CheckReplication(t, clusterInfo, shard0.Vttablets[0], shard0.Vttablets[1:], 10*time.Second)
	utils.WaitForSuccessfulRecoveryCount(t, clusterInfo.ClusterInstance.VTOrcProcesses[0], logic.ElectNewPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	utils.WaitForSuccessfulPRSCount(t, clusterInfo.ClusterInstance.VTOrcProcesses[0], keyspace.Name, shard0.Name, 1)
}

// Cases to test:
// 1. make primary readonly, let vtorc repair
// 2. make replica ReadWrite, let vtorc repair
// 3. stop replication, let vtorc repair
// 4. setup replication from non-primary, let vtorc repair
// 5. make instance A replicates from B and B from A, wait for repair
// 6. disable recoveries and make sure the detected problems are set correctly.
func TestVTOrcRepairs(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 3, 0, []string{"--change-tablets-with-errant-gtid-to-drained"}, cluster.VTOrcConfiguration{
		PreventCrossCellFailover: true,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	vtOrcProcess := clusterInfo.ClusterInstance.VTOrcProcesses[0]
	utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	utils.WaitForSuccessfulPRSCount(t, vtOrcProcess, keyspace.Name, shard0.Name, 1)

	var replica, otherReplica *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias != curPrimary.Alias {
			if replica == nil {
				replica = tablet
			} else {
				otherReplica = tablet
			}
		}
	}
	require.NotNil(t, replica, "should be able to find a replica")
	require.NotNil(t, otherReplica, "should be able to find 2nd replica")

	// check replication is setup correctly
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica, otherReplica}, 15*time.Second)

	t.Run("PrimaryReadOnly", func(t *testing.T) {
		// Make the current primary database read-only.
		_, err := utils.RunSQL(t, "set global read_only=ON", curPrimary, "")
		require.NoError(t, err)

		// wait for repair
		match := utils.WaitForReadOnlyValue(t, curPrimary, 0)
		require.True(t, match)
		utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	})

	t.Run("ReplicaReadWrite", func(t *testing.T) {
		// Make the replica database read-write.
		_, err := utils.RunSQL(t, "set global read_only=OFF", replica, "")
		require.NoError(t, err)

		// wait for repair
		match := utils.WaitForReadOnlyValue(t, replica, 1)
		require.True(t, match)
		utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixReplicaRecoveryName, keyspace.Name, shard0.Name, 1)
	})

	t.Run("StopReplication", func(t *testing.T) {
		// use vtctldclient to stop replication
		_, err := clusterInfo.ClusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StopReplication", replica.Alias)
		require.NoError(t, err)

		// check replication is setup correctly
		utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica, otherReplica}, 15*time.Second)
		utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixReplicaRecoveryName, keyspace.Name, shard0.Name, 2)

		// Stop just the IO thread on the replica
		_, err = utils.RunSQL(t, "STOP REPLICA IO_THREAD", replica, "")
		require.NoError(t, err)

		// check replication is setup correctly
		utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica, otherReplica}, 15*time.Second)
		utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixReplicaRecoveryName, keyspace.Name, shard0.Name, 3)

		// Stop just the SQL thread on the replica
		_, err = utils.RunSQL(t, "STOP REPLICA SQL_THREAD", replica, "")
		require.NoError(t, err)

		// check replication is setup correctly
		utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica, otherReplica}, 15*time.Second)
		utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixReplicaRecoveryName, keyspace.Name, shard0.Name, 4)
	})

	t.Run("ReplicationFromOtherReplica", func(t *testing.T) {
		// point replica at otherReplica
		changeReplicationSourceCommands := []string{
			"STOP REPLICA",
			"RESET REPLICA ALL",
			fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='vt_repl', GET_SOURCE_PUBLIC_KEY = 1, SOURCE_AUTO_POSITION = 1", utils.Hostname, otherReplica.MySQLPort),
			"START REPLICA",
		}
		err := utils.RunSQLs(t, changeReplicationSourceCommands, replica, "")
		require.NoError(t, err)

		// wait until the source port is set back correctly by vtorc
		utils.CheckSourcePort(t, replica, curPrimary, 15*time.Second)
		utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixReplicaRecoveryName, keyspace.Name, shard0.Name, 5)

		// check that writes succeed
		utils.VerifyWritesSucceed(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica, otherReplica}, 15*time.Second)
	})

	t.Run("Replication Misconfiguration", func(t *testing.T) {
		_, err := utils.RunSQL(t, `SET @@global.replica_net_timeout=33`, replica, "")
		require.NoError(t, err)

		// wait until heart beat interval has been fixed by vtorc.
		utils.CheckHeartbeatInterval(t, replica, 16.5, 15*time.Second)
		utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixReplicaRecoveryName, keyspace.Name, shard0.Name, 6)

		// check that writes succeed
		utils.VerifyWritesSucceed(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica, otherReplica}, 15*time.Second)
	})

	t.Run("CircularReplication", func(t *testing.T) {
		// change the replication source on the primary
		changeReplicationSourceCommands := []string{
			"STOP REPLICA",
			"RESET REPLICA ALL",
			fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='vt_repl', GET_SOURCE_PUBLIC_KEY = 1, SOURCE_AUTO_POSITION = 1", replica.VttabletProcess.TabletHostname, replica.MySQLPort),
			"START REPLICA",
		}
		err := utils.RunSQLs(t, changeReplicationSourceCommands, curPrimary, "")
		require.NoError(t, err)

		// wait for curPrimary to reach stable state
		time.Sleep(1 * time.Second)

		// wait for repair
		err = utils.WaitForReplicationToStop(t, curPrimary)
		require.NoError(t, err)
		utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.RecoverPrimaryHasPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
		// check that the writes still succeed
		utils.VerifyWritesSucceed(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica, otherReplica}, 10*time.Second)
	})

	t.Run("Errant GTID Detected", func(t *testing.T) {
		// insert an errant GTID in the replica
		_, err := utils.RunSQL(t, "insert into vt_insert_test(id, msg) values (10173, 'test 178342')", replica, "vt_ks")
		require.NoError(t, err)
		// When VTOrc detects errant GTIDs, it should change the tablet to a drained type.
		utils.WaitForTabletType(t, replica, "drained")
	})

	t.Run("Sets DetectedProblems metric correctly", func(t *testing.T) {
		// Since we're using a boolean metric here, disable recoveries for now.
		status, _, err := utils.MakeAPICall(t, vtOrcProcess, "/api/disable-global-recoveries")
		require.NoError(t, err)
		require.Equal(t, 200, status)

		// Make the current primary database read-only.
		_, err = utils.RunSQL(t, "set global read_only=ON", curPrimary, "")
		require.NoError(t, err)

		// Wait for problems to be set.
		utils.WaitForDetectedProblems(t, vtOrcProcess,
			string(inst.PrimaryIsReadOnly),
			curPrimary.Alias,
			keyspace.Name,
			shard0.Name,
			1,
		)

		// Enable recoveries.
		status, _, err = utils.MakeAPICall(t, vtOrcProcess, "/api/enable-global-recoveries")
		require.NoError(t, err)
		assert.Equal(t, 200, status)

		// wait for detected problem to be cleared.
		utils.WaitForDetectedProblems(t, vtOrcProcess,
			string(inst.PrimaryIsReadOnly),
			curPrimary.Alias,
			keyspace.Name,
			shard0.Name,
			0,
		)
	})

	t.Run("Primary tablet's display type doesn't match the topo record", func(t *testing.T) {
		// There is no easy way to make a tablet type mismatch with the topo record.
		// In production this only happens when the call to update the topo record fails with a timeout,
		// but the operation has succeeded. We can't reliably simulate this in a test.
		// So, instead we are explicitly changing the tablet record for one of the tablets
		// to make it a primary and see that VTOrc detects the mismatch and promotes the tablet.

		// Initially check that replication is working as intended
		utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica, otherReplica}, 15*time.Second)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := clusterInfo.Ts.UpdateTabletFields(ctx, replica.GetAlias(), func(tablet *topodatapb.Tablet) error {
			tablet.Type = topodatapb.TabletType_PRIMARY
			tablet.PrimaryTermStartTime = protoutil.TimeToProto(time.Now())
			return nil
		})
		require.NoError(t, err)

		// Wait for VTOrc to detect the mismatch and promote the tablet.
		require.Eventuallyf(t, func() bool {
			fs := cluster.FullStatus(t, replica, clusterInfo.ClusterInstance.Hostname)
			return fs.TabletType == topodatapb.TabletType_PRIMARY
		}, 10*time.Second, 1*time.Second, "Primary tablet's display type didn't match the topo record")
		// Also check that the replica gets promoted and can accept writes.
		utils.CheckReplication(t, clusterInfo, replica, []*cluster.Vttablet{curPrimary, otherReplica}, 15*time.Second)
	})
}

func TestRepairAfterTER(t *testing.T) {
	// test fails intermittently on CI, skip until it can be fixed.
	t.SkipNow()
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 0, nil, cluster.VTOrcConfiguration{
		PreventCrossCellFailover: true,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// TODO(deepthi): we should not need to do this, the DB should be created automatically
	_, err := curPrimary.VttabletProcess.QueryTablet("create database IF NOT EXISTS vt_"+keyspace.Name, keyspace.Name, false)
	require.NoError(t, err)

	var newPrimary *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias != curPrimary.Alias {
			newPrimary = tablet
			break
		}
	}

	// TER to other tablet
	_, err = clusterInfo.ClusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("TabletExternallyReparented", newPrimary.Alias)
	require.NoError(t, err)

	utils.CheckReplication(t, clusterInfo, newPrimary, []*cluster.Vttablet{curPrimary}, 15*time.Second)
}

// TestSemiSync tests that semi-sync is setup correctly by vtorc if it is incorrectly set
func TestSemiSync(t *testing.T) {
	// stop any vtorc instance running due to a previous test.
	utils.StopVTOrcs(t, clusterInfo)
	newCluster := utils.SetupNewClusterSemiSync(t)
	defer utils.PrintVTOrcLogsOnFailure(t, newCluster.ClusterInstance)
	utils.StartVTOrcs(t, newCluster, nil, cluster.VTOrcConfiguration{
		PreventCrossCellFailover: true,
	}, 1)
	defer func() {
		utils.StopVTOrcs(t, newCluster)
		newCluster.ClusterInstance.Teardown()
	}()
	keyspace := &newCluster.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	primary := utils.ShardPrimaryTablet(t, newCluster, keyspace, shard0)
	assert.NotNil(t, primary, "should have elected a primary")

	var replica1, replica2, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		if tablet.Alias == primary.Alias {
			continue
		}
		if tablet.Type == "rdonly" {
			rdonly = tablet
		} else {
			if replica1 == nil {
				replica1 = tablet
			} else {
				replica2 = tablet
			}
		}
	}

	assert.NotNil(t, replica1, "could not find any replica tablet")
	assert.NotNil(t, replica2, "could not find the second replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// check that the replication is setup correctly
	utils.CheckReplication(t, newCluster, primary, []*cluster.Vttablet{rdonly, replica1, replica2}, 10*time.Second)

	semisyncType, err := utils.SemiSyncExtensionLoaded(t, replica1)
	require.NoError(t, err)
	switch semisyncType {
	case mysql.SemiSyncTypeSource:
		_, err := utils.RunSQL(t, "SET GLOBAL rpl_semi_sync_replica_enabled = 0", replica1, "")
		require.NoError(t, err)
	case mysql.SemiSyncTypeMaster:
		_, err := utils.RunSQL(t, "SET GLOBAL rpl_semi_sync_slave_enabled = 0", replica1, "")
		require.NoError(t, err)
	default:
		require.Fail(t, "unexpected semi-sync type %v", semisyncType)
	}

	semisyncType, err = utils.SemiSyncExtensionLoaded(t, rdonly)
	require.NoError(t, err)
	switch semisyncType {
	case mysql.SemiSyncTypeSource:
		_, err := utils.RunSQL(t, "SET GLOBAL rpl_semi_sync_replica_enabled = 0", rdonly, "")
		require.NoError(t, err)
	case mysql.SemiSyncTypeMaster:
		_, err := utils.RunSQL(t, "SET GLOBAL rpl_semi_sync_slave_enabled = 0", rdonly, "")
		require.NoError(t, err)
	default:
		require.Fail(t, "unexpected semi-sync type %v", semisyncType)
	}

	semisyncType, err = utils.SemiSyncExtensionLoaded(t, primary)
	require.NoError(t, err)
	switch semisyncType {
	case mysql.SemiSyncTypeSource:
		_, err := utils.RunSQL(t, "SET GLOBAL rpl_semi_sync_source_enabled = 0", primary, "")
		require.NoError(t, err)
	case mysql.SemiSyncTypeMaster:
		_, err := utils.RunSQL(t, "SET GLOBAL rpl_semi_sync_master_enabled = 0", primary, "")
		require.NoError(t, err)
	default:
		require.Fail(t, "unexpected semi-sync type %v", semisyncType)
	}

	timeout := time.After(20 * time.Second)
	for {
		select {
		case <-timeout:
			require.Fail(t, "timed out waiting for semi sync settings to be fixed")
			return
		default:
			if utils.IsSemiSyncSetupCorrectly(t, replica1, "ON") &&
				utils.IsSemiSyncSetupCorrectly(t, rdonly, "OFF") &&
				utils.IsPrimarySemiSyncSetupCorrectly(t, primary, "ON") {
				return
			}
			log.Warningf("semi sync settings not fixed yet")
			time.Sleep(1 * time.Second)
		}
	}
}

// TestVTOrcWithPrs tests that VTOrc works fine even when PRS is called from vtctld
func TestVTOrcWithPrs(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 4, 0, nil, cluster.VTOrcConfiguration{
		PreventCrossCellFailover: true,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	vtOrcProcess := clusterInfo.ClusterInstance.VTOrcProcesses[0]
	utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	utils.WaitForSuccessfulPRSCount(t, vtOrcProcess, keyspace.Name, shard0.Name, 1)

	// find any replica tablet other than the current primary
	var replica *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		if tablet.Alias != curPrimary.Alias {
			replica = tablet
			break
		}
	}
	assert.NotNil(t, replica, "could not find any replica tablet")

	// check that the replication is setup correctly before we failover
	utils.CheckReplication(t, clusterInfo, curPrimary, shard0.Vttablets, 10*time.Second)

	output, err := clusterInfo.ClusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(
		"PlannedReparentShard",
		fmt.Sprintf("%s/%s", keyspace.Name, shard0.Name),
		"--wait-replicas-timeout", "31s",
		"--new-primary", replica.Alias)
	require.NoError(t, err, "error in PlannedReparentShard output - %s", output)

	// check that the replica gets promoted
	utils.CheckPrimaryTablet(t, clusterInfo, replica, true)
	// Verify that VTOrc didn't run any other recovery
	utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	utils.WaitForSuccessfulPRSCount(t, vtOrcProcess, keyspace.Name, shard0.Name, 1)
	utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.RecoverDeadPrimaryRecoveryName, keyspace.Name, shard0.Name, 0)
	utils.WaitForSuccessfulERSCount(t, vtOrcProcess, keyspace.Name, shard0.Name, 0)
	utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixPrimaryRecoveryName, keyspace.Name, shard0.Name, 0)
	utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixReplicaRecoveryName, keyspace.Name, shard0.Name, 0)
	utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.RecoverPrimaryHasPrimaryRecoveryName, keyspace.Name, shard0.Name, 0)
	utils.VerifyWritesSucceed(t, clusterInfo, replica, shard0.Vttablets, 10*time.Second)
}

// TestMultipleDurabilities tests that VTOrc works with 2 keyspaces having 2 different durability policies
func TestMultipleDurabilities(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	// Setup a normal cluster and start vtorc
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 1, 1, nil, cluster.VTOrcConfiguration{}, 1, "")
	// Setup a semi-sync cluster
	utils.AddSemiSyncKeyspace(t, clusterInfo)

	keyspaceNone := &clusterInfo.ClusterInstance.Keyspaces[0]
	shardNone := &keyspaceNone.Shards[0]
	utils.CheckPrimaryTablet(t, clusterInfo, shardNone.Vttablets[0], true)
	utils.CheckReplication(t, clusterInfo, shardNone.Vttablets[0], shardNone.Vttablets[1:], 10*time.Second)

	keyspaceSemiSync := &clusterInfo.ClusterInstance.Keyspaces[1]
	shardSemiSync := &keyspaceSemiSync.Shards[0]
	// find primary from topo
	primary := utils.ShardPrimaryTablet(t, clusterInfo, keyspaceSemiSync, shardSemiSync)
	assert.NotNil(t, primary, "should have elected a primary")
}

// TestDrainedTablet tests that we don't forget drained tablets and they still show up in the vtorc output.
func TestDrainedTablet(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)

	// Setup a normal cluster and start vtorc
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 0, nil, cluster.VTOrcConfiguration{}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	vtOrcProcess := clusterInfo.ClusterInstance.VTOrcProcesses[0]

	// find any replica tablet other than the current primary
	var replica *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		if tablet.Alias != curPrimary.Alias {
			replica = tablet
			break
		}
	}
	require.NotNil(t, replica, "could not find any replica tablet")

	output, err := clusterInfo.ClusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(
		"ChangeTabletType", replica.Alias, "DRAINED")
	require.NoError(t, err, "error in changing tablet type output - %s", output)

	// Make sure VTOrc sees the drained tablets and doesn't forget them.
	utils.WaitForDrainedTabletInVTOrc(t, vtOrcProcess, 1)

	output, err = clusterInfo.ClusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(
		"ChangeTabletType", replica.Alias, "REPLICA")
	require.NoError(t, err, "error in changing tablet type output - %s", output)

	// We have no drained tablets anymore. Wait for VTOrc to have processed that.
	utils.WaitForDrainedTabletInVTOrc(t, vtOrcProcess, 0)
}

// TestDurabilityPolicySetLater tests that VTOrc works even if the durability policy of the keyspace is
// set after VTOrc has been started.
func TestDurabilityPolicySetLater(t *testing.T) {
	// stop any vtorc instance running due to a previous test.
	utils.StopVTOrcs(t, clusterInfo)
	newCluster := utils.SetupNewClusterSemiSync(t)
	defer utils.PrintVTOrcLogsOnFailure(t, newCluster.ClusterInstance)
	keyspace := &newCluster.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// Before starting VTOrc we explicity want to set the durability policy of the keyspace to an empty string
	func() {
		ctx, unlock, lockErr := newCluster.Ts.LockKeyspace(context.Background(), keyspace.Name, "TestDurabilityPolicySetLater")
		require.NoError(t, lockErr)
		defer unlock(&lockErr)
		ki, err := newCluster.Ts.GetKeyspace(ctx, keyspace.Name)
		require.NoError(t, err)
		ki.DurabilityPolicy = ""
		err = newCluster.Ts.UpdateKeyspace(ctx, ki)
		require.NoError(t, err)
	}()

	// Verify that the durability policy is indeed empty
	ki, err := newCluster.Ts.GetKeyspace(context.Background(), keyspace.Name)
	require.NoError(t, err)
	require.Empty(t, ki.DurabilityPolicy)

	// Now start the vtorc instances
	utils.StartVTOrcs(t, newCluster, nil, cluster.VTOrcConfiguration{
		PreventCrossCellFailover: true,
	}, 1)
	defer func() {
		utils.StopVTOrcs(t, newCluster)
		newCluster.ClusterInstance.Teardown()
	}()

	// Wait for some time to be sure that VTOrc has started.
	// TODO(GuptaManan100): Once we have a debug page for VTOrc, use that instead
	time.Sleep(30 * time.Second)

	// Now set the correct durability policy
	out, err := newCluster.ClusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", keyspace.Name, "--durability-policy=semi_sync")
	require.NoError(t, err, out)

	// VTOrc should promote a new primary after seeing the durability policy change
	primary := utils.ShardPrimaryTablet(t, newCluster, keyspace, shard0)
	assert.NotNil(t, primary, "should have elected a primary")
	utils.CheckReplication(t, newCluster, primary, shard0.Vttablets, 10*time.Second)
}

func TestFullStatusConnectionPooling(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 4, 0, []string{
		vtutils.GetFlagVariantForTests("--tablet-manager-grpc-concurrency") + "=1",
	}, cluster.VTOrcConfiguration{
		PreventCrossCellFailover: true,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	vtorc := clusterInfo.ClusterInstance.VTOrcProcesses[0]

	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	vtOrcProcess := clusterInfo.ClusterInstance.VTOrcProcesses[0]
	utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	utils.WaitForSuccessfulPRSCount(t, vtOrcProcess, keyspace.Name, shard0.Name, 1)

	// Kill the current primary.
	_ = curPrimary.VttabletProcess.Kill()

	// Wait until VTOrc notices some problems
	status, resp := utils.MakeAPICallRetry(t, vtorc, "/api/replication-analysis", func(_ int, response string) bool {
		return response == "null"
	})
	assert.Equal(t, 200, status)
	assert.Contains(t, resp, "UnreachablePrimary")

	time.Sleep(1 * time.Minute)

	// Change the primaries ports and restart it.
	curPrimary.VttabletProcess.Port = clusterInfo.ClusterInstance.GetAndReservePort()
	curPrimary.VttabletProcess.GrpcPort = clusterInfo.ClusterInstance.GetAndReservePort()
	err := curPrimary.VttabletProcess.Setup()
	require.NoError(t, err)

	// See that VTOrc eventually reports no errors.
	// Wait until there are no problems and the api endpoint returns null
	status, resp = utils.MakeAPICallRetry(t, vtorc, "/api/replication-analysis", func(_ int, response string) bool {
		return response != "null"
	})
	assert.Equal(t, 200, status)
	assert.Equal(t, "null", resp)

	// REPEATED
	// Kill the current primary.
	_ = curPrimary.VttabletProcess.Kill()

	// Wait until VTOrc notices some problems
	status, resp = utils.MakeAPICallRetry(t, vtorc, "/api/replication-analysis", func(_ int, response string) bool {
		return response == "null"
	})
	assert.Equal(t, 200, status)
	assert.Contains(t, resp, "UnreachablePrimary")

	time.Sleep(1 * time.Minute)

	// Change the primaries ports back to original and restart it.
	curPrimary.VttabletProcess.Port = curPrimary.HTTPPort
	curPrimary.VttabletProcess.GrpcPort = curPrimary.GrpcPort
	err = curPrimary.VttabletProcess.Setup()
	require.NoError(t, err)

	// See that VTOrc eventually reports no errors.
	// Wait until there are no problems and the api endpoint returns null
	status, resp = utils.MakeAPICallRetry(t, vtorc, "/api/replication-analysis", func(_ int, response string) bool {
		return response != "null"
	})
	assert.Equal(t, 200, status)
	assert.Equal(t, "null", resp)
}
