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
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/test/endtoend/vtorc/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

// Cases to test:
// 1. create cluster with 2 replicas and 1 rdonly, let orc choose primary
// verify rdonly is not elected, only replica
// verify replication is setup
// verify that with multiple vtorc instances, we still only have 1 PlannedReparentShard call
func TestPrimaryElection(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVtorc(t, clusterInfo, 2, 1, nil, cluster.VtorcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 2)
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

// Cases to test:
// 1. create cluster with 1 replica and 1 rdonly, let orc choose primary
// verify rdonly is not elected, only replica
// verify replication is setup
func TestSingleKeyspace(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVtorc(t, clusterInfo, 1, 1, []string{"--clusters_to_watch", "ks"}, cluster.VtorcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1)
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	utils.CheckPrimaryTablet(t, clusterInfo, shard0.Vttablets[0], true)
	utils.CheckReplication(t, clusterInfo, shard0.Vttablets[0], shard0.Vttablets[1:], 10*time.Second)
}

// Cases to test:
// 1. create cluster with 1 replica and 1 rdonly, let orc choose primary
// verify rdonly is not elected, only replica
// verify replication is setup
func TestKeyspaceShard(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVtorc(t, clusterInfo, 1, 1, []string{"--clusters_to_watch", "ks/0"}, cluster.VtorcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1)
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	utils.CheckPrimaryTablet(t, clusterInfo, shard0.Vttablets[0], true)
	utils.CheckReplication(t, clusterInfo, shard0.Vttablets[0], shard0.Vttablets[1:], 10*time.Second)
}

func waitForReadOnlyValue(t *testing.T, curPrimary *cluster.Vttablet, expectValue int64) (match bool) {
	timeout := 15 * time.Second
	startTime := time.Now()
	for time.Since(startTime) < timeout {
		qr, err := utils.RunSQL(t, "select @@global.read_only as read_only", curPrimary, "")
		require.NoError(t, err)
		require.NotNil(t, qr)
		row := qr.Named().Row()
		require.NotNil(t, row)
		readOnly, err := row.ToInt64("read_only")
		require.NoError(t, err)
		if readOnly == expectValue {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

// 3. make primary readonly, let orc repair
func TestPrimaryReadOnly(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVtorc(t, clusterInfo, 2, 0, nil, cluster.VtorcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1)
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// Make the current primary database read-only.
	_, err := utils.RunSQL(t, "set global read_only=ON", curPrimary, "")
	require.NoError(t, err)

	// wait for repair
	match := waitForReadOnlyValue(t, curPrimary, 0)
	require.True(t, match)
}

// 4. make replica ReadWrite, let orc repair
func TestReplicaReadWrite(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVtorc(t, clusterInfo, 2, 0, nil, cluster.VtorcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1)
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	var replica *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias != curPrimary.Alias {
			replica = tablet
			break
		}
	}
	// Make the replica database read-write.
	_, err := utils.RunSQL(t, "set global read_only=OFF", replica, "")
	require.NoError(t, err)

	// wait for repair
	match := waitForReadOnlyValue(t, replica, 1)
	require.True(t, match)
}

// 5. stop replication, let orc repair
func TestStopReplication(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVtorc(t, clusterInfo, 2, 0, nil, cluster.VtorcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1)
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// TODO(deepthi): we should not need to do this, the DB should be created automatically
	_, err := curPrimary.VttabletProcess.QueryTablet(fmt.Sprintf("create database IF NOT EXISTS vt_%s", keyspace.Name), keyspace.Name, false)
	require.NoError(t, err)

	var replica *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias != curPrimary.Alias {
			replica = tablet
			break
		}
	}
	require.NotNil(t, replica, "should be able to find a replica")
	// use vtctlclient to stop replication
	_, err = clusterInfo.ClusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("StopReplication", replica.Alias)
	require.NoError(t, err)

	// check replication is setup correctly
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica}, 15*time.Second)
}

// 6. setup replication from non-primary, let orc repair
func TestReplicationFromOtherReplica(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVtorc(t, clusterInfo, 3, 0, nil, cluster.VtorcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1)
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// TODO(deepthi): we should not need to do this, the DB should be created automatically
	_, err := curPrimary.VttabletProcess.QueryTablet(fmt.Sprintf("create database IF NOT EXISTS vt_%s", keyspace.Name), keyspace.Name, false)
	require.NoError(t, err)

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

	// point replica at otherReplica
	changeReplicationSourceCommand := fmt.Sprintf("STOP SLAVE; RESET SLAVE ALL;"+
		"CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='vt_repl', MASTER_AUTO_POSITION = 1; START SLAVE", utils.Hostname, otherReplica.MySQLPort)
	_, err = utils.RunSQL(t, changeReplicationSourceCommand, replica, "")
	require.NoError(t, err)

	// wait until the source port is set back correctly by vtorc
	utils.CheckSourcePort(t, replica, curPrimary, 15*time.Second)

	// check that writes succeed
	utils.VerifyWritesSucceed(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica, otherReplica}, 15*time.Second)
}

func TestRepairAfterTER(t *testing.T) {
	// test fails intermittently on CI, skip until it can be fixed.
	t.SkipNow()
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVtorc(t, clusterInfo, 2, 0, nil, cluster.VtorcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1)
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// TODO(deepthi): we should not need to do this, the DB should be created automatically
	_, err := curPrimary.VttabletProcess.QueryTablet(fmt.Sprintf("create database IF NOT EXISTS vt_%s", keyspace.Name), keyspace.Name, false)
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
	_, err = clusterInfo.ClusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("TabletExternallyReparented", newPrimary.Alias)
	require.NoError(t, err)

	utils.CheckReplication(t, clusterInfo, newPrimary, []*cluster.Vttablet{curPrimary}, 15*time.Second)
}

// 7. make instance A replicates from B and B from A, wait for repair
func TestCircularReplication(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVtorc(t, clusterInfo, 2, 0, nil, cluster.VtorcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1)
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	primary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, primary, "should have elected a primary")

	var replica *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias != primary.Alias {
			replica = tablet
			break
		}
	}

	// check replication is setup correctly
	utils.CheckReplication(t, clusterInfo, primary, []*cluster.Vttablet{replica}, 15*time.Second)

	// change the replication source on the primary
	changeReplicationSourceCommands := fmt.Sprintf("STOP SLAVE; RESET SLAVE ALL;"+
		"CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='vt_repl', MASTER_AUTO_POSITION = 1;"+
		"START SLAVE;", replica.VttabletProcess.TabletHostname, replica.MySQLPort)
	_, err := utils.RunSQL(t, changeReplicationSourceCommands, primary, "")
	require.NoError(t, err)

	// wait for primary to reach stable state
	time.Sleep(1 * time.Second)

	// wait for repair
	err = utils.WaitForReplicationToStop(t, primary)
	require.NoError(t, err)
	// check that the writes still succeed
	utils.VerifyWritesSucceed(t, clusterInfo, primary, []*cluster.Vttablet{replica}, 10*time.Second)
}

// TestSemiSync tests that semi-sync is setup correctly by vtorc if it is incorrectly set
func TestSemiSync(t *testing.T) {
	// stop any vtorc instance running due to a previous test.
	utils.StopVtorcs(t, clusterInfo)
	newCluster := utils.SetupNewClusterSemiSync(t)
	utils.StartVtorcs(t, newCluster, nil, cluster.VtorcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
		Durability:                            "semi_sync",
	}, 1)
	defer func() {
		utils.StopVtorcs(t, newCluster)
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

	_, err := utils.RunSQL(t, "SET GLOBAL rpl_semi_sync_slave_enabled = 0", replica1, "")
	require.NoError(t, err)
	_, err = utils.RunSQL(t, "SET GLOBAL rpl_semi_sync_slave_enabled = 1", rdonly, "")
	require.NoError(t, err)
	_, err = utils.RunSQL(t, "SET GLOBAL rpl_semi_sync_master_enabled = 0", primary, "")
	require.NoError(t, err)

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

// TestVtorcWithPrs tests that VTOrc works fine even when PRS is called from vtctld
func TestVtorcWithPrs(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVtorc(t, clusterInfo, 4, 0, nil, cluster.VtorcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1)
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

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

	output, err := clusterInfo.ClusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"PlannedReparentShard",
		"-keyspace_shard", fmt.Sprintf("%s/%s", keyspace.Name, shard0.Name),
		"-wait_replicas_timeout", "31s",
		"-new_primary", replica.Alias)
	require.NoError(t, err, "error in PlannedReparentShard output - %s", output)

	time.Sleep(40 * time.Second)

	// check that the replica gets promoted
	utils.CheckPrimaryTablet(t, clusterInfo, replica, true)
	utils.VerifyWritesSucceed(t, clusterInfo, replica, shard0.Vttablets, 10*time.Second)
}
