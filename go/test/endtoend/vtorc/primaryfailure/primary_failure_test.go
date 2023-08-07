/*
Copyright 2021 The Vitess Authors.

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

package primaryfailure

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
	"vitess.io/vitess/go/vt/vtorc/logic"
)

// bring down primary, let orc promote replica
// covers the test case master-failover from orchestrator
// Also tests that VTOrc can handle multiple failures, if the durability policies allow it
func TestDownPrimary(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	defer cluster.PanicHandler(t)
	// We specify the --wait-replicas-timeout to a small value because we spawn a cross-cell replica later in the test.
	// If that replica is more advanced than the same-cell-replica, then we try to promote the cross-cell replica as an intermediate source.
	// If we don't specify a small value of --wait-replicas-timeout, then we would end up waiting for 30 seconds for the dead-primary to respond, failing this test.
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, []string{"--remote_operation_timeout=10s", "--wait-replicas-timeout=5s"}, cluster.VTOrcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1, "semi_sync")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	vtOrcProcess := clusterInfo.ClusterInstance.VTOrcProcesses[0]
	utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, 1)

	// find the replica and rdonly tablets
	var replica, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias != curPrimary.Alias && tablet.Type == "replica" {
			replica = tablet
		}
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// Start a cross-cell replica
	crossCellReplica := utils.StartVttablet(t, clusterInfo, utils.Cell2, false)

	// check that the replication is setup correctly before we failover
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{rdonly, replica, crossCellReplica}, 10*time.Second)
	// since all tablets are up and running, InstancePollSecondsExceeded should have `0` zero value
	utils.WaitForInstancePollSecondsExceededCount(t, vtOrcProcess, "InstancePollSecondsExceeded", 0, true)
	// Make the rdonly vttablet unavailable
	err := rdonly.VttabletProcess.TearDown()
	require.NoError(t, err)
	err = rdonly.MysqlctlProcess.Stop()
	require.NoError(t, err)
	// We have bunch of Vttablets down. Therefore we expect at least 1 occurrence of InstancePollSecondsExceeded
	utils.WaitForInstancePollSecondsExceededCount(t, vtOrcProcess, "InstancePollSecondsExceeded", 1, false)
	// Make the current primary vttablet unavailable.
	err = curPrimary.VttabletProcess.TearDown()
	require.NoError(t, err)
	err = curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list
		utils.PermanentlyRemoveVttablet(clusterInfo, curPrimary)
		utils.PermanentlyRemoveVttablet(clusterInfo, rdonly)
	}()

	// check that the replica gets promoted
	utils.CheckPrimaryTablet(t, clusterInfo, replica, true)

	// also check that the replication is working correctly after failover
	utils.VerifyWritesSucceed(t, clusterInfo, replica, []*cluster.Vttablet{crossCellReplica}, 10*time.Second)
	utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.RecoverDeadPrimaryRecoveryName, 1)
}

// TestDeadPrimaryRecoversImmediately test Vtorc ability to recover immediately if primary is dead.
// Reason is, unlike other recoveries, in DeadPrimary we don't call DiscoverInstance since we know
// that primary is unreachable. This help us save few seconds depending on value of `RemoteOperationTimeout` flag.
func TestDeadPrimaryRecoversImmediately(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	defer cluster.PanicHandler(t)
	// We specify the --wait-replicas-timeout to a small value because we spawn a cross-cell replica later in the test.
	// If that replica is more advanced than the same-cell-replica, then we try to promote the cross-cell replica as an intermediate source.
	// If we don't specify a small value of --wait-replicas-timeout, then we would end up waiting for 30 seconds for the dead-primary to respond, failing this test.
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, []string{"--remote_operation_timeout=10s", "--wait-replicas-timeout=5s"}, cluster.VTOrcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1, "semi_sync")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	vtOrcProcess := clusterInfo.ClusterInstance.VTOrcProcesses[0]
	utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, 1)

	// find the replica and rdonly tablets
	var replica, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias != curPrimary.Alias && tablet.Type == "replica" {
			replica = tablet
		}
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// Start a cross-cell replica
	crossCellReplica := utils.StartVttablet(t, clusterInfo, utils.Cell2, false)

	// check that the replication is setup correctly before we failover
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{rdonly, replica, crossCellReplica}, 10*time.Second)

	// Make the current primary vttablet unavailable.
	curPrimary.VttabletProcess.Kill()
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list
		utils.PermanentlyRemoveVttablet(clusterInfo, curPrimary)
	}()

	// check that the replica gets promoted
	utils.CheckPrimaryTablet(t, clusterInfo, replica, true)
	utils.WaitForInstancePollSecondsExceededCount(t, vtOrcProcess, "InstancePollSecondsExceeded", 2, false)
	// also check that the replication is working correctly after failover
	utils.VerifyWritesSucceed(t, clusterInfo, replica, []*cluster.Vttablet{crossCellReplica}, 10*time.Second)
	utils.WaitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.RecoverDeadPrimaryRecoveryName, 1)

	// Parse log file and find out how much time it took for DeadPrimary to recover.
	logFile := path.Join(vtOrcProcess.LogDir, vtOrcProcess.LogFileName)
	// log prefix printed at the end of analysis where we conclude we have DeadPrimary
	t1 := extractTimeFromLog(t, logFile, "Proceeding with DeadPrimary recovery validation after acquiring shard lock")
	// log prefix printed at the end of recovery
	t2 := extractTimeFromLog(t, logFile, "auditType:recover-dead-primary")
	curr := time.Now().Format("2006-01-02")
	timeLayout := "2006-01-02 15:04:05.000000"
	timeStr1 := fmt.Sprintf("%s %s", curr, t1)
	timeStr2 := fmt.Sprintf("%s %s", curr, t2)
	time1, err := time.Parse(timeLayout, timeStr1)
	if err != nil {
		t.Errorf("unable to parse time %s", err.Error())
	}
	time2, err := time.Parse(timeLayout, timeStr2)
	if err != nil {
		t.Errorf("unable to parse time %s", err.Error())
	}
	diff := time2.Sub(time1)
	fmt.Printf("The difference between %s and %s is %v seconds.\n", t1, t2, diff.Seconds())
	// assert that it takes less than `remote_operation_timeout` to recover from `DeadPrimary`
	// use the value provided in `remote_operation_timeout` flag to compare with.
	// We are testing against 9.5 seconds to be safe and prevent flakiness.
	assert.Less(t, diff.Seconds(), 9.5)
}

// Failover should not be cross data centers, according to the configuration file
// covers part of the test case master-failover-lost-replicas from orchestrator
func TestCrossDataCenterFailure(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, nil, cluster.VTOrcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica and rdonly tablets
	var replicaInSameCell, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias != curPrimary.Alias && tablet.Type == "replica" {
			replicaInSameCell = tablet
		}
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replicaInSameCell, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica := utils.StartVttablet(t, clusterInfo, utils.Cell2, false)
	// newly started tablet does not replicate from anyone yet, we will allow vtorc to fix this too
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{crossCellReplica, replicaInSameCell, rdonly}, 25*time.Second)

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		utils.PermanentlyRemoveVttablet(clusterInfo, curPrimary)
	}()

	// we have a replica in the same cell, so that is the one which should be promoted and not the one from another cell
	utils.CheckPrimaryTablet(t, clusterInfo, replicaInSameCell, true)
	// also check that the replication is working correctly after failover
	utils.VerifyWritesSucceed(t, clusterInfo, replicaInSameCell, []*cluster.Vttablet{crossCellReplica, rdonly}, 10*time.Second)
}

// Failover should not be cross data centers, according to the configuration file
// In case of no viable candidates, we should error out
func TestCrossDataCenterFailureError(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 1, 1, nil, cluster.VTOrcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the rdonly tablet
	var rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica1 := utils.StartVttablet(t, clusterInfo, utils.Cell2, false)
	crossCellReplica2 := utils.StartVttablet(t, clusterInfo, utils.Cell2, false)
	// newly started tablet does not replicate from anyone yet, we will allow vtorc to fix this too
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{crossCellReplica1, crossCellReplica2, rdonly}, 25*time.Second)

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		utils.PermanentlyRemoveVttablet(clusterInfo, curPrimary)
	}()

	// wait for 20 seconds
	time.Sleep(20 * time.Second)

	// the previous primary should still be the primary since recovery of dead primary should fail
	utils.CheckPrimaryTablet(t, clusterInfo, curPrimary, false)
}

// Failover will sometimes lead to a rdonly which can no longer replicate.
// covers part of the test case master-failover-lost-replicas from orchestrator
func TestLostRdonlyOnPrimaryFailure(t *testing.T) {
	// new version of ERS does not check for lost replicas yet
	// Earlier any replicas that were not able to replicate from the previous primary
	// were detected by vtorc and could be configured to have their sources detached
	t.Skip()
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 2, nil, cluster.VTOrcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// get the tablets
	var replica, rdonly, aheadRdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// find tablets which are not the primary
		if tablet.Alias != curPrimary.Alias {
			if tablet.Type == "replica" {
				replica = tablet
			} else {
				if rdonly == nil {
					rdonly = tablet
				} else {
					aheadRdonly = tablet
				}
			}
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find any rdonly tablet")
	assert.NotNil(t, aheadRdonly, "could not find both rdonly tablet")

	// check that replication is setup correctly
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{rdonly, aheadRdonly, replica}, 15*time.Second)

	// revoke super privileges from vtorc on replica and rdonly so that it is unable to repair the replication
	utils.ChangePrivileges(t, `REVOKE SUPER ON *.* FROM 'orc_client_user'@'%'`, replica, "orc_client_user")
	utils.ChangePrivileges(t, `REVOKE SUPER ON *.* FROM 'orc_client_user'@'%'`, rdonly, "orc_client_user")

	// stop replication on the replica and rdonly.
	err := clusterInfo.ClusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", replica.Alias)
	require.NoError(t, err)
	err = clusterInfo.ClusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", rdonly.Alias)
	require.NoError(t, err)

	// check that aheadRdonly is able to replicate. We also want to add some queries to aheadRdonly which will not be there in replica and rdonly
	utils.VerifyWritesSucceed(t, clusterInfo, curPrimary, []*cluster.Vttablet{aheadRdonly}, 15*time.Second)

	// assert that the replica and rdonly are indeed lagging and do not have the new insertion by checking the count of rows in the tables
	out, err := utils.RunSQL(t, "SELECT * FROM vt_insert_test", replica, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 1, len(out.Rows))
	out, err = utils.RunSQL(t, "SELECT * FROM vt_insert_test", rdonly, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 1, len(out.Rows))

	// Make the current primary database unavailable.
	err = curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		utils.PermanentlyRemoveVttablet(clusterInfo, curPrimary)
	}()

	// grant super privileges back to vtorc on replica and rdonly so that it can repair
	utils.ChangePrivileges(t, `GRANT SUPER ON *.* TO 'orc_client_user'@'%'`, replica, "orc_client_user")
	utils.ChangePrivileges(t, `GRANT SUPER ON *.* TO 'orc_client_user'@'%'`, rdonly, "orc_client_user")

	// vtorc must promote the lagging replica and not the rdonly, since it has a MustNotPromoteRule promotion rule
	utils.CheckPrimaryTablet(t, clusterInfo, replica, true)

	// also check that the replication is setup correctly
	utils.VerifyWritesSucceed(t, clusterInfo, replica, []*cluster.Vttablet{rdonly}, 15*time.Second)

	// check that the rdonly is lost. The lost replica has is detached and its host is prepended with `//`
	out, err = utils.RunSQL(t, "SELECT HOST FROM performance_schema.replication_connection_configuration", aheadRdonly, "")
	require.NoError(t, err)
	require.Equal(t, "//localhost", out.Rows[0][0].ToString())
}

// This test checks that the promotion of a tablet succeeds if it passes the promotion lag test
// covers the test case master-failover-fail-promotion-lag-minutes-success from orchestrator
func TestPromotionLagSuccess(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, nil, cluster.VTOrcConfiguration{
		ReplicationLagQuery:              "select 59",
		FailPrimaryPromotionOnLagMinutes: 1,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica and rdonly tablets
	var replica, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias != curPrimary.Alias && tablet.Type == "replica" {
			replica = tablet
		}
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// check that the replication is setup correctly before we failover
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{rdonly, replica}, 10*time.Second)

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		utils.PermanentlyRemoveVttablet(clusterInfo, curPrimary)
	}()

	// check that the replica gets promoted
	utils.CheckPrimaryTablet(t, clusterInfo, replica, true)
	// also check that the replication is working correctly after failover
	utils.VerifyWritesSucceed(t, clusterInfo, replica, []*cluster.Vttablet{rdonly}, 10*time.Second)
}

// This test checks that the promotion of a tablet succeeds if it passes the promotion lag test
// covers the test case master-failover-fail-promotion-lag-minutes-failure from orchestrator
func TestPromotionLagFailure(t *testing.T) {
	// new version of ERS does not check for promotion lag yet
	// Earlier vtorc used to check that the promotion lag between the new primary and the old one
	// was smaller than the configured value, otherwise it would fail the promotion
	t.Skip()
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 3, 1, nil, cluster.VTOrcConfiguration{
		ReplicationLagQuery:              "select 61",
		FailPrimaryPromotionOnLagMinutes: 1,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica and rdonly tablets
	var replica1, replica2, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias != curPrimary.Alias && tablet.Type == "replica" {
			if replica1 == nil {
				replica1 = tablet
			} else {
				replica2 = tablet
			}
		}
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica1, "could not find replica tablet")
	assert.NotNil(t, replica2, "could not find second replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// check that the replication is setup correctly before we failover
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{rdonly, replica1, replica2}, 10*time.Second)

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		utils.PermanentlyRemoveVttablet(clusterInfo, curPrimary)
	}()

	// wait for 20 seconds
	time.Sleep(20 * time.Second)

	// the previous primary should still be the primary since recovery of dead primary should fail
	utils.CheckPrimaryTablet(t, clusterInfo, curPrimary, false)
}

// covers the test case master-failover-candidate from orchestrator
// We explicitly set one of the replicas to Prefer promotion rule.
// That is the replica which should be promoted in case of primary failure
func TestDownPrimaryPromotionRule(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, nil, cluster.VTOrcConfiguration{
		LockShardTimeoutSeconds: 5,
	}, 1, "test")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica and rdonly tablets
	var replica, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias != curPrimary.Alias && tablet.Type == "replica" {
			replica = tablet
		}
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica := utils.StartVttablet(t, clusterInfo, utils.Cell2, false)
	// newly started tablet does not replicate from anyone yet, we will allow vtorc to fix this too
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{crossCellReplica, rdonly, replica}, 25*time.Second)

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		utils.PermanentlyRemoveVttablet(clusterInfo, curPrimary)
	}()

	// we have a replica with a preferred promotion rule, so that is the one which should be promoted
	utils.CheckPrimaryTablet(t, clusterInfo, crossCellReplica, true)
	// also check that the replication is working correctly after failover
	utils.VerifyWritesSucceed(t, clusterInfo, crossCellReplica, []*cluster.Vttablet{rdonly, replica}, 10*time.Second)
}

// covers the test case master-failover-candidate-lag from orchestrator
// We explicitly set one of the replicas to Prefer promotion rule and make it lag with respect to other replicas.
// That is the replica which should be promoted in case of primary failure
// It should also be caught up when it is promoted
func TestDownPrimaryPromotionRuleWithLag(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, nil, cluster.VTOrcConfiguration{
		LockShardTimeoutSeconds: 5,
	}, 1, "test")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// get the replicas in the same cell
	var replica, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// find tablets which are not the primary
		if tablet.Alias != curPrimary.Alias {
			if tablet.Type == "replica" {
				replica = tablet
			} else {
				rdonly = tablet
			}
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica := utils.StartVttablet(t, clusterInfo, utils.Cell2, false)
	// newly started tablet does not replicate from anyone yet, we will allow vtorc to fix this too
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{crossCellReplica, replica, rdonly}, 25*time.Second)

	// revoke super privileges from vtorc on crossCellReplica so that it is unable to repair the replication
	utils.ChangePrivileges(t, `REVOKE SUPER ON *.* FROM 'orc_client_user'@'%'`, crossCellReplica, "orc_client_user")

	// stop replication on the crossCellReplica.
	err := clusterInfo.ClusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", crossCellReplica.Alias)
	require.NoError(t, err)

	// check that rdonly and replica are able to replicate. We also want to add some queries to replica which will not be there in crossCellReplica
	utils.VerifyWritesSucceed(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica, rdonly}, 15*time.Second)

	// reset the primary logs so that crossCellReplica can never catch up
	utils.ResetPrimaryLogs(t, curPrimary)

	// start replication back on the crossCellReplica.
	err = clusterInfo.ClusterInstance.VtctlclientProcess.ExecuteCommand("StartReplication", crossCellReplica.Alias)
	require.NoError(t, err)

	// grant super privileges back to vtorc on crossCellReplica so that it can repair
	utils.ChangePrivileges(t, `GRANT SUPER ON *.* TO 'orc_client_user'@'%'`, crossCellReplica, "orc_client_user")

	// assert that the crossCellReplica is indeed lagging and does not have the new insertion by checking the count of rows in the table
	out, err := utils.RunSQL(t, "SELECT * FROM vt_insert_test", crossCellReplica, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 1, len(out.Rows))

	// Make the current primary database unavailable.
	err = curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		utils.PermanentlyRemoveVttablet(clusterInfo, curPrimary)
	}()

	// the crossCellReplica is set to be preferred according to the durability requirements. So it must be promoted
	utils.CheckPrimaryTablet(t, clusterInfo, crossCellReplica, true)

	// assert that the crossCellReplica has indeed caught up
	out, err = utils.RunSQL(t, "SELECT * FROM vt_insert_test", crossCellReplica, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 2, len(out.Rows))

	// check that rdonly and replica are able to replicate from the crossCellReplica
	utils.VerifyWritesSucceed(t, clusterInfo, crossCellReplica, []*cluster.Vttablet{replica, rdonly}, 15*time.Second)
}

// covers the test case master-failover-candidate-lag-cross-datacenter from orchestrator
// We explicitly set one of the cross-cell replicas to Prefer promotion rule, but we prevent cross data center promotions.
// We let a replica in our own cell lag. That is the replica which should be promoted in case of primary failure
// It should also be caught up when it is promoted
func TestDownPrimaryPromotionRuleWithLagCrossCenter(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, nil, cluster.VTOrcConfiguration{
		LockShardTimeoutSeconds:               5,
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1, "test")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// get the replicas in the same cell
	var replica, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// find tablets which are not the primary
		if tablet.Alias != curPrimary.Alias {
			if tablet.Type == "replica" {
				replica = tablet
			} else {
				rdonly = tablet
			}
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica := utils.StartVttablet(t, clusterInfo, utils.Cell2, false)
	// newly started tablet does not replicate from anyone yet, we will allow vtorc to fix this too
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{crossCellReplica, replica, rdonly}, 25*time.Second)

	// revoke super privileges from vtorc on replica so that it is unable to repair the replication
	utils.ChangePrivileges(t, `REVOKE SUPER ON *.* FROM 'orc_client_user'@'%'`, replica, "orc_client_user")

	// stop replication on the replica.
	err := clusterInfo.ClusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", replica.Alias)
	require.NoError(t, err)

	// check that rdonly and crossCellReplica are able to replicate. We also want to add some queries to crossCenterReplica which will not be there in replica
	utils.VerifyWritesSucceed(t, clusterInfo, curPrimary, []*cluster.Vttablet{rdonly, crossCellReplica}, 15*time.Second)

	// reset the primary logs so that crossCellReplica can never catch up
	utils.ResetPrimaryLogs(t, curPrimary)

	// start replication back on the replica.
	err = clusterInfo.ClusterInstance.VtctlclientProcess.ExecuteCommand("StartReplication", replica.Alias)
	require.NoError(t, err)

	// grant super privileges back to vtorc on replica so that it can repair
	utils.ChangePrivileges(t, `GRANT SUPER ON *.* TO 'orc_client_user'@'%'`, replica, "orc_client_user")

	// assert that the replica is indeed lagging and does not have the new insertion by checking the count of rows in the table
	out, err := utils.RunSQL(t, "SELECT * FROM vt_insert_test", replica, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 1, len(out.Rows))

	// Make the current primary database unavailable.
	err = curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		utils.PermanentlyRemoveVttablet(clusterInfo, curPrimary)
	}()

	// the replica should be promoted since we have prevented cross cell promotions
	utils.CheckPrimaryTablet(t, clusterInfo, replica, true)

	// assert that the replica has indeed caught up
	out, err = utils.RunSQL(t, "SELECT * FROM vt_insert_test", replica, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 2, len(out.Rows))

	// check that rdonly and crossCellReplica are able to replicate from the replica
	utils.VerifyWritesSucceed(t, clusterInfo, replica, []*cluster.Vttablet{crossCellReplica, rdonly}, 15*time.Second)
}

func extractTimeFromLog(t *testing.T, logFile string, logStatement string) string {
	file, err := os.Open(logFile)
	if err != nil {
		t.Errorf("fail to extract time from log statement %s", err.Error())
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, logStatement) {
			// Regular expression pattern for date format
			pattern := `\d{2}:\d{2}:\d{2}\.\d{6}`
			re := regexp.MustCompile(pattern)
			match := re.FindString(line)
			return match
		}
	}

	if err := scanner.Err(); err != nil {
		t.Errorf("fail to extract time from log statement %s", err.Error())
	}
	return ""
}
