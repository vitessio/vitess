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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/logic"
)

// bring down primary, let orc promote replica
// covers the test case master-failover from orchestrator
// Also tests that VTOrc can handle multiple failures, if the durability policies allow it
func TestDownPrimary(t *testing.T) {
	ctx := t.Context()
	// We specify the --wait-replicas-timeout to a small value because we spawn a cross-cell replica later in the test.
	// If that replica is more advanced than the same-cell-replica, then we try to promote the cross-cell replica as an intermediate source.
	// If we don't specify a small value of --wait-replicas-timeout, then we would end up waiting for 30 seconds for the dead-primary to respond, failing this test.
	clusterInstance, vtOrcProcess := setupVttabletsAndVTOrcs(t, 2, 1, policy.DurabilitySemiSync,
		"--remote-operation-timeout=10s", "--wait-replicas-timeout=5s", "--prevent-cross-cell-failover")
	keyspace := clusterInstance.Keyspace(keyspaceName)
	shard0 := keyspace.Shard(shardName)
	// find primary from topo
	curPrimary := shardPrimaryTablet(ctx, t, clusterInstance, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	waitForSuccessfulRecoveryCount(ctx, t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	waitForSuccessfulPRSCount(ctx, t, vtOrcProcess, keyspace.Name, shard0.Name, 1)

	// find the replica and rdonly tablets
	var replica, rdonly *vitesst.Tablet
	for _, tablet := range shard0.Tablets() {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias() != curPrimary.Alias() && tablet.Type() == "replica" {
			replica = tablet
		}
		if tablet.Type() == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// Start a cross-cell replica
	crossCellReplica := startVttablet(ctx, t, clusterInstance, cell2, false)

	// check that the replication is setup correctly before we failover
	checkReplication(ctx, t, clusterInstance, curPrimary, []*vitesst.Tablet{rdonly, replica, crossCellReplica}, 10*time.Second)
	// since all tablets are up and running, InstancePollSecondsExceeded should have `0` zero value
	waitForInstancePollSecondsExceededCount(ctx, t, vtOrcProcess, 0, true)
	// Make the rdonly vttablet unavailable
	require.NoError(t, rdonly.StopVttablet(ctx))
	require.NoError(t, rdonly.StopMySQL(ctx))
	// Make the current primary vttablet unavailable.
	require.NoError(t, curPrimary.StopVttablet(ctx))
	require.NoError(t, curPrimary.StopMySQL(ctx))
	// We have bunch of Vttablets down. Therefore we expect at least 1 occurrence of InstancePollSecondsExceeded
	waitForInstancePollSecondsExceededCount(ctx, t, vtOrcProcess, 1, false)

	// check that the replica gets promoted
	checkPrimaryTablet(ctx, t, clusterInstance, replica, true)

	// also check that the replication is working correctly after failover
	verifyWritesSucceed(ctx, t, replica, []*vitesst.Tablet{crossCellReplica}, 10*time.Second)
	waitForSuccessfulRecoveryCount(ctx, t, vtOrcProcess, logic.RecoverDeadPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	waitForSuccessfulERSCount(ctx, t, vtOrcProcess, keyspace.Name, shard0.Name, 1)
	t.Run("Check ERS and PRS Vars and Metrics", func(t *testing.T) {
		checkVarExists(ctx, t, vtOrcProcess, "EmergencyReparentCounts")
		checkVarExists(ctx, t, vtOrcProcess, "PlannedReparentCounts")
		checkVarExists(ctx, t, vtOrcProcess, "ReparentShardOperationTimings")

		// Metrics registered in prometheus
		checkMetricExists(ctx, t, vtOrcProcess, "vtorc_emergency_reparent_counts")
		checkMetricExists(ctx, t, vtOrcProcess, "vtorc_planned_reparent_counts")
		checkMetricExists(ctx, t, vtOrcProcess, "vtorc_reparent_shard_operation_timings_bucket")
	})
}

// bring down primary, with keyspace-level ERS disabled via SetVtorcEmergencyReparent --disable.
// confirm no ERS occurs.
func TestDownPrimary_KeyspaceEmergencyReparentDisabled(t *testing.T) {
	ctx := t.Context()
	clusterInstance, vtOrcProcess := setupVttabletsAndVTOrcs(t, 2, 1, policy.DurabilityNone,
		"--remote-operation-timeout=10s", "--wait-replicas-timeout=5s")
	keyspace := clusterInstance.Keyspace(keyspaceName)
	shard0 := keyspace.Shard(shardName)
	// find primary from topo
	curPrimary := shardPrimaryTablet(ctx, t, clusterInstance, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	waitForSuccessfulRecoveryCount(ctx, t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	waitForSuccessfulPRSCount(ctx, t, vtOrcProcess, keyspace.Name, shard0.Name, 1)

	// find the replica and rdonly tablets
	var replica, rdonly *vitesst.Tablet
	for _, tablet := range shard0.Tablets() {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias() != curPrimary.Alias() && tablet.Type() == "replica" {
			replica = tablet
		}
		if tablet.Type() == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// check that the replication is setup correctly before we failover
	checkReplication(ctx, t, clusterInstance, curPrimary, []*vitesst.Tablet{rdonly, replica}, 10*time.Second)

	// check before ERS disabled state is false
	waitForShardERSDisabledState(ctx, t, vtOrcProcess, keyspace.Name, shard0.Name, false)

	// disable ERS on the keyspace via SetVtorcEmergencyReparent --disable
	_, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "SetVtorcEmergencyReparent", "--disable", keyspace.Name)
	assert.NoError(t, err)
	waitForShardERSDisabledState(ctx, t, vtOrcProcess, keyspace.Name, shard0.Name, true)
	checkVarExists(ctx, t, vtOrcProcess, "EmergencyReparentShardDisabled")
	checkMetricExists(ctx, t, vtOrcProcess, "vtorc_emergency_reparent_shard_disabled")

	// make the current primary vttablet unavailable
	assert.NoError(t, curPrimary.StopVttablet(ctx))
	assert.NoError(t, curPrimary.StopMySQL(ctx))

	// check ERS did not occur. For the RecoverDeadPrimary recovery, expect >= 1 skipped recoveries, 0 successful recoveries and 0 ERS operations.
	waitForSkippedRecoveryCount(ctx, t, vtOrcProcess, logic.RecoverDeadPrimaryRecoveryName, keyspace.Name, shard0.Name, logic.RecoverySkipERSDisabled, 1)
	waitForSuccessfulRecoveryCount(ctx, t, vtOrcProcess, logic.RecoverDeadPrimaryRecoveryName, keyspace.Name, shard0.Name, 0)
	waitForSuccessfulERSCount(ctx, t, vtOrcProcess, keyspace.Name, shard0.Name, 0)

	// check that the shard primary remains the same because ERS is disabled on the keyspace
	origPrimary := curPrimary
	curPrimary = shardPrimaryTablet(ctx, t, clusterInstance, shard0)
	assert.NotNil(t, curPrimary)
	assert.Equal(t, origPrimary.Alias(), curPrimary.Alias())

	// enable ERS on the keyspace via SetVtorcEmergencyReparent --enable
	_, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "SetVtorcEmergencyReparent", "--enable", keyspace.Name)
	assert.NoError(t, err)
	waitForShardERSDisabledState(ctx, t, vtOrcProcess, keyspace.Name, shard0.Name, false)

	// check that the replica gets promoted by vtorc
	checkPrimaryTablet(ctx, t, clusterInstance, replica, true)

	// also check that the replication is working correctly after failover
	verifyWritesSucceed(ctx, t, replica, []*vitesst.Tablet{rdonly}, 10*time.Second)
	waitForSuccessfulRecoveryCount(ctx, t, vtOrcProcess, logic.RecoverDeadPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	waitForSuccessfulERSCount(ctx, t, vtOrcProcess, keyspace.Name, shard0.Name, 1)
	t.Run("Check ERS and PRS Vars and Metrics", func(t *testing.T) {
		checkVarExists(ctx, t, vtOrcProcess, "EmergencyReparentCounts")
		checkVarExists(ctx, t, vtOrcProcess, "PlannedReparentCounts")
		checkVarExists(ctx, t, vtOrcProcess, "ReparentShardOperationTimings")

		// Metrics registered in prometheus
		checkMetricExists(ctx, t, vtOrcProcess, "vtorc_emergency_reparent_counts")
		checkMetricExists(ctx, t, vtOrcProcess, "vtorc_planned_reparent_counts")
		checkMetricExists(ctx, t, vtOrcProcess, "vtorc_reparent_shard_operation_timings_bucket")
	})
}

// bring down primary before VTOrc has started, let vtorc repair.
func TestDownPrimaryBeforeVTOrc(t *testing.T) {
	ctx := t.Context()
	clusterInstance := setupVttablets(t, 2, 1, policy.DurabilityNone)
	keyspace := clusterInstance.Keyspace(keyspaceName)
	shard0 := keyspace.Shard(shardName)
	curPrimary := shard0.Tablets()[0]

	// Promote the first tablet as the primary
	initializeShard(ctx, t, clusterInstance, curPrimary)

	// find the replica and rdonly tablets
	var replica, rdonly *vitesst.Tablet
	for _, tablet := range shard0.Tablets() {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias() != curPrimary.Alias() && tablet.Type() == "replica" {
			replica = tablet
		}
		if tablet.Type() == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// check that the replication is setup correctly before we failover
	checkReplication(ctx, t, clusterInstance, curPrimary, []*vitesst.Tablet{rdonly, replica}, 10*time.Second)

	// Make the current primary vttablet unavailable.
	_ = curPrimary.StopVttablet(ctx)
	require.NoError(t, curPrimary.StopMySQL(ctx))

	// Start a VTOrc instance
	vtOrcProcess := startVTOrc(ctx, t, clusterInstance, "--remote-operation-timeout=10s", "--prevent-cross-cell-failover")

	// check that the replica gets promoted
	checkPrimaryTablet(ctx, t, clusterInstance, replica, true)

	// also check that the replication is working correctly after failover
	verifyWritesSucceed(ctx, t, replica, []*vitesst.Tablet{rdonly}, 10*time.Second)
	waitForSuccessfulRecoveryCount(ctx, t, vtOrcProcess, logic.RecoverDeadPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	waitForSuccessfulERSCount(ctx, t, vtOrcProcess, keyspace.Name, shard0.Name, 1)
}

// delete the primary record and let vtorc repair.
func TestDeletedPrimaryTablet(t *testing.T) {
	ctx := t.Context()
	clusterInstance, vtOrcProcess := setupVttabletsAndVTOrcs(t, 2, 1, policy.DurabilityNone, "--remote-operation-timeout=10s")
	keyspace := clusterInstance.Keyspace(keyspaceName)
	shard0 := keyspace.Shard(shardName)
	// find primary from topo
	curPrimary := shardPrimaryTablet(ctx, t, clusterInstance, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	waitForSuccessfulRecoveryCount(ctx, t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	waitForSuccessfulPRSCount(ctx, t, vtOrcProcess, keyspace.Name, shard0.Name, 1)

	// find the replica and rdonly tablets
	var replica, rdonly *vitesst.Tablet
	for _, tablet := range shard0.Tablets() {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias() != curPrimary.Alias() && tablet.Type() == "replica" {
			replica = tablet
		}
		if tablet.Type() == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// check that the replication is setup correctly before we failover
	checkReplication(ctx, t, clusterInstance, curPrimary, []*vitesst.Tablet{replica, rdonly}, 10*time.Second)

	// Disable VTOrc recoveries
	disableGlobalRecoveries(ctx, t, vtOrcProcess)
	// use vtctldclient to stop replication on the replica
	_, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "StopReplication", replica.Alias())
	require.NoError(t, err)
	// insert a write that is not available on the replica.
	verifyWritesSucceed(ctx, t, curPrimary, []*vitesst.Tablet{rdonly}, 10*time.Second)

	// Make the current primary vttablet unavailable and delete its tablet record.
	_ = curPrimary.StopVttablet(ctx)
	require.NoError(t, curPrimary.StopMySQL(ctx))
	// use vtctldclient to start replication on the replica back
	_, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "StartReplication", replica.Alias())
	require.NoError(t, err)
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "DeleteTablets", "--allow-primary", curPrimary.Alias())
	require.NoError(t, err)
	// Enable VTOrc recoveries now
	enableGlobalRecoveries(ctx, t, vtOrcProcess)

	// check that the replica gets promoted. Also verify that it has all the writes.
	checkPrimaryTablet(ctx, t, clusterInstance, replica, true)
	checkTabletUptoDate(ctx, t, replica)

	// also check that the replication is working correctly after failover
	verifyWritesSucceed(ctx, t, replica, []*vitesst.Tablet{rdonly}, 10*time.Second)
	waitForSuccessfulRecoveryCount(ctx, t, vtOrcProcess, logic.RecoverPrimaryTabletDeletedRecoveryName, keyspace.Name, shard0.Name, 1)
}

// TestDeadPrimaryRecoversImmediately test Vtorc ability to recover immediately if primary is dead.
// Reason is, unlike other recoveries, in DeadPrimary we don't call DiscoverInstance since we know
// that primary is unreachable. This help us save few seconds depending on value of `RemoteOperationTimeout` flag.
func TestDeadPrimaryRecoversImmediately(t *testing.T) {
	ctx := t.Context()
	// We specify the --wait-replicas-timeout to a small value because we spawn a cross-cell replica later in the test.
	// If that replica is more advanced than the same-cell-replica, then we try to promote the cross-cell replica as an intermediate source.
	// If we don't specify a small value of --wait-replicas-timeout, then we would end up waiting for 30 seconds for the dead-primary to respond, failing this test.
	clusterInstance, vtOrcProcess := setupVttabletsAndVTOrcs(t, 2, 1, policy.DurabilitySemiSync,
		"--remote-operation-timeout=10s", "--wait-replicas-timeout=5s", "--prevent-cross-cell-failover")
	keyspace := clusterInstance.Keyspace(keyspaceName)
	shard0 := keyspace.Shard(shardName)
	// find primary from topo
	curPrimary := shardPrimaryTablet(ctx, t, clusterInstance, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	waitForSuccessfulRecoveryCount(ctx, t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	waitForSuccessfulPRSCount(ctx, t, vtOrcProcess, keyspace.Name, shard0.Name, 1)

	// find the replica and rdonly tablets
	var replica, rdonly *vitesst.Tablet
	for _, tablet := range shard0.Tablets() {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias() != curPrimary.Alias() && tablet.Type() == "replica" {
			replica = tablet
		}
		if tablet.Type() == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// Start a cross-cell replica
	crossCellReplica := startVttablet(ctx, t, clusterInstance, cell2, false)

	// check that the replication is setup correctly before we failover
	checkReplication(ctx, t, clusterInstance, curPrimary, []*vitesst.Tablet{rdonly, replica, crossCellReplica}, 10*time.Second)

	// Make the current primary vttablet unavailable.
	require.NoError(t, curPrimary.KillVttablet(ctx))
	require.NoError(t, curPrimary.StopMySQL(ctx))

	// check that the replica gets promoted
	checkPrimaryTablet(ctx, t, clusterInstance, replica, true)
	waitForInstancePollSecondsExceededCount(ctx, t, vtOrcProcess, 2, false)
	// also check that the replication is working correctly after failover
	verifyWritesSucceed(ctx, t, replica, []*vitesst.Tablet{crossCellReplica}, 10*time.Second)
	waitForSuccessfulRecoveryCount(ctx, t, vtOrcProcess, logic.RecoverDeadPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)
	waitForSuccessfulERSCount(ctx, t, vtOrcProcess, keyspace.Name, shard0.Name, 1)

	// assert that it takes less than `remote-operation-timeout` to recover from `DeadPrimary`
	// use the value provided in `remote-operation-timeout` flag to compare with.
	// We are testing against 9.5 seconds to be safe and prevent flakiness.
	//
	// TODO: this is really flimsy since it relies on parsing VTOrc's logs and assumes a specific
	// log format. We should change this to something more robust.
	d := recoveryDuration(ctx, t, vtOrcProcess, "DeadPrimary", keyspace.Name, shard0.Name)
	assert.Less(t, d.Seconds(), 9.5)
}

// Failover should not be cross data centers, according to the configuration file
// covers part of the test case master-failover-lost-replicas from orchestrator
func TestCrossDataCenterFailure(t *testing.T) {
	ctx := t.Context()
	clusterInstance, _ := setupVttabletsAndVTOrcs(t, 2, 1, policy.DurabilityNone, "--prevent-cross-cell-failover")
	keyspace := clusterInstance.Keyspace(keyspaceName)
	shard0 := keyspace.Shard(shardName)
	// find primary from topo
	curPrimary := shardPrimaryTablet(ctx, t, clusterInstance, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica and rdonly tablets
	var replicaInSameCell, rdonly *vitesst.Tablet
	for _, tablet := range shard0.Tablets() {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias() != curPrimary.Alias() && tablet.Type() == "replica" {
			replicaInSameCell = tablet
		}
		if tablet.Type() == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replicaInSameCell, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica := startVttablet(ctx, t, clusterInstance, cell2, false)
	// newly started tablet does not replicate from anyone yet, we will allow vtorc to fix this too
	checkReplication(ctx, t, clusterInstance, curPrimary, []*vitesst.Tablet{crossCellReplica, replicaInSameCell, rdonly}, 25*time.Second)

	// Make the current primary database unavailable.
	require.NoError(t, curPrimary.StopMySQL(ctx))

	// we have a replica in the same cell, so that is the one which should be promoted and not the one from another cell
	checkPrimaryTablet(ctx, t, clusterInstance, replicaInSameCell, true)
	// also check that the replication is working correctly after failover
	verifyWritesSucceed(ctx, t, replicaInSameCell, []*vitesst.Tablet{crossCellReplica, rdonly}, 10*time.Second)
}

// Failover should not be cross data centers, according to the configuration file
// In case of no viable candidates, we should error out
func TestCrossDataCenterFailureError(t *testing.T) {
	ctx := t.Context()
	clusterInstance, _ := setupVttabletsAndVTOrcs(t, 1, 1, policy.DurabilityNone, "--prevent-cross-cell-failover")
	keyspace := clusterInstance.Keyspace(keyspaceName)
	shard0 := keyspace.Shard(shardName)
	// find primary from topo
	curPrimary := shardPrimaryTablet(ctx, t, clusterInstance, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the rdonly tablet
	var rdonly *vitesst.Tablet
	for _, tablet := range shard0.Tablets() {
		if tablet.Type() == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica1 := startVttablet(ctx, t, clusterInstance, cell2, false)
	crossCellReplica2 := startVttablet(ctx, t, clusterInstance, cell2, false)
	// newly started tablet does not replicate from anyone yet, we will allow vtorc to fix this too
	checkReplication(ctx, t, clusterInstance, curPrimary, []*vitesst.Tablet{crossCellReplica1, crossCellReplica2, rdonly}, 25*time.Second)

	// Make the current primary database unavailable.
	require.NoError(t, curPrimary.StopMySQL(ctx))

	// wait for 20 seconds
	time.Sleep(20 * time.Second)

	// the previous primary should still be the primary since recovery of dead primary should fail
	checkPrimaryTablet(ctx, t, clusterInstance, curPrimary, false)
}

// Failover will sometimes lead to a rdonly which can no longer replicate.
// covers part of the test case master-failover-lost-replicas from orchestrator
func TestLostRdonlyOnPrimaryFailure(t *testing.T) {
	// new version of ERS does not check for lost replicas yet
	// Earlier any replicas that were not able to replicate from the previous primary
	// were detected by vtorc and could be configured to have their sources detached
	t.Skip()
	ctx := t.Context()
	clusterInstance, vtOrcProcess := setupVttabletsAndVTOrcs(t, 2, 2, policy.DurabilityNone, "--prevent-cross-cell-failover")
	keyspace := clusterInstance.Keyspace(keyspaceName)
	shard0 := keyspace.Shard(shardName)
	// find primary from topo
	curPrimary := shardPrimaryTablet(ctx, t, clusterInstance, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// get the tablets
	var replica, rdonly, aheadRdonly *vitesst.Tablet
	for _, tablet := range shard0.Tablets() {
		// find tablets which are not the primary
		if tablet.Alias() != curPrimary.Alias() {
			if tablet.Type() == "replica" {
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
	checkReplication(ctx, t, clusterInstance, curPrimary, []*vitesst.Tablet{rdonly, aheadRdonly, replica}, 15*time.Second)

	// disable recoveries on vtorc so that it is unable to repair the replication
	disableGlobalRecoveries(ctx, t, vtOrcProcess)

	// stop replication on the replica and rdonly.
	_, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "StopReplication", replica.Alias())
	require.NoError(t, err)
	_, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "StopReplication", rdonly.Alias())
	require.NoError(t, err)

	// check that aheadRdonly is able to replicate. We also want to add some queries to aheadRdonly which will not be there in replica and rdonly
	verifyWritesSucceed(ctx, t, curPrimary, []*vitesst.Tablet{aheadRdonly}, 15*time.Second)

	// assert that the replica and rdonly are indeed lagging and do not have the new insertion by checking the count of rows in the tables
	out, err := runSQL(ctx, "SELECT * FROM vt_insert_test", replica, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 1, len(out.Rows))
	out, err = runSQL(ctx, "SELECT * FROM vt_insert_test", rdonly, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 1, len(out.Rows))

	// Make the current primary database unavailable.
	require.NoError(t, curPrimary.StopMySQL(ctx))

	// enable recoveries back on vtorc so that it can repair
	enableGlobalRecoveries(ctx, t, vtOrcProcess)

	// vtorc must promote the lagging replica and not the rdonly, since it has a MustNotPromoteRule promotion rule
	checkPrimaryTablet(ctx, t, clusterInstance, replica, true)

	// also check that the replication is setup correctly
	verifyWritesSucceed(ctx, t, replica, []*vitesst.Tablet{rdonly}, 15*time.Second)

	// check that the rdonly is lost. The lost replica has is detached and its host is prepended with `//`
	out, err = runSQL(ctx, "SELECT HOST FROM performance_schema.replication_connection_configuration", aheadRdonly, "")
	require.NoError(t, err)
	require.Equal(t, "//localhost", out.Rows[0][0].ToString())
}

// This test checks that the promotion of a tablet succeeds if it passes the promotion lag test
// covers the test case master-failover-fail-promotion-lag-minutes-success from orchestrator
func TestPromotionLagSuccess(t *testing.T) {
	ctx := t.Context()
	clusterInstance, _ := setupVttabletsAndVTOrcs(t, 2, 1, policy.DurabilityNone)
	keyspace := clusterInstance.Keyspace(keyspaceName)
	shard0 := keyspace.Shard(shardName)
	// find primary from topo
	curPrimary := shardPrimaryTablet(ctx, t, clusterInstance, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica and rdonly tablets
	var replica, rdonly *vitesst.Tablet
	for _, tablet := range shard0.Tablets() {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias() != curPrimary.Alias() && tablet.Type() == "replica" {
			replica = tablet
		}
		if tablet.Type() == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// check that the replication is setup correctly before we failover
	checkReplication(ctx, t, clusterInstance, curPrimary, []*vitesst.Tablet{rdonly, replica}, 10*time.Second)

	// Make the current primary database unavailable.
	require.NoError(t, curPrimary.StopMySQL(ctx))

	// check that the replica gets promoted
	checkPrimaryTablet(ctx, t, clusterInstance, replica, true)
	// also check that the replication is working correctly after failover
	verifyWritesSucceed(ctx, t, replica, []*vitesst.Tablet{rdonly}, 10*time.Second)
}

// This test checks that the promotion of a tablet succeeds if it passes the promotion lag test
// covers the test case master-failover-fail-promotion-lag-minutes-failure from orchestrator
func TestPromotionLagFailure(t *testing.T) {
	// new version of ERS does not check for promotion lag yet
	// Earlier vtorc used to check that the promotion lag between the new primary and the old one
	// was smaller than the configured value, otherwise it would fail the promotion
	t.Skip()
	ctx := t.Context()
	clusterInstance, _ := setupVttabletsAndVTOrcs(t, 3, 1, policy.DurabilityNone)
	keyspace := clusterInstance.Keyspace(keyspaceName)
	shard0 := keyspace.Shard(shardName)
	// find primary from topo
	curPrimary := shardPrimaryTablet(ctx, t, clusterInstance, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica and rdonly tablets
	var replica1, replica2, rdonly *vitesst.Tablet
	for _, tablet := range shard0.Tablets() {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias() != curPrimary.Alias() && tablet.Type() == "replica" {
			if replica1 == nil {
				replica1 = tablet
			} else {
				replica2 = tablet
			}
		}
		if tablet.Type() == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica1, "could not find replica tablet")
	assert.NotNil(t, replica2, "could not find second replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// check that the replication is setup correctly before we failover
	checkReplication(ctx, t, clusterInstance, curPrimary, []*vitesst.Tablet{rdonly, replica1, replica2}, 10*time.Second)

	// Make the current primary database unavailable.
	require.NoError(t, curPrimary.StopMySQL(ctx))

	// wait for 20 seconds
	time.Sleep(20 * time.Second)

	// the previous primary should still be the primary since recovery of dead primary should fail
	checkPrimaryTablet(ctx, t, clusterInstance, curPrimary, false)
}

// covers the test case master-failover-candidate from orchestrator
// We explicitly set one of the replicas to Prefer promotion rule.
// That is the replica which should be promoted in case of primary failure
func TestDownPrimaryPromotionRule(t *testing.T) {
	ctx := t.Context()
	clusterInstance := setupVttablets(t, 2, 1, policy.DurabilityTest, promotionRuleTabletUID)
	keyspace := clusterInstance.Keyspace(keyspaceName)
	shard0 := keyspace.Shard(shardName)
	// elect a same-cell replica so the preferred cross-cell replica is not the initial primary
	initializeShard(ctx, t, clusterInstance, firstCell1Replica(t, shard0))
	startVTOrc(ctx, t, clusterInstance)
	// find primary from topo
	curPrimary := shardPrimaryTablet(ctx, t, clusterInstance, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica and rdonly tablets
	var replica, rdonly *vitesst.Tablet
	for _, tablet := range shard0.Tablets() {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Cell == cell1 && tablet.Alias() != curPrimary.Alias() && tablet.Type() == "replica" {
			replica = tablet
		}
		if tablet.Type() == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica := cell2Tablet(t, shard0)
	// newly started tablet does not replicate from anyone yet, we will allow vtorc to fix this too
	checkReplication(ctx, t, clusterInstance, curPrimary, []*vitesst.Tablet{crossCellReplica, rdonly, replica}, 25*time.Second)

	// Make the current primary database unavailable.
	require.NoError(t, curPrimary.StopMySQL(ctx))

	// we have a replica with a preferred promotion rule, so that is the one which should be promoted
	checkPrimaryTablet(ctx, t, clusterInstance, crossCellReplica, true)
	// also check that the replication is working correctly after failover
	verifyWritesSucceed(ctx, t, crossCellReplica, []*vitesst.Tablet{rdonly, replica}, 10*time.Second)
}

// covers the test case master-failover-candidate-lag from orchestrator
// We explicitly set one of the replicas to Prefer promotion rule and make it lag with respect to other replicas.
// That is the replica which should be promoted in case of primary failure
// It should also be caught up when it is promoted
func TestDownPrimaryPromotionRuleWithLag(t *testing.T) {
	ctx := t.Context()
	clusterInstance := setupVttablets(t, 2, 1, policy.DurabilityTest, promotionRuleTabletUID)
	keyspace := clusterInstance.Keyspace(keyspaceName)
	shard0 := keyspace.Shard(shardName)
	// elect a same-cell replica so the preferred cross-cell replica is not the initial primary
	initializeShard(ctx, t, clusterInstance, firstCell1Replica(t, shard0))
	vtOrcProcess := startVTOrc(ctx, t, clusterInstance)
	// find primary from topo
	curPrimary := shardPrimaryTablet(ctx, t, clusterInstance, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// get the replicas in the same cell
	var replica, rdonly *vitesst.Tablet
	for _, tablet := range shard0.Tablets() {
		// find tablets which are not the primary
		if tablet.Cell == cell1 && tablet.Alias() != curPrimary.Alias() {
			if tablet.Type() == "replica" {
				replica = tablet
			} else {
				rdonly = tablet
			}
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica := cell2Tablet(t, shard0)
	// newly started tablet does not replicate from anyone yet, we will allow vtorc to fix this too
	checkReplication(ctx, t, clusterInstance, curPrimary, []*vitesst.Tablet{crossCellReplica, replica, rdonly}, 25*time.Second)

	// disable recoveries for vtorc so that it is unable to repair the replication.
	disableGlobalRecoveries(ctx, t, vtOrcProcess)

	// stop replication on the crossCellReplica.
	_, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "StopReplication", crossCellReplica.Alias())
	require.NoError(t, err)

	// check that rdonly and replica are able to replicate. We also want to add some queries to replica which will not be there in crossCellReplica
	verifyWritesSucceed(ctx, t, curPrimary, []*vitesst.Tablet{replica, rdonly}, 15*time.Second)

	// reset the primary logs so that crossCellReplica can never catch up
	resetPrimaryLogs(ctx, t, curPrimary)

	// start replication back on the crossCellReplica.
	_, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "StartReplication", crossCellReplica.Alias())
	require.NoError(t, err)

	// enable recoveries back on vtorc so that it can repair
	enableGlobalRecoveries(ctx, t, vtOrcProcess)

	// assert that the crossCellReplica is indeed lagging and does not have the new insertion by checking the count of rows in the table
	out, err := runSQL(ctx, "SELECT * FROM vt_insert_test", crossCellReplica, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 1, len(out.Rows))

	// Make the current primary database unavailable.
	require.NoError(t, curPrimary.StopMySQL(ctx))

	// the crossCellReplica is set to be preferred according to the durability requirements. So it must be promoted
	checkPrimaryTablet(ctx, t, clusterInstance, crossCellReplica, true)

	// assert that the crossCellReplica has indeed caught up
	out, err = runSQL(ctx, "SELECT * FROM vt_insert_test", crossCellReplica, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 2, len(out.Rows))

	// check that rdonly and replica are able to replicate from the crossCellReplica
	verifyWritesSucceed(ctx, t, crossCellReplica, []*vitesst.Tablet{replica, rdonly}, 15*time.Second)
}

// covers the test case master-failover-candidate-lag-cross-datacenter from orchestrator
// We explicitly set one of the cross-cell replicas to Prefer promotion rule, but we prevent cross data center promotions.
// We let a replica in our own cell lag. That is the replica which should be promoted in case of primary failure
// It should also be caught up when it is promoted
func TestDownPrimaryPromotionRuleWithLagCrossCenter(t *testing.T) {
	ctx := t.Context()
	clusterInstance := setupVttablets(t, 2, 1, policy.DurabilityTest, promotionRuleTabletUID)
	keyspace := clusterInstance.Keyspace(keyspaceName)
	shard0 := keyspace.Shard(shardName)
	// elect a same-cell replica so the preferred cross-cell replica is not the initial primary
	initializeShard(ctx, t, clusterInstance, firstCell1Replica(t, shard0))
	vtOrcProcess := startVTOrc(ctx, t, clusterInstance, "--prevent-cross-cell-failover")
	// find primary from topo
	curPrimary := shardPrimaryTablet(ctx, t, clusterInstance, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// get the replicas in the same cell
	var replica, rdonly *vitesst.Tablet
	for _, tablet := range shard0.Tablets() {
		// find tablets which are not the primary
		if tablet.Cell == cell1 && tablet.Alias() != curPrimary.Alias() {
			if tablet.Type() == "replica" {
				replica = tablet
			} else {
				rdonly = tablet
			}
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica := cell2Tablet(t, shard0)
	// newly started tablet does not replicate from anyone yet, we will allow vtorc to fix this too
	checkReplication(ctx, t, clusterInstance, curPrimary, []*vitesst.Tablet{crossCellReplica, replica, rdonly}, 25*time.Second)

	// disable recoveries from vtorc so that it is unable to repair the replication
	disableGlobalRecoveries(ctx, t, vtOrcProcess)

	// stop replication on the replica.
	_, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "StopReplication", replica.Alias())
	require.NoError(t, err)

	// check that rdonly and crossCellReplica are able to replicate. We also want to add some queries to crossCenterReplica which will not be there in replica
	verifyWritesSucceed(ctx, t, curPrimary, []*vitesst.Tablet{rdonly, crossCellReplica}, 15*time.Second)

	// reset the primary logs so that crossCellReplica can never catch up
	resetPrimaryLogs(ctx, t, curPrimary)

	// start replication back on the replica.
	_, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "StartReplication", replica.Alias())
	require.NoError(t, err)

	// enable recoveries back on vtorc so that it can repair
	enableGlobalRecoveries(ctx, t, vtOrcProcess)

	// assert that the replica is indeed lagging and does not have the new insertion by checking the count of rows in the table
	out, err := runSQL(ctx, "SELECT * FROM vt_insert_test", replica, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 1, len(out.Rows))

	// Make the current primary database unavailable.
	require.NoError(t, curPrimary.StopMySQL(ctx))

	// the replica should be promoted since we have prevented cross cell promotions
	checkPrimaryTablet(ctx, t, clusterInstance, replica, true)

	// assert that the replica has indeed caught up
	out, err = runSQL(ctx, "SELECT * FROM vt_insert_test", replica, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 2, len(out.Rows))

	// check that rdonly and crossCellReplica are able to replicate from the replica
	verifyWritesSucceed(ctx, t, replica, []*vitesst.Tablet{crossCellReplica, rdonly}, 15*time.Second)
}

// firstCell1Replica returns the first cell1 replica tablet of the shard.
func firstCell1Replica(t *testing.T, shard *vitesst.Shard) *vitesst.Tablet {
	t.Helper()
	for _, tablet := range shard.Tablets() {
		if tablet.Cell == cell1 && tablet.Type() == "replica" {
			return tablet
		}
	}
	require.FailNow(t, "could not find a cell1 replica tablet")
	return nil
}
