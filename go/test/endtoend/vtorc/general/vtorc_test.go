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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vitesst"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/logic"
)

// Cases to test:
// 1. create cluster with 2 replicas and 1 rdonly, let orc choose primary
// verify rdonly is not elected, only replica
// verify replication is setup
// verify that with multiple vtorc instances, we still only have 1 PlannedReparentShard call
func TestPrimaryElection(t *testing.T) {
	vc := setupVtorcCluster(t)
	vc.setupVttabletsAndVTOrcs(t, 2, 1, nil, true, 2, "")

	primary := shardPrimaryTablet(t, vc, keyspaceName, shardName, vc.active)
	assert.NotNil(t, primary, "should have elected a primary")
	checkReplication(t, vc, primary, vc.active, 10*time.Second)

	for _, vttablet := range vc.active {
		if vttablet.Type() == "rdonly" && primary.Alias() == vttablet.Alias() {
			assert.Failf(t, "rdonly promoted", "Rdonly tablet promoted as primary - %v", primary.Alias())
		}
	}

	res, err := runSQL(t.Context(), t, "select * from reparent_journal", primary, "_vt")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1, "There should only be 1 primary tablet which was elected")
}

// TestErrantGTIDOnPreviousPrimary tests that VTOrc is able to detect errant GTIDs on a previously demoted primary
// if it has an errant GTID.
func TestErrantGTIDOnPreviousPrimary(t *testing.T) {
	vc := setupVtorcCluster(t)
	vc.setupVttabletsAndVTOrcs(t, 3, 0, []string{"--change-tablets-with-errant-gtid-to-drained"}, false, 1, "")

	// find primary from topo
	curPrimary := shardPrimaryTablet(t, vc, keyspaceName, shardName, vc.active)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	vtOrcProcess := vc.vtorcs[0]
	waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, keyspaceName, shardName, 1)
	waitForSuccessfulPRSCount(t, vtOrcProcess, keyspaceName, shardName, 1)

	var replica, otherReplica *vitesst.Tablet
	for _, tablet := range vc.active {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias() != curPrimary.Alias() {
			if replica == nil {
				replica = tablet
			} else {
				otherReplica = tablet
			}
		}
	}
	require.NotNil(t, replica, "should be able to find a replica")
	require.NotNil(t, otherReplica, "should be able to find 2nd replica")
	checkReplication(t, vc, curPrimary, []*vitesst.Tablet{replica, otherReplica}, 15*time.Second)

	// Disable global recoveries for the cluster.
	disableGlobalRecoveries(t, vtOrcProcess)

	// Run PRS to promote a different replica.
	output, err := vc.cluster.Vtctld().ExecuteCommandWithOutput(
		t.Context(),
		"PlannedReparentShard",
		fmt.Sprintf("%s/%s", keyspaceName, shardName),
		"--new-primary", replica.Alias(),
	)
	require.NoError(t, err, "error in PlannedReparentShard output - %s", output)

	// Stop replicatin on the previous primary to simulate it not reparenting properly.
	// Also insert an errant GTID on the previous primary.
	err = runSQLs(t.Context(), t, []string{
		"STOP REPLICA",
		"RESET REPLICA ALL",
		"set global read_only=OFF",
		"insert into vt_ks.vt_insert_test(id, msg) values (10173, 'test 178342')",
	}, curPrimary, "")
	require.NoError(t, err)

	// Wait for VTOrc to detect the errant GTID and change the tablet to a drained type.
	enableGlobalRecoveries(t, vtOrcProcess)

	// Wait for the tablet to be drained.
	waitForTabletType(t, curPrimary, "drained")
}

// Cases to test:
// 1. create cluster with 1 replica and 1 rdonly, let orc choose primary
// verify rdonly is not elected, only replica
// verify replication is setup
func TestSingleKeyspace(t *testing.T) {
	vc := setupVtorcCluster(t)
	vc.setupVttabletsAndVTOrcs(t, 1, 1, []string{"--clusters-to-watch", "ks"}, true, 1, "")

	checkPrimaryTablet(t, vc, vc.active[0], true)
	checkReplication(t, vc, vc.active[0], vc.active[1:], 10*time.Second)
	waitForSuccessfulRecoveryCount(t, vc.vtorcs[0], logic.ElectNewPrimaryRecoveryName, keyspaceName, shardName, 1)
	waitForSuccessfulPRSCount(t, vc.vtorcs[0], keyspaceName, shardName, 1)
}

// Cases to test:
// 1. create cluster with 1 replica and 1 rdonly, let orc choose primary
// verify rdonly is not elected, only replica
// verify replication is setup
func TestKeyspaceShard(t *testing.T) {
	vc := setupVtorcCluster(t)
	vc.setupVttabletsAndVTOrcs(t, 1, 1, []string{"--clusters-to-watch", "ks/0"}, true, 1, "")

	checkPrimaryTablet(t, vc, vc.active[0], true)
	checkReplication(t, vc, vc.active[0], vc.active[1:], 10*time.Second)
	waitForSuccessfulRecoveryCount(t, vc.vtorcs[0], logic.ElectNewPrimaryRecoveryName, keyspaceName, shardName, 1)
	waitForSuccessfulPRSCount(t, vc.vtorcs[0], keyspaceName, shardName, 1)
}

// Cases to test:
// 1. make primary readonly, let vtorc repair
// 2. make replica ReadWrite, let vtorc repair
// 3. stop replication, let vtorc repair
// 4. setup replication from non-primary, let vtorc repair
// 5. make instance A replicates from B and B from A, wait for repair
// 6. disable recoveries and make sure the detected problems are set correctly.
func TestVTOrcRepairs(t *testing.T) {
	vc := setupVtorcCluster(t)
	vc.setupVttabletsAndVTOrcs(t, 3, 0, []string{"--change-tablets-with-errant-gtid-to-drained"}, true, 1, "")

	// find primary from topo
	curPrimary := shardPrimaryTablet(t, vc, keyspaceName, shardName, vc.active)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	vtOrcProcess := vc.vtorcs[0]
	waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, keyspaceName, shardName, 1)
	waitForSuccessfulPRSCount(t, vtOrcProcess, keyspaceName, shardName, 1)

	var replica, otherReplica *vitesst.Tablet
	for _, tablet := range vc.active {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias() != curPrimary.Alias() {
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
	checkReplication(t, vc, curPrimary, []*vitesst.Tablet{replica, otherReplica}, 15*time.Second)

	t.Run("PrimaryReadOnly", func(t *testing.T) {
		// Make the current primary database read-only.
		_, err := runSQL(t.Context(), t, "set global read_only=ON", curPrimary, "")
		require.NoError(t, err)

		// wait for repair
		match := waitForReadOnlyValue(t, curPrimary, 0)
		require.True(t, match)
		waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixPrimaryRecoveryName, keyspaceName, shardName, 1)
	})

	t.Run("ReplicaReadWrite", func(t *testing.T) {
		// Make the replica database read-write.
		_, err := runSQL(t.Context(), t, "set global read_only=OFF", replica, "")
		require.NoError(t, err)

		// wait for repair
		match := waitForReadOnlyValue(t, replica, 1)
		require.True(t, match)
		waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixReplicaRecoveryName, keyspaceName, shardName, 1)
	})

	t.Run("StopReplication", func(t *testing.T) {
		// use vtctldclient to stop replication
		_, err := vc.cluster.Vtctld().ExecuteCommandWithOutput(t.Context(), "StopReplication", replica.Alias())
		require.NoError(t, err)

		// check replication is setup correctly
		checkReplication(t, vc, curPrimary, []*vitesst.Tablet{replica, otherReplica}, 15*time.Second)
		waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixReplicaRecoveryName, keyspaceName, shardName, 2)

		// Stop just the IO thread on the replica
		_, err = runSQL(t.Context(), t, "STOP REPLICA IO_THREAD", replica, "")
		require.NoError(t, err)

		// check replication is setup correctly
		checkReplication(t, vc, curPrimary, []*vitesst.Tablet{replica, otherReplica}, 15*time.Second)
		waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixReplicaRecoveryName, keyspaceName, shardName, 3)

		// Stop just the SQL thread on the replica
		_, err = runSQL(t.Context(), t, "STOP REPLICA SQL_THREAD", replica, "")
		require.NoError(t, err)

		// check replication is setup correctly
		checkReplication(t, vc, curPrimary, []*vitesst.Tablet{replica, otherReplica}, 15*time.Second)
		waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixReplicaRecoveryName, keyspaceName, shardName, 4)
	})

	t.Run("ReplicationFromOtherReplica", func(t *testing.T) {
		// point replica at otherReplica
		changeReplicationSourceCommands := []string{
			"STOP REPLICA",
			"RESET REPLICA ALL",
			fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='vt_repl', GET_SOURCE_PUBLIC_KEY = 1, SOURCE_AUTO_POSITION = 1", otherReplica.Name(), tabletMySQLPort),
			"START REPLICA",
		}
		err := runSQLs(t.Context(), t, changeReplicationSourceCommands, replica, "")
		require.NoError(t, err)

		// wait until the source port is set back correctly by vtorc
		checkSourcePort(t, replica, curPrimary, 15*time.Second)
		waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixReplicaRecoveryName, keyspaceName, shardName, 5)

		// check that writes succeed
		verifyWritesSucceed(t, vc, curPrimary, []*vitesst.Tablet{replica, otherReplica}, 15*time.Second)
	})

	t.Run("Replication Misconfiguration", func(t *testing.T) {
		_, err := runSQL(t.Context(), t, `SET @@global.replica_net_timeout=33`, replica, "")
		require.NoError(t, err)

		// wait until heart beat interval has been fixed by vtorc.
		checkHeartbeatInterval(t, replica, 16.5, 15*time.Second)
		waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixReplicaRecoveryName, keyspaceName, shardName, 6)

		// check that writes succeed
		verifyWritesSucceed(t, vc, curPrimary, []*vitesst.Tablet{replica, otherReplica}, 15*time.Second)
	})

	t.Run("CircularReplication", func(t *testing.T) {
		// change the replication source on the primary
		changeReplicationSourceCommands := []string{
			"STOP REPLICA",
			"RESET REPLICA ALL",
			fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='vt_repl', GET_SOURCE_PUBLIC_KEY = 1, SOURCE_AUTO_POSITION = 1", replica.Name(), tabletMySQLPort),
			"START REPLICA",
		}
		err := runSQLs(t.Context(), t, changeReplicationSourceCommands, curPrimary, "")
		require.NoError(t, err)

		// wait for curPrimary to reach stable state
		time.Sleep(1 * time.Second)

		// wait for repair
		err = waitForReplicationToStop(t.Context(), t, curPrimary)
		require.NoError(t, err)
		waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.RecoverPrimaryHasPrimaryRecoveryName, keyspaceName, shardName, 1)
		// check that the writes still succeed
		verifyWritesSucceed(t, vc, curPrimary, []*vitesst.Tablet{replica, otherReplica}, 10*time.Second)
	})

	t.Run("Errant GTID Detected", func(t *testing.T) {
		// insert an errant GTID in the replica
		_, err := runSQL(t.Context(), t, "insert into vt_insert_test(id, msg) values (10173, 'test 178342')", replica, "vt_ks")
		require.NoError(t, err)
		// When VTOrc detects errant GTIDs, it should change the tablet to a drained type.
		waitForTabletType(t, replica, "drained")
	})

	t.Run("Sets DetectedProblems metric correctly", func(t *testing.T) {
		// Since we're using a boolean metric here, disable recoveries for now.
		status, _, err := makeAPICall(t, vtOrcProcess, "/api/disable-global-recoveries")
		require.NoError(t, err)
		require.Equal(t, 200, status)

		// Make the current primary database read-only.
		_, err = runSQL(t.Context(), t, "set global read_only=ON", curPrimary, "")
		require.NoError(t, err)

		// Wait for problems to be set.
		waitForDetectedProblems(
			t, vtOrcProcess,
			string(inst.PrimaryIsReadOnly),
			canonicalAlias(curPrimary),
			keyspaceName,
			shardName,
			1,
		)

		// Enable recoveries.
		status, _, err = makeAPICall(t, vtOrcProcess, "/api/enable-global-recoveries")
		require.NoError(t, err)
		assert.Equal(t, 200, status)

		// wait for detected problem to be cleared.
		waitForDetectedProblems(
			t, vtOrcProcess,
			string(inst.PrimaryIsReadOnly),
			canonicalAlias(curPrimary),
			keyspaceName,
			shardName,
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
		checkReplication(t, vc, curPrimary, []*vitesst.Tablet{replica, otherReplica}, 15*time.Second)

		err := updateTabletFields(t.Context(), vc.etcdAddr, replica.Cell, replica.UID, func(tablet *topodatapb.Tablet) error {
			tablet.Type = topodatapb.TabletType_PRIMARY
			tablet.PrimaryTermStartTime = protoutil.TimeToProto(time.Now())
			return nil
		})
		require.NoError(t, err)

		// Wait for VTOrc to detect the mismatch and promote the tablet.
		require.Eventuallyf(t, func() bool {
			return fullStatusTabletType(t, vc, replica) == topodatapb.TabletType_PRIMARY
		}, 10*time.Second, 1*time.Second, "Primary tablet's display type didn't match the topo record")
		// Also check that the replica gets promoted and can accept writes.
		checkReplication(t, vc, replica, []*vitesst.Tablet{curPrimary, otherReplica}, 15*time.Second)
	})
}

func TestRepairAfterTER(t *testing.T) {
	// test fails intermittently on CI, skip until it can be fixed.
	t.SkipNow()
	vc := setupVtorcCluster(t)
	vc.setupVttabletsAndVTOrcs(t, 2, 0, nil, true, 1, "")

	// find primary from topo
	curPrimary := shardPrimaryTablet(t, vc, keyspaceName, shardName, vc.active)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// TODO(deepthi): we should not need to do this, the DB should be created automatically
	_, err := runSQL(t.Context(), t, "create database IF NOT EXISTS vt_"+keyspaceName, curPrimary, "")
	require.NoError(t, err)

	var newPrimary *vitesst.Tablet
	for _, tablet := range vc.active {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias() != curPrimary.Alias() {
			newPrimary = tablet
			break
		}
	}

	// TER to other tablet
	_, err = vc.cluster.Vtctld().ExecuteCommandWithOutput(t.Context(), "TabletExternallyReparented", newPrimary.Alias())
	require.NoError(t, err)

	checkReplication(t, vc, newPrimary, []*vitesst.Tablet{curPrimary}, 15*time.Second)
}

// TestStalePrimary tests that an old primary that remains writable and of tablet type PRIMARY in the topo
// is properly demoted to a read-only replica by VTOrc.
func TestStalePrimary(t *testing.T) {
	vc := setupVtorcCluster(t)
	ctx := t.Context()

	vc.setupVttabletsAndVTOrcs(t, 4, 0, []string{"--topo-information-refresh-duration", "1s"}, true, 1, policy.DurabilitySemiSync)

	curPrimary := shardPrimaryTablet(t, vc, keyspaceName, shardName, vc.active)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	checkPrimaryTablet(t, vc, curPrimary, true)

	var badPrimary, healthyReplica *vitesst.Tablet
	for _, tablet := range vc.active {
		if tablet.Alias() == curPrimary.Alias() {
			continue
		}

		if badPrimary == nil {
			badPrimary = tablet
			continue
		}

		healthyReplica = tablet
	}

	checkReplication(t, vc, curPrimary, []*vitesst.Tablet{badPrimary, healthyReplica}, 15*time.Second)

	curPrimaryTopo, err := getTabletRecord(ctx, vc.cluster, curPrimary.Alias())
	require.NoError(t, err, "expected to read current primary topo record")

	curPrimaryTermStart := protoutil.TimeFromProto(curPrimaryTopo.PrimaryTermStartTime)
	require.False(t, curPrimaryTermStart.IsZero(), "expected current primary term start time to be set")

	err = runSQLs(ctx, t, []string{"SET GLOBAL read_only = OFF"}, badPrimary, "")
	require.NoError(t, err)
	require.True(t, waitForReadOnlyValue(t, badPrimary, 0))

	// We set the tablet's type in the topology to PRIMARY. This mimics the situation where during a demotion
	// in a hypothetical ERS, the old primary starts running as a replica, but fails before updating the topology
	// accordingly.
	err = updateTabletFields(ctx, vc.etcdAddr, badPrimary.Cell, badPrimary.UID, func(tablet *topodatapb.Tablet) error {
		tablet.Type = topodatapb.TabletType_PRIMARY
		tablet.PrimaryTermStartTime = protoutil.TimeToProto(curPrimaryTermStart.Add(-1 * time.Minute))
		return nil
	})
	require.NoError(t, err)

	// Expect VTOrc to demote the stale primary to a read-only replica.
	require.Eventuallyf(t, func() bool {
		topoTablet, topoErr := getTabletRecord(ctx, vc.cluster, badPrimary.Alias())
		if topoErr != nil {
			t.Logf("stale primary probe: topo error=%v", topoErr)
			return false
		}

		readOnly := getDBVar(ctx, t, badPrimary, "read_only")
		return topoTablet.Type == topodatapb.TabletType_REPLICA && readOnly == "ON"
	}, 30*time.Second, time.Second, "expected demotion to REPLICA with read_only=ON")
}

// TestSemiSync tests that semi-sync is setup correctly by vtorc if it is incorrectly set
func TestSemiSync(t *testing.T) {
	newCluster := setupNewClusterSemiSync(t)
	newCluster.startVTOrcs(t, nil, true, 1)

	// find primary from topo
	primary := shardPrimaryTablet(t, newCluster, keyspaceName, shardName, newCluster.active)
	assert.NotNil(t, primary, "should have elected a primary")

	var replica1, replica2, rdonly *vitesst.Tablet
	for _, tablet := range newCluster.active {
		if tablet.Alias() == primary.Alias() {
			continue
		}
		if tablet.Type() == "rdonly" {
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
	checkReplication(t, newCluster, primary, []*vitesst.Tablet{rdonly, replica1, replica2}, 10*time.Second)

	semisyncType, err := semiSyncExtensionLoaded(t.Context(), replica1)
	require.NoError(t, err)
	switch semisyncType {
	case mysql.SemiSyncTypeSource:
		_, err := runSQL(t.Context(), t, "SET GLOBAL rpl_semi_sync_replica_enabled = 0", replica1, "")
		require.NoError(t, err)
	case mysql.SemiSyncTypeMaster:
		_, err := runSQL(t.Context(), t, "SET GLOBAL rpl_semi_sync_slave_enabled = 0", replica1, "")
		require.NoError(t, err)
	default:
		require.Fail(t, "unexpected semi-sync type %v", semisyncType)
	}

	semisyncType, err = semiSyncExtensionLoaded(t.Context(), rdonly)
	require.NoError(t, err)
	switch semisyncType {
	case mysql.SemiSyncTypeSource:
		_, err := runSQL(t.Context(), t, "SET GLOBAL rpl_semi_sync_replica_enabled = 0", rdonly, "")
		require.NoError(t, err)
	case mysql.SemiSyncTypeMaster:
		_, err := runSQL(t.Context(), t, "SET GLOBAL rpl_semi_sync_slave_enabled = 0", rdonly, "")
		require.NoError(t, err)
	default:
		require.Fail(t, "unexpected semi-sync type %v", semisyncType)
	}

	semisyncType, err = semiSyncExtensionLoaded(t.Context(), primary)
	require.NoError(t, err)
	switch semisyncType {
	case mysql.SemiSyncTypeSource:
		_, err := runSQL(t.Context(), t, "SET GLOBAL rpl_semi_sync_source_enabled = 0", primary, "")
		require.NoError(t, err)
	case mysql.SemiSyncTypeMaster:
		_, err := runSQL(t.Context(), t, "SET GLOBAL rpl_semi_sync_master_enabled = 0", primary, "")
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
			if isSemiSyncSetupCorrectly(t, replica1, "ON") &&
				isSemiSyncSetupCorrectly(t, rdonly, "OFF") &&
				isPrimarySemiSyncSetupCorrectly(t, primary, "ON") {
				return
			}
			time.Sleep(1 * time.Second)
		}
	}
}

// TestVTOrcWithPrs tests that VTOrc works fine even when PRS is called from vtctld
func TestVTOrcWithPrs(t *testing.T) {
	vc := setupVtorcCluster(t)
	vc.setupVttabletsAndVTOrcs(t, 4, 0, nil, true, 1, "")

	// find primary from topo
	curPrimary := shardPrimaryTablet(t, vc, keyspaceName, shardName, vc.active)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	vtOrcProcess := vc.vtorcs[0]
	waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, keyspaceName, shardName, 1)
	waitForSuccessfulPRSCount(t, vtOrcProcess, keyspaceName, shardName, 1)

	// find any replica tablet other than the current primary
	var replica *vitesst.Tablet
	for _, tablet := range vc.active {
		if tablet.Alias() != curPrimary.Alias() {
			replica = tablet
			break
		}
	}
	assert.NotNil(t, replica, "could not find any replica tablet")

	// check that the replication is setup correctly before we failover
	checkReplication(t, vc, curPrimary, vc.active, 10*time.Second)

	output, err := vc.cluster.Vtctld().ExecuteCommandWithOutput(
		t.Context(),
		"PlannedReparentShard",
		fmt.Sprintf("%s/%s", keyspaceName, shardName),
		"--wait-replicas-timeout", "31s",
		"--new-primary", replica.Alias(),
	)
	require.NoError(t, err, "error in PlannedReparentShard output - %s", output)

	// check that the replica gets promoted
	checkPrimaryTablet(t, vc, replica, true)
	// Verify that VTOrc didn't run any other recovery
	waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, keyspaceName, shardName, 1)
	waitForSuccessfulPRSCount(t, vtOrcProcess, keyspaceName, shardName, 1)
	waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.RecoverDeadPrimaryRecoveryName, keyspaceName, shardName, 0)
	waitForSuccessfulERSCount(t, vtOrcProcess, keyspaceName, shardName, 0)
	waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixPrimaryRecoveryName, keyspaceName, shardName, 0)
	waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.FixReplicaRecoveryName, keyspaceName, shardName, 0)
	waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.RecoverPrimaryHasPrimaryRecoveryName, keyspaceName, shardName, 0)
	verifyWritesSucceed(t, vc, replica, vc.active, 10*time.Second)
}

// TestMultipleDurabilities tests that VTOrc works with 2 keyspaces having 2 different durability policies
func TestMultipleDurabilities(t *testing.T) {
	vc := setupVtorcCluster(t)
	// Setup a normal cluster and start vtorc
	vc.setupVttabletsAndVTOrcs(t, 1, 1, nil, false, 1, "")
	// Setup a semi-sync cluster
	keyspaceSemiSync := addSemiSyncKeyspace(t, vc)

	checkPrimaryTablet(t, vc, vc.active[0], true)
	checkReplication(t, vc, vc.active[0], vc.active[1:], 10*time.Second)

	// find primary from topo
	primary := shardPrimaryTablet(t, vc, keyspaceSemiSync.Name, shardName, keyspaceSemiSync.Shard(shardName).Tablets())
	assert.NotNil(t, primary, "should have elected a primary")
}

// TestDrainedTablet tests that we don't forget drained tablets and they still show up in the vtorc output.
func TestDrainedTablet(t *testing.T) {
	vc := setupVtorcCluster(t)

	// Setup a normal cluster and start vtorc
	vc.setupVttabletsAndVTOrcs(t, 2, 0, nil, false, 1, "")

	// find primary from topo
	curPrimary := shardPrimaryTablet(t, vc, keyspaceName, shardName, vc.active)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	vtOrcProcess := vc.vtorcs[0]

	// find any replica tablet other than the current primary
	var replica *vitesst.Tablet
	for _, tablet := range vc.active {
		if tablet.Alias() != curPrimary.Alias() {
			replica = tablet
			break
		}
	}
	require.NotNil(t, replica, "could not find any replica tablet")

	output, err := vc.cluster.Vtctld().ExecuteCommandWithOutput(
		t.Context(),
		"ChangeTabletType", replica.Alias(), "DRAINED",
	)
	require.NoError(t, err, "error in changing tablet type output - %s", output)

	// Make sure VTOrc sees the drained tablets and doesn't forget them.
	waitForDrainedTabletInVTOrc(t, vtOrcProcess, 1)

	output, err = vc.cluster.Vtctld().ExecuteCommandWithOutput(
		t.Context(),
		"ChangeTabletType", replica.Alias(), "REPLICA",
	)
	require.NoError(t, err, "error in changing tablet type output - %s", output)

	// We have no drained tablets anymore. Wait for VTOrc to have processed that.
	waitForDrainedTabletInVTOrc(t, vtOrcProcess, 0)
}

// TestDurabilityPolicySetLater tests that VTOrc works even if the durability policy of the keyspace is
// set after VTOrc has been started.
func TestDurabilityPolicySetLater(t *testing.T) {
	newCluster := setupNewClusterSemiSync(t)

	// Before starting VTOrc we explicity want to set the durability policy of the keyspace to an empty string
	func() {
		ctx, unlock, lockErr := newCluster.ts.LockKeyspace(t.Context(), keyspaceName, "TestDurabilityPolicySetLater")
		require.NoError(t, lockErr)
		defer unlock(&lockErr)
		ki, err := newCluster.ts.GetKeyspace(ctx, keyspaceName)
		require.NoError(t, err)
		ki.DurabilityPolicy = ""
		err = newCluster.ts.UpdateKeyspace(ctx, ki)
		require.NoError(t, err)
	}()

	// Verify that the durability policy is indeed empty
	ki, err := newCluster.ts.GetKeyspace(t.Context(), keyspaceName)
	require.NoError(t, err)
	require.Empty(t, ki.DurabilityPolicy)

	// Now start the vtorc instances
	newCluster.startVTOrcs(t, nil, true, 1)

	// Wait for some time to be sure that VTOrc has started.
	// TODO(GuptaManan100): Once we have a debug page for VTOrc, use that instead
	time.Sleep(30 * time.Second)

	// Now set the correct durability policy
	out, err := newCluster.cluster.Vtctld().ExecuteCommandWithOutput(t.Context(), "SetKeyspaceDurabilityPolicy", keyspaceName, "--durability-policy=semi_sync")
	require.NoError(t, err, out)

	// VTOrc should promote a new primary after seeing the durability policy change
	primary := shardPrimaryTablet(t, newCluster, keyspaceName, shardName, newCluster.active)
	assert.NotNil(t, primary, "should have elected a primary")
	checkReplication(t, newCluster, primary, newCluster.active, 10*time.Second)
}

func TestFullStatusConnectionPooling(t *testing.T) {
	vc := setupVtorcCluster(t)
	vc.setupVttabletsAndVTOrcs(t, 4, 0, []string{
		"--tablet-manager-grpc-concurrency" + "=1",
	}, true, 1, "")
	vtorc := vc.vtorcs[0]

	// find primary from topo
	curPrimary := shardPrimaryTablet(t, vc, keyspaceName, shardName, vc.active)
	assert.NotNil(t, curPrimary, "should have elected a primary")
	vtOrcProcess := vc.vtorcs[0]
	waitForSuccessfulRecoveryCount(t, vtOrcProcess, logic.ElectNewPrimaryRecoveryName, keyspaceName, shardName, 1)
	waitForSuccessfulPRSCount(t, vtOrcProcess, keyspaceName, shardName, 1)

	// Kill the current primary.
	_ = curPrimary.KillVttablet(t.Context())

	// Wait until VTOrc notices the failure.
	require.Eventually(t, func() bool {
		status, resp, err := makeAPICall(t, vtorc, "/api/detection-analysis")
		return err == nil && status == 200 && strings.Contains(resp, "UnreachablePrimary")
	}, 90*time.Second, time.Second, "timed out waiting for UnreachablePrimary analysis")

	// Restart the primary's vttablet.
	err := curPrimary.StartVttablet(t.Context())
	require.NoError(t, err)

	// See that VTOrc eventually reports no errors.
	require.Eventually(t, func() bool {
		status, resp, err := makeAPICall(t, vtorc, "/api/detection-analysis")
		return err == nil && status == 200 && strings.TrimSpace(resp) == "null"
	}, 90*time.Second, time.Second, "timed out waiting for replication analysis to clear")

	// REPEATED
	// Kill the current primary.
	_ = curPrimary.KillVttablet(t.Context())

	// Wait until VTOrc notices the failure.
	require.Eventually(t, func() bool {
		status, resp, err := makeAPICall(t, vtorc, "/api/detection-analysis")
		return err == nil && status == 200 && strings.Contains(resp, "UnreachablePrimary")
	}, 90*time.Second, time.Second, "timed out waiting for UnreachablePrimary analysis")

	// Restart the primary's vttablet again.
	err = curPrimary.StartVttablet(t.Context())
	require.NoError(t, err)

	// See that VTOrc eventually reports no errors.
	require.Eventually(t, func() bool {
		status, resp, err := makeAPICall(t, vtorc, "/api/detection-analysis")
		return err == nil && status == 200 && strings.TrimSpace(resp) == "null"
	}, 90*time.Second, time.Second, "timed out waiting for replication analysis to clear")
}

// TestSemiSyncRecoveryOrdering verifies that when the durability policy changes
// to semi_sync, VTOrc fixes ReplicaSemiSyncMustBeSet before PrimarySemiSyncMustBeSet.
// This ordering is enforced by the AfterAnalyses/BeforeAnalyses dependencies.
func TestSemiSyncRecoveryOrdering(t *testing.T) {
	vc := setupVtorcCluster(t)
	// Start with durability "none" so no semi-sync is required initially.
	vc.setupVttabletsAndVTOrcs(t, 2, 0, nil, true, 1, policy.DurabilityNone)

	// Wait for primary election and healthy replication.
	primary := shardPrimaryTablet(t, vc, keyspaceName, shardName, vc.active)
	assert.NotNil(t, primary, "should have elected a primary")
	checkReplication(t, vc, primary, vc.active, 10*time.Second)

	vtorc := vc.vtorcs[0]
	waitForSuccessfulRecoveryCount(t, vtorc, logic.ElectNewPrimaryRecoveryName, keyspaceName, shardName, 1)

	// Change durability to semi_sync. VTOrc should detect that replicas and primary
	// need semi-sync enabled, and fix them in the correct order.
	out, err := vc.cluster.Vtctld().ExecuteCommandWithOutput(
		t.Context(),
		"SetKeyspaceDurabilityPolicy", keyspaceName, "--durability-policy="+policy.DurabilitySemiSync,
	)
	require.NoError(t, err, out)

	// Poll the database-state API to verify recovery ordering.
	// The topology_recovery table has auto-incremented recovery_id values that
	// reflect execution order. All ReplicaSemiSyncMustBeSet recovery_ids should
	// be less than any PrimarySemiSyncMustBeSet recovery_id.
	type tableState struct {
		TableName string
		Rows      []map[string]any
	}

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		status, response, err := makeAPICall(t, vtorc, "/api/database-state")
		assert.NoError(c, err)
		assert.Equal(c, 200, status)

		var tables []tableState
		if !assert.NoError(c, json.Unmarshal([]byte(response), &tables)) {
			return
		}

		var maxReplicaRecoveryID, minPrimaryRecoveryID int
		var replicaCount, primaryCount int
		for _, table := range tables {
			if table.TableName != "topology_recovery" {
				continue
			}
			for _, row := range table.Rows {
				analysis, _ := row["analysis"].(string)
				recoveryIDStr, _ := row["recovery_id"].(string)
				recoveryID, err := strconv.Atoi(recoveryIDStr)
				if err != nil {
					continue
				}
				switch inst.AnalysisCode(analysis) {
				case inst.ReplicaSemiSyncMustBeSet:
					replicaCount++
					if replicaCount == 1 || recoveryID > maxReplicaRecoveryID {
						maxReplicaRecoveryID = recoveryID
					}
				case inst.PrimarySemiSyncMustBeSet:
					primaryCount++
					if primaryCount == 1 || recoveryID < minPrimaryRecoveryID {
						minPrimaryRecoveryID = recoveryID
					}
				}
			}
		}

		assert.Greater(c, replicaCount, 0, "should have ReplicaSemiSyncMustBeSet recoveries")
		assert.Greater(c, primaryCount, 0, "should have PrimarySemiSyncMustBeSet recoveries")
		if replicaCount > 0 && primaryCount > 0 {
			assert.Less(c, maxReplicaRecoveryID, minPrimaryRecoveryID,
				"all ReplicaSemiSyncMustBeSet recoveries should have lower recovery_id than PrimarySemiSyncMustBeSet")
		}
	}, 30*time.Second, time.Second)
}

// TestReplicationStoppedWithSemiSyncBlocked verifies that VTOrc restarts
// replication on a semi-sync acker replica under semi_sync durability.
// This is a regression test for a deadlock where recheckPrimaryHealth
// would abort fixReplica because the primary had PrimarySemiSyncBlocked,
// but the primary problem existed because replicas weren't replicating.
// The precise deadlock timing (blocked write + simultaneous detection) is
// covered by unit tests; this test exercises the acker detection path and
// the suppression bypass end-to-end.
// See https://github.com/vitessio/vitess/issues/19921.
func TestReplicationStoppedWithSemiSyncBlocked(t *testing.T) {
	vc := setupVtorcCluster(t)
	// 2 REPLICA (semi-sync ackers) + 1 RDONLY (not an acker).
	// The RDONLY is included to verify semi-sync acker detection
	// is selective — only the REPLICA acker's replication is stopped.
	vc.setupVttabletsAndVTOrcs(t, 2, 1, nil, true, 1, policy.DurabilitySemiSync)

	primary := shardPrimaryTablet(t, vc, keyspaceName, shardName, vc.active)
	require.NotNil(t, primary, "should have elected a primary")
	vtorc := vc.vtorcs[0]
	waitForSuccessfulRecoveryCount(t, vtorc, logic.ElectNewPrimaryRecoveryName, keyspaceName, shardName, 1)

	// Identify the replica (acker) and rdonly (non-acker).
	// One of the 2 replicas was elected primary, so 1 replica remains.
	var replica, rdonly *vitesst.Tablet
	for _, tablet := range vc.active {
		if tablet.Alias() == primary.Alias() {
			continue
		}
		if tablet.Type() == "rdonly" {
			rdonly = tablet
		} else {
			replica = tablet
		}
	}
	require.NotNil(t, replica, "should have a REPLICA tablet")
	require.NotNil(t, rdonly, "should have an RDONLY tablet")

	allNonPrimary := []*vitesst.Tablet{replica, rdonly}
	checkReplication(t, vc, primary, allNonPrimary, 15*time.Second)

	// Verify semi-sync acker status after VTOrc has had time to converge:
	// replica ON, rdonly OFF.
	assert.Eventually(t, func() bool {
		return isSemiSyncSetupCorrectly(t, replica, "ON")
	}, 15*time.Second, 500*time.Millisecond, "REPLICA should be a semi-sync acker")
	assert.Eventually(t, func() bool {
		return isSemiSyncSetupCorrectly(t, rdonly, "OFF")
	}, 15*time.Second, 500*time.Millisecond, "RDONLY should not be a semi-sync acker")

	// Snapshot the FixReplica counter before stopping replication.
	// VTOrc may have already incremented it during setup (e.g., for
	// ReplicaSemiSyncMustBeSet under semi_sync durability).
	fixReplicaCount := getSuccessfulRecoveryCount(t, vtorc, logic.FixReplicaRecoveryName, keyspaceName, shardName)

	// Stop replication on the semi-sync acker (REPLICA). VTOrc should
	// detect ReplicationStopped and fix it. The GetDetectionAnalysis
	// change ensures the acker's analysis is not suppressed by
	// hasShardWideAction even if PrimarySemiSyncBlocked is also present.
	//
	// Note: we cannot reliably assert PrimarySemiSyncBlocked is detected
	// in this test because it requires a blocked write (SemiSyncBlocked
	// only flips when a write is waiting for acks), and VTOrc fixes the
	// replica faster than we can create the blocking condition. The
	// deadlock scenario is covered by unit tests in analysis_dao_test.go
	// (TestDeclaresBefore) and topology_recovery_test.go
	// (TestRecheckPrimaryHealth).
	_, err := runSQL(t.Context(), t, "STOP REPLICA", replica, "")
	require.NoError(t, err)

	// Wait for VTOrc to record at least one new FixReplica recovery before
	// checking replication. Use > instead of exact match because concurrent
	// fix-replica recoveries (e.g., semi-sync related) can also increment
	// this metric.
	assert.Eventually(t, func() bool {
		return getSuccessfulRecoveryCount(t, vtorc, logic.FixReplicaRecoveryName, keyspaceName, shardName) > fixReplicaCount
	}, 30*time.Second, time.Second)
	checkReplication(t, vc, primary, allNonPrimary, 30*time.Second)
}

// TestRecoveryDeadlocks exercises the `BeforeAnalyses` suppression mechanism
// added by #19925 and extended by #20015 end-to-end: when a tablet-level
// problem coexists with a shard-wide reachable-but-unhealthy primary problem,
// VTOrc must dispatch the tablet-level recovery first and must NOT dispatch
// ERS for the shard-wide problem.
//
// Pre-#19925/#20015, the shard-wide problem caused `recheckPrimaryHealth`
// to abort the tablet-level recovery mid-flight, so the
// `SuccessfulRecoveries[FixPrimary|FixReplica]` counter never incremented.
// The fixes route the tablet-level recovery first via `BeforeAnalyses`, so
// the counter ticks even while the shard-wide problem still exists.
//
// Coverage:
//
//   - `PrimaryIsReadOnly × PrimarySemiSyncBlocked` (covered here, this is
//     the customer-facing scenario from issue #20011).
//
//   - `PrimaryIsReadOnly × PrimaryDiskStalled` and
//     `ReplicationStopped × PrimaryDiskStalled` (NOT covered here). Both
//     pairings need `PrimaryDiskStalled` to fire, which requires
//     `!LastCheckValid && IsDiskStalled` simultaneously. Synthetic fault
//     injection (e.g. `chmod 000` on a probe dir) flips `IsDiskStalled` but
//     not `LastCheckValid` — the matcher does not match. Anything that
//     flips `LastCheckValid` (pausing vttablet, killing mysqld) also breaks
//     `fixPrimary`'s ability to run, so the assertion can't succeed either.
//     The ordering logic is identical to pair 1 (same `BeforeAnalyses`
//     bypass code path); coverage for these two pairings comes from unit
//     tests in `analysis_dao_test.go` (`TestDeclaresBefore`,
//     `TestDeclaresAfter`) and `topology_recovery_test.go`
//     (`TestRecheckPrimaryHealth`).
//
//   - `ReplicationStopped × PrimarySemiSyncBlocked` (#19925's pairing) is
//     covered separately by `TestReplicationStoppedWithSemiSyncBlocked`.
//
// The narrow window in which both problems coexist is ~1–2 analysis cycles
// (long enough for VTOrc to detect both and dispatch recovery once); we do
// not require sustained co-occurrence.
func TestRecoveryDeadlocks(t *testing.T) {
	vc := setupVtorcCluster(t)
	t.Run("PrimaryIsReadOnly+PrimarySemiSyncBlocked", func(t *testing.T) {
		disableSemiSyncOnAllTablets(t, vc)
		vc.setupVttabletsAndVTOrcs(t, 2, 1, nil, true, 1, policy.DurabilitySemiSync)
		primary, replica, _ := waitForPrimaryAndPick(t, vc)
		vtorc := vc.vtorcs[0]
		waitForSuccessfulRecoveryCount(t, vtorc, logic.ElectNewPrimaryRecoveryName, keyspaceName, shardName, 1)

		fixPrimaryBefore := getSuccessfulRecoveryCount(t, vtorc, logic.FixPrimaryRecoveryName, keyspaceName, shardName)
		ersBefore := getSuccessfulRecoveryCount(t, vtorc, logic.RecoverDeadPrimaryRecoveryName, keyspaceName, shardName)

		// Stop the acker's IO thread so semi-sync ACKs cannot flow.
		_, err := runSQL(t.Context(), t, "STOP REPLICA IO_THREAD", replica, "")
		require.NoError(t, err)

		// Issue a write that will block on the semi-sync wait. The connection
		// will return only once fixReplica restarts the acker's IO thread,
		// after which an ACK flows and the write commits.
		//
		// Note: we cannot reliably assert PrimarySemiSyncBlocked is detected
		// in this test (same caveat as TestReplicationStoppedWithSemiSyncBlocked).
		// SemiSyncBlocked only flips when a write is waiting for acks, and
		// VTOrc fixes the replica faster than we can sustain the blocking
		// condition. The deadlock scenario is covered by unit tests in
		// analysis_dao_test.go (TestDeclaresBefore) and
		// topology_recovery_test.go (TestRecheckPrimaryHealth).
		// The blocking write and the cleanup that unblocks it outlive the
		// test body, so they run under a context that ignores the test's
		// cancellation.
		bgCtx := context.WithoutCancel(t.Context())
		var wg sync.WaitGroup
		wg.Go(func() {
			_, _ = runSQL(bgCtx, t, "CREATE TABLE IF NOT EXISTS test_recovery_deadlocks (id INT PRIMARY KEY)", primary, "vt_ks")
		})
		t.Cleanup(func() {
			// Defensively unblock the goroutine if the test fails before
			// the cluster recovers naturally.
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
			defer cancel()
			_, _ = runSQL(cleanupCtx, t, "SET GLOBAL super_read_only = OFF", primary, "")
			_, _ = runSQL(cleanupCtx, t, "START REPLICA", replica, "")

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-cleanupCtx.Done():
				t.Logf("timed out waiting for blocked write cleanup: %v", cleanupCtx.Err())
			}
		})

		// Set the primary read-only while the write is hanging.
		_, err = runSQL(t.Context(), t, "SET GLOBAL super_read_only = ON", primary, "")
		require.NoError(t, err)

		// PrimaryIsReadOnly is detected within ~1 analysis cycle.
		waitForDetectedProblems(t, vtorc, string(inst.PrimaryIsReadOnly), canonicalAlias(primary), keyspaceName, shardName, 1)

		// fixPrimary must complete despite the shard-wide problem also being
		// present. Pre-fix this counter never increments because
		// recheckPrimaryHealth aborts the recovery mid-flight.
		waitForSuccessfulRecoveryCount(t, vtorc, logic.FixPrimaryRecoveryName, keyspaceName, shardName, fixPrimaryBefore+1)

		// ERS must not have been dispatched.
		ersAfter := getSuccessfulRecoveryCount(t, vtorc, logic.RecoverDeadPrimaryRecoveryName, keyspaceName, shardName)
		assert.Equal(t, ersBefore, ersAfter, "ERS should not have been dispatched")

		// Primary should no longer be read-only.
		assert.True(t, waitForReadOnlyValue(t, primary, 0))
	})
}

// disableSemiSyncOnAllTablets clears `rpl_semi_sync_source_enabled` and
// `rpl_semi_sync_replica_enabled` on every tablet's mysqld in the shared
// cluster. Required before setupVttabletsAndVTOrcs when semi-sync is enabled
// left a primary with semi-sync source on: the tablet restart issues a DROP
// DATABASE before restarting vttablet, which hangs forever in the semi-sync
// wait because no acker is connected.
//
// Connects directly (not runSQL) so that tablets whose mysqld is not
// reachable are silently skipped rather than failing the test.
func disableSemiSyncOnAllTablets(t *testing.T, vc *vtorcCluster) {
	t.Helper()
	ctx := t.Context()
	for _, tablet := range append(append([]*vitesst.Tablet{}, vc.replicaPool...), vc.rdonlyPool...) {
		conn := connectTablet(ctx, tablet, "")
		if conn == nil {
			continue
		}
		_, _ = conn.ExecuteFetch("SET GLOBAL rpl_semi_sync_source_enabled = 0, GLOBAL rpl_semi_sync_replica_enabled = 0", 1, false)
		conn.Close()
	}
}

// waitForPrimaryAndPick blocks until VTOrc has elected a primary and returns
// it along with the surviving REPLICA (semi-sync acker) and RDONLY tablets.
func waitForPrimaryAndPick(t *testing.T, vc *vtorcCluster) (primary, replica, rdonly *vitesst.Tablet) {
	t.Helper()
	primary = shardPrimaryTablet(t, vc, keyspaceName, shardName, vc.active)
	require.NotNil(t, primary, "should have elected a primary")
	for _, tablet := range vc.active {
		if tablet.Alias() == primary.Alias() {
			continue
		}
		if tablet.Type() == "rdonly" {
			rdonly = tablet
		} else {
			replica = tablet
		}
	}
	require.NotNil(t, replica, "should have a REPLICA tablet")
	require.NotNil(t, rdonly, "should have an RDONLY tablet")
	return primary, replica, rdonly
}
