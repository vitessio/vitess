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
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// TestRecoverWithMultipleVttabletFailures tests that ERS succeeds with the default values
// even when there are multiple vttablet failures. In this test we use the semi_sync policy
// to allow multiple failures to happen and still be recoverable.
// The test takes down the vttablets of the primary and a rdonly tablet and runs ERS with the
// default values of remote-operation-timeout, lock-timeout flags and wait_replicas_timeout subflag.
func TestRecoverWithMultipleVttabletFailures(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)
	ConfirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

	// make tablets[1] a rdonly tablet.
	err := clusterInstance.Vtctld().ExecuteCommand(t.Context(), "ChangeTabletType", tablets[1].Alias(), "rdonly")
	require.NoError(t, err)

	// Confirm that replication is still working as intended
	ConfirmReplication(t, tablets[0], tablets[1:])

	// Make the rdonly and primary tablets and databases unavailable.
	StopTablet(t, tablets[1], true)
	StopTablet(t, tablets[0], true)

	// We expect this to succeed since we only have 1 primary eligible tablet which is down
	out, err := Ers(t.Context(), clusterInstance, nil, "", "")
	require.NoError(t, err, out)

	newPrimary := GetNewPrimary(t, clusterInstance)
	ConfirmReplication(t, newPrimary, []*vitesst.Tablet{tablets[2], tablets[3]})
}

// TetsSingeReplicaERS tests that ERS works even when there is only 1 tablet left
// as long the durability policy allows this failure. Moreover, this also tests that the
// replica is one such that it was a primary itself before. This way its executed gtid set
// will have atleast 2 tablets in it. We want to make sure this tablet is not marked as errant
// and ERS succeeds.
func TestSingleReplicaERS(t *testing.T) {
	// Set up a cluster with none durability policy
	clusterInstance := SetupReparentCluster(t, policy.DurabilityNone)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)
	// Confirm that the replication is setup correctly in the beginning.
	// tablets[0] is the primary tablet in the beginning.
	ConfirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

	// Delete and stop two tablets. We only want to have 2 tablets for this test.
	DeleteTablet(t, clusterInstance, tablets[2])
	DeleteTablet(t, clusterInstance, tablets[3])
	StopTablet(t, tablets[2], true)
	StopTablet(t, tablets[3], true)

	// Reparent to the other replica
	output, err := Prs(t, clusterInstance, tablets[1])
	require.NoError(t, err, "error in PlannedReparentShard output - %s", output)

	// Check the replication is set up correctly before we failover
	ConfirmReplication(t, tablets[1], []*vitesst.Tablet{tablets[0]})

	// Make the current primary vttablet unavailable.
	StopTablet(t, tablets[1], true)

	// Run an ERS with only one replica reachable. Also, this replica is such that it was a primary before.
	output, err = Ers(t.Context(), clusterInstance, tablets[0], "", "")
	require.NoError(t, err, "error in Emergency Reparent Shard output - %s", output)

	// Check the tablet is indeed promoted
	CheckPrimaryTablet(t, clusterInstance, tablets[0])
	// Also check the writes succeed after failover
	ConfirmReplication(t, tablets[0], []*vitesst.Tablet{})
}

// TestTabletRestart tests that a running tablet can be  restarted and everything is still fine
func TestTabletRestart(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)

	StopTablet(t, tablets[1], false)
	restartTablet(t, tablets[1])
}

// Tests ensures that ChangeTabletType works even when semi-sync plugins are not loaded.
func TestChangeTypeWithoutSemiSync(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilityNone)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)

	ctx := t.Context()

	primary, replica := tablets[0], tablets[1]

	// Unload semi sync plugins
	for _, tablet := range tablets[0:4] {
		qr := RunSQL(ctx, t, "select @@global.super_read_only", tablet)
		result := qr.Rows[0][0].ToString()
		if result == "1" {
			RunSQL(ctx, t, "set global super_read_only = 0", tablet)
		}

		semisyncType, err := SemiSyncExtensionLoaded(ctx, tablet)
		require.NoError(t, err)
		switch semisyncType {
		case mysql.SemiSyncTypeSource:
			RunSQL(ctx, t, "UNINSTALL PLUGIN rpl_semi_sync_replica", tablet)
			RunSQL(ctx, t, "UNINSTALL PLUGIN rpl_semi_sync_source", tablet)
		case mysql.SemiSyncTypeMaster:
			RunSQL(ctx, t, "UNINSTALL PLUGIN rpl_semi_sync_slave", tablet)
			RunSQL(ctx, t, "UNINSTALL PLUGIN rpl_semi_sync_master", tablet)
		default:
			require.Fail(t, "Unknown semi sync type")
		}
	}

	ValidateTopology(t, clusterInstance, true)
	CheckPrimaryTablet(t, clusterInstance, primary)

	// Change replica's type to rdonly
	err := clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", replica.Alias(), "rdonly")
	require.NoError(t, err)

	// Change tablets type from rdonly back to replica
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", replica.Alias(), "replica")
	require.NoError(t, err)
}

// TestERSWithWriteInPromoteReplica tests that ERS doesn't fail even if there is a
// write that happens when PromoteReplica is called.
func TestERSWithWriteInPromoteReplica(t *testing.T) {
	clusterInstance := SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)
	ConfirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

	// Drop a table so that when sidecardb changes are checked, we run a DML query.
	RunSQLs(t.Context(), t, []string{
		"set sql_log_bin=0",
		`SET @@global.super_read_only=0`,
		`DROP TABLE _vt.heartbeat`,
		"set sql_log_bin=1",
	}, tablets[3])
	_, err := Ers(t.Context(), clusterInstance, tablets[3], "60s", "30s")
	require.NoError(t, err, "ERS should not fail even if there is a sidecardb change")
}

func TestBufferingWithMultipleDisruptions(t *testing.T) {
	clusterInstance := SetupShardedReparentCluster(t, policy.DurabilitySemiSync, nil)
	defer TeardownCluster(t, clusterInstance)

	ctx := t.Context()

	// Stop all VTOrc instances, so that they don't interfere with the test.
	for _, vtorc := range clusterInstance.VTOrcs() {
		err := vtorc.StopContainer(ctx, 30*time.Second)
		require.NoError(t, err)
	}

	// Start by reparenting all the shards to the first tablet.
	keyspace := clusterInstance.Keyspace(KeyspaceName)
	shards := keyspace.Shards()
	for _, shard := range shards {
		err := PlannedReparentShard(ctx, clusterInstance, keyspace.Name, shard.Name, shard.Tablets()[0].Alias())
		require.NoError(t, err)
	}

	// We simulate start of external reparent or a PRS where the healthcheck update from the tablet gets lost in transit
	// to vtgate by just setting the primary read only. This is also why we needed to shutdown all VTOrcs, so that they don't
	// fix this.
	RunSQL(ctx, t, "set global read_only=1", shards[0].Tablets()[0])
	RunSQL(ctx, t, "set global read_only=1", shards[1].Tablets()[0])

	wg := sync.WaitGroup{}
	rowCount := 10
	vtParams := clusterInstance.VTParams(ctx, "")
	// We now spawn writes for a bunch of go routines.
	// The ones going to shard 1 and shard 2 should block, since
	// they're in the midst of a reparenting operation (as seen by the buffering code).
	for i := 1; i <= rowCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := mysql.Connect(t.Context(), &vtParams)
			if err != nil {
				return
			}
			defer conn.Close()
			_, err = conn.ExecuteFetch(GetInsertQuery(i), 0, false)
			if !assert.NoError(t, err) {
				return
			}
		}(i)
	}

	// Now, run a PRS call on the last shard. This shouldn't unbuffer the queries that are buffered for shards 1 and 2
	// since the disruption on the two shards hasn't stopped.
	err := PlannedReparentShard(ctx, clusterInstance, keyspace.Name, shards[2].Name, shards[2].Tablets()[1].Alias())
	require.NoError(t, err)
	// We wait a second just to make sure the PRS changes are processed by the buffering logic in vtgate.
	time.Sleep(1 * time.Second)
	// Finally, we'll now make the 2 shards healthy again by running PRS.
	err = PlannedReparentShard(ctx, clusterInstance, keyspace.Name, shards[0].Name, shards[0].Tablets()[1].Alias())
	require.NoError(t, err)
	err = PlannedReparentShard(ctx, clusterInstance, keyspace.Name, shards[1].Name, shards[1].Tablets()[1].Alias())
	require.NoError(t, err)
	// Wait for all the writes to have succeeded.
	wg.Wait()
}

// TestSemiSyncBlockDueToDisruption tests that Vitess can recover from a situation
// where a primary is stuck waiting for semi-sync ACKs due to a network issue,
// even if no new writes from the user arrives.
func TestSemiSyncBlockDueToDisruption(t *testing.T) {
	// This is always set to "true" on GitHub Actions runners:
	// https://docs.github.com/en/actions/learn-github-actions/variables#default-environment-variables
	ci, ok := os.LookupEnv("CI")
	if ok && strings.ToLower(ci) == "true" {
		t.Skip("Test not meant to be run on CI")
	}
	clusterInstance := SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)
	ConfirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

	// stop heartbeats on all the replicas
	for idx, tablet := range tablets {
		if idx == 0 {
			continue
		}
		RunSQLs(t.Context(), t, []string{
			"stop slave;",
			"change master to MASTER_HEARTBEAT_PERIOD = 0;",
			"start slave;",
		}, tablet)
	}

	// Disrupt the network between the primary and the replicas by detaching the
	// replicas from the cluster network, so the primary can no longer receive
	// their semi-sync ACKs.
	for idx, tablet := range tablets {
		if idx == 0 {
			continue
		}
		require.NoError(t, tablet.DisconnectNetwork(t.Context()))
	}

	// Start a write that will be blocked by the primary waiting for semi-sync ACKs
	ch := make(chan any)
	go func() {
		defer func() {
			close(ch)
		}()
		ConfirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]}) //nolint:testifylint
	}()

	// Starting VTOrc later now, because we don't want it to fix the heartbeat interval
	// on the replica's before the disruption has been introduced.
	_, err := clusterInstance.AddVTOrc(t.Context(), cell1)
	require.NoError(t, err)
	go func() {
		for {
			select {
			case <-ch:
				return
			case <-time.After(1 * time.Second):
				vars, err := tablets[0].GetVars(t.Context())
				if err != nil {
					continue
				}
				str, isPresent := vars["SemiSyncMonitorWritesBlocked"]
				if isPresent {
					t.Logf("SemiSyncMonitorWritesBlocked - %v", str)
				}
			}
		}
	}()
	// If the network disruption is too long lived, then we will end up running ERS from VTOrc.
	networkDisruptionDuration := 43 * time.Second
	time.Sleep(networkDisruptionDuration)

	// Restore the network
	for idx, tablet := range tablets {
		if idx == 0 {
			continue
		}
		require.NoError(t, tablet.ReconnectNetwork(t.Context()))
	}

	// We expect the problem to be resolved in less than 30 seconds.
	select {
	case <-time.After(30 * time.Second):
		assert.Fail(t, "Timed out waiting for semi-sync to be unblocked")
	case <-ch:
		t.Logf("Woohoo, write finished!")
	}
}
