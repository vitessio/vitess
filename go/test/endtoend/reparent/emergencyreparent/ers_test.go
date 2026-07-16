/*
Copyright 2019 The Vitess Authors.

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

package emergencyreparent

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestTrivialERS(t *testing.T) {
	ctx := t.Context()
	clusterInstance := setupReparentCluster(t, policy.DurabilitySemiSync)
	tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()

	confirmReplication(t, tablets[0], tablets[1:])

	// We should be able to do a series of ERS-es, even if nothing
	// is down, without issue
	for i := 1; i <= 4; i++ {
		out, err := ers(ctx, clusterInstance, nil, "60s", "30s")
		log.Info(fmt.Sprintf("ERS loop %d.  EmergencyReparentShard Output: %v", i, out))
		require.NoError(t, err)
		time.Sleep(5 * time.Second)
	}
	// We should do the same for vtctl binary
	for i := 1; i <= 4; i++ {
		out, err := ersWithVtctldClient(ctx, clusterInstance)
		log.Info(fmt.Sprintf("ERS-vtctldclient loop %d.  EmergencyReparentShard Output: %v", i, out))
		require.NoError(t, err)
		time.Sleep(5 * time.Second)
	}
}

func TestReparentIgnoreReplicas(t *testing.T) {
	clusterInstance := setupReparentCluster(t, policy.DurabilitySemiSync)
	tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()
	var err error

	ctx := t.Context()

	insertVal := confirmReplication(t, tablets[0], tablets[1:])

	// Make the current primary agent and database unavailable.
	stopTablet(ctx, t, tablets[0], true)

	// Take down a replica - this should cause the emergency reparent to fail.
	stopTablet(ctx, t, tablets[2], true)

	// We expect this one to fail because we have an unreachable replica
	out, err := ers(ctx, clusterInstance, nil, "60s", "30s")
	require.NotNil(t, err, out)

	// Now let's run it again, but set the command to ignore the unreachable replica.
	out, err = ersIgnoreTablet(ctx, clusterInstance, nil, "60s", "30s", []*vitesst.Tablet{tablets[2]}, false)
	require.Nil(t, err, out)

	// We'll bring back the replica we took down.
	restartTablet(ctx, t, tablets[2])

	// Check that old primary tablet is left around for human intervention.
	confirmOldPrimaryIsHangingAround(ctx, t, clusterInstance)
	deleteTablet(ctx, t, clusterInstance, tablets[0])
	validateTopology(ctx, t, clusterInstance, false)

	newPrimary := getNewPrimary(ctx, t, clusterInstance)
	// Check new primary has latest transaction.
	err = checkInsertedValues(ctx, t, newPrimary, insertVal)
	require.Nil(t, err)

	// bring back the old primary as a replica, check that it catches up
	resurrectTablet(ctx, t, tablets[0])
}

func TestReparentDownPrimary(t *testing.T) {
	clusterInstance := setupReparentCluster(t, policy.DurabilitySemiSync)
	tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()

	ctx := t.Context()

	// Make the current primary agent and database unavailable.
	stopTablet(ctx, t, tablets[0], true)

	// Perform a planned reparent operation, will try to contact
	// the current primary and fail somewhat quickly
	_, err := prsWithTimeout(ctx, clusterInstance, tablets[1], false, "1s", "5s")
	require.Error(t, err)

	validateTopology(ctx, t, clusterInstance, false)

	// Run forced reparent operation, this should now proceed unimpeded.
	out, err := ers(ctx, clusterInstance, tablets[1], "60s", "30s")
	log.Info(fmt.Sprintf("EmergencyReparentShard Output: %v", out))
	require.NoError(t, err)

	// Check that old primary tablet is left around for human intervention.
	confirmOldPrimaryIsHangingAround(ctx, t, clusterInstance)

	// Now we'll manually remove it, simulating a human cleaning up a dead primary.
	deleteTablet(ctx, t, clusterInstance, tablets[0])

	// Now validate topo is correct.
	validateTopology(ctx, t, clusterInstance, false)
	checkPrimaryTablet(ctx, t, clusterInstance, tablets[1])
	confirmReplication(t, tablets[1], []*vitesst.Tablet{tablets[2], tablets[3]})
	resurrectTablet(ctx, t, tablets[0])
}

func TestEmergencyReparentWithBlockedPrimary(t *testing.T) {
	clusterInstance := setupReparentCluster(t, policy.DurabilitySemiSync)

	ctx := t.Context()

	// start vtgate w/disabled buffering
	vtgate, err := clusterInstance.AddVTGate(ctx,
		"--enable-buffer=false",
		"--query-timeout", "3000")
	require.NoError(t, err)

	vtParams := clusterInstance.VTParams(ctx, "")
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("CREATE TABLE test (id INT PRIMARY KEY, msg VARCHAR(64))", 0, false)
	require.NoError(t, err)

	tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()

	// Simulate no semi-sync replicas being available by disabling semi-sync on all replicas
	for _, tablet := range tablets[1:] {
		runSQL(ctx, t, "STOP REPLICA IO_THREAD", tablet)

		// Disable semi-sync on replicas to simulate blocking
		semisyncType, err := semiSyncExtensionLoaded(t.Context(), tablet)
		require.NoError(t, err)
		switch semisyncType {
		case mysql.SemiSyncTypeSource:
			runSQL(t.Context(), t, "SET GLOBAL rpl_semi_sync_replica_enabled = false", tablet)
		case mysql.SemiSyncTypeMaster:
			runSQL(t.Context(), t, "SET GLOBAL rpl_semi_sync_slave_enabled = false", tablet)
		}

		runSQL(t.Context(), t, "START REPLICA IO_THREAD", tablet)
	}

	// Try performing a write and ensure that it blocks.
	writeSQL := `insert into test(id, msg) values (1, 'test 1')`
	wg := sync.WaitGroup{}
	wg.Go(func() {
		// Attempt writing via vtgate against the primary. This should block (because there's no replicas to ack the semi-sync),
		// and fail on the vtgate query timeout. Async replicas will still receive this write (probably), because it is written
		// to the PRIMARY binlog even when no ackers exist. This means we need to disable the vtgate buffer (above), because it
		// will attempt the write on the promoted, unblocked primary - and this will hit a dupe key error.
		_, err := conn.ExecuteFetch(writeSQL, 0, false)

		// The error here could be one of:
		// * target: ks.0.primary: vttablet: rpc error: code = DeadlineExceeded desc = context deadline exceeded (errno 1317) (sqlstate 70100) during query: insert into test(id, msg) values (1, 'test 1')
		// * target: ks.0.primary: vttablet: rpc error: code = DeadlineExceeded desc = stream terminated by RST_STREAM with error code: CANCEL (errno 1317) (sqlstate 70100) during query: insert into test(id, msg) values (1, 'test 1')
		// * ...
		//
		// So we only check for the part of the error message that's consistent.
		require.ErrorContains(t, err, "(errno 1317) (sqlstate 70100) during query: insert into test(id, msg) values (1, 'test 1')")

		// Verify vtgate really processed the insert in case something unrelated caused the deadline exceeded.
		vtgateVars, err := vtgate.GetVars(ctx)
		require.NoError(t, err)
		require.NotNil(t, vtgateVars)
		require.NotNil(t, vtgateVars["QueryRoutes"])
		require.NotNil(t, vtgateVars["VtgateApiErrorCounts"])
		require.EqualValues(t, map[string]any{
			"DDL.DirectDDL.PRIMARY":      float64(1),
			"INSERT.Passthrough.PRIMARY": float64(1),
		}, vtgateVars["QueryRoutes"])
		require.EqualValues(t, map[string]any{
			"Execute.ks.primary.DEADLINE_EXCEEDED": float64(1),
		}, vtgateVars["VtgateApiErrorCounts"])
	})

	wg.Add(1)
	waitReplicasTimeout := time.Second * 10
	go func() {
		defer wg.Done()

		// Ensure the write (other goroutine above) is blocked waiting on ACKs on the primary.
		waitForQueryWithStateInProcesslist(t.Context(), t, tablets[0], writeSQL, "Waiting for semi-sync ACK from replica", time.Second*20) //nolint:testifylint

		// Send SIGSTOP to primary to simulate it being unresponsive.
		require.NoError(t, tablets[0].FreezeVttablet(ctx)) //nolint:testifylint

		// Run forced reparent operation, this should now proceed unimpeded.
		out, err := ers(ctx, clusterInstance, tablets[1], "15s", waitReplicasTimeout.String())
		assert.NoError(t, err, out)
	}()

	wg.Wait()

	// We need to wait at least 10 seconds here to ensure the wait-for-replicas-timeout has passed,
	// before we resume the old primary - otherwise the old primary will receive a `SetReplicationSource` call.
	time.Sleep(waitReplicasTimeout * 2)

	// Bring back the demoted primary
	require.NoError(t, tablets[0].UnfreezeVttablet(ctx))

	// Give the old primary some time to realize it's no longer the primary,
	// and for a new primary to be promoted.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		// Ensure the old primary was demoted correctly
		typ, err := tabletTopoType(ctx, clusterInstance, tablets[0])
		require.NoError(c, err)

		// The old primary should have noticed there's a new primary tablet now and should
		// have demoted itself to REPLICA.
		require.Equal(c, topodatapb.TabletType_REPLICA, typ)

		oldPrimaryVars, err := tablets[0].GetVars(ctx)
		require.NoError(c, err)

		// The old primary should be in not serving mode because we should be unable to re-attach it
		// as a replica due to the errant GTID caused by semi-sync writes that were never replicated out.
		//
		// Note: The writes that were not replicated were caused by the semi sync unblocker, which
		//       performed writes after ERS.
		require.Equal(c, "NOT_SERVING", oldPrimaryVars["TabletStateName"])
		require.Equal(c, "replica", oldPrimaryVars["TabletType"])

		newPrimaryVars, err := tablets[1].GetVars(ctx)
		require.NoError(c, err)

		// Check the 2nd tablet becomes PRIMARY.
		require.Equal(c, "SERVING", newPrimaryVars["TabletStateName"])
		require.Equal(c, "primary", newPrimaryVars["TabletType"])
	}, 30*time.Second, time.Second, "could not validate primary was demoted")
}

func TestReparentNoChoiceDownPrimary(t *testing.T) {
	clusterInstance := setupReparentCluster(t, policy.DurabilitySemiSync)
	tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()
	var err error

	ctx := t.Context()

	insertVal := confirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

	// Make the current primary agent and database unavailable.
	stopTablet(ctx, t, tablets[0], true)

	// Run forced reparent operation, this should now proceed unimpeded.
	out, err := ers(ctx, clusterInstance, nil, "120s", "61s")
	require.NoError(t, err, out)

	// Check that old primary tablet is left around for human intervention.
	confirmOldPrimaryIsHangingAround(ctx, t, clusterInstance)
	// Now we'll manually remove the old primary, simulating a human cleaning up a dead primary.
	deleteTablet(ctx, t, clusterInstance, tablets[0])
	validateTopology(ctx, t, clusterInstance, false)
	newPrimary := getNewPrimary(ctx, t, clusterInstance)
	// Validate new primary is not old primary.
	require.NotEqual(t, newPrimary.Alias(), tablets[0].Alias())

	// Check new primary has latest transaction.
	err = checkInsertedValues(ctx, t, newPrimary, insertVal)
	require.NoError(t, err)

	// bring back the old primary as a replica, check that it catches up
	resurrectTablet(ctx, t, tablets[0])
}

func TestSemiSyncSetupCorrectly(t *testing.T) {
	t.Run("semi-sync enabled", func(t *testing.T) {
		ctx := t.Context()
		clusterInstance := setupReparentCluster(t, policy.DurabilitySemiSync)
		tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()

		confirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})
		// Run forced reparent operation, this should proceed unimpeded.
		out, err := ers(ctx, clusterInstance, tablets[1], "60s", "30s")
		require.NoError(t, err, out)

		confirmReplication(t, tablets[1], []*vitesst.Tablet{tablets[0], tablets[2], tablets[3]})

		for _, tablet := range tablets {
			checkSemiSyncSetupCorrectly(ctx, t, tablet, "ON")
		}

		// Run forced reparent operation, this should proceed unimpeded.
		out, err = prs(ctx, clusterInstance, tablets[0])
		require.NoError(t, err, out)

		confirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

		for _, tablet := range tablets {
			checkSemiSyncSetupCorrectly(ctx, t, tablet, "ON")
		}
	})

	t.Run("semi-sync disabled", func(t *testing.T) {
		ctx := t.Context()
		clusterInstance := setupReparentCluster(t, policy.DurabilityNone)
		tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()

		confirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})
		// Run forced reparent operation, this should proceed unimpeded.
		out, err := ers(ctx, clusterInstance, tablets[1], "60s", "30s")
		require.NoError(t, err, out)

		confirmReplication(t, tablets[1], []*vitesst.Tablet{tablets[0], tablets[2], tablets[3]})

		for _, tablet := range tablets {
			checkSemiSyncSetupCorrectly(ctx, t, tablet, "OFF")
		}

		// Run forced reparent operation, this should proceed unimpeded.
		out, err = prs(ctx, clusterInstance, tablets[0])
		require.NoError(t, err, out)

		confirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

		for _, tablet := range tablets {
			checkSemiSyncSetupCorrectly(ctx, t, tablet, "OFF")
		}
	})
}

// TestERSPromoteRdonly tests that we never end up promoting a rdonly instance as the primary
func TestERSPromoteRdonly(t *testing.T) {
	ctx := t.Context()
	clusterInstance := setupReparentCluster(t, policy.DurabilitySemiSync)
	tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()
	var err error

	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", tablets[1].Alias(), "rdonly")
	require.NoError(t, err)

	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", tablets[2].Alias(), "rdonly")
	require.NoError(t, err)

	confirmReplication(t, tablets[0], tablets[1:])

	// Make the current primary agent and database unavailable.
	stopTablet(ctx, t, tablets[0], true)

	// We expect this one to fail because we have ignored all the replicas and have only the rdonly's which should not be promoted
	out, err := ersIgnoreTablet(ctx, clusterInstance, nil, "30s", "30s", []*vitesst.Tablet{tablets[3]}, false)
	require.NotNil(t, err, out)

	out, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "GetShard", keyspaceShard)
	require.NoError(t, err)
	require.Contains(t, out, `"uid": 101`, "the primary should still be 101 in the shard info")
}

// TestERSPreventCrossCellPromotion tests that we promote a replica in the same cell as the previous primary if prevent cross cell promotion flag is set
func TestERSPreventCrossCellPromotion(t *testing.T) {
	ctx := t.Context()
	clusterInstance := setupReparentCluster(t, policy.DurabilitySemiSync)
	tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()
	var err error

	// confirm that replication is going smoothly
	confirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

	// Make the current primary agent and database unavailable.
	stopTablet(ctx, t, tablets[0], true)

	// We expect that tablets[2] will be promoted since it is in the same cell as the previous primary
	out, err := ersIgnoreTablet(ctx, clusterInstance, nil, "60s", "30s", []*vitesst.Tablet{tablets[1]}, true)
	require.NoError(t, err, out)

	newPrimary := getNewPrimary(ctx, t, clusterInstance)
	require.Equal(t, newPrimary.Alias(), tablets[2].Alias(), "tablets[2] should be the promoted primary")
}

// TestPullFromRdonly tests that if a rdonly tablet is the most advanced, then our promoted primary should have
// caught up to it by pulling transactions from it
func TestPullFromRdonly(t *testing.T) {
	clusterInstance := setupReparentCluster(t, policy.DurabilitySemiSync)
	tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()
	var err error

	ctx := t.Context()
	// make tablets[1] a rdonly tablet.
	// rename tablet so that the test is not confusing
	rdonly := tablets[1]
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", rdonly.Alias(), "rdonly")
	require.NoError(t, err)

	// confirm that all the tablets can replicate successfully right now
	confirmReplication(t, tablets[0], []*vitesst.Tablet{rdonly, tablets[2], tablets[3]})

	// stop replication on the other two tablets
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "StopReplication", tablets[2].Alias())
	require.NoError(t, err)
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "StopReplication", tablets[3].Alias())
	require.NoError(t, err)

	// stop semi-sync on the primary so that any transaction now added does not require an ack
	semisyncType, err := semiSyncExtensionLoaded(ctx, tablets[0])
	require.NoError(t, err)
	switch semisyncType {
	case mysql.SemiSyncTypeSource:
		runSQL(ctx, t, "SET GLOBAL rpl_semi_sync_source_enabled = false", tablets[0])
	case mysql.SemiSyncTypeMaster:
		runSQL(ctx, t, "SET GLOBAL rpl_semi_sync_master_enabled = false", tablets[0])
	}

	// confirm that rdonly is able to replicate from our primary
	// This will also introduce a new transaction into the rdonly tablet which the other 2 replicas don't have
	insertVal := confirmReplication(t, tablets[0], []*vitesst.Tablet{rdonly})

	// Make the current primary agent and database unavailable.
	stopTablet(ctx, t, tablets[0], true)

	// start the replication back on the two tablets
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "StartReplication", tablets[2].Alias())
	require.NoError(t, err)
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "StartReplication", tablets[3].Alias())
	require.NoError(t, err)

	// check that tablets[2] and tablets[3] still only has 1 value
	err = checkCountOfInsertedValues(ctx, t, tablets[2], 1)
	require.NoError(t, err)
	err = checkCountOfInsertedValues(ctx, t, tablets[3], 1)
	require.NoError(t, err)

	// At this point we have successfully made our rdonly tablet more advanced than tablets[2] and tablets[3] without introducing errant GTIDs
	// We have simulated a network partition in which the primary and rdonly got isolated and then the primary went down leaving the rdonly most advanced

	// We expect that tablets[2] will be promoted since it is in the same cell as the previous primary
	// since we are preventing cross cell promotions
	// Also it must be fully caught up
	out, err := ersIgnoreTablet(ctx, clusterInstance, nil, "60s", "30s", nil, true)
	require.NoError(t, err, out)

	newPrimary := getNewPrimary(ctx, t, clusterInstance)
	require.Equal(t, newPrimary.Alias(), tablets[2].Alias(), "tablets[2] should be the promoted primary")

	// check that the new primary has the last transaction that only the rdonly had
	err = checkInsertedValues(ctx, t, newPrimary, insertVal)
	require.NoError(t, err)
}

// TestNoReplicationStatusAndIOThreadStopped checks that ERS is able to fix
// replicas which do not have any replication status and also succeeds if the io thread
// is stopped on the primary elect.
func TestNoReplicationStatusAndIOThreadStopped(t *testing.T) {
	ctx := t.Context()
	clusterInstance := setupReparentCluster(t, policy.DurabilitySemiSync)
	tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()
	confirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

	err := clusterInstance.Vtctld().ExecuteCommand(ctx, "ExecuteFetchAsDBA", tablets[1].Alias(), `STOP REPLICA`)
	require.NoError(t, err)
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ExecuteFetchAsDBA", tablets[1].Alias(), `RESET REPLICA ALL`)
	require.NoError(t, err)
	//
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ExecuteFetchAsDBA", tablets[3].Alias(), `STOP REPLICA IO_THREAD;`)
	require.NoError(t, err)
	// Run an additional command in the current primary which will only be acked by tablets[2] and be in its relay log.
	insertedVal := confirmReplication(t, tablets[0], nil)
	// Failover to tablets[3]
	out, err := ers(ctx, clusterInstance, tablets[3], "60s", "30s")
	require.NoError(t, err, out)
	// Verify that the tablet has the inserted value
	err = checkInsertedValues(ctx, t, tablets[3], insertedVal)
	require.NoError(t, err)
	// Confirm that replication is setup correctly from tablets[3] to tablets[0]
	confirmReplication(t, tablets[3], tablets[:1])
	// Confirm that tablets[2] which had no replication status initially now has its replication started
	checkReplicationStatus(ctx, t, tablets[1], true, true)
}

// TestERSForInitialization tests whether calling ERS in the beginning sets up the cluster properly or not
func TestERSForInitialization(t *testing.T) {
	ctx := t.Context()

	keyspace := vitesst.WithKeyspace(keyspaceName).
		WithShardNames(shardName).
		WithReplicas(3).
		WithDurabilityPolicy(policy.DurabilitySemiSync).
		WithoutPrimaryElection()

	clusterInstance, err := vitesst.NewCluster(
		vitesst.WithoutVTGate(),
		vitesst.WithVTTabletArgs(
			"--lock-tables-timeout", "5s",
			"--track-schema-versions=true",
		),
		keyspace,
	)
	require.NoError(t, err)

	startCluster(t, clusterInstance)

	tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()

	// Force the replica to reparent assuming that all the datasets are identical.
	res, err := ers(ctx, clusterInstance, tablets[0], "60s", "30s")
	require.NoError(t, err, res)

	validateTopology(ctx, t, clusterInstance, true)
	// create Tables
	runSQL(ctx, t, "create table vt_insert_test (id bigint, msg varchar(64), primary key (id)) Engine=InnoDB", tablets[0])
	checkPrimaryTablet(ctx, t, clusterInstance, tablets[0])
	validateTopology(ctx, t, clusterInstance, false)
	waitForReplicationToStart(ctx, t, clusterInstance, keyspaceName, shardName, len(tablets))
	confirmReplication(t, tablets[0], tablets[1:])
}

func TestRecoverWithMultipleFailures(t *testing.T) {
	ctx := t.Context()
	clusterInstance := setupReparentCluster(t, policy.DurabilitySemiSync)
	tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()
	confirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

	// make tablets[1] a rdonly tablet.
	err := clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", tablets[1].Alias(), "rdonly")
	require.NoError(t, err)

	// Confirm that replication is still working as intended
	confirmReplication(t, tablets[0], tablets[1:])

	// Make the rdonly and primary tablets and databases unavailable.
	stopTablet(ctx, t, tablets[1], true)
	stopTablet(ctx, t, tablets[0], true)

	// We expect this to succeed since we only have 1 primary eligible tablet which is down
	out, err := ers(ctx, clusterInstance, nil, "30s", "10s")
	require.NoError(t, err, out)

	newPrimary := getNewPrimary(ctx, t, clusterInstance)
	confirmReplication(t, newPrimary, []*vitesst.Tablet{tablets[2], tablets[3]})
}

// TestERSFailFast tests that ERS will fail fast if it cannot find any tablet which can be safely promoted instead of promoting
// a tablet and hanging while inserting a row in the reparent journal on getting semi-sync ACKs
func TestERSFailFast(t *testing.T) {
	clusterInstance := setupReparentCluster(t, policy.DurabilitySemiSync)
	tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()
	confirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

	// make tablets[1] a rdonly tablet.
	err := clusterInstance.Vtctld().ExecuteCommand(t.Context(), "ChangeTabletType", tablets[1].Alias(), "rdonly")
	require.NoError(t, err)

	// Confirm that replication is still working as intended
	confirmReplication(t, tablets[0], tablets[1:])

	// Context to be used in the go-routine to cleanly exit it after the test ends
	ctx := t.Context()
	strChan := make(chan string)
	go func() {
		// We expect this to fail since we have ignored all replica tablets and only the rdonly is left, which is not capable of sending semi-sync ACKs
		out, err := ersIgnoreTablet(ctx, clusterInstance, tablets[2], "240s", "90s", []*vitesst.Tablet{tablets[0], tablets[3]}, false)
		assert.Error(t, err)
		select {
		case strChan <- out:
			return
		case <-ctx.Done():
			return
		}
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
	ctx := t.Context()
	clusterInstance := setupReparentCluster(t, policy.DurabilitySemiSync)
	tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()
	confirmReplication(t, tablets[0], []*vitesst.Tablet{tablets[1], tablets[2], tablets[3]})

	err := clusterInstance.Vtctld().ExecuteCommand(ctx, "ExecuteFetchAsDBA", tablets[1].Alias(), `STOP REPLICA SQL_THREAD;`)
	require.NoError(t, err)
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ExecuteFetchAsDBA", tablets[2].Alias(), `STOP REPLICA;`)
	require.NoError(t, err)
	// Run an additional command in the current primary which will only be acked by tablets[3] and be in its relay log.
	insertedVal := confirmReplication(t, tablets[0], nil)
	// Failover to tablets[3]
	_, err = ers(ctx, clusterInstance, tablets[3], "60s", "30s")
	require.Error(t, err, "ERS should fail with 2 replicas having replication stopped")

	// Start replication back on tablet[1]
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ExecuteFetchAsDBA", tablets[1].Alias(), `START REPLICA;`)
	require.NoError(t, err)
	// Failover to tablets[3] again. This time it should succeed
	out, err := ers(ctx, clusterInstance, tablets[3], "60s", "30s")
	require.NoError(t, err, out)
	// Verify that the tablet has the inserted value
	err = checkInsertedValues(ctx, t, tablets[3], insertedVal)
	require.NoError(t, err)
	// Confirm that replication is setup correctly from tablets[3] to tablets[0]
	confirmReplication(t, tablets[3], tablets[:1])
	// Confirm that tablets[2] which had replication stopped initially still has its replication stopped
	checkReplicationStatus(ctx, t, tablets[2], false, false)
}
