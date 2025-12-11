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
	"context"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestTrivialERS(t *testing.T) {
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	utils.ConfirmReplication(t, tablets[0], tablets[1:])

	// We should be able to do a series of ERS-es, even if nothing
	// is down, without issue
	for i := 1; i <= 4; i++ {
		out, err := utils.Ers(clusterInstance, nil, "60s", "30s")
		log.Infof("ERS loop %d.  EmergencyReparentShard Output: %v", i, out)
		require.NoError(t, err)
		time.Sleep(5 * time.Second)
	}
	// We should do the same for vtctl binary
	for i := 1; i <= 4; i++ {
		out, err := utils.ErsWithVtctldClient(clusterInstance)
		log.Infof("ERS-vtctldclient loop %d.  EmergencyReparentShard Output: %v", i, out)
		require.NoError(t, err)
		time.Sleep(5 * time.Second)
	}
}

func TestReparentIgnoreReplicas(t *testing.T) {
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	var err error

	ctx := context.Background()

	insertVal := utils.ConfirmReplication(t, tablets[0], tablets[1:])

	// Make the current primary agent and database unavailable.
	utils.StopTablet(t, tablets[0], true)

	// Take down a replica - this should cause the emergency reparent to fail.
	utils.StopTablet(t, tablets[2], true)

	// We expect this one to fail because we have an unreachable replica
	out, err := utils.Ers(clusterInstance, nil, "60s", "30s")
	require.NotNil(t, err, out)

	// Now let's run it again, but set the command to ignore the unreachable replica.
	out, err = utils.ErsIgnoreTablet(clusterInstance, nil, "60s", "30s", []*cluster.Vttablet{tablets[2]}, false)
	require.Nil(t, err, out)

	// We'll bring back the replica we took down.
	utils.RestartTablet(t, clusterInstance, tablets[2])

	// Check that old primary tablet is left around for human intervention.
	utils.ConfirmOldPrimaryIsHangingAround(t, clusterInstance)
	utils.DeleteTablet(t, clusterInstance, tablets[0])
	utils.ValidateTopology(t, clusterInstance, false)

	newPrimary := utils.GetNewPrimary(t, clusterInstance)
	// Check new primary has latest transaction.
	err = utils.CheckInsertedValues(ctx, t, newPrimary, insertVal)
	require.Nil(t, err)

	// bring back the old primary as a replica, check that it catches up
	utils.ResurrectTablet(ctx, t, clusterInstance, tablets[0])
}

func TestReparentDownPrimary(t *testing.T) {
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	ctx := context.Background()

	// Make the current primary agent and database unavailable.
	utils.StopTablet(t, tablets[0], true)

	// Perform a planned reparent operation, will try to contact
	// the current primary and fail somewhat quickly
	_, err := utils.PrsWithTimeout(t, clusterInstance, tablets[1], false, "1s", "5s")
	require.Error(t, err)

	utils.ValidateTopology(t, clusterInstance, false)

	// Run forced reparent operation, this should now proceed unimpeded.
	out, err := utils.Ers(clusterInstance, tablets[1], "60s", "30s")
	log.Infof("EmergencyReparentShard Output: %v", out)
	require.NoError(t, err)

	// Check that old primary tablet is left around for human intervention.
	utils.ConfirmOldPrimaryIsHangingAround(t, clusterInstance)

	// Now we'll manually remove it, simulating a human cleaning up a dead primary.
	utils.DeleteTablet(t, clusterInstance, tablets[0])

	// Now validate topo is correct.
	utils.ValidateTopology(t, clusterInstance, false)
	utils.CheckPrimaryTablet(t, clusterInstance, tablets[1])
	utils.ConfirmReplication(t, tablets[1], []*cluster.Vttablet{tablets[2], tablets[3]})
	utils.ResurrectTablet(ctx, t, clusterInstance, tablets[0])
}

func TestEmergencyReparentWithBlockedPrimary(t *testing.T) {
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)

	if clusterInstance.VtTabletMajorVersion < 24 {
		t.Skip("Skipping test since `DemotePrimary` on earlier versions does not handle blocked primaries correctly")
	}

	// start vtgate w/disabled buffering
	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
		"--enable_buffer=false",
		"--query-timeout", "3000")
	err := clusterInstance.StartVtgate()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := mysql.Connect(ctx, &mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	})
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("CREATE TABLE test (id INT PRIMARY KEY, msg VARCHAR(64))", 0, false)
	require.NoError(t, err)

	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	// Simulate no semi-sync replicas being available by disabling semi-sync on all replicas
	for _, tablet := range tablets[1:] {
		utils.RunSQL(ctx, t, "STOP REPLICA IO_THREAD", tablet)

		// Disable semi-sync on replicas to simulate blocking
		semisyncType, err := utils.SemiSyncExtensionLoaded(context.Background(), tablet)
		require.NoError(t, err)
		switch semisyncType {
		case mysql.SemiSyncTypeSource:
			utils.RunSQL(context.Background(), t, "SET GLOBAL rpl_semi_sync_replica_enabled = false", tablet)
		case mysql.SemiSyncTypeMaster:
			utils.RunSQL(context.Background(), t, "SET GLOBAL rpl_semi_sync_slave_enabled = false", tablet)
		}

		utils.RunSQL(context.Background(), t, "START REPLICA IO_THREAD", tablet)
	}

	// Try performing a write and ensure that it blocks.
	writeSQL := `insert into test(id, msg) values (1, 'test 1')`
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

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
		vtgateVars := clusterInstance.VtgateProcess.GetVars()
		require.NotNil(t, vtgateVars)
		require.NotNil(t, vtgateVars["QueryRoutes"])
		require.NotNil(t, vtgateVars["VtgateApiErrorCounts"])
		require.EqualValues(t, map[string]interface{}{
			"DDL.DirectDDL.PRIMARY":      float64(1),
			"INSERT.Passthrough.PRIMARY": float64(1),
		}, vtgateVars["QueryRoutes"])
		require.EqualValues(t, map[string]interface{}{
			"Execute.ks.primary.DEADLINE_EXCEEDED": float64(1),
		}, vtgateVars["VtgateApiErrorCounts"])
	}()

	wg.Add(1)
	waitReplicasTimeout := time.Second * 10
	go func() {
		defer wg.Done()

		// Ensure the write (other goroutine above) is blocked waiting on ACKs on the primary.
		utils.WaitForQueryWithStateInProcesslist(context.Background(), t, tablets[0], writeSQL, "Waiting for semi-sync ACK from replica", time.Second*20)

		// Send SIGSTOP to primary to simulate it being unresponsive.
		tablets[0].VttabletProcess.Stop()

		// Run forced reparent operation, this should now proceed unimpeded.
		out, err := utils.Ers(clusterInstance, tablets[1], "15s", waitReplicasTimeout.String())
		require.NoError(t, err, out)
	}()

	wg.Wait()

	// We need to wait at least 10 seconds here to ensure the wait-for-replicas-timeout has passed,
	// before we resume the old primary - otherwise the old primary will receive a `SetReplicationSource` call.
	time.Sleep(waitReplicasTimeout * 2)

	// Bring back the demoted primary
	tablets[0].VttabletProcess.Resume()

	// Give the old primary some time to realize it's no longer the primary,
	// and for a new primary to be promoted.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		// Ensure the old primary was demoted correctly
		tabletInfo, err := clusterInstance.VtctldClientProcess.GetTablet(tablets[0].Alias)
		require.NoError(c, err)

		// The old primary should have noticed there's a new primary tablet now and should
		// have demoted itself to REPLICA.
		require.Equal(c, topodatapb.TabletType_REPLICA, tabletInfo.GetType())

		// The old primary should be in not serving mode because we should be unable to re-attach it
		// as a replica due to the errant GTID caused by semi-sync writes that were never replicated out.
		//
		// Note: The writes that were not replicated were caused by the semi sync unblocker, which
		//       performed writes after ERS.
		require.Equal(c, "NOT_SERVING", tablets[0].VttabletProcess.GetTabletStatus())
		require.Equal(c, "replica", tablets[0].VttabletProcess.GetTabletType())

		// Check the 2nd tablet becomes PRIMARY.
		require.Equal(c, "SERVING", tablets[1].VttabletProcess.GetTabletStatus())
		require.Equal(c, "primary", tablets[1].VttabletProcess.GetTabletType())
	}, 30*time.Second, time.Second, "could not validate primary was demoted")
}

func TestReparentNoChoiceDownPrimary(t *testing.T) {
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	var err error

	ctx := context.Background()

	insertVal := utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// Make the current primary agent and database unavailable.
	utils.StopTablet(t, tablets[0], true)

	// Run forced reparent operation, this should now proceed unimpeded.
	out, err := utils.Ers(clusterInstance, nil, "120s", "61s")
	require.NoError(t, err, out)

	// Check that old primary tablet is left around for human intervention.
	utils.ConfirmOldPrimaryIsHangingAround(t, clusterInstance)
	// Now we'll manually remove the old primary, simulating a human cleaning up a dead primary.
	utils.DeleteTablet(t, clusterInstance, tablets[0])
	utils.ValidateTopology(t, clusterInstance, false)
	newPrimary := utils.GetNewPrimary(t, clusterInstance)
	// Validate new primary is not old primary.
	require.NotEqual(t, newPrimary.Alias, tablets[0].Alias)

	// Check new primary has latest transaction.
	err = utils.CheckInsertedValues(ctx, t, newPrimary, insertVal)
	require.NoError(t, err)

	// bring back the old primary as a replica, check that it catches up
	utils.ResurrectTablet(ctx, t, clusterInstance, tablets[0])
}

func TestSemiSyncSetupCorrectly(t *testing.T) {
	t.Run("semi-sync enabled", func(t *testing.T) {
		clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
		defer utils.TeardownCluster(clusterInstance)
		tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

		utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})
		// Run forced reparent operation, this should proceed unimpeded.
		out, err := utils.Ers(clusterInstance, tablets[1], "60s", "30s")
		require.NoError(t, err, out)

		utils.ConfirmReplication(t, tablets[1], []*cluster.Vttablet{tablets[0], tablets[2], tablets[3]})

		for _, tablet := range tablets {
			utils.CheckSemiSyncSetupCorrectly(t, tablet, "ON")
		}

		// Run forced reparent operation, this should proceed unimpeded.
		out, err = utils.Prs(t, clusterInstance, tablets[0])
		require.NoError(t, err, out)

		utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

		for _, tablet := range tablets {
			utils.CheckSemiSyncSetupCorrectly(t, tablet, "ON")
		}
	})

	t.Run("semi-sync disabled", func(t *testing.T) {
		clusterInstance := utils.SetupReparentCluster(t, policy.DurabilityNone)
		defer utils.TeardownCluster(clusterInstance)
		tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

		utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})
		// Run forced reparent operation, this should proceed unimpeded.
		out, err := utils.Ers(clusterInstance, tablets[1], "60s", "30s")
		require.NoError(t, err, out)

		utils.ConfirmReplication(t, tablets[1], []*cluster.Vttablet{tablets[0], tablets[2], tablets[3]})

		for _, tablet := range tablets {
			utils.CheckSemiSyncSetupCorrectly(t, tablet, "OFF")
		}

		// Run forced reparent operation, this should proceed unimpeded.
		out, err = utils.Prs(t, clusterInstance, tablets[0])
		require.NoError(t, err, out)

		utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

		for _, tablet := range tablets {
			utils.CheckSemiSyncSetupCorrectly(t, tablet, "OFF")
		}
	})
}

// TestERSPromoteRdonly tests that we never end up promoting a rdonly instance as the primary
func TestERSPromoteRdonly(t *testing.T) {
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	var err error

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("ChangeTabletType", tablets[1].Alias, "rdonly")
	require.NoError(t, err)

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("ChangeTabletType", tablets[2].Alias, "rdonly")
	require.NoError(t, err)

	utils.ConfirmReplication(t, tablets[0], tablets[1:])

	// Make the current primary agent and database unavailable.
	utils.StopTablet(t, tablets[0], true)

	// We expect this one to fail because we have ignored all the replicas and have only the rdonly's which should not be promoted
	out, err := utils.ErsIgnoreTablet(clusterInstance, nil, "30s", "30s", []*cluster.Vttablet{tablets[3]}, false)
	require.NotNil(t, err, out)

	out, err = clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("GetShard", utils.KeyspaceShard)
	require.NoError(t, err)
	require.Contains(t, out, `"uid": 101`, "the primary should still be 101 in the shard info")
}

// TestERSPreventCrossCellPromotion tests that we promote a replica in the same cell as the previous primary if prevent cross cell promotion flag is set
func TestERSPreventCrossCellPromotion(t *testing.T) {
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	var err error

	// confirm that replication is going smoothly
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// Make the current primary agent and database unavailable.
	utils.StopTablet(t, tablets[0], true)

	// We expect that tablets[2] will be promoted since it is in the same cell as the previous primary
	out, err := utils.ErsIgnoreTablet(clusterInstance, nil, "60s", "30s", []*cluster.Vttablet{tablets[1]}, true)
	require.NoError(t, err, out)

	newPrimary := utils.GetNewPrimary(t, clusterInstance)
	require.Equal(t, newPrimary.Alias, tablets[2].Alias, "tablets[2] should be the promoted primary")
}

// TestPullFromRdonly tests that if a rdonly tablet is the most advanced, then our promoted primary should have
// caught up to it by pulling transactions from it
func TestPullFromRdonly(t *testing.T) {
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	var err error

	ctx := context.Background()
	// make tablets[1] a rdonly tablet.
	// rename tablet so that the test is not confusing
	rdonly := tablets[1]
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("ChangeTabletType", rdonly.Alias, "rdonly")
	require.NoError(t, err)

	// confirm that all the tablets can replicate successfully right now
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{rdonly, tablets[2], tablets[3]})

	// stop replication on the other two tablets
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("StopReplication", tablets[2].Alias)
	require.NoError(t, err)
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("StopReplication", tablets[3].Alias)
	require.NoError(t, err)

	// stop semi-sync on the primary so that any transaction now added does not require an ack
	semisyncType, err := utils.SemiSyncExtensionLoaded(ctx, tablets[0])
	require.NoError(t, err)
	switch semisyncType {
	case mysql.SemiSyncTypeSource:
		utils.RunSQL(ctx, t, "SET GLOBAL rpl_semi_sync_source_enabled = false", tablets[0])
	case mysql.SemiSyncTypeMaster:
		utils.RunSQL(ctx, t, "SET GLOBAL rpl_semi_sync_master_enabled = false", tablets[0])
	}

	// confirm that rdonly is able to replicate from our primary
	// This will also introduce a new transaction into the rdonly tablet which the other 2 replicas don't have
	insertVal := utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{rdonly})

	// Make the current primary agent and database unavailable.
	utils.StopTablet(t, tablets[0], true)

	// start the replication back on the two tablets
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("StartReplication", tablets[2].Alias)
	require.NoError(t, err)
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("StartReplication", tablets[3].Alias)
	require.NoError(t, err)

	// check that tablets[2] and tablets[3] still only has 1 value
	err = utils.CheckCountOfInsertedValues(ctx, t, tablets[2], 1)
	require.NoError(t, err)
	err = utils.CheckCountOfInsertedValues(ctx, t, tablets[3], 1)
	require.NoError(t, err)

	// At this point we have successfully made our rdonly tablet more advanced than tablets[2] and tablets[3] without introducing errant GTIDs
	// We have simulated a network partition in which the primary and rdonly got isolated and then the primary went down leaving the rdonly most advanced

	// We expect that tablets[2] will be promoted since it is in the same cell as the previous primary
	// since we are preventing cross cell promotions
	// Also it must be fully caught up
	out, err := utils.ErsIgnoreTablet(clusterInstance, nil, "60s", "30s", nil, true)
	require.NoError(t, err, out)

	newPrimary := utils.GetNewPrimary(t, clusterInstance)
	require.Equal(t, newPrimary.Alias, tablets[2].Alias, "tablets[2] should be the promoted primary")

	// check that the new primary has the last transaction that only the rdonly had
	err = utils.CheckInsertedValues(ctx, t, newPrimary, insertVal)
	require.NoError(t, err)
}

// TestNoReplicationStatusAndIOThreadStopped checks that ERS is able to fix
// replicas which do not have any replication status and also succeeds if the io thread
// is stopped on the primary elect.
func TestNoReplicationStatusAndIOThreadStopped(t *testing.T) {
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	err := clusterInstance.VtctldClientProcess.ExecuteCommand("ExecuteFetchAsDBA", tablets[1].Alias, `STOP REPLICA`)
	require.NoError(t, err)
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("ExecuteFetchAsDBA", tablets[1].Alias, `RESET REPLICA ALL`)
	require.NoError(t, err)
	//
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("ExecuteFetchAsDBA", tablets[3].Alias, `STOP REPLICA IO_THREAD;`)
	require.NoError(t, err)
	// Run an additional command in the current primary which will only be acked by tablets[2] and be in its relay log.
	insertedVal := utils.ConfirmReplication(t, tablets[0], nil)
	// Failover to tablets[3]
	out, err := utils.Ers(clusterInstance, tablets[3], "60s", "30s")
	require.NoError(t, err, out)
	// Verify that the tablet has the inserted value
	err = utils.CheckInsertedValues(context.Background(), t, tablets[3], insertedVal)
	require.NoError(t, err)
	// Confirm that replication is setup correctly from tablets[3] to tablets[0]
	utils.ConfirmReplication(t, tablets[3], tablets[:1])
	// Confirm that tablets[2] which had no replication status initially now has its replication started
	utils.CheckReplicationStatus(context.Background(), t, tablets[1], true, true)
}

// TestERSForInitialization tests whether calling ERS in the beginning sets up the cluster properly or not
func TestERSForInitialization(t *testing.T) {
	var tablets []*cluster.Vttablet
	clusterInstance := cluster.NewCluster("zone1", "localhost")
	defer clusterInstance.Teardown()
	keyspace := &cluster.Keyspace{Name: utils.KeyspaceName}
	// Start topo server
	err := clusterInstance.StartTopo()
	require.NoError(t, err)
	err = clusterInstance.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+"zone1")
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		tablet := clusterInstance.NewVttabletInstance("replica", 100+i, "zone1")
		tablets = append(tablets, tablet)
	}

	shard := &cluster.Shard{Name: utils.ShardName}
	shard.Vttablets = tablets
	clusterInstance.VtTabletExtraArgs = []string{
		//TODO: Remove underscore(_) flags in v25, replace them with dashed(-) notation
		"--lock_tables_timeout", "5s",
		"--track_schema_versions=true",
	}

	// Initialize Cluster
	err = clusterInstance.SetupCluster(keyspace, []cluster.Shard{*shard})
	require.NoError(t, err)
	if clusterInstance.VtctlMajorVersion >= 14 {
		vtctldClientProcess := cluster.VtctldClientProcessInstance(clusterInstance.VtctldProcess.GrpcPort, clusterInstance.TopoPort, "localhost", clusterInstance.TmpDirectory)
		out, err := vtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", keyspace.Name, "--durability-policy=semi_sync")
		require.NoError(t, err, out)
	}

	//Start MySql
	var mysqlCtlProcessList []*exec.Cmd
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			log.Infof("Starting MySql for tablet %v", tablet.Alias)
			proc, err := tablet.MysqlctlProcess.StartProcess()
			require.NoError(t, err)
			mysqlCtlProcessList = append(mysqlCtlProcessList, proc)
		}
	}
	// Wait for mysql processes to start
	for _, proc := range mysqlCtlProcessList {
		if err := proc.Wait(); err != nil {
			t.Fatalf("Error starting mysql: %s", err.Error())
		}
	}

	for _, tablet := range tablets {
		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}
	for _, tablet := range tablets {
		err := tablet.VttabletProcess.WaitForTabletStatuses([]string{"SERVING", "NOT_SERVING"})
		require.NoError(t, err)
	}

	// Force the replica to reparent assuming that all the datasets are identical.
	res, err := utils.Ers(clusterInstance, tablets[0], "60s", "30s")
	require.NoError(t, err, res)

	utils.ValidateTopology(t, clusterInstance, true)
	// create Tables
	utils.RunSQL(context.Background(), t, "create table vt_insert_test (id bigint, msg varchar(64), primary key (id)) Engine=InnoDB", tablets[0])
	utils.CheckPrimaryTablet(t, clusterInstance, tablets[0])
	utils.ValidateTopology(t, clusterInstance, false)
	utils.WaitForReplicationToStart(t, clusterInstance, utils.KeyspaceName, utils.ShardName, len(tablets), true)
	utils.ConfirmReplication(t, tablets[0], tablets[1:])
}

func TestRecoverWithMultipleFailures(t *testing.T) {
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// make tablets[1] a rdonly tablet.
	err := clusterInstance.VtctldClientProcess.ExecuteCommand("ChangeTabletType", tablets[1].Alias, "rdonly")
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
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// make tablets[1] a rdonly tablet.
	err := clusterInstance.VtctldClientProcess.ExecuteCommand("ChangeTabletType", tablets[1].Alias, "rdonly")
	require.NoError(t, err)

	// Confirm that replication is still working as intended
	utils.ConfirmReplication(t, tablets[0], tablets[1:])

	// Context to be used in the go-routine to cleanly exit it after the test ends
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	strChan := make(chan string)
	go func() {
		// We expect this to fail since we have ignored all replica tablets and only the rdonly is left, which is not capable of sending semi-sync ACKs
		out, err := utils.ErsIgnoreTablet(clusterInstance, tablets[2], "240s", "90s", []*cluster.Vttablet{tablets[0], tablets[3]}, false)
		require.Error(t, err)
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
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	err := clusterInstance.VtctldClientProcess.ExecuteCommand("ExecuteFetchAsDBA", tablets[1].Alias, `STOP REPLICA SQL_THREAD;`)
	require.NoError(t, err)
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("ExecuteFetchAsDBA", tablets[2].Alias, `STOP REPLICA;`)
	require.NoError(t, err)
	// Run an additional command in the current primary which will only be acked by tablets[3] and be in its relay log.
	insertedVal := utils.ConfirmReplication(t, tablets[0], nil)
	// Failover to tablets[3]
	_, err = utils.Ers(clusterInstance, tablets[3], "60s", "30s")
	require.Error(t, err, "ERS should fail with 2 replicas having replication stopped")

	// Start replication back on tablet[1]
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("ExecuteFetchAsDBA", tablets[1].Alias, `START REPLICA;`)
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
