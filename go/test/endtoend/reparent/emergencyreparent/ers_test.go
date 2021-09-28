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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

func TestTrivialERS(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()

	confirmReplication(t, tab1, []*cluster.Vttablet{tab2, tab3, tab4})

	// We should be able to do a series of ERS-es, even if nothing
	// is down, without issue
	for i := 1; i <= 4; i++ {
		out, err := ers(nil, "60s", "30s")
		log.Infof("ERS loop %d.  EmergencyReparentShard Output: %v", i, out)
		require.NoError(t, err)
		time.Sleep(5 * time.Second)
	}
	// We should do the same for vtctl binary
	for i := 1; i <= 4; i++ {
		out, err := ersWithVtctl()
		log.Infof("ERS-vtctl loop %d.  EmergencyReparentShard Output: %v", i, out)
		require.NoError(t, err)
		time.Sleep(5 * time.Second)
	}
}

func TestReparentIgnoreReplicas(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()
	var err error

	ctx := context.Background()

	confirmReplication(t, tab1, []*cluster.Vttablet{tab2, tab3, tab4})

	// Make the current primary agent and database unavailable.
	stopTablet(t, tab1, true)

	// Take down a replica - this should cause the emergency reparent to fail.
	stopTablet(t, tab3, true)

	// We expect this one to fail because we have an unreachable replica
	out, err := ers(nil, "60s", "30s")
	require.NotNil(t, err, out)

	// Now let's run it again, but set the command to ignore the unreachable replica.
	out, err = ersIgnoreTablet(nil, "60s", "30s", []*cluster.Vttablet{tab3})
	require.Nil(t, err, out)

	// We'll bring back the replica we took down.
	restartTablet(t, tab3)

	// Check that old primary tablet is left around for human intervention.
	confirmOldPrimaryIsHangingAround(t)
	deleteTablet(t, tab1)
	validateTopology(t, false)

	newPrimary := getNewPrimary(t)
	// Check new primary has latest transaction.
	err = checkInsertedValues(ctx, t, newPrimary, insertVal)
	require.Nil(t, err)

	// bring back the old primary as a replica, check that it catches up
	resurrectTablet(ctx, t, tab1)
}

// TestERSPromoteRdonly tests that we never end up promoting a rdonly instance as the primary
func TestERSPromoteRdonly(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()
	var err error

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", tab2.Alias, "rdonly")
	require.NoError(t, err)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", tab3.Alias, "rdonly")
	require.NoError(t, err)

	confirmReplication(t, tab1, []*cluster.Vttablet{tab2, tab3, tab4})

	// Make the current primary agent and database unavailable.
	stopTablet(t, tab1, true)

	// We expect this one to fail because we have ignored all the replicas and have only the rdonly's which should not be promoted
	out, err := ersIgnoreTablet(nil, "30s", "30s", []*cluster.Vttablet{tab4})
	require.NotNil(t, err, out)

	out, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShard", keyspaceShard)
	require.NoError(t, err)
	require.Contains(t, out, `"uid": 101`, "the primary should still be 101 in the shard info")
}

// TestERSPrefersSameCell tests that we prefer to promote a replica in the same cell as the previous primary
func TestERSPrefersSameCell(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()
	var err error

	// confirm that replication is going smoothly
	confirmReplication(t, tab1, []*cluster.Vttablet{tab2, tab3, tab4})

	// Make the current primary agent and database unavailable.
	stopTablet(t, tab1, true)

	// We expect that tab3 will be promoted since it is in the same cell as the previous primary
	out, err := ersIgnoreTablet(nil, "60s", "30s", []*cluster.Vttablet{tab2})
	require.NoError(t, err, out)

	newPrimary := getNewPrimary(t)
	require.Equal(t, newPrimary.Alias, tab3.Alias, "tab3 should be the promoted primary")
}

// TestPullFromRdonly tests that if a rdonly tablet is the most advanced, then our promoted primary should have
// caught up to it by pulling transactions from it
func TestPullFromRdonly(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()
	var err error

	ctx := context.Background()
	// make tab2 a rdonly tablet.
	// rename tablet so that the test is not confusing
	rdonly := tab2
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonly.Alias, "rdonly")
	require.NoError(t, err)

	// confirm that all the tablets can replicate successfully right now
	confirmReplication(t, tab1, []*cluster.Vttablet{rdonly, tab3, tab4})

	// stop replication on the other two tablets
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", tab3.Alias)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", tab4.Alias)
	require.NoError(t, err)

	// stop semi-sync on the primary so that any transaction now added does not require an ack
	runSQL(ctx, t, "SET GLOBAL rpl_semi_sync_master_enabled = false", tab1)

	// confirm that rdonly is able to replicate from our primary
	// This will also introduce a new transaction into the rdonly tablet which the other 2 replicas don't have
	confirmReplication(t, tab1, []*cluster.Vttablet{rdonly})

	// Make the current primary agent and database unavailable.
	stopTablet(t, tab1, true)

	// start the replication back on the two tablets
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StartReplication", tab3.Alias)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StartReplication", tab4.Alias)
	require.NoError(t, err)

	// check that tab3 and tab4 still only has 1 value
	err = checkCountOfInsertedValues(ctx, t, tab3, 1)
	require.NoError(t, err)
	err = checkCountOfInsertedValues(ctx, t, tab4, 1)
	require.NoError(t, err)

	// At this point we have successfully made our rdonly tablet more advanced than tab3 and tab4 without introducing errant GTIDs
	// We have simulated a network partition in which the primary and rdonly got isolated and then the primary went down leaving the rdonly most advanced

	// We expect that tab3 will be promoted since it is in the same cell as the previous primary
	// Also it must be fully caught up
	out, err := ers(nil, "60s", "30s")
	require.NoError(t, err, out)

	newPrimary := getNewPrimary(t)
	require.Equal(t, newPrimary.Alias, tab3.Alias, "tab3 should be the promoted primary")

	// check that the new primary has the last transaction that only the rdonly had
	err = checkInsertedValues(ctx, t, newPrimary, insertVal)
	require.NoError(t, err)
}
