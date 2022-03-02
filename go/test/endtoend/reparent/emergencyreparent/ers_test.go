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
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	"vitess.io/vitess/go/vt/log"
)

func TestTrivialERS(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentClusterLegacy(t, true)
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
		out, err := utils.ErsWithVtctl(clusterInstance)
		log.Infof("ERS-vtctl loop %d.  EmergencyReparentShard Output: %v", i, out)
		require.NoError(t, err)
		time.Sleep(5 * time.Second)
	}
}

func TestReparentIgnoreReplicas(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentClusterLegacy(t, true)
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
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentClusterLegacy(t, true)
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

func TestReparentNoChoiceDownPrimary(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentClusterLegacy(t, true)
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
		defer cluster.PanicHandler(t)
		clusterInstance := utils.SetupReparentClusterLegacy(t, true)
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
		defer cluster.PanicHandler(t)
		clusterInstance := utils.SetupReparentClusterLegacy(t, false)
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
