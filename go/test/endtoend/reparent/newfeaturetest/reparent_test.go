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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
)

// TestCrossCellDurability tests 2 things -
// 1. When PRS is run with the cross_cell durability policy setup, then the semi-sync settings on all the tablets are as expected
// 2. Bringing up a new vttablet should have its replication and semi-sync setup correctly without any external interference
func TestCrossCellDurability(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "cross_cell")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// When tablets[0] is the primary, the only tablet in a different cell is tablets[3].
	// So the other two should have semi-sync turned off
	utils.CheckSemiSyncSetupCorrectly(t, tablets[0], "ON")
	utils.CheckSemiSyncSetupCorrectly(t, tablets[3], "ON")
	utils.CheckSemiSyncSetupCorrectly(t, tablets[1], "OFF")
	utils.CheckSemiSyncSetupCorrectly(t, tablets[2], "OFF")

	// Run forced reparent operation, this should proceed unimpeded.
	out, err := utils.Prs(t, clusterInstance, tablets[3])
	require.NoError(t, err, out)

	utils.ConfirmReplication(t, tablets[3], []*cluster.Vttablet{tablets[0], tablets[1], tablets[2]})

	// All the tablets will have semi-sync setup since tablets[3] is in Cell2 and all
	// others are in Cell1, so all of them are eligible to send semi-sync ACKs
	for _, tablet := range tablets {
		utils.CheckSemiSyncSetupCorrectly(t, tablet, "ON")
	}

	for i, supportsBackup := range []bool{false, true} {
		// Bring up a new replica tablet
		// In this new tablet, we do not disable active reparents, otherwise replication will not be started.
		newReplica := utils.StartNewVTTablet(t, clusterInstance, 300+i, supportsBackup)
		// Add the tablet to the list of tablets in this shard
		clusterInstance.Keyspaces[0].Shards[0].Vttablets = append(clusterInstance.Keyspaces[0].Shards[0].Vttablets, newReplica)
		// Check that we can replicate to it and semi-sync is setup correctly on it
		utils.ConfirmReplication(t, tablets[3], []*cluster.Vttablet{tablets[0], tablets[1], tablets[2], newReplica})
		utils.CheckSemiSyncSetupCorrectly(t, newReplica, "ON")
	}
}
