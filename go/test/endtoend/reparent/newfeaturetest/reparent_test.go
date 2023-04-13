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

// TestRecoverWithMultipleVttabletFailures tests that ERS succeeds with the default values
// even when there are multiple vttablet failures. In this test we use the semi_sync policy
// to allow multiple failures to happen and still be recoverable.
// The test takes down the vttablets of the primary and a rdonly tablet and runs ERS with the
// default values of remote_operation_timeout, lock-timeout flags and wait_replicas_timeout subflag.
func TestRecoverWithMultipleVttabletFailures(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// make tablets[1] a rdonly tablet.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", tablets[1].Alias, "rdonly")
	require.NoError(t, err)

	// Confirm that replication is still working as intended
	utils.ConfirmReplication(t, tablets[0], tablets[1:])

	// Make the rdonly and primary tablets and databases unavailable.
	utils.StopTablet(t, tablets[1], true)
	utils.StopTablet(t, tablets[0], true)

	// We expect this to succeed since we only have 1 primary eligible tablet which is down
	out, err := utils.Ers(clusterInstance, nil, "", "")
	require.NoError(t, err, out)

	newPrimary := utils.GetNewPrimary(t, clusterInstance)
	utils.ConfirmReplication(t, newPrimary, []*cluster.Vttablet{tablets[2], tablets[3]})
}

// TetsSingeReplicaERS tests that ERS works even when there is only 1 tablet left
// as long the durability policy allows this failure. Moreover, this also tests that the
// replica is one such that it was a primary itself before. This way its executed gtid set
// will have atleast 2 tablets in it. We want to make sure this tablet is not marked as errant
// and ERS succeeds.
func TestSingleReplicaERS(t *testing.T) {
	// Set up a cluster with none durability policy
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "none")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	// Confirm that the replication is setup correctly in the beginning.
	// tablets[0] is the primary tablet in the beginning.
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// Delete and stop two tablets. We only want to have 2 tablets for this test.
	utils.DeleteTablet(t, clusterInstance, tablets[2])
	utils.DeleteTablet(t, clusterInstance, tablets[3])
	utils.StopTablet(t, tablets[2], true)
	utils.StopTablet(t, tablets[3], true)

	// Reparent to the other replica
	output, err := utils.Prs(t, clusterInstance, tablets[1])
	require.NoError(t, err, "error in PlannedReparentShard output - %s", output)

	// Check the replication is set up correctly before we failover
	utils.ConfirmReplication(t, tablets[1], []*cluster.Vttablet{tablets[0]})

	// Make the current primary vttablet unavailable.
	utils.StopTablet(t, tablets[1], true)

	// Run an ERS with only one replica reachable. Also, this replica is such that it was a primary before.
	output, err = utils.Ers(clusterInstance, tablets[0], "", "")
	require.NoError(t, err, "error in Emergency Reparent Shard output - %s", output)

	// Check the tablet is indeed promoted
	utils.CheckPrimaryTablet(t, clusterInstance, tablets[0])
	// Also check the writes succeed after failover
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{})
}

// TestTabletRestart tests that a running tablet can be  restarted and everything is still fine
func TestTabletRestart(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "semi_sync")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	utils.StopTablet(t, tablets[1], false)
	tablets[1].VttabletProcess.ServingStatus = "SERVING"
	err := tablets[1].VttabletProcess.Setup()
	require.NoError(t, err)
}
