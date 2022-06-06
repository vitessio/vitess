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
	"context"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"

	"github.com/stretchr/testify/require"
)

// ERS TESTS

func TestRecoverWithMultipleFailures(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, true)
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
	out, err := utils.Ers(clusterInstance, nil, "30s", "10s")
	require.NoError(t, err, out)

	newPrimary := utils.GetNewPrimary(t, clusterInstance)
	utils.ConfirmReplication(t, newPrimary, []*cluster.Vttablet{tablets[2], tablets[3]})
}

// TestERSFailFast tests that ERS will fail fast if it cannot find any tablet which can be safely promoted instead of promoting
// a tablet and hanging while inserting a row in the reparent journal on getting semi-sync ACKs
func TestERSFailFast(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, true)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// make tablets[1] a rdonly tablet.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", tablets[1].Alias, "rdonly")
	require.NoError(t, err)

	// Confirm that replication is still working as intended
	utils.ConfirmReplication(t, tablets[0], tablets[1:])

	strChan := make(chan string)
	go func() {
		// We expect this to fail since we have ignored all replica tablets and only the rdonly is left, which is not capable of sending semi-sync ACKs
		out, err := utils.ErsIgnoreTablet(clusterInstance, tablets[2], "240s", "90s", []*cluster.Vttablet{tablets[0], tablets[3]}, false)
		require.Error(t, err)
		strChan <- out
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
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, true)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	err := clusterInstance.VtctlclientProcess.ExecuteCommand("ExecuteFetchAsDba", tablets[1].Alias, `STOP SLAVE SQL_THREAD;`)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ExecuteFetchAsDba", tablets[2].Alias, `STOP SLAVE;`)
	require.NoError(t, err)
	// Run an additional command in the current primary which will only be acked by tablets[3] and be in its relay log.
	insertedVal := utils.ConfirmReplication(t, tablets[0], nil)
	// Failover to tablets[3]
	_, err = utils.Ers(clusterInstance, tablets[3], "60s", "30s")
	require.Error(t, err, "ERS should fail with 2 replicas having replication stopped")

	// Start replication back on tablet[1]
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ExecuteFetchAsDba", tablets[1].Alias, `START SLAVE;`)
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
