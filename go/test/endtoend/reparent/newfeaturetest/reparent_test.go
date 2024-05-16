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
	"fmt"
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

// Tests ensures that ChangeTabletType works even when semi-sync plugins are not loaded.
func TestChangeTypeWithoutSemiSync(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := utils.SetupReparentCluster(t, "none")
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	ctx := context.Background()

	primary, replica := tablets[0], tablets[1]

	// Unload semi sync plugins
	for _, tablet := range tablets[0:4] {
		qr := utils.RunSQL(ctx, t, "select @@global.super_read_only", tablet)
		result := fmt.Sprintf("%v", qr.Rows[0][0].ToString())
		if result == "1" {
			utils.RunSQL(ctx, t, "set global super_read_only = 0", tablet)
		}

		utils.RunSQL(ctx, t, "UNINSTALL PLUGIN rpl_semi_sync_slave;", tablet)
		utils.RunSQL(ctx, t, "UNINSTALL PLUGIN rpl_semi_sync_master;", tablet)
	}

	utils.ValidateTopology(t, clusterInstance, true)
	utils.CheckPrimaryTablet(t, clusterInstance, primary)

	// Change replica's type to rdonly
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", replica.Alias, "rdonly")
	require.NoError(t, err)

	// Change tablets type from rdonly back to replica
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", replica.Alias, "replica")
	require.NoError(t, err)
}
