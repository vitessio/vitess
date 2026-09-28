/*
Copyright 2025 The Vitess Authors.

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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/logic"
)

// startRingVTOrc starts a single VTOrc with the given extra args (including its
// own --vtorc-ring-index) and registers it with the cluster so the shared
// teardown stops it.
func startRingVTOrc(t *testing.T, config cluster.VTOrcConfiguration, cell string, extraArgs []string) *cluster.VTOrcProcess {
	t.Helper()
	p := clusterInfo.ClusterInstance.NewVTOrcProcess(config, cell)
	p.ExtraArgs = extraArgs
	require.NoError(t, p.Setup())
	clusterInfo.ClusterInstance.VTOrcProcesses = append(clusterInfo.ClusterInstance.VTOrcProcesses, p)
	return p
}

// ringShardsWatched returns the number of keyspace/shard pairs the given VTOrc
// currently watches, as published by the KeyspaceShardsWatched var.
func ringShardsWatched(t *testing.T, vtorc *cluster.VTOrcProcess) int {
	t.Helper()
	return utils.GetIntFromValue(vtorc.GetVars()["KeyspaceShardsWatched"])
}

// TestConsistentHashRing brings up two VTOrcs that together form a ring of size
// 2 with a single watcher per shard, so exactly one of them owns the test shard
// under rendezvous hashing. It verifies that:
//   - the owner watches the shard's tablets and the non-owner watches none,
//   - only the owner elects the initial primary,
//   - when the primary is killed, only the owner recovers it and promotes a
//     replica, while the non-owner keeps watching nothing.
//
// This exercises the partitioning itself: with the ring gating removed the
// non-owner would watch the shard and the coverage assertions would fail.
func TestConsistentHashRing(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)

	config := cluster.VTOrcConfiguration{PreventCrossCellFailover: true}
	// Small reparent timeouts keep the dead-primary recovery quick.
	ringArgs := []string{
		"--vtorc-ring-size=2",
		"--vtorc-ring-watchers-per-shard=1",
		"--remote-operation-timeout=10s",
		"--wait-replicas-timeout=5s",
	}

	// Set up the tablets but no VTOrcs (count 0); we start two below with
	// distinct ring indices, which the shared setup cannot do.
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, nil, config,
		map[string]int{cluster.DefaultCell: 0}, policy.DurabilitySemiSync)

	vtorc0 := startRingVTOrc(t, config, cluster.DefaultCell, append([]string{"--vtorc-ring-index=0"}, ringArgs...))
	vtorc1 := startRingVTOrc(t, config, cluster.DefaultCell, append([]string{"--vtorc-ring-index=1"}, ringArgs...))

	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	shardKey := fmt.Sprintf("%s.%s", keyspace.Name, shard0.Name)

	// Both instances must come up healthy and publish their ring config.
	for _, vtorc := range []*cluster.VTOrcProcess{vtorc0, vtorc1} {
		status, _ := utils.MakeAPICallRetry(t, vtorc, "/debug/health", func(code int, _ string) bool {
			return code != 200
		})
		require.Equal(t, 200, status)
		utils.CheckVarExists(t, vtorc, "VtorcRingSize")
		utils.CheckVarExists(t, vtorc, "VtorcRingIndex")
		utils.CheckVarExists(t, vtorc, "VtorcRingWatchersPerShard")
		require.Equal(t, 2, utils.GetIntFromValue(vtorc.GetVars()["VtorcRingSize"]))
		require.Equal(t, 1, utils.GetIntFromValue(vtorc.GetVars()["VtorcRingWatchersPerShard"]))
	}

	// Exactly one instance should own (watch) the shard. Identify which.
	var owner, nonOwner *cluster.VTOrcProcess
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		w0, w1 := ringShardsWatched(t, vtorc0), ringShardsWatched(t, vtorc1)
		if !assert.Equal(c, 1, w0+w1, "exactly one instance must watch the single shard (vtorc0=%d vtorc1=%d)", w0, w1) {
			return
		}
		if w0 == 1 {
			owner, nonOwner = vtorc0, vtorc1
		} else {
			owner, nonOwner = vtorc1, vtorc0
		}
	}, 30*time.Second, time.Second, "ring did not converge to a single watcher for the shard")

	// The owner watches the shard's tablets; the non-owner watches none.
	tabletsByShard, ok := owner.GetVars()["TabletsWatchedByShard"].(map[string]any)
	require.True(t, ok, "owner must publish TabletsWatchedByShard")
	require.Positive(t, utils.GetIntFromValue(tabletsByShard[shardKey]),
		"owner must watch the shard's tablets")
	require.Zero(t, ringShardsWatched(t, nonOwner), "non-owner must watch no shards")

	// Only the owner elects the initial primary.
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	require.NotNil(t, curPrimary, "should have elected a primary")
	utils.WaitForSuccessfulRecoveryCount(t, owner, logic.ElectNewPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)

	replica, rdonly := utils.FindReplicaAndRdonly(t, shard0, curPrimary)
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica, rdonly}, 10*time.Second)

	// Kill the primary; the owner must recover it and promote the replica.
	require.NoError(t, curPrimary.VttabletProcess.TearDown())
	require.NoError(t, curPrimary.MysqlctlProcess.Stop())
	defer utils.PermanentlyRemoveVttablet(clusterInfo, curPrimary)

	utils.CheckPrimaryTablet(t, clusterInfo, replica, true)
	utils.WaitForSuccessfulRecoveryCount(t, owner, logic.RecoverDeadPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)

	// The non-owner never watched the shard, so it still watches nothing.
	require.Zero(t, ringShardsWatched(t, nonOwner), "non-owner must still watch no shards")
}
