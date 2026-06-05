/*
Copyright 2026 The Vitess Authors.

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

package tablethealth

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// currentPrimaryAlias returns the alias string of the shard's current primary, or "" if the
// shard has no primary recorded in the topo yet. Unlike utils.ShardPrimaryTablet it never calls
// t.Fatal/assert.FailNow, so it is safe to call repeatedly inside an assert.Eventually poll while
// a reparent is in flight (when the shard transiently has no primary).
func currentPrimaryAlias(t *testing.T, keyspace *cluster.Keyspace, shard *cluster.Shard) string {
	t.Helper()
	si, err := clusterInfo.ClusterInstance.VtctldClientProcess.GetShard(keyspace.Name, shard.Name)
	if err != nil || si.Shard.PrimaryAlias == nil {
		return ""
	}
	return topoproto.TabletAliasString(si.Shard.PrimaryAlias)
}

// TestPrimaryVttabletProcessDeath verifies that when the primary's vttablet process dies while
// its mysqld keeps running, VTOrc detects the primary as unreachable, a quorum of the shard's
// replicas confirms it down, and VTOrc runs an emergency reparent to promote a replica.
//
// This is the foundation test for tablet-liveness-driven recovery. Additional scenarios that
// exercise the shard-peer health quorum (for example partial quorum, stale observers, or
// cross-cell promotion) should be added to this package as further test functions.
func TestPrimaryVttabletProcessDeath(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)

	// Bring up two replicas and one rdonly in zone1, plus a single VTOrc. The VTOrc opts in to
	// quorum-confirmed ERS for an unreachable primary vttablet and, with a unanimous fraction and
	// a single required observer, treats the primary as down once any one fresh shard peer reports
	// it unreachable. ERS is enabled by default in VTOrc, so no extra reparent flag is needed.
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, []string{
		"--emergency-reparent-on-tablet-unreachable",
		"--shard-quorum-fraction=1.0",
		"--shard-quorum-min-observers=1",
	}, cluster.VTOrcConfiguration{}, cluster.DefaultVtorcsByCell, policy.DurabilityNone)

	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	vtOrcProcess := clusterInfo.ClusterInstance.VTOrcProcesses[0]

	// Wait for the initial primary to be elected.
	primary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	require.NotNil(t, primary, "should have elected a primary")
	t.Logf("initial primary: %s", primary.Alias)

	// Identify a replica that should be promotable once the primary's vttablet dies, and collect
	// the remaining (non-primary) tablets so we can verify replication before the failure.
	var replica *cluster.Vttablet
	var nonPrimaryTablets []*cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		if tablet.Alias == primary.Alias {
			continue
		}
		nonPrimaryTablets = append(nonPrimaryTablets, tablet)
		if replica == nil && tablet.Type == "replica" {
			replica = tablet
		}
	}
	require.NotNil(t, replica, "could not find a replica tablet to promote")

	// Make sure replication is healthy before we induce the failure.
	utils.CheckReplication(t, clusterInfo, primary, nonPrimaryTablets, 10*time.Second)

	// SIGKILL ONLY the primary's vttablet process, matching the issue's `kill -9`. We must use
	// Kill (SIGKILL), not TearDown (graceful SIGTERM): a graceful shutdown runs the tablet
	// manager's Close(), which stamps TabletShutdownTime on the tablet record, and VTOrc then
	// treats the tablet as intentionally shut down and does not fail over. A crash leaves no
	// shutdown time, which is the scenario this feature targets. The mysqld is a separate process
	// and keeps running, so the replicas keep replicating and the standard dead-primary detection
	// (which relies on the underlying MySQL being gone) does not fire — only the new quorum path can.
	// Kill (SIGKILL) returns the process's "signal: killed" wait error, which is expected for an
	// abrupt termination; the kill itself still succeeds, so we ignore it (as other vtorc e2e tests do).
	_ = primary.VttabletProcess.Kill()

	// VTOrc should promote a replica. We use a non-fatal primary lookup so the poll tolerates the
	// brief window during the reparent when the shard has no primary. The timeout is generous
	// because CI runners can be slow and several detection intervals must elapse first.
	assert.Eventually(t, func() bool {
		newPrimaryAlias := currentPrimaryAlias(t, keyspace, shard0)
		return newPrimaryAlias != "" && newPrimaryAlias != primary.Alias
	}, 90*time.Second, 1*time.Second, "expected VTOrc to promote a new primary after the primary vttablet died")

	// Confirm the new primary is fully healthy and that the promotion was specifically an ERS.
	utils.CheckPrimaryTablet(t, clusterInfo, replica, true)
	utils.WaitForSuccessfulERSCount(t, vtOrcProcess, keyspace.Name, shard0.Name, 1)

	// We killed this tablet's vttablet, so drop it from the global list before the suite tears down.
	utils.PermanentlyRemoveVttablet(clusterInfo, primary)
}
