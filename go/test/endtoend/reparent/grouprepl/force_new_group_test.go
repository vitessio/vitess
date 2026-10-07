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

package grouprepl

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// TestGroupReplicationForceNewGroup checks the forced EmergencyReparentShard of a shard whose group lost its
// majority: two of the three voters die with their hosts, and the primary, alone, leaves the group. VTOrc
// does not bootstrap a group while voters do not answer, and an EmergencyReparentShard without the flag
// cannot follow a group without quorum. With --group-replication-force-new-group, ERS drops the dead voters,
// bootstraps a new group on the survivor, which held every acknowledged write, and the shard serves writes
// again.
func TestGroupReplicationForceNewGroup(t *testing.T) {
	tc := migratedCluster(t)
	primary, zone2, zone3 := tc.replicas[0], tc.replicas[1], tc.replicas[2]
	incarnationBefore := shardRecord(t, tc.TopoProcess.Server).GetGroupReplicationIncarnation()
	require.NotEmpty(t, incarnationBefore)

	w := startWriter(t, tc)
	require.Eventually(t, func() bool { return w.ok.Load() > 20 }, waitTimeout, pollInterval)
	killHost(t, zone2)
	killHost(t, zone3)
	// The primary loses the majority of its group, and leaves it.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		status, err := fullStatus(t, tc, primary)
		require.NoError(c, err)
		assert.False(c, mysql.IsGroupMemberActive(status.GroupReplicationStatus))
	}, waitTimeout, pollInterval)
	_, _, _ = w.stop()
	acknowledged := w.ackedIDs()
	require.NotEmpty(t, acknowledged)

	out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("EmergencyReparentShard",
		"--group-replication-force-new-group", keyspaceName+"/"+shardName)
	require.NoError(t, err, out)

	// The dead voters are dropped, and the survivor's new group is recorded.
	assert.Equal(t, []string{primary.Alias}, shardVoters(t, tc))
	assert.NotEqual(t, incarnationBefore, shardRecord(t, tc.TopoProcess.Server).GetGroupReplicationIncarnation())
	assert.Equal(t, primary.Alias, shardPrimary(t, tc))
	waitForGroup(t, tc, primary, []*cluster.Vttablet{primary})

	// Every write that the old group acknowledged is there: the survivor was its primary.
	requireAckedWrites(t, primary, acknowledged)

	// And the shard serves writes again.
	w = startWriter(t, tc)
	require.Eventually(t, func() bool { return w.ok.Load() > 10 }, waitTimeout, pollInterval)
	_, _, _ = w.stop()
}

// TestGroupReplicationGrowsFromSingleVoter checks that a group left with a single voter grows again
// without a pause of the writes: a forced EmergencyReparentShard keeps one voter of three, and VTOrc
// makes the spare of zone2, an asynchronous replica, join the group first (JoinSpareBeforeGrow), then
// gives it the seat (GrowVoter), while the primary keeps serving.
func TestGroupReplicationGrowsFromSingleVoter(t *testing.T) {
	opts := defaultClusterOptions()
	opts.rdonly = false
	opts.replicaCells = []string{cells[0], cells[1], cells[2], cells[1]}
	tc := setupCluster(t, opts)
	out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("MigrateReplicationMode",
		"--durability-policy", policy.DurabilityGroupReplicationCrossCell, keyspaceName)
	require.NoError(t, err, out)
	primary := tc.replicas[0]
	var voters, spares []*cluster.Vttablet
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		voters, spares = nil, nil
		listed := shardVoters(t, tc)
		assert.Len(c, listed, 3)
		for _, tablet := range tc.replicas {
			if slices.Contains(listed, tablet.Alias) {
				voters = append(voters, tablet)
			} else {
				spares = append(spares, tablet)
			}
		}
	}, waitTimeout, pollInterval)
	waitForGroup(t, tc, primary, voters)
	require.Len(t, spares, 1)
	spare := spares[0]

	// The voters of zone2 and zone3 die; the forced reparent keeps the primary as the only voter.
	for _, voter := range voters {
		if voter != primary {
			killHost(t, voter)
		}
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		status, err := fullStatus(t, tc, primary)
		require.NoError(c, err)
		assert.False(c, mysql.IsGroupMemberActive(status.GroupReplicationStatus))
	}, waitTimeout, pollInterval)
	out, err = tc.VtctldClientProcess.ExecuteCommandWithOutput("EmergencyReparentShard",
		"--group-replication-force-new-group", keyspaceName+"/"+shardName)
	require.NoError(t, err, out)
	require.Equal(t, []string{primary.Alias}, shardVoters(t, tc))

	// The writes go on while the spare joins and takes the seat of zone2.
	w := startWriter(t, tc)
	require.Eventually(t, func() bool { return w.ok.Load() > 10 }, waitTimeout, pollInterval)
	waitForGroup(t, tc, primary, []*cluster.Vttablet{primary, spare})
	before := w.ok.Load()
	require.Eventually(t, func() bool { return w.ok.Load() > before+10 }, waitTimeout, pollInterval)
	ok, fail, lastErr := w.stop()
	assert.Positive(t, ok)
	assert.Zero(t, fail, "writes failed while the group grew from a single voter, last error: %v", lastErr)
}
