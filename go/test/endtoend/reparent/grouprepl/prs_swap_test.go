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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// TestGroupReplicationPlannedReparentToNonVoter checks PlannedReparentShard to a tablet that is not a voter:
// it takes the seat of the voter of its cell and joins the group, then the reparent goes on. First to the
// spare of zone2, whose voter is not the primary: the swap happens while the primary serves. Then to the spare
// of zone1, the primary's cell: PRS demotes the primary, swaps the spare in for it, the spare joins, and PRS
// promotes it. vtgate buffers the writes throughout, and the group keeps one voter per cell.
func TestGroupReplicationPlannedReparentToNonVoter(t *testing.T) {
	opts := defaultClusterOptions()
	opts.rdonly = false
	opts.replicaCells = []string{cells[0], cells[1], cells[2], cells[0], cells[1]}
	tc := setupCluster(t, opts)
	out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("MigrateReplicationMode",
		"--durability-policy", policy.DurabilityGroupReplicationCrossCell, keyspaceName)
	require.NoError(t, err, out)
	primary := tc.replicas[0]

	// The voters are one tablet per cell; the second tablets of zone1 and zone2 are spares.
	var voters []*cluster.Vttablet
	spareOf := make(map[string]*cluster.Vttablet)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		voters, spareOf = nil, make(map[string]*cluster.Vttablet)
		listed := shardVoters(t, tc)
		assert.Len(c, listed, 3)
		for _, tablet := range tc.replicas {
			if slices.Contains(listed, tablet.Alias) {
				voters = append(voters, tablet)
			} else {
				spareOf[tablet.Cell] = tablet
			}
		}
	}, waitTimeout, pollInterval)
	waitForGroup(t, tc, primary, voters)
	require.Contains(t, spareOf, cells[0])
	require.Contains(t, spareOf, cells[1])

	reparentTo := func(t *testing.T, target *cluster.Vttablet) []*cluster.Vttablet {
		w := startWriter(t, tc)
		require.Eventually(t, func() bool { return w.ok.Load() > 10 }, waitTimeout, pollInterval)
		var err error
		reparent := timed(func() {
			err = tc.VtctldClientProcess.PlannedReparentShard(keyspaceName, shardName, target.Alias)
		})
		require.NoError(t, err)
		// The target took the seat of the voter of its cell.
		newVoters := []*cluster.Vttablet{target}
		for _, voter := range voters {
			if voter.Cell != target.Cell {
				newVoters = append(newVoters, voter)
			}
		}
		waitForGroup(t, tc, target, newVoters)
		before := w.ok.Load()
		require.Eventually(t, func() bool { return w.ok.Load() > before+10 }, waitTimeout, pollInterval)
		ok, fail, lastErr := w.stop()
		assert.Positive(t, ok)
		requireNoFailedWrites(t, fail, w.watcherLagFailures(), []span{reparent}, lastErr, "during the planned reparent to a tablet that was not a voter")
		t.Logf("planned reparent to %s took %v; the longest write took %v", target.Alias, reparent.end.Sub(reparent.start).Round(time.Millisecond), w.longestWrite().Round(time.Millisecond))
		return newVoters
	}

	t.Run("to the spare of a cell whose voter is not the primary", func(t *testing.T) {
		voters = reparentTo(t, spareOf[cells[1]])
		primary = spareOf[cells[1]]
	})
	t.Run("to the spare of the primary's cell, after the primary moved back to it", func(t *testing.T) {
		// Move the primary to zone1's voter, so that zone1's spare replaces the primary itself.
		var zone1Voter *cluster.Vttablet
		for _, voter := range voters {
			if voter.Cell == cells[0] {
				zone1Voter = voter
			}
		}
		require.NotNil(t, zone1Voter)
		require.NoError(t, tc.VtctldClientProcess.PlannedReparentShard(keyspaceName, shardName, zone1Voter.Alias))
		moved := time.Now()
		waitForGroup(t, tc, zone1Voter, voters)
		// vtgate does not buffer the shard's writes again within --buffer-min-time-between-failovers
		// of that reparent.
		require.Eventually(t, func() bool { return time.Since(moved) > msBufferCooldown }, waitTimeout, pollInterval)
		voters = reparentTo(t, spareOf[cells[0]])
	})
}
