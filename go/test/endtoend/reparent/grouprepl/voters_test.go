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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// TestGroupReplicationOneVoterPerCell checks that the cross-cell policy makes one tablet per
// cell a voting member, that the other tablets replicate asynchronously, and that VTOrc
// replaces a voter whose host died with another tablet of the same cell.
func TestGroupReplicationOneVoterPerCell(t *testing.T) {
	opts := defaultClusterOptions()
	// Two REPLICA tablets in zone2.
	opts.replicaCells = []string{"zone1", "zone2", "zone2", "zone3"}
	opts.rdonly = false
	opts.vtorcExtraArgs = []string{"--group-replication-voter-replacement-grace-period", "10s"}
	tc := setupCluster(t, opts)
	primary, zone2a, zone2b, zone3 := tc.replicas[0], tc.replicas[1], tc.replicas[2], tc.replicas[3]

	out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("MigrateReplicationMode",
		"--durability-policy", policy.DurabilityGroupReplicationCrossCell, keyspaceName)
	require.NoError(t, err, out)
	waitForGroup(t, tc, primary, []*cluster.Vttablet{primary, zone2a, zone3})

	// The second tablet of zone2 is not a member: it replicates asynchronously from the primary.
	status, err := fullStatus(t, tc, zone2b)
	require.NoError(t, err)
	assert.False(t, mysql.IsGroupMemberActive(status.GroupReplicationStatus))
	require.NotNil(t, status.ReplicationStatus)
	assert.Equal(t, int32(primary.MySQLPort), status.ReplicationStatus.SourcePort)

	w := startWriter(t, tc)
	killHost(t, zone2a)

	// Once the grace period expires, VTOrc makes the other tablet of zone2 the cell's voter.
	waitForGroup(t, tc, primary, []*cluster.Vttablet{primary, zone2b, zone3})
	ok, fail, lastErr := w.stop()
	assert.Positive(t, ok)
	// The group kept its majority throughout, so the primary never stopped accepting writes.
	assert.Zero(t, fail, "writes failed while a secondary was replaced, last error: %v", lastErr)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		want, err := rowCount(t, primary)
		require.NoError(c, err)
		for _, tablet := range []*cluster.Vttablet{zone2b, zone3} {
			got, err := rowCount(t, tablet)
			require.NoError(c, err)
			assert.Equal(c, want, got, tablet.Alias)
		}
	}, waitTimeout, pollInterval)
}

// TestGroupReplicationSwapsIneligibleVoter checks that a secondary voter whose tablet type changes to
// RDONLY, which the policy does not allow as a voter, gives its seat to the other REPLICA of its cell
// right away (SwapVoter, without the grace period, while its MySQL is still ONLINE), that its tablet
// then leaves the group, and that no write fails meanwhile.
func TestGroupReplicationSwapsIneligibleVoter(t *testing.T) {
	opts := defaultClusterOptions()
	// Two REPLICA tablets in zone2.
	opts.replicaCells = []string{"zone1", "zone2", "zone2", "zone3"}
	opts.rdonly = false
	// A swap that waited for the grace period would not happen within the test's waits.
	opts.vtorcExtraArgs = []string{"--group-replication-voter-replacement-grace-period", "1h"}
	tc := setupCluster(t, opts)
	primary, zone2a, zone2b, zone3 := tc.replicas[0], tc.replicas[1], tc.replicas[2], tc.replicas[3]

	out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("MigrateReplicationMode",
		"--durability-policy", policy.DurabilityGroupReplicationCrossCell, keyspaceName)
	require.NoError(t, err, out)
	waitForGroup(t, tc, primary, []*cluster.Vttablet{primary, zone2a, zone3})

	w := startWriter(t, tc)
	require.Eventually(t, func() bool { return w.ok.Load() > 20 }, waitTimeout, pollInterval)
	out, err = tc.VtctldClientProcess.ExecuteCommandWithOutput("ChangeTabletType", zone2a.Alias, "rdonly")
	require.NoError(t, err, out)

	// VTOrc gives zone2a's seat to zone2b, which joins the group.
	waitForGroup(t, tc, primary, []*cluster.Vttablet{primary, zone2b, zone3})
	// zone2a, no longer a voter, leaves the group.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		status, err := fullStatus(t, tc, zone2a)
		require.NoError(c, err)
		assert.False(c, mysql.IsGroupMemberActive(status.GroupReplicationStatus))
	}, waitTimeout, pollInterval)
	before := w.ok.Load()
	require.Eventually(t, func() bool { return w.ok.Load() > before+20 }, waitTimeout, pollInterval)
	ok, fail, lastErr := w.stop()
	assert.Positive(t, ok)
	// The primary kept a majority of its voters ONLINE throughout.
	assert.Zero(t, fail, "writes failed while an ineligible voter was replaced, last error: %v", lastErr)
	requireAckedWrites(t, primary, w.ackedIDs())
}
