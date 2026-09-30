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
