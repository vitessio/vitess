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
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

// TestGroupReplicationDeposedPrimaryStaysDeposed reproduces FLAG 1 of the soak: the primary's mysqld
// and vttablet are paused (SIGSTOP), the group expels it and elects another voter, whose tablet
// becomes the shard primary. When the old primary resumes, its MySQL still shows its stale view, in
// which it is the ONLINE primary of the three voters, until it learns of its expulsion. Its tablet
// promoted itself on that view and wrote a newer primary term, and the new primary's tablet stepped
// down: the shard record flipped between the two for up to about 3s. The shard record must keep the
// new primary.
func TestGroupReplicationDeposedPrimaryStaysDeposed(t *testing.T) {
	tc := migratedCluster(t)
	old := tc.replicas[0]
	require.Equal(t, old.Alias, shardPrimary(t, tc))

	data, err := os.ReadFile(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vt_%010d", old.TabletUID), "mysql.pid"))
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	require.NoError(t, err)
	resumed := false
	resume := func() {
		if !resumed {
			resumed = true
			_ = syscall.Kill(pid, syscall.SIGCONT)
			old.VttabletProcess.Resume()
		}
	}
	t.Cleanup(resume)
	require.NoError(t, syscall.Kill(pid, syscall.SIGSTOP))
	old.VttabletProcess.Stop()

	// The group expels the paused primary and elects another voter, whose tablet becomes the shard
	// primary.
	var newPrimary *cluster.Vttablet
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		newPrimary = nil
		for _, tablet := range tc.replicas[1:] {
			if tablet.Alias == shardPrimary(t, tc) {
				newPrimary = tablet
			}
		}
		require.NotNil(c, newPrimary)
		status, err := fullStatus(t, tc, newPrimary)
		require.NoError(c, err)
		assert.True(c, mysql.IsGroupPrimary(status.GroupReplicationStatus))
		assert.Len(c, status.GroupReplicationStatus.GetMembers(), 2)
	}, waitTimeout, pollInterval)

	resume()
	// Until the old primary's MySQL learns of its expulsion and rejoins, the shard record keeps the
	// new primary.
	assert.Never(t, func() bool { return shardPrimary(t, tc) != newPrimary.Alias }, 20*time.Second, 50*time.Millisecond,
		"the shard record named another primary than %s after the old primary resumed", newPrimary.Alias)
	waitForGroup(t, tc, newPrimary, tc.replicas)
}
