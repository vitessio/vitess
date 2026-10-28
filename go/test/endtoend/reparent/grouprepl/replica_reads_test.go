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
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// TestGroupReplicationReplicaReadsWithPollingLag checks that the secondaries of a group serve
// replica reads through vtgate when the tablets track replication lag by polling MySQL
// (--enable-replication-reporter) rather than with heartbeats. A member of a group has no default
// replication channel, so a poller that only reads SHOW REPLICA STATUS reports an error, and every
// secondary stops serving.
func TestGroupReplicationReplicaReadsWithPollingLag(t *testing.T) {
	opts := defaultClusterOptions()
	opts.pollingLag = true
	opts.cellsAlias = true
	tc := setupCluster(t, opts)
	primary := tc.replicas[0]
	secondaries := tc.replicas[1:]

	out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("MigrateReplicationMode",
		"--durability-policy", policy.DurabilityGroupReplicationCrossCell, keyspaceName)
	require.NoError(t, err, out)
	waitForGroup(t, tc, primary, tc.replicas)

	// The server_uuid of each secondary's MySQL tells which secondary answered a read.
	secondaryOf := make(map[string]string)
	for _, secondary := range secondaries {
		status, err := fullStatus(t, tc, secondary)
		require.NoError(t, err)
		require.True(t, mysql.IsGroupMemberActive(status.GroupReplicationStatus), secondary.Alias)
		secondaryOf[status.ServerUuid] = secondary.Alias
	}

	ctx := t.Context()
	primaryConn, err := mysql.Connect(ctx, &mysql.ConnParams{Host: tc.Hostname, Port: tc.VtgateMySQLPort, DbName: keyspaceName + "@primary"})
	require.NoError(t, err)
	t.Cleanup(primaryConn.Close)
	_, err = primaryConn.ExecuteFetch("insert into writes (val) values ('replica read')", 0, false)
	require.NoError(t, err)

	replicaConn, err := mysql.Connect(ctx, &mysql.ConnParams{Host: tc.Hostname, Port: tc.VtgateMySQLPort, DbName: keyspaceName + "@replica"})
	require.NoError(t, err)
	t.Cleanup(replicaConn.Close)

	// vtgate spreads replica reads over the healthy REPLICA tablets of its cell alias, which are
	// the two secondaries: the primary is not a REPLICA tablet and the async replica is RDONLY.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		answered := make(map[string]int)
		for range 40 {
			qr, err := replicaConn.ExecuteFetch("select @@global.server_uuid from writes limit 1", 1, false)
			require.NoError(c, err)
			require.Len(c, qr.Rows, 1)
			uuid := qr.Rows[0][0].ToString()
			alias, ok := secondaryOf[uuid]
			require.True(c, ok, "a replica read was answered by %s, which is not a secondary", uuid)
			answered[alias]++
		}
		for _, secondary := range secondaries {
			assert.Positive(c, answered[secondary.Alias], "secondary %s answered no replica read: %v", secondary.Alias, answered)
		}
	}, waitTimeout, pollInterval)

	for _, secondary := range secondaries {
		assert.Equal(t, "SERVING", secondary.VttabletProcess.GetTabletStatus(), secondary.Alias)
	}
}
