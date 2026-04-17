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

package tabletbalancer

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

var replicaStr = strings.ToLower(topodata.TabletType_REPLICA.String())

// TestCellModeBalancer tests the default "cell" mode which shuffles tablets in the local cell
func TestCellModeBalancer(t *testing.T) {
	// Start vtgate in cell1 with cell mode (default)
	vtgateProcess := cluster.VtgateProcessInstance(
		clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		cell1,
		fmt.Sprintf("%s,%s", cell1, cell2), // watch both cells but should only use local
		clusterInstance.Hostname,
		replicaStr,
		clusterInstance.TopoProcess.Port,
		clusterInstance.TmpDirectory,
		[]string{
			"--vtgate-balancer-mode", "cell",
		},
		plancontext.PlannerVersion(0),
	)
	require.NoError(t, vtgateProcess.Setup())
	defer vtgateProcess.TearDown()

	// Verify vtgate started successfully
	require.True(t, vtgateProcess.WaitForStatus())

	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: vtgateProcess.MySQLServerPort,
	}

	allTablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	shardName := clusterInstance.Keyspaces[0].Shards[0].Name
	replicaTablets := replicaTablets(allTablets)

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Wait for tablets to be discovered
	err = vtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", keyspaceName, shardName), 1, 30*time.Second)
	require.NoError(t, err)

	err = vtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), len(replicaTablets), 30*time.Second)
	require.NoError(t, err)

	// Fetch a map of server_id to tablet alias for later verification
	aliases := mapTabletAliasToMySQLServerID(t, clusterInstance.Keyspaces[0].Shards[0].Vttablets)

	// Insert test data
	_, err = conn.ExecuteFetch("INSERT INTO balancer_test (value) VALUES ('cell_mode_test')", 1, false)
	require.NoError(t, err)

	// wait for replication
	waitForReplication(t, replicaTablets, "cell_mode_test")

	// Execute queries against replicas - cell mode should route to local cell only
	counts := executeReplicaQueries(t, conn, 100)

	// Cell mode: verify ONLY local cell (cell1) tablets received queries
	cell1Count := 0
	cell2Count := 0
	for _, tablet := range replicaTablets {
		count := counts[aliases[tablet.Alias]]

		switch tablet.Cell {
		case cell1:
			assert.Greater(t, count, 0, "Expected local cell tablet %s to receive queries", tablet.Alias)
			cell1Count += count
		case cell2:
			assert.Equal(t, 0, count, "Expected remote cell tablet %s to receive NO queries in cell mode", tablet.Alias)
			cell2Count += count
		}
	}

	assert.Greater(t, cell1Count, 0, "Expected cell1 (local) to receive queries")
	assert.Equal(t, 0, cell2Count, "Expected cell2 (remote) to receive NO queries in cell mode")
}

// TestPreferCell tests the "prefer-cell" mode which maintains cell affinity while balancing load
func TestPreferCell(t *testing.T) {
	// Start vtgate in cell1 with prefer-cell mode
	vtgateProcess := cluster.VtgateProcessInstance(
		clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		cell1,
		fmt.Sprintf("%s,%s", cell1, cell2), // watch both cells
		clusterInstance.Hostname,
		replicaStr,
		clusterInstance.TopoProcess.Port,
		clusterInstance.TmpDirectory,
		[]string{
			"--vtgate-balancer-mode", "prefer-cell",
			"--balancer-vtgate-cells", fmt.Sprintf("%s,%s", cell1, cell2),
		},
		plancontext.PlannerVersion(0),
	)
	require.NoError(t, vtgateProcess.Setup())
	defer vtgateProcess.TearDown()

	// Verify vtgate started successfully
	require.True(t, vtgateProcess.WaitForStatus())

	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: vtgateProcess.MySQLServerPort,
	}

	allTablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	shardName := clusterInstance.Keyspaces[0].Shards[0].Name
	replicaTablets := replicaTablets(allTablets)

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Wait for tablets to be discovered
	err = vtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", keyspaceName, shardName), 1, 30*time.Second)
	require.NoError(t, err)

	err = vtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), len(replicaTablets), 30*time.Second)
	require.NoError(t, err)

	// Fetch a map of server_id to tablet alias for later verification
	aliases := mapTabletAliasToMySQLServerID(t, clusterInstance.Keyspaces[0].Shards[0].Vttablets)

	// Insert test data
	_, err = conn.ExecuteFetch("INSERT INTO balancer_test (value) VALUES ('flow_mode_test')", 1, false)
	require.NoError(t, err)

	// wait for replication
	waitForReplication(t, replicaTablets, "flow_mode_test")

	// Execute queries against replicas - prefer-cell mode supports cross-cell routing
	counts := executeReplicaQueries(t, conn, 100)

	// Prefer cell mode: verify BOTH cells received queries
	cell1Count := 0
	cell2Count := 0
	for _, tablet := range replicaTablets {
		count := counts[aliases[tablet.Alias]]

		switch tablet.Cell {
		case cell1:
			cell1Count += count
		case cell2:
			cell2Count += count
		}
	}

	assert.Greater(t, cell1Count, 0, "Expected cell1 to receive queries in prefer-cell mode")
	assert.Greater(t, cell2Count, 0, "Expected cell2 to receive queries in prefer-cell mode")
}

// TestRandomModeBalancer tests the "random" mode which uniformly distributes load
func TestRandomModeBalancer(t *testing.T) {
	// Start vtgate in cell1 with random mode
	vtgateProcess := cluster.VtgateProcessInstance(
		clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		cell1,
		fmt.Sprintf("%s,%s", cell1, cell2), // watch both cells
		clusterInstance.Hostname,
		replicaStr,
		clusterInstance.TopoProcess.Port,
		clusterInstance.TmpDirectory,
		[]string{
			"--vtgate-balancer-mode", "random",
		},
		plancontext.PlannerVersion(0),
	)
	require.NoError(t, vtgateProcess.Setup())
	defer vtgateProcess.TearDown()

	// Verify vtgate started successfully
	require.True(t, vtgateProcess.WaitForStatus())

	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: vtgateProcess.MySQLServerPort,
	}

	allTablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	shardName := clusterInstance.Keyspaces[0].Shards[0].Name
	replicaTablets := replicaTablets(allTablets)

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Wait for tablets to be discovered
	err = vtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", keyspaceName, shardName), 1, 30*time.Second)
	require.NoError(t, err)

	err = vtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), len(replicaTablets), 30*time.Second)
	require.NoError(t, err)

	// Fetch a map of server_id to tablet alias for later verification
	aliases := mapTabletAliasToMySQLServerID(t, clusterInstance.Keyspaces[0].Shards[0].Vttablets)

	// Insert test data
	_, err = conn.ExecuteFetch("INSERT INTO balancer_test (value) VALUES ('random_mode_test')", 1, false)
	require.NoError(t, err)

	// wait for replication
	waitForReplication(t, replicaTablets, "random_mode_test")

	// Execute queries against replicas - random mode distributes uniformly
	numQueries := 500
	counts := executeReplicaQueries(t, conn, numQueries)

	expectedPerReplica := numQueries / len(replicaTablets)
	tolerance := int(float64(expectedPerReplica) * 0.25) // 25% tolerance for statistical variance

	cell1Count := 0
	cell2Count := 0

	// Verify each replica got roughly equal queries
	for _, tablet := range replicaTablets {
		count := counts[aliases[tablet.Alias]]
		assert.Greater(t, count, 0, "Expected replica %s to receive queries", tablet.Alias)
		assert.InDelta(t, expectedPerReplica, count, float64(tolerance),
			"Expected replica %s to receive ~%d queries (±%d), got %d",
			tablet.Alias, expectedPerReplica, tolerance, count)

		switch tablet.Cell {
		case cell1:
			cell1Count += count
		case cell2:
			cell2Count += count
		}
	}

	// Verify both cells received queries
	assert.Greater(t, cell1Count, 0, "Expected cell1 to receive queries")
	assert.Greater(t, cell2Count, 0, "Expected cell2 to receive queries")
}

// TestRandomModeWithCellFiltering tests random mode with cell filtering via balancer-vtgate-cells
func TestRandomModeWithCellFiltering(t *testing.T) {
	// Start vtgate in cell1 with random mode but filter to only cell1 tablets
	vtgateProcess := cluster.VtgateProcessInstance(
		clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		cell1,
		fmt.Sprintf("%s,%s", cell1, cell2), // watch both cells
		clusterInstance.Hostname,
		replicaStr,
		clusterInstance.TopoProcess.Port,
		clusterInstance.TmpDirectory,
		[]string{
			"--vtgate-balancer-mode", "random",
			"--balancer-vtgate-cells", cell1, // only consider cell1 tablets
		},
		plancontext.PlannerVersion(0),
	)
	require.NoError(t, vtgateProcess.Setup())
	defer vtgateProcess.TearDown()

	// Verify vtgate started successfully
	require.True(t, vtgateProcess.WaitForStatus())

	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: vtgateProcess.MySQLServerPort,
	}

	allTablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	shardName := clusterInstance.Keyspaces[0].Shards[0].Name
	replicaTablets := replicaTablets(allTablets)

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Wait for tablets to be discovered
	err = vtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", keyspaceName, shardName), 1, 30*time.Second)
	require.NoError(t, err)

	err = vtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), len(replicaTablets), 30*time.Second)
	require.NoError(t, err)

	// Fetch a map of server_id to tablet alias for later verification
	aliases := mapTabletAliasToMySQLServerID(t, clusterInstance.Keyspaces[0].Shards[0].Vttablets)

	// Insert test data
	_, err = conn.ExecuteFetch("INSERT INTO balancer_test (value) VALUES ('random_filtered_test')", 1, false)
	require.NoError(t, err)

	// wait for replication
	waitForReplication(t, replicaTablets, "random_filtered_test")

	// Execute queries against replicas - random mode with cell filtering
	numQueries := 200
	counts := executeReplicaQueries(t, conn, numQueries)

	// Random mode with cell filtering: verify ONLY cell1 tablets received queries
	cell1Count := 0
	cell2Count := 0

	for _, tablet := range replicaTablets {
		count := counts[aliases[tablet.Alias]]
		switch tablet.Cell {
		case cell1:
			assert.Greater(t, count, 0, "Expected cell1 replica %s to receive queries", tablet.Alias)
			cell1Count += count
		case cell2:
			assert.Equal(t, 0, count, "Expected cell2 replica %s to receive NO queries (filtered out)", tablet.Alias)
			cell2Count += count
		}
	}

	assert.Equal(t, cell1Count, 200, "Expected cell1 to receive all queries")
	assert.Equal(t, 0, cell2Count, "Expected cell2 to receive NO queries (filtered out)")
	assert.Equal(t, numQueries, cell1Count, "Expected all queries to go to cell1")
}

// TestDeprecatedEnableBalancerFlag tests backward compatibility with --enable-balancer flag
func TestDeprecatedEnableBalancerFlag(t *testing.T) {
	// Start vtgate with deprecated --enable-balancer flag
	vtgateProcess := cluster.VtgateProcessInstance(
		clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		cell1,
		fmt.Sprintf("%s,%s", cell1, cell2),
		clusterInstance.Hostname,
		replicaStr,
		clusterInstance.TopoProcess.Port,
		clusterInstance.TmpDirectory,
		[]string{
			"--enable-balancer",
			"--balancer-vtgate-cells", fmt.Sprintf("%s,%s", cell1, cell2),
		},
		plancontext.PlannerVersion(0),
	)
	require.NoError(t, vtgateProcess.Setup())
	defer vtgateProcess.TearDown()

	// Verify vtgate started successfully (should map to prefer-cell mode)
	require.True(t, vtgateProcess.WaitForStatus())

	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: vtgateProcess.MySQLServerPort,
	}

	allTablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	shardName := clusterInstance.Keyspaces[0].Shards[0].Name
	replicaTablets := replicaTablets(allTablets)

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Wait for tablets to be discovered
	err = vtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", keyspaceName, shardName), 1, 30*time.Second)
	require.NoError(t, err)

	err = vtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), len(replicaTablets), 30*time.Second)
	require.NoError(t, err)

	// Fetch a map of server_id to tablet alias for later verification
	aliases := mapTabletAliasToMySQLServerID(t, clusterInstance.Keyspaces[0].Shards[0].Vttablets)

	// Insert test data
	_, err = conn.ExecuteFetch("INSERT INTO balancer_test (value) VALUES ('deprecated_flag_test')", 1, false)
	require.NoError(t, err)

	// wait for replication
	waitForReplication(t, replicaTablets, "deprecated_flag_test")

	// Execute queries against replicas - deprecated flag should behave like prefer-cell mode
	counts := executeReplicaQueries(t, conn, 100)

	// Deprecated flag should behave like prefer-cell mode: verify BOTH cells received queries
	cell1Count := 0
	cell2Count := 0
	for _, tablet := range replicaTablets {
		count := counts[aliases[tablet.Alias]]
		switch tablet.Cell {
		case cell1:
			cell1Count += count
		case cell2:
			cell2Count += count
		}
	}

	assert.Greater(t, cell1Count, 0, "Expected cell1 to receive queries (flow mode behavior)")
	assert.Greater(t, cell2Count, 0, "Expected cell2 to receive queries (flow mode behavior)")
}

// Helper functions for e2e testing of balancer modes

// executeReplicaQueries executes queries against replicas and verifies they succeed
func executeReplicaQueries(t *testing.T, conn *mysql.Conn, numQueries int) map[int64]int {
	t.Helper()

	// Use @replica to target replica tablets
	_, err := conn.ExecuteFetch("USE @replica", 1, false)
	defer func() {
		_, err = conn.ExecuteFetch("USE @primary", 1, false)
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	counts := make(map[int64]int)

	// Execute queries and verify they succeed
	for range numQueries {
		res, err := conn.ExecuteFetch("SELECT @@server_id, id FROM balancer_test LIMIT 1", 10, false)
		require.NoError(t, err)
		require.Equal(t, 1, len(res.Rows), "expected one row from balancer_test")

		serverID, err := res.Rows[0][0].ToInt64()
		require.NoError(t, err)
		counts[serverID]++
	}

	return counts
}

func mapTabletAliasToMySQLServerID(t *testing.T, tablets []*cluster.Vttablet) map[string]int64 {
	aliases := make(map[string]int64)

	for _, tablet := range tablets {
		id, err := tablet.VttabletProcess.QueryTablet("SELECT @@server_id", tablet.VttabletProcess.Keyspace, false)
		require.NoError(t, err)
		require.Equal(t, 1, len(id.Rows), "expected one row for server_id query")

		serverID, err := id.Rows[0][0].ToInt64()
		assert.NoError(t, err)
		aliases[tablet.Alias] = serverID
	}

	assert.Equal(t, len(aliases), 6, "expected six tablet aliases, got: %d", len(aliases))

	return aliases
}

func waitForReplication(t *testing.T, replicaTablets []*cluster.Vttablet, value string) {
	require.Eventually(t, func() bool {
		query := fmt.Sprintf("SELECT count(*) FROM balancer_test WHERE value = '%s'", value)
		for _, replica := range replicaTablets {
			res, err := replica.VttabletProcess.QueryTablet(query, replica.VttabletProcess.Keyspace, true)
			if err != nil || len(res.Rows) == 0 {
				return false
			}
			if val, err := res.Rows[0][0].ToUint64(); err != nil || val != 1 {
				return false
			}
		}
		return true
	}, 15*time.Second, 500*time.Millisecond)
}

func replicaTablets(allTablets []*cluster.Vttablet) []*cluster.Vttablet {
	var replicaTablets []*cluster.Vttablet
	for _, tablet := range allTablets {
		if tablet.Type == replicaStr {
			replicaTablets = append(replicaTablets, tablet)
		}
	}
	return replicaTablets
}
