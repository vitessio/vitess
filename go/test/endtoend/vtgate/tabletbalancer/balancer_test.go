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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/proto/topodata"
)

var replicaStr = strings.ToLower(topodata.TabletType_REPLICA.String())

// TestCellModeBalancer tests the default "cell" mode which shuffles tablets in the local cell
func TestCellModeBalancer(t *testing.T) {
	ctx := t.Context()
	clusterInstance := setupCluster(t)

	// Start vtgate in cell1 with cell mode (default)
	vtgate, err := clusterInstance.AddVTGate(
		t,
		ctx,
		"--vtgate-balancer-mode", "cell",
	)
	require.NoError(t, err)

	vtParams := vtgateParams(t, ctx, vtgate)

	shard := clusterInstance.Keyspace(keyspaceName).Shards()[0]
	allTablets := shard.Tablets()
	shardName := shard.Name
	replicaTablets := replicaTablets(allTablets)

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Wait for tablets to be discovered
	waitForTablets(t, ctx, vtgate, fmt.Sprintf("%s.%s.primary", keyspaceName, shardName), 1)
	waitForTablets(t, ctx, vtgate, fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), len(replicaTablets))

	// Fetch a map of server_id to tablet alias for later verification
	aliases := mapTabletAliasToMySQLServerID(t, ctx, allTablets)

	// Insert test data
	_, err = conn.ExecuteFetch("INSERT INTO balancer_test (value) VALUES ('cell_mode_test')", 1, false)
	require.NoError(t, err)

	// wait for replication
	waitForReplication(t, ctx, replicaTablets, "cell_mode_test")

	// Execute queries against replicas - cell mode should route to local cell only
	counts := executeReplicaQueries(t, conn, 100)

	// Cell mode: verify ONLY local cell (cell1) tablets received queries
	cell1Count := 0
	cell2Count := 0
	for _, tablet := range replicaTablets {
		count := counts[aliases[tablet.Alias()]]

		switch tablet.Cell {
		case cell1:
			assert.Greater(t, count, 0, "Expected local cell tablet %s to receive queries", tablet.Alias())
			cell1Count += count
		case cell2:
			assert.Equal(t, 0, count, "Expected remote cell tablet %s to receive NO queries in cell mode", tablet.Alias())
			cell2Count += count
		}
	}

	assert.Greater(t, cell1Count, 0, "Expected cell1 (local) to receive queries")
	assert.Equal(t, 0, cell2Count, "Expected cell2 (remote) to receive NO queries in cell mode")
}

// TestPreferCell tests the "prefer-cell" mode which maintains cell affinity while balancing load
func TestPreferCell(t *testing.T) {
	ctx := t.Context()
	clusterInstance := setupCluster(t)

	// Start vtgate in cell1 with prefer-cell mode
	vtgate, err := clusterInstance.AddVTGate(
		t,
		ctx,
		"--vtgate-balancer-mode", "prefer-cell",
		"--balancer-vtgate-cells", fmt.Sprintf("%s,%s", cell1, cell2),
	)
	require.NoError(t, err)

	vtParams := vtgateParams(t, ctx, vtgate)

	shard := clusterInstance.Keyspace(keyspaceName).Shards()[0]
	allTablets := shard.Tablets()
	shardName := shard.Name
	replicaTablets := replicaTablets(allTablets)

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Wait for tablets to be discovered
	waitForTablets(t, ctx, vtgate, fmt.Sprintf("%s.%s.primary", keyspaceName, shardName), 1)
	waitForTablets(t, ctx, vtgate, fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), len(replicaTablets))

	// Fetch a map of server_id to tablet alias for later verification
	aliases := mapTabletAliasToMySQLServerID(t, ctx, allTablets)

	// Insert test data
	_, err = conn.ExecuteFetch("INSERT INTO balancer_test (value) VALUES ('flow_mode_test')", 1, false)
	require.NoError(t, err)

	// wait for replication
	waitForReplication(t, ctx, replicaTablets, "flow_mode_test")

	// Execute queries against replicas - prefer-cell mode supports cross-cell routing
	counts := executeReplicaQueries(t, conn, 100)

	// Prefer cell mode: verify BOTH cells received queries
	cell1Count := 0
	cell2Count := 0
	for _, tablet := range replicaTablets {
		count := counts[aliases[tablet.Alias()]]

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
	ctx := t.Context()
	clusterInstance := setupCluster(t)

	// Start vtgate in cell1 with random mode
	vtgate, err := clusterInstance.AddVTGate(
		t,
		ctx,
		"--vtgate-balancer-mode", "random",
	)
	require.NoError(t, err)

	vtParams := vtgateParams(t, ctx, vtgate)

	shard := clusterInstance.Keyspace(keyspaceName).Shards()[0]
	allTablets := shard.Tablets()
	shardName := shard.Name
	replicaTablets := replicaTablets(allTablets)

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Wait for tablets to be discovered
	waitForTablets(t, ctx, vtgate, fmt.Sprintf("%s.%s.primary", keyspaceName, shardName), 1)
	waitForTablets(t, ctx, vtgate, fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), len(replicaTablets))

	// Fetch a map of server_id to tablet alias for later verification
	aliases := mapTabletAliasToMySQLServerID(t, ctx, allTablets)

	// Insert test data
	_, err = conn.ExecuteFetch("INSERT INTO balancer_test (value) VALUES ('random_mode_test')", 1, false)
	require.NoError(t, err)

	// wait for replication
	waitForReplication(t, ctx, replicaTablets, "random_mode_test")

	// Execute queries against replicas - random mode distributes uniformly
	numQueries := 500
	counts := executeReplicaQueries(t, conn, numQueries)

	expectedPerReplica := numQueries / len(replicaTablets)
	tolerance := int(float64(expectedPerReplica) * 0.25) // 25% tolerance for statistical variance

	cell1Count := 0
	cell2Count := 0

	// Verify each replica got roughly equal queries
	for _, tablet := range replicaTablets {
		count := counts[aliases[tablet.Alias()]]
		assert.Greater(t, count, 0, "Expected replica %s to receive queries", tablet.Alias())
		assert.InDelta(t, expectedPerReplica, count, float64(tolerance),
			"Expected replica %s to receive ~%d queries (±%d), got %d",
			tablet.Alias(), expectedPerReplica, tolerance, count)

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
	ctx := t.Context()
	clusterInstance := setupCluster(t)

	// Start vtgate in cell1 with random mode but filter to only cell1 tablets
	vtgate, err := clusterInstance.AddVTGate(
		t,
		ctx,
		"--vtgate-balancer-mode", "random",
		"--balancer-vtgate-cells", cell1, // only consider cell1 tablets
	)
	require.NoError(t, err)

	vtParams := vtgateParams(t, ctx, vtgate)

	shard := clusterInstance.Keyspace(keyspaceName).Shards()[0]
	allTablets := shard.Tablets()
	shardName := shard.Name
	replicaTablets := replicaTablets(allTablets)

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Wait for tablets to be discovered
	waitForTablets(t, ctx, vtgate, fmt.Sprintf("%s.%s.primary", keyspaceName, shardName), 1)
	waitForTablets(t, ctx, vtgate, fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), len(replicaTablets))

	// Fetch a map of server_id to tablet alias for later verification
	aliases := mapTabletAliasToMySQLServerID(t, ctx, allTablets)

	// Insert test data
	_, err = conn.ExecuteFetch("INSERT INTO balancer_test (value) VALUES ('random_filtered_test')", 1, false)
	require.NoError(t, err)

	// wait for replication
	waitForReplication(t, ctx, replicaTablets, "random_filtered_test")

	// Execute queries against replicas - random mode with cell filtering
	numQueries := 200
	counts := executeReplicaQueries(t, conn, numQueries)

	// Random mode with cell filtering: verify ONLY cell1 tablets received queries
	cell1Count := 0
	cell2Count := 0

	for _, tablet := range replicaTablets {
		count := counts[aliases[tablet.Alias()]]
		switch tablet.Cell {
		case cell1:
			assert.Greater(t, count, 0, "Expected cell1 replica %s to receive queries", tablet.Alias())
			cell1Count += count
		case cell2:
			assert.Equal(t, 0, count, "Expected cell2 replica %s to receive NO queries (filtered out)", tablet.Alias())
			cell2Count += count
		}
	}

	assert.Equal(t, cell1Count, 200, "Expected cell1 to receive all queries")
	assert.Equal(t, 0, cell2Count, "Expected cell2 to receive NO queries (filtered out)")
	assert.Equal(t, numQueries, cell1Count, "Expected all queries to go to cell1")
}

// TestDeprecatedEnableBalancerFlag tests backward compatibility with --enable-balancer flag
func TestDeprecatedEnableBalancerFlag(t *testing.T) {
	ctx := t.Context()
	clusterInstance := setupCluster(t)

	// Start vtgate with deprecated --enable-balancer flag
	vtgate, err := clusterInstance.AddVTGate(
		t,
		ctx,
		"--enable-balancer",
		"--balancer-vtgate-cells", fmt.Sprintf("%s,%s", cell1, cell2),
	)
	require.NoError(t, err)

	vtParams := vtgateParams(t, ctx, vtgate)

	shard := clusterInstance.Keyspace(keyspaceName).Shards()[0]
	allTablets := shard.Tablets()
	shardName := shard.Name
	replicaTablets := replicaTablets(allTablets)

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Wait for tablets to be discovered
	waitForTablets(t, ctx, vtgate, fmt.Sprintf("%s.%s.primary", keyspaceName, shardName), 1)
	waitForTablets(t, ctx, vtgate, fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), len(replicaTablets))

	// Fetch a map of server_id to tablet alias for later verification
	aliases := mapTabletAliasToMySQLServerID(t, ctx, allTablets)

	// Insert test data
	_, err = conn.ExecuteFetch("INSERT INTO balancer_test (value) VALUES ('deprecated_flag_test')", 1, false)
	require.NoError(t, err)

	// wait for replication
	waitForReplication(t, ctx, replicaTablets, "deprecated_flag_test")

	// Execute queries against replicas - deprecated flag should behave like prefer-cell mode
	counts := executeReplicaQueries(t, conn, 100)

	// Deprecated flag should behave like prefer-cell mode: verify BOTH cells received queries
	cell1Count := 0
	cell2Count := 0
	for _, tablet := range replicaTablets {
		count := counts[aliases[tablet.Alias()]]
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

// vtgateParams returns MySQL connection parameters for the given vtgate.
func vtgateParams(t *testing.T, ctx context.Context, vtgate *vitesst.VTGate) mysql.ConnParams {
	t.Helper()

	addr, err := vtgate.MySQLAddr(ctx)
	require.NoError(t, err)

	host, portStr, ok := strings.Cut(addr, ":")
	require.True(t, ok, "malformed vtgate mysql address %q", addr)

	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	return mysql.ConnParams{
		Host: host,
		Port: port,
	}
}

// waitForTablets waits until the vtgate healthcheck reports the expected number
// of endpoints for the named target (keyspace.shard.tablettype).
func waitForTablets(t *testing.T, ctx context.Context, vtgate *vitesst.VTGate, name string, count int) {
	t.Helper()

	require.Eventually(t, func() bool {
		vars, err := vtgate.GetVars(ctx)
		if err != nil {
			return false
		}
		conns, ok := vars["HealthcheckConnections"].(map[string]any)
		if !ok {
			return false
		}
		value, ok := conns[name].(float64)
		return ok && int(value) == count
	}, 30*time.Second, 300*time.Millisecond)
}

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

func mapTabletAliasToMySQLServerID(t *testing.T, ctx context.Context, tablets []*vitesst.Tablet) map[string]int64 {
	aliases := make(map[string]int64)

	for _, tablet := range tablets {
		id, err := tablet.QueryTablet(ctx, "SELECT @@server_id")
		require.NoError(t, err)
		require.Equal(t, 1, len(id.Rows), "expected one row for server_id query")

		serverID, err := id.Rows[0][0].ToInt64()
		assert.NoError(t, err)
		aliases[tablet.Alias()] = serverID
	}

	assert.Equal(t, len(aliases), 6, "expected six tablet aliases, got: %d", len(aliases))

	return aliases
}

func waitForReplication(t *testing.T, ctx context.Context, replicaTablets []*vitesst.Tablet, value string) {
	require.Eventually(t, func() bool {
		query := fmt.Sprintf("SELECT count(*) FROM balancer_test WHERE value = '%s'", value)
		for _, replica := range replicaTablets {
			res, err := replica.QueryTablet(ctx, query)
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

func replicaTablets(allTablets []*vitesst.Tablet) []*vitesst.Tablet {
	var replicaTablets []*vitesst.Tablet
	for _, tablet := range allTablets {
		if tablet.Type() == replicaStr {
			replicaTablets = append(replicaTablets, tablet)
		}
	}
	return replicaTablets
}
