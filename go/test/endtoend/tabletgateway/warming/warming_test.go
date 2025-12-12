/*
Copyright 2024 The Vitess Authors.

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

package warming

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

// TestWarmingBalancerTrafficDistribution verifies that the warming balancer
// routes ~90% of traffic to old replicas and ~10% to new replicas.
func TestWarmingBalancerTrafficDistribution(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Wait for all tablets to be healthy
	waitForHealthyTablets(t)

	// Get server IDs for old and new replicas
	oldServerID := getServerID(t, oldReplica)
	newServerID := getServerID(t, newReplica)
	t.Logf("Old replica server_id: %d, New replica server_id: %d", oldServerID, newServerID)

	// Insert test data
	_, err = conn.ExecuteFetch("INSERT INTO test_table (val) VALUES ('warming_test')", 1, false)
	require.NoError(t, err)

	// Wait for replication
	waitForReplication(t, "warming_test")

	// Execute many queries and track distribution
	const numQueries = 1000
	counts := executeReplicaQueries(t, conn, numQueries)

	oldCount := counts[oldServerID]
	newCount := counts[newServerID]
	t.Logf("Query distribution: old=%d (%.1f%%), new=%d (%.1f%%)",
		oldCount, float64(oldCount)/float64(numQueries)*100,
		newCount, float64(newCount)/float64(numQueries)*100)

	// Verify distribution: old should get ~90%, new should get ~10%
	// Use 20% tolerance for statistical variance
	expectedOldPercent := 0.90
	expectedNewPercent := 0.10
	tolerance := 0.20

	actualOldPercent := float64(oldCount) / float64(numQueries)
	actualNewPercent := float64(newCount) / float64(numQueries)

	assert.InEpsilon(t, expectedOldPercent, actualOldPercent, tolerance,
		"Old replica should receive ~90%% of traffic, got %.1f%%", actualOldPercent*100)
	assert.InEpsilon(t, expectedNewPercent, actualNewPercent, tolerance,
		"New replica should receive ~10%% of traffic, got %.1f%%", actualNewPercent*100)
}

// TestWarmingBalancerQueriesSucceed verifies queries work correctly with warming mode.
func TestWarmingBalancerQueriesSucceed(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Wait for all tablets to be healthy
	waitForHealthyTablets(t)

	// Insert data
	_, err = conn.ExecuteFetch("INSERT INTO test_table (val) VALUES ('query_test')", 1, false)
	require.NoError(t, err)

	// Wait for replication
	waitForReplication(t, "query_test")

	// Run queries - all should succeed
	_, err = conn.ExecuteFetch("USE @replica", 1, false)
	require.NoError(t, err)

	for range 50 {
		qr, err := conn.ExecuteFetch("SELECT * FROM test_table WHERE val = 'query_test'", 10, false)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(qr.Rows), 1, "Expected at least 1 row")
	}
}

// TestWarmingBalancerAllNewTablets verifies that when all tablets are "new",
// traffic is distributed evenly (no warming needed since nothing to warm against).
func TestWarmingBalancerAllNewTablets(t *testing.T) {
	// Set the "old" replica's start time to recent so both are "new"
	setTabletStartTime(t, oldReplica, time.Now().Add(-5*time.Minute))
	defer setTabletStartTime(t, oldReplica, time.Now().Add(-1*time.Hour)) // restore

	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	waitForHealthyTablets(t)

	// Insert data and wait for replication
	_, err = conn.ExecuteFetch("INSERT INTO test_table (val) VALUES ('all_new_test')", 1, false)
	require.NoError(t, err)
	waitForReplicationOnTablets(t, "all_new_test", []*cluster.Vttablet{oldReplica, newReplica})

	// Get server IDs
	oldServerID := getServerID(t, oldReplica)
	newServerID := getServerID(t, newReplica)

	// Execute queries and verify even distribution
	const numQueries = 1000
	counts := executeReplicaQueries(t, conn, numQueries)

	oldCount := counts[oldServerID]
	newCount := counts[newServerID]
	t.Logf("All-new distribution: replica1=%d (%.1f%%), replica2=%d (%.1f%%)",
		oldCount, float64(oldCount)/float64(numQueries)*100,
		newCount, float64(newCount)/float64(numQueries)*100)

	// Both should get ~50% each (random distribution)
	expectedPercent := 0.50
	tolerance := 0.20 // 20% tolerance for random distribution

	actualOldPercent := float64(oldCount) / float64(numQueries)
	actualNewPercent := float64(newCount) / float64(numQueries)

	assert.InEpsilon(t, expectedPercent, actualOldPercent, tolerance,
		"Replica1 should receive ~50%% of traffic in all-new mode, got %.1f%%", actualOldPercent*100)
	assert.InEpsilon(t, expectedPercent, actualNewPercent, tolerance,
		"Replica2 should receive ~50%% of traffic in all-new mode, got %.1f%%", actualNewPercent*100)
}

// TestWarmingBalancerOldReplicaFailure verifies that when the old replica fails,
// queries still succeed by routing to the new replica.
func TestWarmingBalancerOldReplicaFailure(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	waitForHealthyTablets(t)

	// Insert data and wait for replication before stopping old replica
	_, err = conn.ExecuteFetch("INSERT INTO test_table (val) VALUES ('failover_test')", 1, false)
	require.NoError(t, err)
	waitForReplication(t, "failover_test")

	// Get new replica server ID
	newServerID := getServerID(t, newReplica)

	// Stop the old replica
	err = oldReplica.VttabletProcess.TearDown()
	require.NoError(t, err)
	defer func() {
		// Restore old replica by restarting it - we need to actually restart
		// the process since we called TearDown(). The tablet will get a new
		// start time when it registers with topo, and we update it to be "old".
		shard := &clusterInstance.Keyspaces[0].Shards[0]
		oldReplica.VttabletProcess = cluster.VttabletProcessInstance(
			oldReplica.HTTPPort,
			oldReplica.GrpcPort,
			oldReplica.TabletUID,
			cell,
			shard.Name,
			keyspaceName,
			clusterInstance.VtctldProcess.Port,
			oldReplica.Type,
			clusterInstance.TopoProcess.Port,
			clusterInstance.Hostname,
			clusterInstance.TmpDirectory,
			clusterInstance.VtTabletExtraArgs,
			clusterInstance.DefaultCharset)
		oldReplica.VttabletProcess.ServingStatus = "SERVING"
		if err := oldReplica.VttabletProcess.Setup(); err != nil {
			t.Logf("Warning: failed to restart old replica in cleanup: %v", err)
			return
		}
		// Wait for it to be healthy, then set the start time to "old"
		shardName := clusterInstance.Keyspaces[0].Shards[0].Name
		require.Eventually(t, func() bool {
			err := clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(
				fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), 2, 5*time.Second)
			return err == nil
		}, 60*time.Second, 1*time.Second, "Tablet did not become healthy after restart")
		setTabletStartTime(t, oldReplica, time.Now().Add(-1*time.Hour))
	}()

	// Wait for VTGate to notice the old replica is gone
	shardName := clusterInstance.Keyspaces[0].Shards[0].Name
	require.Eventually(t, func() bool {
		err := clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(
			fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), 1, 5*time.Second)
		return err == nil
	}, 60*time.Second, 1*time.Second, "VTGate did not detect replica count change")

	// All queries should succeed and go to the new replica
	_, err = conn.ExecuteFetch("USE @replica", 1, false)
	require.NoError(t, err)

	const numQueries = 50
	for range numQueries {
		res, err := conn.ExecuteFetch("SELECT @@server_id FROM test_table LIMIT 1", 1, false)
		require.NoError(t, err, "Query should succeed even with old replica down")
		require.Len(t, res.Rows, 1)

		serverID, err := res.Rows[0][0].ToInt64()
		require.NoError(t, err)
		assert.Equal(t, newServerID, serverID, "All queries should go to new replica")
	}
}

// TestWarmingBalancerAllOldTablets verifies that when all tablets are "old"
// (past the warming period), traffic is distributed evenly.
// This also validates that warming period expiration works correctly.
func TestWarmingBalancerAllOldTablets(t *testing.T) {
	// Set the "new" replica's start time to old so both are "old"
	setTabletStartTime(t, newReplica, time.Now().Add(-1*time.Hour))
	defer setTabletStartTime(t, newReplica, time.Now()) // restore to "new"

	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	waitForHealthyTablets(t)

	// Insert data and wait for replication
	_, err = conn.ExecuteFetch("INSERT INTO test_table (val) VALUES ('all_old_test')", 1, false)
	require.NoError(t, err)
	waitForReplicationOnTablets(t, "all_old_test", []*cluster.Vttablet{oldReplica, newReplica})

	// Get server IDs
	oldServerID := getServerID(t, oldReplica)
	newServerID := getServerID(t, newReplica)

	// Execute queries and verify even distribution
	const numQueries = 1000
	counts := executeReplicaQueries(t, conn, numQueries)

	oldCount := counts[oldServerID]
	newCount := counts[newServerID]
	t.Logf("All-old distribution: replica1=%d (%.1f%%), replica2=%d (%.1f%%)",
		oldCount, float64(oldCount)/float64(numQueries)*100,
		newCount, float64(newCount)/float64(numQueries)*100)

	// Both should get ~50% each (random distribution among old tablets)
	expectedPercent := 0.50
	tolerance := 0.20

	actualOldPercent := float64(oldCount) / float64(numQueries)
	actualNewPercent := float64(newCount) / float64(numQueries)

	assert.InEpsilon(t, expectedPercent, actualOldPercent, tolerance,
		"Replica1 should receive ~50%% of traffic in all-old mode, got %.1f%%", actualOldPercent*100)
	assert.InEpsilon(t, expectedPercent, actualNewPercent, tolerance,
		"Replica2 should receive ~50%% of traffic in all-old mode, got %.1f%%", actualNewPercent*100)
}

// Helper functions

func waitForHealthyTablets(t *testing.T) {
	t.Helper()
	shardName := clusterInstance.Keyspaces[0].Shards[0].Name

	// Wait for primary
	require.Eventually(t, func() bool {
		err := clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(
			fmt.Sprintf("%s.%s.primary", keyspaceName, shardName), 1, 5*time.Second)
		return err == nil
	}, 60*time.Second, 1*time.Second, "Primary tablet did not become healthy")

	// Wait for replicas
	require.Eventually(t, func() bool {
		err := clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(
			fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), 2, 5*time.Second)
		return err == nil
	}, 60*time.Second, 1*time.Second, "Replica tablets did not become healthy")
}

func waitForReplication(t *testing.T, value string) {
	t.Helper()
	waitForReplicationOnTablets(t, value, []*cluster.Vttablet{oldReplica, newReplica})
}

func waitForReplicationOnTablets(t *testing.T, value string, replicas []*cluster.Vttablet) {
	t.Helper()
	query := fmt.Sprintf("SELECT count(*) FROM test_table WHERE val = '%s'", value)

	require.Eventually(t, func() bool {
		for _, replica := range replicas {
			res, err := replica.VttabletProcess.QueryTablet(query, keyspaceName, true)
			if err != nil || len(res.Rows) == 0 {
				return false
			}
			if val, err := res.Rows[0][0].ToUint64(); err != nil || val < 1 {
				return false
			}
		}
		return true
	}, 30*time.Second, 500*time.Millisecond, "Replication did not complete")
}

func setTabletStartTime(t *testing.T, tablet *cluster.Vttablet, startTime time.Time) {
	t.Helper()

	// Update the TabletStartTime in topo
	err := updateTabletStartTime(tablet, startTime.Unix())
	require.NoError(t, err, "Failed to update tablet start time in topo")

	// Wait for VTGate to pick up the topo change by verifying the tablet
	// info in VTGate's health check is updated. VTGate periodically refreshes
	// tablet info from topo, so we wait until the change propagates.
	require.Eventually(t, func() bool {
		// Check that VTGate still sees the tablet as healthy - this confirms
		// the topo watcher has refreshed. The actual start time verification
		// happens implicitly through the query distribution in the tests.
		shardName := clusterInstance.Keyspaces[0].Shards[0].Name
		err := clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(
			fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), 2, 1*time.Second)
		return err == nil
	}, 30*time.Second, 500*time.Millisecond, "VTGate did not refresh tablet info after topo change")
}

func getServerID(t *testing.T, tablet *cluster.Vttablet) int64 {
	t.Helper()
	res, err := tablet.VttabletProcess.QueryTablet("SELECT @@server_id", keyspaceName, false)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	serverID, err := res.Rows[0][0].ToInt64()
	require.NoError(t, err)
	return serverID
}

func executeReplicaQueries(t *testing.T, conn *mysql.Conn, numQueries int) map[int64]int {
	t.Helper()

	_, err := conn.ExecuteFetch("USE @replica", 1, false)
	require.NoError(t, err)
	defer func() {
		_, _ = conn.ExecuteFetch("USE @primary", 1, false)
	}()

	counts := make(map[int64]int)
	for range numQueries {
		res, err := conn.ExecuteFetch("SELECT @@server_id FROM test_table LIMIT 1", 1, false)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)

		serverID, err := res.Rows[0][0].ToInt64()
		require.NoError(t, err)
		counts[serverID]++
	}

	return counts
}
