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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// TestSessionModeBalancer tests the "session" mode routes each session consistently to the same tablet.
func TestSessionModeBalancer(t *testing.T) {
	vtgateProcess, vtParams, _, _ := setupCluster(t)
	defer vtgateProcess.TearDown()

	// Create 2 session connections that route to different tablets
	conns := createSessionConnections(t, &vtParams, 2)
	for conn := range conns {
		defer conn.Close()
	}

	verifyStickiness(t, conns, 20)
}

// TestSessionModeRemoveTablet tests that when a tablet is killed, connections switch to remaining tablets
func TestSessionModeRemoveTablet(t *testing.T) {
	vtgateProcess, vtParams, replicaTablets, aliases := setupCluster(t)
	defer vtgateProcess.TearDown()

	// Create 2 connections to different tablets
	conns := createSessionConnections(t, &vtParams, 2)
	for conn := range conns {
		defer conn.Close()
	}

	// Find the first replica tablet that one of our connections is using
	var tabletToKill *cluster.Vttablet
	var affectedConn *mysql.Conn
	var killedServerID int64

	for _, tablet := range replicaTablets {
		tabletServerID := aliases[tablet.Alias]

		// Check if any connection is using this tablet
		for conn, connServerID := range conns {
			if connServerID != tabletServerID {
				continue
			}

			// We found a connection that's using this tablet, let's kill this tablet
			tabletToKill = tablet
			affectedConn = conn
			killedServerID = tabletServerID
			break
		}

		// We found a tablet, no need to check other tablets
		if tabletToKill != nil {
			break
		}
	}

	require.NotNil(t, tabletToKill, "Should find a tablet to kill")

	// Kill the tablet immediately
	err := tabletToKill.VttabletProcess.Kill()
	require.Error(t, err)

	// Wait for the connection to switch to a new tablet and update the map
	require.Eventually(t, func() bool {
		newServerID := getServerID(t, affectedConn)
		if newServerID != killedServerID {
			conns[affectedConn] = newServerID
			return true
		}

		return false
	}, 10*time.Millisecond, 1*time.Millisecond, "Connection should switch to a different tablet")

	verifyStickiness(t, conns, 20)
}

// setupCluster sets up a cluster with a vtgate using the session balancer.
func setupCluster(t *testing.T) (*cluster.VtgateProcess, mysql.ConnParams, []*cluster.Vttablet, map[string]int64) {
	t.Helper()

	// Start vtgate in cell1 with session mode
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
			"--vtgate-balancer-mode", "session",
		},
		plancontext.PlannerVersion(0),
	)
	require.NoError(t, vtgateProcess.Setup())
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

	aliases := mapTabletAliasToMySQLServerID(t, allTablets)

	// Insert test data
	testValue := fmt.Sprintf("session_test_%d", time.Now().UnixNano())
	_, err = conn.ExecuteFetch(fmt.Sprintf("INSERT INTO balancer_test (value) VALUES ('%s')", testValue), 1, false)
	require.NoError(t, err)
	waitForReplication(t, replicaTablets, testValue)

	return vtgateProcess, vtParams, replicaTablets, aliases
}

// getServerID returns the server ID that the connection is currently routing to.
func getServerID(t *testing.T, conn *mysql.Conn) int64 {
	t.Helper()

	res, err := conn.ExecuteFetch("SELECT @@server_id", 1, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(res.Rows), "expected one row from server_id query")

	serverID, err := res.Rows[0][0].ToInt64()
	require.NoError(t, err)

	return serverID
}

// createSessionConnections creates `n` connections that route to different tablets.
// Returns a map of mysql.Conn -> serverID.
func createSessionConnections(t *testing.T, vtParams *mysql.ConnParams, numConnections int) map[*mysql.Conn]int64 {
	t.Helper()

	conns := make(map[*mysql.Conn]int64)
	seenServerIDs := make(map[int64]bool)

	// Try up to 50 times to get numConnections with different server IDs
	for range 50 {
		conn, err := mysql.Connect(context.Background(), vtParams)
		require.NoError(t, err)

		_, err = conn.ExecuteFetch("USE @replica", 1, false)
		require.NoError(t, err)

		// Get the server ID this connection routes to
		serverID := getServerID(t, conn)

		// If this is a new tablet, keep the connection
		if !seenServerIDs[serverID] {
			seenServerIDs[serverID] = true
			conns[conn] = serverID

			// If we have enough connections, return
			if len(conns) == numConnections {
				return conns
			}

			continue
		}

		// Already seen this tablet, close and try again
		conn.Close()
	}

	t.Fatalf("could not create %d connections with different tablets after 50 attempts, only got %d", numConnections, len(conns))
	return nil
}

// verifyStickiness validates whether the given connections remain connected to the same
// server `n` times in a row.
func verifyStickiness(t *testing.T, conns map[*mysql.Conn]int64, n uint) {
	t.Helper()

	for conn, expectedServerID := range conns {
		for range n {
			currentServerID := getServerID(t, conn)
			require.Equal(t, expectedServerID, currentServerID, "Connection should stick to tablet %d, got %d", expectedServerID, currentServerID)
		}
	}
}
