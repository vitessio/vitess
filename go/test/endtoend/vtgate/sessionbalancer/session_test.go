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

package sessionbalancer

import (
	"context"
	_ "embed"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

const (
	cell     = "test_misc"
	keyspace = "uks"
)

//go:embed uschema.sql
var uschemaSQL string

func createCluster(t *testing.T, replicaCount int) (*cluster.LocalProcessCluster, *mysql.ConnParams) {
	t.Helper()

	// Create a new clusterInstance
	clusterInstance := cluster.NewCluster(cell, "localhost")

	// Start topo server
	err := clusterInstance.StartTopo()
	require.NoError(t, err, "Failed to start topo server")

	// Enable session balancer in vtgate
	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
		"--enable-balancer",
		"--balancer-vtgate-cells", clusterInstance.Cell,
		"--balancer-type", "session")

	ks := cluster.Keyspace{
		Name:      keyspace,
		SchemaSQL: uschemaSQL,
	}
	err = clusterInstance.StartUnshardedKeyspace(ks, replicaCount, false)
	require.NoError(t, err, "Failed to start keyspace")

	err = clusterInstance.StartVtgate()
	require.NoError(t, err, "Failed to start vtgate")

	vtParams := clusterInstance.GetVTParams(keyspace)

	return clusterInstance, &vtParams
}

// TestSessionBalancer validates that the session balancer consistently routes
// queries for the same session to the same tablet.
func TestSessionBalancer(t *testing.T) {
	cluster, vtParams := createCluster(t, 2)
	defer cluster.Teardown()

	// Get connections that route to different tablets
	conns, ids := connections(t, vtParams, "replica", 2)

	for _, conn := range conns {
		defer conn.Close()
	}

	// Validate that each connection consistently returns the same server ID
	for range 20 {
		for i, conn := range conns {
			id := serverID(t, conn)
			require.Equal(t, ids[i], id)
		}
	}
}

// TestSessionBalancerRemoveTablet validates that when a tablet is killed,
// connections that were using that tablet get rerouted to remaining tablets.
func TestSessionBalancerRemoveTablet(t *testing.T) {
	cluster, vtParams := createCluster(t, 2)
	defer cluster.Teardown()

	// Get connections that route to different tablets
	conns, _ := connections(t, vtParams, "replica", 2)
	conn1, conn2 := conns[0], conns[1]

	defer conn1.Close()
	defer conn2.Close()

	tablets := tablets(t, cluster, "replica")
	require.NotNil(t, tablets)
	require.Len(t, tablets, 2)

	err := tablets[0].VttabletProcess.TearDown()
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	for range 20 {
		newID1 := serverID(t, conn1)
		newID2 := serverID(t, conn2)

		require.Equal(t, newID1, newID2)
	}
}

// TestSessionBalancerAddTablet validates that when a new tablet is added,
// new connections get routed to the new tablet.
func TestSessionBalancerAddTablet(t *testing.T) {
	cluster, vtParams := createCluster(t, 3)
	defer cluster.Teardown()

	// Get 3 connections that route to different tablets
	conns, ids := connections(t, vtParams, "replica", 3)
	for _, conn := range conns {
		defer conn.Close()
	}

	tablets := tablets(t, cluster, "replica")
	require.NotNil(t, tablets)
	require.Len(t, tablets, 3)

	// Start with only 2 tablets serving
	tablet := tablets[2]
	err := tablet.VttabletProcess.TearDown()
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	// Find the connection that moved
	var conn *mysql.Conn
	for i, c := range conns {
		newID := serverID(t, c)
		if newID != ids[i] {
			conn = c
			break
		}
	}

	require.NotNil(t, conn, "One connection should've moved tablets")

	// Start up the tablet again
	err = tablet.RestartOnlyTablet()
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	// All connections should route to the same IDs again
	for range 20 {
		for i, conn := range conns {
			id := ids[i]
			newID := serverID(t, conn)
			require.Equal(t, id, newID)
		}
	}
}

// connections returns the specified number of connections that should route to different tablets.
func connections(t *testing.T, vtParams *mysql.ConnParams, tabletType string, numConnections int) ([]*mysql.Conn, []string) {
	t.Helper()

	vtParams.DbName = fmt.Sprintf("%s@%s", keyspace, tabletType)

	conns := make([]*mysql.Conn, 0, numConnections)
	ids := make([]string, 0, numConnections)

	// Keep creating connections until we have the required number with different server IDs
	for range 20 {
		conn, err := mysql.Connect(context.Background(), vtParams)
		require.NoError(t, err)

		id := serverID(t, conn)
		newID := !slices.Contains(ids, id)

		// If we found a new tablet, add it to the list of connections
		if newID {
			conns = append(conns, conn)
			ids = append(ids, id)

			if len(conns) == numConnections {
				return conns, ids
			}

			continue
		}

		conn.Close()
	}

	t.Fatalf("could not create %d connections with different tablet connections", numConnections)
	return nil, nil
}

// serverID runs a `SELECT @@server_id` on the given connection and returns the server's ID.
func serverID(t *testing.T, conn *mysql.Conn) string {
	t.Helper()

	result1, err := conn.ExecuteFetch("SELECT @@server_id", 1, false)
	require.NoError(t, err)

	tablet1Bytes, err := result1.Rows[0][0].ToBytes()
	require.NoError(t, err)

	return string(tablet1Bytes)
}

func tablets(t *testing.T, clusterInstance *cluster.LocalProcessCluster, tabletType string) []*cluster.Vttablet {
	t.Helper()

	if len(clusterInstance.Keyspaces) == 0 {
		return nil
	}

	if len(clusterInstance.Keyspaces[0].Shards) == 0 {
		return nil
	}

	tablets := make([]*cluster.Vttablet, 0, 2)
	for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
		if tablet.Type == tabletType {
			tablets = append(tablets, tablet)
		}
	}

	return tablets
}
