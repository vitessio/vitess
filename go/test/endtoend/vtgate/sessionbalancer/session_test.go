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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

// TestSessionBalancer validates that the session balancer consistently routes
// queries for the same session to the same tablet.
func TestSessionBalancer(t *testing.T) {
	// Get connections that route to different tablets
	conn1, conn2, id1, id2 := connections(t)

	defer conn1.Close()
	defer conn2.Close()

	// Validate that each connection consistently returns the same server ID
	for range 20 {
		newID1 := serverID(t, conn1)
		require.Equal(t, id1, newID1)

		newID2 := serverID(t, conn2)
		require.Equal(t, id2, newID2)

		require.NotEqual(t, newID2, newID1)
	}
}

// connections returns two connections that should route to different servers.
func connections(t *testing.T) (conn1, conn2 *mysql.Conn, id1, id2 string) {
	t.Helper()

	// Keep creating connections until we find two that route to different server IDs
	vtParams.DbName = uks + "@replica"
	for {
		conn1, err := mysql.Connect(context.Background(), &vtParams)
		require.NoError(t, err)

		conn2, err = mysql.Connect(context.Background(), &vtParams)
		require.NoError(t, err)

		id1 = serverID(t, conn1)
		id2 = serverID(t, conn2)

		// Break if we found connections with different server IDs
		if id1 != id2 {
			return conn1, conn2, id1, id2
		}

		// If not, close the connections and try again
		conn1.Close()
		conn2.Close()
	}
}

// serverID runs a `SELECT @@server_id` on the given connection and returns the
// server's ID.
func serverID(t *testing.T, conn *mysql.Conn) string {
	t.Helper()

	result1, err := conn.ExecuteFetch("SELECT @@server_uuid", 1, false)
	require.NoError(t, err)

	tablet1Bytes, err := result1.Rows[0][0].ToBytes()
	require.NoError(t, err)

	return string(tablet1Bytes)
}
