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

package connectiondrain

import (
	"context"
	_ "embed"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

var (
	keyspaceName = "ks"
	cell         = "zone-1"

	//go:embed schema.sql
	schemaSQL string
)

// drainTimeout is the grace period given to docker stop. It exceeds the
// vtgate --onterm-timeout of 30s so the vtgate always decides when to exit:
// either after draining its connections or after reaching its own timeout.
const drainTimeout = time.Minute

func setupCluster(t *testing.T) (*vitesst.Cluster, mysql.ConnParams) {
	ctx := t.Context()

	clusterInstance, err := vitesst.NewCluster(
		vitesst.WithCells(cell),
		vitesst.WithKeyspace(keyspaceName).
			WithSchema(schemaSQL),
		vitesst.WithVTGateArgs("--mysql-server-drain-onterm", "--onterm-timeout", "30s"),
	)
	require.NoError(t, err)

	cleanup, err := clusterInstance.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := cleanup(context.WithoutCancel(ctx)); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	vtParams := clusterInstance.VTParams(ctx, "")
	return clusterInstance, vtParams
}

func start(t *testing.T, vtParams mysql.ConnParams) (*mysql.Conn, func()) {
	vtConn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = vitesst.ExecAllowError(t, vtConn, "set workload = oltp")

		tables := []string{"t1"}
		for _, table := range tables {
			_, _ = vitesst.ExecAllowError(t, vtConn, "delete from "+table)
		}
	}

	deleteAll()

	return vtConn, func() {
		deleteAll()
		vtConn.Close()
	}
}

func TestConnectionDrainCloseConnections(t *testing.T) {
	clusterInstance, vtParams := setupCluster(t)

	vtConn, closer := start(t, vtParams)
	defer closer()

	// Create a second connection, this connection will be used to create a transaction.
	vtConn2, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)

	// Start the transaction with the second connection
	_, err = vtConn2.ExecuteFetch("BEGIN", 1, false)
	require.NoError(t, err)
	_, err = vtConn2.ExecuteFetch("select id1 from t1", 1, false)
	require.NoError(t, err)

	_, err = vtConn.ExecuteFetch("select id1 from t1", 1, false)
	require.NoError(t, err)

	// Tearing down vtgate here, from there on vtConn should still be able to conclude in-flight transaction and
	// execute queries with idle connections. However, no new connections are allowed.
	vtgate := clusterInstance.VTGate()
	go func() {
		_ = vtgate.StopContainer(context.WithoutCancel(t.Context()), drainTimeout)
	}()

	// Give enough time to vtgate to receive and start processing the SIGTERM signal
	time.Sleep(2 * time.Second)

	// Create a third connection, this connection should not be allowed.
	// Set a connection timeout to 1s otherwise the connection will take forever
	// and eventually vtgate will reach the --onterm-timeout.
	vtParams.ConnectTimeoutMs = 1000
	defer func() {
		vtParams.ConnectTimeoutMs = 0
	}()
	_, err = mysql.Connect(t.Context(), &vtParams)
	require.Error(t, err)

	// Idle connections should be allowed to execute queries until they are drained
	_, err = vtConn.ExecuteFetch("select id1 from t1", 1, false)
	require.NoError(t, err)

	// Finish the transaction
	_, err = vtConn2.ExecuteFetch("select id1 from t1", 1, false)
	require.NoError(t, err)
	_, err = vtConn2.ExecuteFetch("COMMIT", 1, false)
	require.NoError(t, err)
	vtConn2.Close()

	// vtgate should still be running
	require.True(t, vtgate.IsRunning())

	// This connection should still be allowed
	_, err = vtConn.ExecuteFetch("select id1 from t1", 1, false)
	require.NoError(t, err)
	vtConn.Close()

	// Give enough time for vtgate to finish all the onterm hooks without reaching the 30s of --onterm-timeout
	time.Sleep(10 * time.Second)

	// By now the vtgate should have shutdown on its own and without reaching --onterm-timeout
	require.False(t, vtgate.IsRunning())
}

func TestConnectionDrainOnTermTimeout(t *testing.T) {
	clusterInstance, vtParams := setupCluster(t)

	// Connect to vtgate again, this should work
	vtConn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	vtConn2, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)

	defer func() {
		vtConn.Close()
		vtConn2.Close()
	}()

	// Tearing down vtgate here, we want to reach the onterm-timeout of 30s
	vtgate := clusterInstance.VTGate()
	stopped := make(chan struct{})
	go func() {
		_ = vtgate.StopContainer(context.WithoutCancel(t.Context()), drainTimeout)
		close(stopped)
	}()

	// Run a busy query that returns only after the onterm-timeout is reached, this should fail when we reach the timeout
	_, err = vtConn.ExecuteFetch("select sleep(40)", 1, false)
	require.Error(t, err)

	// Running a query after we have reached the onterm-timeout should fail
	_, err = vtConn2.ExecuteFetch("select id from t1", 1, false)
	require.Error(t, err)

	// By now vtgate will be shutdown becaused it reached its onterm-timeout, despite idle connections still being opened
	<-stopped
	require.False(t, vtgate.IsRunning())
}
