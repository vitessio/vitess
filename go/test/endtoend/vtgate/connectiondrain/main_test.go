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
	"flag"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	keyspaceName = "ks"
	cell         = "zone-1"

	//go:embed schema.sql
	schemaSQL string
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()
	os.Exit(m.Run())
}

func setupCluster(t *testing.T) (*cluster.LocalProcessCluster, mysql.ConnParams) {
	clusterInstance := cluster.NewCluster(cell, "localhost")

	// Start topo server
	err := clusterInstance.StartTopo()
	require.NoError(t, err)

	// Start keyspace
	keyspace := &cluster.Keyspace{
		Name:      keyspaceName,
		SchemaSQL: schemaSQL,
	}
	err = clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false)
	require.NoError(t, err)

	// Start vtgate
	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "--mysql-server-drain-onterm", "--onterm_timeout", "30s")
	err = clusterInstance.StartVtgate()
	require.NoError(t, err)

	vtParams := clusterInstance.GetVTParams(keyspaceName)
	return clusterInstance, vtParams
}

func start(t *testing.T, vtParams mysql.ConnParams) (*mysql.Conn, func()) {
	vtConn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, vtConn, "set workload = oltp")

		tables := []string{"t1"}
		for _, table := range tables {
			_, _ = utils.ExecAllowError(t, vtConn, "delete from "+table)
		}
	}

	deleteAll()

	return vtConn, func() {
		deleteAll()
		vtConn.Close()
		cluster.PanicHandler(t)
	}
}

func TestConnectionDrainCloseConnections(t *testing.T) {
	clusterInstance, vtParams := setupCluster(t)
	defer clusterInstance.Teardown()

	vtConn, closer := start(t, vtParams)
	defer closer()

	// Create a second connection, this connection will be used to create a transaction.
	vtConn2, err := mysql.Connect(context.Background(), &vtParams)
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
	err = clusterInstance.VtgateProcess.Terminate()
	require.NoError(t, err)

	// Give enough time to vtgate to receive and start processing the SIGTERM signal
	time.Sleep(2 * time.Second)

	// Create a third connection, this connection should not be allowed.
	// Set a connection timeout to 1s otherwise the connection will take forever
	// and eventually vtgate will reach the --onterm_timeout.
	vtParams.ConnectTimeoutMs = 1000
	defer func() {
		vtParams.ConnectTimeoutMs = 0
	}()
	_, err = mysql.Connect(context.Background(), &vtParams)
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
	require.False(t, clusterInstance.VtgateProcess.IsShutdown())

	// This connection should still be allowed
	_, err = vtConn.ExecuteFetch("select id1 from t1", 1, false)
	require.NoError(t, err)
	vtConn.Close()

	// Give enough time for vtgate to finish all the onterm hooks without reaching the 30s of --onterm_timeout
	time.Sleep(10 * time.Second)

	// By now the vtgate should have shutdown on its own and without reaching --onterm_timeout
	require.True(t, clusterInstance.VtgateProcess.IsShutdown())
}

func TestConnectionDrainOnTermTimeout(t *testing.T) {
	clusterInstance, vtParams := setupCluster(t)
	defer clusterInstance.Teardown()

	// Connect to vtgate again, this should work
	vtConn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	vtConn2, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)

	defer func() {
		vtConn.Close()
		vtConn2.Close()
	}()

	// Tearing down vtgate here, we want to reach the onterm_timeout of 30s
	err = clusterInstance.VtgateProcess.Terminate()
	require.NoError(t, err)

	// Run a busy query that returns only after the onterm_timeout is reached, this should fail when we reach the timeout
	_, err = vtConn.ExecuteFetch("select sleep(40)", 1, false)
	require.Error(t, err)

	// Running a query after we have reached the onterm_timeout should fail
	_, err = vtConn2.ExecuteFetch("select id from t1", 1, false)
	require.Error(t, err)

	// By now vtgate will be shutdown becaused it reached its onterm_timeout, despite idle connections still being opened
	require.True(t, clusterInstance.VtgateProcess.IsShutdown())
}
