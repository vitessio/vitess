//go:build !windows

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

package reuseport

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/log"
)

var (
	keyspaceName = "ks"
	cell         = "zone-1"

	//go:embed schema.sql
	schemaSQL string
)

func TestMain(m *testing.M) {
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
	err = clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false, clusterInstance.Cell)
	require.NoError(t, err)

	// Start vtgate
	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "--reuse-port")
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
	}
}

func TestReusePort(t *testing.T) {
	clusterInstance, vtParams := setupCluster(t)
	defer clusterInstance.Teardown()

	// Create a connection to the first vtgate
	vtConn, closer := start(t, vtParams)
	defer closer()

	// Create a second vtgate with the same configuration
	duplicateVtGate := cluster.VtgateProcessInstance(
		clusterInstance.GetAndReservePort(),
		clusterInstance.VtgateGrpcPort,
		clusterInstance.VtgateMySQLPort,
		clusterInstance.Cell,
		clusterInstance.Cell,
		clusterInstance.Hostname,
		"PRIMARY",
		clusterInstance.TopoProcess.Port,
		clusterInstance.TmpDirectory,
		clusterInstance.VtGateExtraArgs,
		clusterInstance.VtGatePlannerVersion)
	// Unix domain sockets do not support multiplexing
	duplicateVtGate.MySQLServerSocketPath = ""
	err := duplicateVtGate.Setup()
	require.NoError(t, err)
	defer func() {
		if err := duplicateVtGate.TearDown(); err != nil {
			log.Error(fmt.Sprintf("Error in vtgate teardown: %v", err))
		}
	}()

	// Should be able to connect to the first vtgate
	_, err = vtConn.ExecuteFetch("select id1 from t1", 1, false)
	require.NoError(t, err)

	// Tear down the first vtgate
	err = clusterInstance.VtgateProcess.TearDown()
	require.NoError(t, err)
	require.True(t, clusterInstance.VtgateProcess.IsShutdown())

	// Should fail since the vtgate has stopped
	_, err = vtConn.ExecuteFetch("select id1 from t1", 1, false)
	require.Error(t, err, "first vtgate should be stopped and should not serve requests")

	// Create a second connection with the same parameters, which will
	// now go to the duplicate vtgate
	vtConn2, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err, "second vtgate should handle the connection")
	defer vtConn2.Close()

	// Should be able to fetch from the same host:port on the duplicate vtgate
	_, err = vtConn2.ExecuteFetch("select id1 from t1", 1, false)
	require.NoError(t, err, "second vtgate should serve requests")
}
