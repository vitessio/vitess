/*
Copyright 2022 The Vitess Authors.

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

package misc

import (
	"context"
	_ "embed"
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	cell            = "test"

	//go:embed schema.sql
	schemaSQL string
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: schemaSQL,
		}
		err = clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
			"--planner-version=gen4",
			"--mysql_default_workload=olap")
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

/*
TestStreamTxRestart tests that when a connection is killed my mysql (may be due to restart),
then the transaction should not continue to serve the query via reconnect.
*/
func TestStreamTxRestart(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "begin")
	// BeginStreamExecute
	_ = utils.Exec(t, conn, "select connection_id()")

	// StreamExecute
	_ = utils.Exec(t, conn, "select connection_id()")

	// restart the mysql to terminate all the existing connections.
	primTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
	err = primTablet.MysqlctlProcess.Stop()
	require.NoError(t, err)
	err = primTablet.MysqlctlProcess.StartProvideInit(false)
	require.NoError(t, err)

	// query should return connection error
	_, err = utils.ExecAllowError(t, conn, "select connection_id()")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "broken pipe (errno 2006) (sqlstate HY000)")
}
