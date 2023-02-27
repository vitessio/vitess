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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	rutils "vitess.io/vitess/go/test/endtoend/reparent/utils"
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
		clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs,
			"--queryserver-config-query-timeout=9000",
			"--queryserver-config-pool-size=3",
			"--queryserver-config-stream-pool-size=3",
			"--queryserver-config-transaction-cap=2",
			"--queryserver-config-transaction-timeout=20",
			"--shutdown_grace_period=3",
			"--queryserver-config-schema-change-signal=false")
		err = clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
			"--planner-version=gen4",
			"--mysql_default_workload=olap",
			"--schema_change_signal=false")
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
TestAcquireSameConnID tests that a query started on a connection gets reconnected with a new connection.
Another query acquires the old connection ID and does not override the query list maintained by the vttablet process.
PRS should not fail as the query list is maintained appropriately.
*/
func TestAcquireSameConnID(t *testing.T) {
	defer func() {
		err := recover()
		if err != nil {
			require.Equal(t, "Fail in goroutine after TestAcquireSameConnID has completed", err)
		}
	}()
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// start a reserved connection
	utils.Exec(t, conn, "set sql_mode=''")
	_ = utils.Exec(t, conn, "select connection_id()")

	// restart the mysql to trigger reconnect on next query.
	primTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
	err = primTablet.MysqlctlProcess.Stop()
	require.NoError(t, err)
	err = primTablet.MysqlctlProcess.StartProvideInit(false)
	require.NoError(t, err)

	go func() {
		// this will trigger reconnect with a new connection id, which will be lower than the origin connection id.
		_, _ = utils.ExecAllowError(t, conn, "select connection_id(), sleep(4000)")
	}()
	time.Sleep(5 * time.Second)

	totalErrCount := 0
	// run through 100 times to acquire new connection, this might override the original connection id.
	var conn2 *mysql.Conn
	for i := 0; i < 100; i++ {
		conn2, err = mysql.Connect(ctx, &vtParams)
		require.NoError(t, err)

		utils.Exec(t, conn2, "set sql_mode=''")
		// ReserveExecute
		_, err = utils.ExecAllowError(t, conn2, "select connection_id()")
		if err != nil {
			totalErrCount++
		}
		// Execute
		_, err = utils.ExecAllowError(t, conn2, "select connection_id()")
		if err != nil {
			totalErrCount++
		}
	}

	// We run the above loop 100 times so we execute 200 queries, of which only some should fail due to MySQL restart.
	assert.Less(t, totalErrCount, 10, "MySQL restart can cause some errors, but not too many.")

	// prs should happen without any error.
	text, err := rutils.Prs(t, clusterInstance, clusterInstance.Keyspaces[0].Shards[0].Replica())
	require.NoError(t, err, text)
}
