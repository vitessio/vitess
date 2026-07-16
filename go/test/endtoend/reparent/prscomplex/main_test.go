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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	keyspaceName = "ks"

	//go:embed schema.sql
	schemaSQL string
)

func setup(t *testing.T) (*vitesst.Cluster, mysql.ConnParams) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(keyspaceName).
			WithReplicas(1).
			WithSchema(schemaSQL),
		vitesst.WithVTTabletArgs(
			"--queryserver-config-query-timeout=9000s",
			"--queryserver-config-pool-size=3",
			"--queryserver-config-stream-pool-size=3",
			"--queryserver-config-transaction-cap=2",
			"--queryserver-config-transaction-timeout=20s",
			"--shutdown-grace-period=3s",
			"--queryserver-config-schema-change-signal=false",
		),
		vitesst.WithVTGateArgs(
			"--planner-version=gen4",
			"--mysql-default-workload=olap",
			"--schema-change-signal=false",
		),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	return cluster, cluster.VTParams(ctx, "")
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
	ctx := t.Context()
	clusterInstance, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// start a reserved connection
	vitesst.Exec(t, conn, "set sql_mode=''")
	_ = vitesst.Exec(t, conn, "select connection_id()")

	// restart the mysql to trigger reconnect on next query.
	shard := clusterInstance.Keyspace(keyspaceName).Shard("-")
	primTablet := shard.Primary()
	err = primTablet.StopMySQL(ctx)
	require.NoError(t, err)
	err = primTablet.StartMySQL(ctx)
	require.NoError(t, err)

	go func() {
		// this will trigger reconnect with a new connection id, which will be lower than the origin connection id.
		_, _ = vitesst.ExecAllowError(t, conn, "select connection_id(), sleep(4000)")
	}()
	time.Sleep(5 * time.Second)

	totalErrCount := 0
	// run through 100 times to acquire new connection, this might override the original connection id.
	var conn2 *mysql.Conn
	for range 100 {
		conn2, err = mysql.Connect(ctx, &vtParams)
		require.NoError(t, err)

		vitesst.Exec(t, conn2, "set sql_mode=''")
		// ReserveExecute
		_, err = vitesst.ExecAllowError(t, conn2, "select connection_id()")
		if err != nil {
			totalErrCount++
		}
		// Execute
		_, err = vitesst.ExecAllowError(t, conn2, "select connection_id()")
		if err != nil {
			totalErrCount++
		}
	}

	// We run the above loop 100 times so we execute 200 queries, of which only some should fail due to MySQL restart.
	assert.Less(t, totalErrCount, 10, "MySQL restart can cause some errors, but not too many.")

	// prs should happen without any error.
	replica := shard.Replicas()[0]
	text, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx,
		"PlannedReparentShard", shard.Ref(), "--new-primary", replica.Alias())
	require.NoError(t, err, text)
}
