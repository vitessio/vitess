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

package restart

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

	//go:embed schema.sql
	schemaSQL string
)

func startCluster(t *testing.T) (*vitesst.Cluster, mysql.ConnParams) {
	t.Helper()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(keyspaceName).
			WithReplicas(1).
			WithSchema(schemaSQL),
		vitesst.WithVTTabletArgs("--shutdown-grace-period=0s"),
		vitesst.WithVTGateArgs("--planner-version=gen4", "--mysql-default-workload=olap"),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t.Context())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(ctx, t.Logf)
		}
		if err := cleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	return cluster, cluster.VTParams(t.Context(), "")
}

/*
TestStreamTxRestart tests that when a connection is killed my mysql (may be due to restart),
then the transaction should not continue to serve the query via reconnect.
*/
func TestStreamTxRestart(t *testing.T) {
	ctx := t.Context()
	cluster, vtParams := startCluster(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "begin")
	// BeginStreamExecute
	_ = vitesst.Exec(t, conn, "select connection_id()")

	// StreamExecute
	_ = vitesst.Exec(t, conn, "select connection_id()")

	// restart the mysql to terminate all the existing connections.
	primTablet := cluster.Keyspace(keyspaceName).Shards()[0].Primary()
	err = primTablet.StopMySQL(ctx)
	require.NoError(t, err)
	err = primTablet.StartMySQL(ctx)
	require.NoError(t, err)

	// query should return connection error
	_, err = vitesst.ExecAllowError(t, conn, "select connection_id()")
	require.Error(t, err)
}
