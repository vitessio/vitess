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
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"

	//go:embed schema.sql
	schemaSQL string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithKeyspace(keyspaceName).
				WithReplicas(1).
				WithSchema(schemaSQL),
			vitesst.WithVTTabletArgs("--shutdown-grace-period=0s"),
			vitesst.WithVTGateArgs("--planner-version=gen4", "--mysql-default-workload=olap"),
		)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		cleanup, err := cluster.Start(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer func() {
			if err := cleanup(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "cluster teardown:", err)
			}
		}()

		clusterInstance = cluster
		vtParams = cluster.VTParams(ctx, "")
		return m.Run()
	}()
	os.Exit(exitCode)
}

/*
TestStreamTxRestart tests that when a connection is killed my mysql (may be due to restart),
then the transaction should not continue to serve the query via reconnect.
*/
func TestStreamTxRestart(t *testing.T) {
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "begin")
	// BeginStreamExecute
	_ = vitesst.Exec(t, conn, "select connection_id()")

	// StreamExecute
	_ = vitesst.Exec(t, conn, "select connection_id()")

	// restart the mysql to terminate all the existing connections.
	primTablet := clusterInstance.Keyspace(keyspaceName).Shards()[0].Primary()
	err = primTablet.StopMySQL(ctx)
	require.NoError(t, err)
	err = primTablet.StartMySQL(ctx)
	require.NoError(t, err)

	// query should return connection error
	_, err = vitesst.ExecAllowError(t, conn, "select connection_id()")
	require.Error(t, err)
}
