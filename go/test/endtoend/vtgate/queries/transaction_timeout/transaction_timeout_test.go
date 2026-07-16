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

package transactiontimeout

import (
	_ "embed"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	uks             = "uks"

	//go:embed uschema.sql
	uschemaSQL string
)

func createCluster(t *testing.T, vttabletArgs ...string) func() {
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace(uks).
			WithSchema(uschemaSQL),
		vitesst.WithVTTabletArgs(vttabletArgs...),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	require.NoError(t, err)

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")

	_, closer, err := vitesst.NewMySQL(t, ctx, cluster, uks, uschemaSQL)
	require.NoError(t, err)

	return func() {
		if err := cleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
		if err := closer(ctx); err != nil {
			t.Logf("comparison mysqld teardown: %v", err)
		}
	}
}

func TestTransactionTimeout(t *testing.T) {
	// Start cluster with no vtgate or vttablet timeouts
	teardown := createCluster(t)
	defer teardown()

	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// No timeout set, transaction shouldn't timeout
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(sleep(0.5))")
	vitesst.Exec(t, conn, "commit")

	// Set session transaction timeout
	vitesst.Exec(t, conn, "set transaction_timeout=100")

	// Sleeping outside of query will allow the transaction killer to kill the transaction
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	time.Sleep(3 * time.Second)
	_, err = vitesst.ExecAllowError(t, conn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	require.ErrorContains(t, err, "Aborted")

	// Sleeping in MySQL will cause a context timeout instead (different error)
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	_, err = vitesst.ExecAllowError(t, conn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(sleep(0.5))")
	require.ErrorContains(t, err, "Query execution was interrupted")

	// Get new connection
	conn, err = mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)

	// Set session transaction timeout to 0
	vitesst.Exec(t, conn, "set transaction_timeout=0")

	// Should time out using tablet transaction timeout
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	vitesst.Exec(t, conn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(sleep(2))")
	vitesst.Exec(t, conn, "commit")
}

func TestSmallerTimeout(t *testing.T) {
	// Start vttablet with a transaction timeout
	teardown := createCluster(t, "--queryserver-config-transaction-timeout", "1s")
	defer teardown()

	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)

	// Set session transaction timeout larger than tablet transaction timeout
	vitesst.Exec(t, conn, "set transaction_timeout=2000")

	// Transaction should get killed with lower timeout
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	time.Sleep(1500 * time.Millisecond)
	_, err = vitesst.ExecAllowError(t, conn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	require.ErrorContains(t, err, "Aborted")

	// Set session transaction timeout smaller than tablet transaction timeout
	vitesst.Exec(t, conn, "set transaction_timeout=250")

	// Session timeout should be used this time
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	time.Sleep(500 * time.Millisecond)
	_, err = vitesst.ExecAllowError(t, conn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	require.ErrorContains(t, err, "Aborted")
}
