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
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	mysqlParams     mysql.ConnParams
	keyspaceName    = "ks_misc"
	uks             = "uks"
	cell            = "test_misc"

	//go:embed uschema.sql
	uschemaSQL string

	//go:embed schema.sql
	schemaSQL string
)

func createCluster(t *testing.T, vttabletArgs ...string) func() {
	clusterInstance = cluster.NewCluster(cell, "localhost")

	// Start topo server
	err := clusterInstance.StartTopo()
	require.NoError(t, err)

	clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs, vttabletArgs...)

	// Start unsharded keyspace
	ukeyspace := &cluster.Keyspace{
		Name:      uks,
		SchemaSQL: uschemaSQL,
	}
	err = clusterInstance.StartUnshardedKeyspace(*ukeyspace, 0, false)
	require.NoError(t, err)

	// Start vtgate
	err = clusterInstance.StartVtgate()
	require.NoError(t, err)

	vtParams = clusterInstance.GetVTParams(keyspaceName)

	// create mysql instance and connection parameters
	conn, closer, err := utils.NewMySQL(clusterInstance, keyspaceName, schemaSQL)
	require.NoError(t, err)

	mysqlParams = conn

	return func() {
		defer clusterInstance.Teardown()
		defer closer()
	}
}

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		tables := []string{"t1", "uks.unsharded"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
	}
}

func TestTransactionTimeout(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 21, "vttablet")

	// Start cluster with no vtgate or vttablet timeouts
	teardown := createCluster(t)
	defer teardown()

	mcmp, closer := start(t)
	defer closer()

	// No timeout set, transaction shouldn't timeout
	utils.Exec(t, mcmp.VtConn, "begin")
	utils.Exec(t, mcmp.VtConn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(sleep(0.5))")
	utils.Exec(t, mcmp.VtConn, "commit")

	// Set session transaction timeout
	utils.Exec(t, mcmp.VtConn, "set transaction_timeout=100")

	// Sleeping outside of query will allow the transaction killer to kill the transaction
	utils.Exec(t, mcmp.VtConn, "begin")
	utils.Exec(t, mcmp.VtConn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	time.Sleep(1 * time.Second)
	_, err := utils.ExecAllowError(t, mcmp.VtConn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	require.ErrorContains(t, err, "Aborted")

	// Sleeping in MySQL will cause a context timeout instead (different error)
	utils.Exec(t, mcmp.VtConn, "begin")
	utils.Exec(t, mcmp.VtConn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(sleep(0.5))")
	require.ErrorContains(t, err, "Query execution was interrupted")
}

func TestSmallerTimeout(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 21, "vttablet")

	// Start vttablet with a transaction timeout
	teardown := createCluster(t, "--queryserver-config-transaction-timeout", "1s")
	defer teardown()

	mcmp, closer := start(t)
	defer closer()

	// Set session transaction timeout larger than tablet transaction timeout
	utils.Exec(t, mcmp.VtConn, "set transaction_timeout=2000")

	// Transaction should get killed with lower timeout
	utils.Exec(t, mcmp.VtConn, "begin")
	utils.Exec(t, mcmp.VtConn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	time.Sleep(1500 * time.Millisecond)
	_, err := utils.ExecAllowError(t, mcmp.VtConn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	require.ErrorContains(t, err, "Aborted")

	// Set session transaction timeout smaller than tablet transaction timeout
	utils.Exec(t, mcmp.VtConn, "set transaction_timeout=250")

	// Session timeout should be used this time
	utils.Exec(t, mcmp.VtConn, "begin")
	utils.Exec(t, mcmp.VtConn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	time.Sleep(500 * time.Millisecond)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "insert into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	require.ErrorContains(t, err, "Aborted")
}
