/*
Copyright 2019 The Vitess Authors.

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

package transaction

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	cell            = "zone1"
	hostname        = "localhost"

	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Reserve vtGate port in order to pass it to vtTablet
		clusterInstance.VtgateGrpcPort = clusterInstance.GetAndReservePort()
		// Set extra tablet args for twopc
		clusterInstance.VtTabletExtraArgs = []string{
			"--twopc_enable",
			"--twopc_coordinator_address", fmt.Sprintf("localhost:%d", clusterInstance.VtgateGrpcPort),
			"--twopc_abandon_age", "3600",
		}

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: SchemaSQL,
			VSchema:   VSchema,
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, false); err != nil {
			return 1, err
		}

		// Starting Vtgate in default MULTI transaction mode
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1, err
		}
		vtParams = clusterInstance.GetVTParams(keyspaceName)

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}

// TestTransactionModes tests transactions using twopc mode
func TestTransactionModes(t *testing.T) {
	defer cluster.PanicHandler(t)

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// set transaction mode to SINGLE.
	utils.Exec(t, conn, "set transaction_mode = 'single'")

	// Insert targeted to multiple tables should fail as Transaction mode is SINGLE
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into twopc_user(user_id, name) values(1,'john')")
	_, err = conn.ExecuteFetch("insert into twopc_user(user_id, name) values(6,'vick')", 1000, false)
	want := "multi-db transaction attempted"
	require.Error(t, err)
	require.Contains(t, err.Error(), want)
	utils.Exec(t, conn, "rollback")

	// set transaction mode to TWOPC.
	utils.Exec(t, conn, "set transaction_mode = 'twopc'")

	// Insert targeted to multiple db should PASS with TWOPC trx mode
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into twopc_user(user_id, name) values(3,'mark')")
	utils.Exec(t, conn, "insert into twopc_user(user_id, name) values(4,'doug')")
	utils.Exec(t, conn, "insert into twopc_lookup(name, id) values('Tim',7)")
	utils.Exec(t, conn, "commit")

	// Verify the values are present
	utils.AssertMatches(t, conn, "select user_id from twopc_user where name='mark'", `[[INT64(3)]]`)
	utils.AssertMatches(t, conn, "select name from twopc_lookup where id=3", `[[VARCHAR("mark")]]`)

	// DELETE from multiple tables using TWOPC transaction mode
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "delete from twopc_user where user_id = 3")
	utils.Exec(t, conn, "delete from twopc_lookup where id = 3")
	utils.Exec(t, conn, "commit")

	// VERIFY that values are deleted
	utils.AssertMatches(t, conn, "select user_id from twopc_user where user_id=3", `[]`)
	utils.AssertMatches(t, conn, "select name from twopc_lookup where id=3", `[]`)
}

// TestTransactionIsolation tests transaction isolation level.
func TestTransactionIsolation(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// inserting some data.
	utils.Exec(t, conn, "insert into test(id, msg) values (1,'v1'), (2, 'v2')")
	defer utils.Exec(t, conn, "delete from test")

	conn1, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn1.Close()

	conn2, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	// on connection 1 change the isolation level to read-committed.
	// start a transaction and read the data for id = 1.
	utils.Exec(t, conn1, "set transaction isolation level read committed")
	utils.Exec(t, conn1, "begin")
	utils.AssertMatches(t, conn1, "select id, msg from test where id = 1", `[[INT64(1) VARCHAR("v1")]]`)

	// change the value of msg for id = 1 on connection 2.
	utils.Exec(t, conn2, "update test set msg = 'foo' where id = 1")

	// new value should be reflected on connection 1 within the open transaction.
	utils.AssertMatches(t, conn1, "select id, msg from test where id = 1", `[[INT64(1) VARCHAR("foo")]]`)
	utils.Exec(t, conn1, "rollback")
}

func TestTransactionAccessModes(t *testing.T) {
	closer := start(t)
	defer closer()

	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// start a transaction with read-only characteristic.
	utils.Exec(t, conn, "start transaction read only")
	_, err = utils.ExecAllowError(t, conn, "insert into test(id, msg) values (42,'foo')")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot execute statement in a READ ONLY transaction")
	utils.Exec(t, conn, "rollback")

	// trying autocommit, this should pass as transaction characteristics are limited to single transaction.
	utils.Exec(t, conn, "insert into test(id, msg) values (42,'foo')")

	// target replica
	utils.Exec(t, conn, "use `ks@replica`")
	// start a transaction with read-only characteristic.
	utils.Exec(t, conn, "start transaction read only")
	utils.Exec(t, conn, "select * from test")

	// start a transaction with read-write characteristic. This should fail
	utils.Exec(t, conn, "start transaction read write")
	_, err = utils.ExecAllowError(t, conn, "select connection_id()")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot start read write transaction on a read only tablet")
	utils.Exec(t, conn, "rollback")
}

// TestTransactionIsolationInTx tests transaction isolation level inside transaction
// and setting isolation level to different values.
func TestTransactionIsolationInTx(t *testing.T) {
	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "set transaction isolation level read committed")
	utils.Exec(t, conn, "begin")
	utils.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("READ-COMMITTED")]]`)
	utils.Exec(t, conn, "commit")

	utils.Exec(t, conn, "set transaction isolation level serializable")
	utils.Exec(t, conn, "begin")
	utils.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("SERIALIZABLE")]]`)
	utils.Exec(t, conn, "commit")

	utils.Exec(t, conn, "set transaction isolation level read committed")
	utils.Exec(t, conn, "begin")
	utils.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("READ-COMMITTED")]]`)
	utils.Exec(t, conn, "commit")
}

func start(t *testing.T) func() {
	deleteAll := func() {
		conn, err := mysql.Connect(context.Background(), &vtParams)
		require.NoError(t, err)
		tables := []string{"test", "twopc_user"}
		for _, table := range tables {
			_, _ = utils.ExecAllowError(t, conn, "delete from "+table)
		}
		conn.Close()
	}

	deleteAll()

	return func() {
		deleteAll()
		cluster.PanicHandler(t)
	}
}
