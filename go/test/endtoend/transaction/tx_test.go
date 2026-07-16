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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

var (
	keyspaceName = "ks"

	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
)

func startCluster(t *testing.T) mysql.ConnParams {
	t.Helper()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithVTTabletArgs("--twopc-abandon-age", "3600"),
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames("-80", "80-").
			WithReplicas(1).
			WithSchema(SchemaSQL).
			WithVSchema(VSchema).
			WithDurabilityPolicy(policy.DurabilitySemiSync),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, t.Context())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if err := cleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	return cluster.VTParams(t.Context(), "")
}

// TestTransactionModes tests transactions using twopc mode
func TestTransactionModes(t *testing.T) {
	ctx := t.Context()
	vtParams := startCluster(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// set transaction mode to SINGLE.
	vitesst.Exec(t, conn, "set transaction_mode = 'single'")

	// Insert targeted to multiple tables should fail as Transaction mode is SINGLE
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into twopc_user(user_id, name) values(1,'john')")
	_, err = conn.ExecuteFetch("insert into twopc_user(user_id, name) values(6,'vick')", 1000, false)
	want := "multi-db transaction attempted"
	require.Error(t, err)
	require.Contains(t, err.Error(), want)
	vitesst.Exec(t, conn, "rollback")

	// set transaction mode to TWOPC.
	vitesst.Exec(t, conn, "set transaction_mode = 'twopc'")

	// Insert targeted to multiple db should PASS with TWOPC trx mode
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into twopc_user(user_id, name) values(3,'mark')")
	vitesst.Exec(t, conn, "insert into twopc_user(user_id, name) values(4,'doug')")
	vitesst.Exec(t, conn, "insert into twopc_lookup(name, id) values('Tim',7)")
	vitesst.Exec(t, conn, "commit")

	// Verify the values are present
	vitesst.AssertMatches(t, conn, "select user_id from twopc_user where name='mark'", `[[INT64(3)]]`)
	vitesst.AssertMatches(t, conn, "select name from twopc_lookup where id=3", `[[VARCHAR("mark")]]`)

	// DELETE from multiple tables using TWOPC transaction mode
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "delete from twopc_user where user_id = 3")
	vitesst.Exec(t, conn, "delete from twopc_lookup where id = 3")
	vitesst.Exec(t, conn, "commit")

	// VERIFY that values are deleted
	vitesst.AssertMatches(t, conn, "select user_id from twopc_user where user_id=3", `[]`)
	vitesst.AssertMatches(t, conn, "select name from twopc_lookup where id=3", `[]`)
}

// TestTransactionIsolation tests transaction isolation level.
func TestTransactionIsolation(t *testing.T) {
	ctx := t.Context()
	vtParams := startCluster(t)

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// inserting some data.
	vitesst.Exec(t, conn, "insert into test(id, msg) values (1,'v1'), (2, 'v2')")
	defer vitesst.Exec(t, conn, "delete from test")

	conn1, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn1.Close()

	conn2, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	// on connection 1 change the isolation level to read-committed.
	// start a transaction and read the data for id = 1.
	vitesst.Exec(t, conn1, "set transaction isolation level read committed")
	vitesst.Exec(t, conn1, "begin")
	vitesst.AssertMatches(t, conn1, "select id, msg from test where id = 1", `[[INT64(1) VARCHAR("v1")]]`)

	// change the value of msg for id = 1 on connection 2.
	vitesst.Exec(t, conn2, "update test set msg = 'foo' where id = 1")

	// new value should be reflected on connection 1 within the open transaction.
	vitesst.AssertMatches(t, conn1, "select id, msg from test where id = 1", `[[INT64(1) VARCHAR("foo")]]`)
	vitesst.Exec(t, conn1, "rollback")
}

func TestTransactionAccessModes(t *testing.T) {
	vtParams := startCluster(t)
	closer := clearTables(t, vtParams)
	defer closer()

	ctx := t.Context()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// start a transaction with read-only characteristic.
	vitesst.Exec(t, conn, "start transaction read only")
	_, err = vitesst.ExecAllowError(t, conn, "insert into test(id, msg) values (42,'foo')")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot execute statement in a READ ONLY transaction")
	vitesst.Exec(t, conn, "rollback")

	// trying autocommit, this should pass as transaction characteristics are limited to single transaction.
	vitesst.Exec(t, conn, "insert into test(id, msg) values (42,'foo')")

	// target replica
	vitesst.Exec(t, conn, "use `ks@replica`")
	// start a transaction with read-only characteristic.
	vitesst.Exec(t, conn, "start transaction read only")
	vitesst.Exec(t, conn, "select * from test")

	// start a transaction with read-write characteristic. This should fail
	vitesst.Exec(t, conn, "start transaction read write")
	_, err = vitesst.ExecAllowError(t, conn, "select connection_id()")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot start read write transaction on a read only tablet")
	vitesst.Exec(t, conn, "rollback")
}

// TestTransactionIsolationInTx tests transaction isolation level inside transaction
// and setting isolation level to different values.
func TestTransactionIsolationInTx(t *testing.T) {
	ctx := t.Context()
	vtParams := startCluster(t)

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "set transaction isolation level read committed")
	vitesst.Exec(t, conn, "begin")
	vitesst.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("READ-COMMITTED")]]`)
	vitesst.Exec(t, conn, "commit")

	vitesst.Exec(t, conn, "set transaction isolation level serializable")
	vitesst.Exec(t, conn, "begin")
	vitesst.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("SERIALIZABLE")]]`)
	vitesst.Exec(t, conn, "commit")

	vitesst.Exec(t, conn, "set transaction isolation level read committed")
	vitesst.Exec(t, conn, "begin")
	vitesst.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("READ-COMMITTED")]]`)
	vitesst.Exec(t, conn, "commit")
}

func clearTables(t *testing.T, vtParams mysql.ConnParams) func() {
	deleteAll := func() {
		conn, err := mysql.Connect(t.Context(), &vtParams)
		require.NoError(t, err)
		tables := []string{"test", "twopc_user"}
		for _, table := range tables {
			_, _ = vitesst.ExecAllowError(t, conn, "delete from "+table)
		}
		conn.Close()
	}

	deleteAll()

	return func() {
		deleteAll()
	}
}
