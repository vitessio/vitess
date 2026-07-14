/*
Copyright 2021 The Vitess Authors.

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

package reservedconn

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
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithKeyspace(keyspaceName).
				WithShardNames("-40", "40-80", "80-c0", "c0-").
				WithSchema(SchemaSQL).
				WithVSchema(VSchema),
			vitesst.WithVTGateArgs("--planner-version", "Gen4Fallback"),
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

func testAllModes(t *testing.T, stmts func(conn *mysql.Conn)) {
	t.Helper()

	tcases := []struct {
		mode string
		qs   []string
	}{
		{"oltp", []string{"set workload = oltp"}},
		{"oltp-reserved", []string{"set workload = oltp", "set sql_mode = ''"}},
		{"olap", []string{"set workload = olap"}},
		{"olap-reserved", []string{"set workload = olap", "set sql_mode = ''"}},
		{"oltp", []string{"set workload = oltp"}}, // to make a circle on the workload change.
	}

	for _, tc := range tcases {
		t.Run(tc.mode, func(t *testing.T) {
			conn, err := mysql.Connect(t.Context(), &vtParams)
			require.NoError(t, err)
			defer conn.Close()

			// setup the mode
			for _, q := range tc.qs {
				vitesst.Exec(t, conn, q)
			}

			// cleanup previous run data from table.
			vitesst.Exec(t, conn, `delete from test`)

			// execute all the test stmts.
			stmts(conn)
		})
	}
}

func TestPartialQueryFailureExplicitTx(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		vitesst.Exec(t, conn, `begin`)
		vitesst.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// primary vindex is duplicate
		vitesst.AssertContainsError(t, conn, `insert into test(id, val1) values (1,'D'),(4,'E')`, `reverted partial DML execution failure`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		vitesst.Exec(t, conn, `commit`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
	})
}

func TestPartialVindexQueryFailureExplicitTx(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		vitesst.Exec(t, conn, `begin`)
		vitesst.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// lookup vindex is duplicate
		vitesst.AssertContainsError(t, conn, `insert into test(id, val1) values (4,'D'),(5,'C')`, `reverted partial DML execution failure`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		vitesst.Exec(t, conn, `insert into test(id, val1) values (4,'D'),(5,'E')`)
		vitesst.Exec(t, conn, `commit`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")] [INT64(4) VARCHAR("D")] [INT64(5) VARCHAR("E")]]`)
	})
}

func TestPartialQueryFailureNoAutoCommit(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		// autocommit is false.
		vitesst.Exec(t, conn, `set autocommit = off`)
		vitesst.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// primary vindex is duplicate
		vitesst.AssertContainsError(t, conn, `insert into test(id, val1) values (1,'D'),(4,'E')`, `reverted partial DML execution failure`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		vitesst.Exec(t, conn, `commit`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
	})
}

func TestPartialVindexQueryFailureNoAutoCommit(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		// autocommit is false.
		vitesst.Exec(t, conn, `set autocommit = off`)
		vitesst.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// lookup vindex is duplicate
		vitesst.AssertContainsError(t, conn, `insert into test(id, val1) values (4,'D'),(5,'C')`, `reverted partial DML execution failure`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		vitesst.Exec(t, conn, `insert into test(id, val1) values (4,'D'),(5,'E')`)
		vitesst.Exec(t, conn, `commit`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")] [INT64(4) VARCHAR("D")] [INT64(5) VARCHAR("E")]]`)
	})
}

func TestPartialQueryFailureAutoCommit(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		vitesst.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// primary vindex is duplicate, transaction is rolled back as it was an implicit transaction started by vtgate.
		vitesst.AssertContainsError(t, conn, `insert into test(id, val1) values (1,'D'),(4,'E')`, `transaction rolled back to reverse changes of partial DML execution`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		// commit will not have any effect on the state in autocommit mode.
		vitesst.Exec(t, conn, `commit`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
	})
}

func TestPartialVindexQueryFailureAutoCommit(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		vitesst.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// lookup vindex is duplicate, transaction is rolled back as it was an implicit transaction started by vtgate.
		vitesst.AssertContainsError(t, conn, `insert into test(id, val1) values (4,'D'),(5,'C')`, `transaction rolled back to reverse changes of partial DML execution`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		vitesst.Exec(t, conn, `insert into test(id, val1) values (4,'D'),(5,'E')`)
		// commit will not have any effect on the state in autocommit mode.
		vitesst.Exec(t, conn, `commit`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")] [INT64(4) VARCHAR("D")] [INT64(5) VARCHAR("E")]]`)
	})
}

func TestPartialQueryFailureRollback(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		vitesst.Exec(t, conn, `begin`)
		vitesst.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// primary vindex is duplicate
		vitesst.AssertContainsError(t, conn, `insert into test(id, val1) values (1,'D'),(4,'E')`, `reverted partial DML execution failure`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		vitesst.Exec(t, conn, `rollback`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[]`)
	})
}

func TestPartialVindexQueryFailureRollback(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		vitesst.Exec(t, conn, `begin`)
		vitesst.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// lookup vindex is duplicate
		vitesst.AssertContainsError(t, conn, `insert into test(id, val1) values (4,'D'),(5,'C')`, `reverted partial DML execution failure`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		vitesst.Exec(t, conn, `insert into test(id, val1) values (4,'D'),(5,'E')`)
		vitesst.Exec(t, conn, `rollback`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[]`)
	})
}

func TestPartialQueryFailureNoAutoCommitRollback(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		// autocommit is false.
		vitesst.Exec(t, conn, `set autocommit = off`)
		vitesst.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// primary vindex is duplicate
		vitesst.AssertContainsError(t, conn, `insert into test(id, val1) values (1,'D'),(4,'E')`, `reverted partial DML execution failure`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		vitesst.Exec(t, conn, `rollback`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[]`)
	})
}

func TestPartialVindexQueryFailureNoAutoCommitRollback(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		// autocommit is false.  y 6
		vitesst.Exec(t, conn, `set autocommit = off`)
		vitesst.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// lookup vindex is duplicate
		vitesst.AssertContainsError(t, conn, `insert into test(id, val1) values (4,'D'),(5,'C')`, `reverted partial DML execution failure`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		vitesst.Exec(t, conn, `insert into test(id, val1) values (4,'D'),(5,'E')`)
		vitesst.Exec(t, conn, `rollback`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[]`)
	})
}

func TestPartialQueryFailureAutoCommitRollback(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		vitesst.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// primary vindex is duplicate, transaction is rolled back as it was an implicit transaction started by vtgate.
		vitesst.AssertContainsError(t, conn, `insert into test(id, val1) values (1,'D'),(4,'E')`, `transaction rolled back to reverse changes of partial DML execution`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		// rollback will not have any effect on the state in autocommit mode.
		vitesst.Exec(t, conn, `rollback`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
	})
}

func TestPartialVindexQueryFailureAutoCommitRollback(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		vitesst.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// lookup vindex is duplicate, transaction is rolled back as it was an implicit transaction started by vtgate.
		vitesst.AssertContainsError(t, conn, `insert into test(id, val1) values (4,'D'),(5,'C')`, `transaction rolled back to reverse changes of partial DML execution`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		vitesst.Exec(t, conn, `insert into test(id, val1) values (4,'D'),(5,'E')`)
		// rollback will not have any effect on the state in autocommit mode.
		vitesst.Exec(t, conn, `rollback`)
		vitesst.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")] [INT64(4) VARCHAR("D")] [INT64(5) VARCHAR("E")]]`)
	})
}
