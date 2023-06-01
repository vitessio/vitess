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
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
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

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: SchemaSQL,
			VSchema:   VSchema,
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-40", "40-80", "80-c0", "c0-"}, 0, false); err != nil {

			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "--planner-version", "Gen4Fallback")
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1
		}
		vtParams = clusterInstance.GetVTParams(keyspaceName)
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
			conn, err := mysql.Connect(context.Background(), &vtParams)
			require.NoError(t, err)
			defer conn.Close()

			// setup the mode
			for _, q := range tc.qs {
				utils.Exec(t, conn, q)
			}

			// cleanup previous run data from table.
			utils.Exec(t, conn, `delete from test`)

			// execute all the test stmts.
			stmts(conn)
		})
	}
}
func TestPartialQueryFailureExplicitTx(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		utils.Exec(t, conn, `begin`)
		utils.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// primary vindex is duplicate
		utils.AssertContainsError(t, conn, `insert into test(id, val1) values (1,'D'),(4,'E')`, `reverted partial DML execution failure`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		utils.Exec(t, conn, `commit`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
	})
}

func TestPartialVindexQueryFailureExplicitTx(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		utils.Exec(t, conn, `begin`)
		utils.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// lookup vindex is duplicate
		utils.AssertContainsError(t, conn, `insert into test(id, val1) values (4,'D'),(5,'C')`, `reverted partial DML execution failure`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		utils.Exec(t, conn, `insert into test(id, val1) values (4,'D'),(5,'E')`)
		utils.Exec(t, conn, `commit`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")] [INT64(4) VARCHAR("D")] [INT64(5) VARCHAR("E")]]`)
	})
}

func TestPartialQueryFailureNoAutoCommit(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		// autocommit is false.
		utils.Exec(t, conn, `set autocommit = off`)
		utils.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// primary vindex is duplicate
		utils.AssertContainsError(t, conn, `insert into test(id, val1) values (1,'D'),(4,'E')`, `reverted partial DML execution failure`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		utils.Exec(t, conn, `commit`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
	})
}

func TestPartialVindexQueryFailureNoAutoCommit(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		// autocommit is false.
		utils.Exec(t, conn, `set autocommit = off`)
		utils.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// lookup vindex is duplicate
		utils.AssertContainsError(t, conn, `insert into test(id, val1) values (4,'D'),(5,'C')`, `reverted partial DML execution failure`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		utils.Exec(t, conn, `insert into test(id, val1) values (4,'D'),(5,'E')`)
		utils.Exec(t, conn, `commit`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")] [INT64(4) VARCHAR("D")] [INT64(5) VARCHAR("E")]]`)
	})
}

func TestPartialQueryFailureAutoCommit(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		utils.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// primary vindex is duplicate, transaction is rolled back as it was an implicit transaction started by vtgate.
		utils.AssertContainsError(t, conn, `insert into test(id, val1) values (1,'D'),(4,'E')`, `transaction rolled back to reverse changes of partial DML execution`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		// commit will not have any effect on the state in autocommit mode.
		utils.Exec(t, conn, `commit`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
	})
}

func TestPartialVindexQueryFailureAutoCommit(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		utils.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// lookup vindex is duplicate, transaction is rolled back as it was an implicit transaction started by vtgate.
		utils.AssertContainsError(t, conn, `insert into test(id, val1) values (4,'D'),(5,'C')`, `transaction rolled back to reverse changes of partial DML execution`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		utils.Exec(t, conn, `insert into test(id, val1) values (4,'D'),(5,'E')`)
		// commit will not have any effect on the state in autocommit mode.
		utils.Exec(t, conn, `commit`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")] [INT64(4) VARCHAR("D")] [INT64(5) VARCHAR("E")]]`)
	})
}

func TestPartialQueryFailureRollback(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		utils.Exec(t, conn, `begin`)
		utils.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// primary vindex is duplicate
		utils.AssertContainsError(t, conn, `insert into test(id, val1) values (1,'D'),(4,'E')`, `reverted partial DML execution failure`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		utils.Exec(t, conn, `rollback`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[]`)
	})
}

func TestPartialVindexQueryFailureRollback(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		utils.Exec(t, conn, `begin`)
		utils.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// lookup vindex is duplicate
		utils.AssertContainsError(t, conn, `insert into test(id, val1) values (4,'D'),(5,'C')`, `reverted partial DML execution failure`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		utils.Exec(t, conn, `insert into test(id, val1) values (4,'D'),(5,'E')`)
		utils.Exec(t, conn, `rollback`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[]`)
	})
}

func TestPartialQueryFailureNoAutoCommitRollback(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		// autocommit is false.
		utils.Exec(t, conn, `set autocommit = off`)
		utils.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// primary vindex is duplicate
		utils.AssertContainsError(t, conn, `insert into test(id, val1) values (1,'D'),(4,'E')`, `reverted partial DML execution failure`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		utils.Exec(t, conn, `rollback`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[]`)
	})
}

func TestPartialVindexQueryFailureNoAutoCommitRollback(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		// autocommit is false.  y 6
		utils.Exec(t, conn, `set autocommit = off`)
		utils.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// lookup vindex is duplicate
		utils.AssertContainsError(t, conn, `insert into test(id, val1) values (4,'D'),(5,'C')`, `reverted partial DML execution failure`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		utils.Exec(t, conn, `insert into test(id, val1) values (4,'D'),(5,'E')`)
		utils.Exec(t, conn, `rollback`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[]`)
	})
}

func TestPartialQueryFailureAutoCommitRollback(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		utils.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// primary vindex is duplicate, transaction is rolled back as it was an implicit transaction started by vtgate.
		utils.AssertContainsError(t, conn, `insert into test(id, val1) values (1,'D'),(4,'E')`, `transaction rolled back to reverse changes of partial DML execution`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		// rollback will not have any effect on the state in autocommit mode.
		utils.Exec(t, conn, `rollback`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
	})
}

func TestPartialVindexQueryFailureAutoCommitRollback(t *testing.T) {
	testAllModes(t, func(conn *mysql.Conn) {
		utils.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
		// lookup vindex is duplicate, transaction is rolled back as it was an implicit transaction started by vtgate.
		utils.AssertContainsError(t, conn, `insert into test(id, val1) values (4,'D'),(5,'C')`, `transaction rolled back to reverse changes of partial DML execution`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		utils.Exec(t, conn, `insert into test(id, val1) values (4,'D'),(5,'E')`)
		// rollback will not have any effect on the state in autocommit mode.
		utils.Exec(t, conn, `rollback`)
		utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")] [INT64(4) VARCHAR("D")] [INT64(5) VARCHAR("E")]]`)
	})
}
