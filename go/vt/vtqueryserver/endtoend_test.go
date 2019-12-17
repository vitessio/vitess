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
package vtqueryserver

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttest"

	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var (
	queryServer     *tabletserver.TabletServer
	mysqlConnParams mysql.ConnParams
	proxyConnParams mysql.ConnParams
)

func TestMain(m *testing.M) {
	flag.Parse() // Do not remove this comment, import into google3 depends on it
	tabletenv.Init()

	exitCode := func() int {
		// Launch MySQL.
		// We need a Keyspace in the topology, so the DbName is set.
		// We need a Shard too, so the database 'vttest' is created.
		cfg := vttest.Config{
			Topology: &vttestpb.VTTestTopology{
				Keyspaces: []*vttestpb.Keyspace{
					{
						Name: "vttest",
						Shards: []*vttestpb.Shard{
							{
								Name:           "0",
								DbNameOverride: "vttest",
							},
						},
					},
				},
			},
			OnlyMySQL: true,
		}
		if err := cfg.InitSchemas("vttest", testSchema, nil); err != nil {
			fmt.Fprintf(os.Stderr, "InitSchemas failed: %v\n", err)
			return 1
		}
		defer os.RemoveAll(cfg.SchemaDir)
		cluster := vttest.LocalCluster{
			Config: cfg,
		}
		if err := cluster.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "could not launch mysql: %v\n", err)
			return 1
		}
		defer cluster.TearDown()
		mysqlConnParams = cluster.MySQLConnParams()

		// Setup a unix socket to connect to the proxy.
		// We use a temporary file.
		unixSocket, err := ioutil.TempFile("", "mysqlproxy.sock")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create temp file: %v", err)
			return 1
		}
		proxySock := unixSocket.Name()
		os.Remove(proxySock)

		proxyConnParams.UnixSocket = proxySock
		proxyConnParams.Uname = "proxy"
		proxyConnParams.Pass = "letmein"

		*mysqlServerSocketPath = proxyConnParams.UnixSocket
		*mysqlAuthServerImpl = "none"

		// set a short query timeout and a constrained connection pool
		// to test that end to end timeouts work
		tabletenv.Config.QueryTimeout = 2
		tabletenv.Config.PoolSize = 1
		tabletenv.Config.QueryPoolTimeout = 0.1
		defer func() { tabletenv.Config = tabletenv.DefaultQsConfig }()

		// Initialize the query service on top of the vttest MySQL database.
		dbcfgs := dbconfigs.NewTestDBConfigs(mysqlConnParams, mysqlConnParams, cluster.DbName())
		queryServer, err = initProxy(dbcfgs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not start proxy: %v\n", err)
			return 1
		}
		defer queryServer.StopService()

		// Initialize the MySQL server protocol to talk to the query service.
		initMySQLProtocol()
		defer shutdownMySQLProtocol()

		return m.Run()
	}()
	os.Exit(exitCode)
}

var testSchema = `
create table test(id int, val varchar(256), primary key(id));
create table valtest(intval int default 0, floatval float default null, charval varchar(256) default null, binval varbinary(256) default null, primary key(intval));
`

func testFetch(t *testing.T, conn *mysql.Conn, sql string, expectedRows int) *sqltypes.Result {
	t.Helper()

	result, err := conn.ExecuteFetch(sql, 1000, false)
	if err != nil {
		t.Errorf("error: %v", err)
		return nil
	}

	if len(result.Rows) != expectedRows {
		t.Errorf("expected %d rows but got %d", expectedRows, len(result.Rows))
	}

	return result
}

func testDML(t *testing.T, conn *mysql.Conn, sql string, expectedNumQueries int64, expectedRowsAffected uint64) {
	t.Helper()

	numQueries := tabletenv.MySQLStats.Counts()["Exec"]
	result, err := conn.ExecuteFetch(sql, 1000, false)
	if err != nil {
		t.Errorf("error: %v", err)
	}
	numQueries = tabletenv.MySQLStats.Counts()["Exec"] - numQueries

	if numQueries != expectedNumQueries {
		t.Errorf("expected %d mysql queries but got %d", expectedNumQueries, numQueries)
	}

	if result.RowsAffected != expectedRowsAffected {
		t.Errorf("expected %d rows affected but got %d", expectedRowsAffected, result.RowsAffected)
	}
}

func TestQueries(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}

	// Try a simple query case.
	testFetch(t, conn, "select * from test", 0)

	// Try a simple error case.
	_, err = conn.ExecuteFetch("select * from aa", 1000, true)
	if err == nil || !strings.Contains(err.Error(), "table aa not found in schema") {
		t.Fatalf("expected error but got: %v", err)
	}
}

func TestAutocommitDMLs(t *testing.T) {
	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}

	conn2, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}

	testDML(t, conn, "insert into test (id, val) values(1, 'hello')", 3, 1)

	testFetch(t, conn, "select * from test", 1)
	testFetch(t, conn2, "select * from test", 1)

	testDML(t, conn, "delete from test", 4, 1)

	testFetch(t, conn, "select * from test", 0)
	testFetch(t, conn2, "select * from test", 0)
}

func TestPassthroughDMLs(t *testing.T) {
	ctx := context.Background()

	queryServer.SetPassthroughDMLs(true)
	conn, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}

	conn2, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}

	testDML(t, conn, "insert into test (id, val) values(1, 'hello')", 3, 1)
	testDML(t, conn, "insert into test (id, val) values(2, 'hello')", 3, 1)
	testDML(t, conn, "insert into test (id, val) values(3, 'hello')", 3, 1)

	// Subquery DMLs are errors in passthrough mode with SBR, unless
	// SetAllowUnsafeDMLs is set
	_, err = conn.ExecuteFetch("update test set val='goodbye'", 1000, true)
	if err == nil || !strings.Contains(err.Error(), "cannot identify primary key of statement") {
		t.Fatalf("expected error but got: %v", err)
	}

	queryServer.SetAllowUnsafeDMLs(true)

	// This is 3 queries in passthrough mode and not 4 queries as it would
	// be in non-passthrough mode
	testDML(t, conn, "update test set val='goodbye'", 3, 3)

	testFetch(t, conn, "select * from test where val='goodbye'", 3)
	testFetch(t, conn2, "select * from test where val='goodbye'", 3)

	testDML(t, conn, "delete from test", 4, 3)

	testFetch(t, conn, "select * from test", 0)
	testFetch(t, conn2, "select * from test", 0)
}

func TestTransactions(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}
	conn2, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}

	testDML(t, conn, "begin", 1, 0)
	testDML(t, conn, "insert into test (id, val) values(1, 'hello')", 1, 1)
	testFetch(t, conn, "select * from test", 1)
	testFetch(t, conn2, "select * from test", 0)
	testDML(t, conn, "commit", 1, 0)
	testFetch(t, conn, "select * from test", 1)
	testFetch(t, conn2, "select * from test", 1)

	testDML(t, conn, "begin", 1, 0)
	testDML(t, conn, "delete from test", 2, 1)
	testFetch(t, conn, "select * from test", 0)
	testFetch(t, conn2, "select * from test", 1)
	testDML(t, conn, "rollback", 1, 0)

	testFetch(t, conn, "select * from test", 1)
	testFetch(t, conn2, "select * from test", 1)

	testDML(t, conn2, "begin", 1, 0)
	testDML(t, conn2, "delete from test", 2, 1)
	testDML(t, conn2, "commit", 1, 0)

	testFetch(t, conn, "select * from test", 0)
	testFetch(t, conn2, "select * from test", 0)
}

func TestNoAutocommit(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}
	conn2, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}

	testFetch(t, conn, "set autocommit=0", 0)

	testDML(t, conn, "insert into test (id, val) values(1, 'hello')", 2, 1)
	testFetch(t, conn, "select * from test", 1)
	testFetch(t, conn2, "select * from test", 0)
	testDML(t, conn, "commit", 1, 0)
	testFetch(t, conn, "select * from test", 1)
	testFetch(t, conn2, "select * from test", 1)

	testDML(t, conn, "delete from test", 3, 1)
	testFetch(t, conn, "select * from test", 0)
	testFetch(t, conn2, "select * from test", 1)
	testDML(t, conn, "rollback", 1, 0)

	testFetch(t, conn, "select * from test", 1)
	testFetch(t, conn2, "select * from test", 1)

	testFetch(t, conn2, "set autocommit=0", 0)
	testDML(t, conn2, "delete from test", 3, 1)
	testDML(t, conn2, "commit", 1, 0)

	testFetch(t, conn, "select * from test", 0)
	testFetch(t, conn2, "select * from test", 0)
}

func TestTransactionsInProcess(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}
	conn2, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}

	testDML(t, conn, "begin", 1, 0)
	testDML(t, conn, "insert into test (id, val) values(1, 'hello')", 1, 1)
	testFetch(t, conn, "select * from test", 1)
	testFetch(t, conn2, "select * from test", 0)

	// A second begin causes the first transaction to commit and then
	// runs the begin
	testDML(t, conn, "begin", 2, 0)
	testFetch(t, conn, "select * from test", 1)
	testFetch(t, conn2, "select * from test", 1)
	testDML(t, conn, "rollback", 1, 0)

	testFetch(t, conn, "select * from test", 1)
	testFetch(t, conn2, "select * from test", 1)

	// Setting autocommit=1 (when it was previously 0) causes the existing transaction to commit
	testDML(t, conn, "set autocommit=0", 0, 0)
	// (2 queries -- begin b/c autocommit = 0; insert)
	testDML(t, conn, "insert into test (id, val) values(2, 'hello')", 2, 1)
	testFetch(t, conn, "select * from test", 2)
	testFetch(t, conn2, "select * from test", 1)

	// (1 query -- commit b/c of autocommit change)
	testDML(t, conn, "set autocommit=1", 1, 0)
	testFetch(t, conn, "select * from test", 2)
	testFetch(t, conn2, "select * from test", 2)

	// Setting autocommit=1 doesn't cause the existing transaction to commit if it was already
	// autocommit=1. Therefore rollback is effective.
	testDML(t, conn, "set autocommit=1", 0, 0)
	testDML(t, conn, "begin", 1, 0)
	testDML(t, conn, "insert into test (id, val) values(3, 'hello')", 1, 1)
	testFetch(t, conn, "select * from test", 3)
	testFetch(t, conn2, "select * from test", 2)

	testDML(t, conn, "set autocommit=1", 0, 0)
	testDML(t, conn, "rollback", 1, 0)
	testFetch(t, conn, "select * from test", 2)
	testFetch(t, conn2, "select * from test", 2)

	// Basic autocommit test
	testDML(t, conn, "insert into test (id, val) values(3, 'hello')", 3, 1)
	testFetch(t, conn, "select * from test", 3)
	testFetch(t, conn2, "select * from test", 3)

	// Cleanup
	testDML(t, conn2, "begin", 1, 0)
	testDML(t, conn2, "delete from test", 2, 3)
	testDML(t, conn2, "commit", 1, 0)

	testFetch(t, conn, "select * from test", 0)
	testFetch(t, conn2, "select * from test", 0)

}

func TestErrorDoesntDropTransaction(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}
	conn2, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}

	testDML(t, conn, "begin", 1, 0)
	testDML(t, conn, "insert into test (id, val) values(1, 'hello')", 1, 1)
	_, err = conn.ExecuteFetch("select this is garbage", 1, false)
	if err == nil {
		t.Log("Expected error for garbage sql request")
		t.FailNow()
	}
	// First connection should still see its data.
	testFetch(t, conn, "select * from test", 1)
	// But second won't, since it's uncommitted.
	testFetch(t, conn2, "select * from test", 0)

	// Commit works still.
	testDML(t, conn, "commit", 1, 0)
	testFetch(t, conn, "select * from test", 1)
	testFetch(t, conn2, "select * from test", 1)

	// Cleanup
	testDML(t, conn2, "begin", 1, 0)
	testDML(t, conn2, "delete from test", 2, 1)
	testDML(t, conn2, "commit", 1, 0)
}

func TestOther(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}

	testFetch(t, conn, "explain select * from test", 1)
	testFetch(t, conn, "select table_name, table_rows from information_schema.tables where table_name='test'", 1)
}

func TestQueryDeadline(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}

	// First run a query that is killed by the slow query killer after 2s
	_, err = conn.ExecuteFetch("select sleep(5) from dual", 1000, false)
	wantErr := "EOF (errno 2013) (sqlstate HY000) (CallerID: userData1): Sql: \"select sleep(:vtp1) from dual\", " +
		"BindVars: {#maxLimit: \"type:INT64 value:\\\"10001\\\" \"vtp1: \"type:INT64 value:\\\"5\\\" \"} " +
		"(errno 2013) (sqlstate HY000) during query: select sleep(5) from dual"
	if err == nil || err.Error() != wantErr {
		t.Errorf("error want '%v', got '%v'", wantErr, err)
	}

	sqlErr, ok := err.(*mysql.SQLError)
	if !ok {
		t.Fatalf("Unexpected error type: %T, want %T", err, &mysql.SQLError{})
	}
	if got, want := sqlErr.Number(), mysql.CRServerLost; got != want {
		t.Errorf("Unexpected error code: %d, want %d", got, want)
	}

	conn, err = mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}
	conn2, err := mysql.Connect(ctx, &proxyConnParams)
	if err != nil {
		t.Fatal(err)
	}

	// Now send another query to tie up the connection, followed up by
	// a query that should fail due to not getting the conn from the
	// conn pool
	err = conn.WriteComQuery("select sleep(1.75) from dual")
	if err != nil {
		t.Errorf("unexpected error sending query: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	_, err = conn2.ExecuteFetch("select 1 from dual", 1000, false)
	wantErr = "query pool wait time exceeded"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("want error %v, got %v", wantErr, err)
	}
	sqlErr, ok = err.(*mysql.SQLError)
	if !ok {
		t.Fatalf("Unexpected error type: %T, want %T", err, &mysql.SQLError{})
	}
	if got, want := sqlErr.Number(), mysql.ERTooManyUserConnections; got != want {
		t.Errorf("Unexpected error code: %d, want %d", got, want)
	}

	_, _, _, err = conn.ReadQueryResult(1000, false)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}

}
