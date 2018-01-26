/*
Copyright 2017 Google Inc.

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
	"os"
	"path"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"github.com/youtube/vitess/go/vt/vttest"

	vttestpb "github.com/youtube/vitess/go/vt/proto/vttest"
)

var (
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

		proxySock := path.Join(cluster.Env.Directory(), "mysqlproxy.sock")

		proxyConnParams.UnixSocket = proxySock
		proxyConnParams.Uname = "proxy"
		proxyConnParams.Pass = "letmein"

		*mysqlServerSocketPath = proxyConnParams.UnixSocket
		*mysqlAuthServerImpl = "none"

		dbcfgs := dbconfigs.DBConfigs{
			App: mysqlConnParams,
		}
		qs, err := initProxy(&dbcfgs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not start proxy: %v\n", err)
			return 1
		}
		defer qs.StopService()

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

func testFetch(t *testing.T, conn *mysql.Conn, sql string, expectedRows int) {
	t.Helper()

	result, err := conn.ExecuteFetch(sql, 1000, true)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if len(result.Rows) != expectedRows {
		t.Fatalf("expected %d rows but got %d", expectedRows, len(result.Rows))
	}
}

func testDML(t *testing.T, conn *mysql.Conn, sql string, expectedRows int) {
	t.Helper()

	result, err := conn.ExecuteFetch(sql, 1000, true)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if int(result.RowsAffected) != expectedRows {
		t.Fatalf("expected %d rows affected but got %d", expectedRows, result.RowsAffected)
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

	testDML(t, conn, "insert into test (id, val) values(1, 'hello')", 1)

	testFetch(t, conn, "select * from test", 1)
	testFetch(t, conn2, "select * from test", 1)

	testDML(t, conn, "delete from test", 1)

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

	testDML(t, conn, "begin", 0)
	testDML(t, conn, "insert into test (id, val) values(1, 'hello')", 1)
	testFetch(t, conn, "select * from test", 1)
	testFetch(t, conn2, "select * from test", 0)
	testDML(t, conn, "commit", 0)
	testFetch(t, conn, "select * from test", 1)
	testFetch(t, conn2, "select * from test", 1)

	testDML(t, conn, "begin", 0)
	testDML(t, conn, "delete from test", 1)
	testFetch(t, conn, "select * from test", 0)
	testFetch(t, conn2, "select * from test", 1)
	testDML(t, conn, "rollback", 0)

	testFetch(t, conn, "select * from test", 1)
	testFetch(t, conn2, "select * from test", 1)

	testDML(t, conn2, "begin", 0)
	testDML(t, conn2, "delete from test", 1)
	testDML(t, conn2, "commit", 0)

	testFetch(t, conn, "select * from test", 0)
	testFetch(t, conn2, "select * from test", 0)
}
