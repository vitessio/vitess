/*
Copyright 2020 The Vitess Authors.

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

package withddl

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttest"
)

var connParams mysql.ConnParams

func TestExec(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &connParams)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.ExecuteFetch("create database t", 10000, true)
	require.NoError(t, err)
	defer conn.ExecuteFetch("drop database t", 10000, true)

	testcases := []struct {
		name    string
		ddls    []string
		init    []string
		query   string
		qr      *sqltypes.Result
		err     string
		cleanup []string
	}{{
		name: "TableExists",
		ddls: []string{
			"invalid sql",
		},
		init: []string{
			"create table a(id int, primary key(id))",
		},
		query: "insert into a values(1)",
		qr:    &sqltypes.Result{RowsAffected: 1},
		cleanup: []string{
			"drop table a",
		},
	}, {
		name: "TableDoesNotExist",
		ddls: []string{
			"create table if not exists a(id int, primary key(id))",
		},
		query: "insert into a values(1)",
		qr:    &sqltypes.Result{RowsAffected: 1},
		cleanup: []string{
			"drop table a",
		},
	}, {
		name: "TableMismatch",
		ddls: []string{
			"create table if not exists a(id int, val int, primary key(id))",
		},
		init: []string{
			"create table a(id int, primary key(id))",
		},
		query: "insert into a values(1, 2)",
		err:   "Column count doesn't match value",
		cleanup: []string{
			"drop table a",
		},
	}, {
		name: "TableMustBeAltered",
		ddls: []string{
			"create table if not exists a(id int, primary key(id))",
			"alter table a add column val int",
		},
		init: []string{
			"create table a(id int, primary key(id))",
		},
		query: "insert into a values(1, 2)",
		qr:    &sqltypes.Result{RowsAffected: 1},
		cleanup: []string{
			"drop table a",
		},
	}, {
		name: "NonidempotentDDL",
		ddls: []string{
			"create table a(id int, primary key(id))",
			"alter table a add column val int",
		},
		init: []string{
			"create table a(id int, primary key(id))",
		},
		query: "insert into a values(1, 2)",
		qr:    &sqltypes.Result{RowsAffected: 1},
		cleanup: []string{
			"drop table a",
		},
	}, {
		name: "DupFieldInDDL",
		ddls: []string{
			// error for adding v1 should be ignored.
			"alter table a add column v1 int",
			"alter table a add column v2 int",
		},
		init: []string{
			"create table a(id int, v1 int, primary key(id))",
		},
		query: "insert into a values(1, 2, 3)",
		qr:    &sqltypes.Result{RowsAffected: 1},
		cleanup: []string{
			"drop table a",
		},
	}, {
		name: "NonSchemaError",
		ddls: []string{
			"invalid sql",
		},
		query: "syntax error",
		err:   "error in your SQL syntax",
	}, {
		name: "BadDDL",
		ddls: []string{
			"invalid sql",
		},
		query: "insert into a values(1)",
		err:   "doesn't exist",
	}}

	withdb := connParams
	withdb.DbName = "t"
	execconn, err := mysql.Connect(ctx, &withdb)
	require.NoError(t, err)
	defer execconn.Close()

	funcs := []struct {
		name string
		f    interface{}
	}{{
		name: "f1",
		f: func(query string) (*sqltypes.Result, error) {
			return execconn.ExecuteFetch(query, 10000, true)
		},
	}, {
		name: "f2",
		f: func(query string, maxrows int) (*sqltypes.Result, error) {
			return execconn.ExecuteFetch(query, maxrows, true)
		},
	}, {
		name: "f3",
		f:    execconn.ExecuteFetch,
	}, {
		name: "f4",
		f: func(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
			return execconn.ExecuteFetch(query, maxrows, wantfields)
		},
	}}

	for _, test := range testcases {
		for _, fun := range funcs {
			t.Run(fmt.Sprintf("%v-%v", test.name, fun.name), func(t *testing.T) {
				for _, query := range test.init {
					_, err = execconn.ExecuteFetch(query, 10000, true)
					require.NoError(t, err)
				}

				wd := New(test.ddls)
				qr, err := wd.Exec(ctx, test.query, fun.f)
				if test.qr != nil {
					test.qr.StatusFlags = sqltypes.ServerStatusAutocommit
				}
				checkResult(t, test.qr, test.err, qr, err)

				for _, query := range test.cleanup {
					_, err = execconn.ExecuteFetch(query, 10000, true)
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestExecIgnore(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &connParams)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.ExecuteFetch("create database t", 10000, true)
	require.NoError(t, err)
	defer conn.ExecuteFetch("drop database t", 10000, true)

	withdb := connParams
	withdb.DbName = "t"
	execconn, err := mysql.Connect(ctx, &withdb)
	require.NoError(t, err)
	defer execconn.Close()

	wd := New([]string{})
	qr, err := wd.ExecIgnore(ctx, "select * from a", execconn.ExecuteFetch)
	require.NoError(t, err)
	assert.Equal(t, &sqltypes.Result{}, qr)

	_, err = wd.ExecIgnore(ctx, "syntax error", execconn.ExecuteFetch)
	// This should fail.
	assert.Error(t, err)

	_, _ = execconn.ExecuteFetch("create table a(id int, primary key(id))", 10000, false)
	defer execconn.ExecuteFetch("drop table a", 10000, false)
	_, _ = execconn.ExecuteFetch("insert into a values(1)", 10000, false)
	qr, err = wd.ExecIgnore(ctx, "select * from a", execconn.ExecuteFetch)
	require.NoError(t, err)
	assert.Equal(t, 1, len(qr.Rows))
}

func checkResult(t *testing.T, wantqr *sqltypes.Result, wanterr string, qr *sqltypes.Result, err error) {
	t.Helper()

	assert.Equal(t, wantqr, qr)
	var goterr string
	if err != nil {
		goterr = err.Error()
	}
	if wanterr == "" {
		assert.Equal(t, "", goterr)
	}
	assert.Contains(t, goterr, wanterr)
}

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
		defer os.RemoveAll(cfg.SchemaDir)
		cluster := vttest.LocalCluster{
			Config: cfg,
		}
		if err := cluster.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "could not launch mysql: %v\n", err)
			return 1
		}
		defer cluster.TearDown()

		connParams = cluster.MySQLConnParams()

		return m.Run()
	}()
	os.Exit(exitCode)
}
