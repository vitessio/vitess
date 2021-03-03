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

package unsharded

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	cell            = "zone1"
	hostname        = "localhost"
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
			Name: keyspaceName,
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false); err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = []string{"-dbddl_plugin", "noop"}
		vtgateProcess := clusterInstance.NewVtgateInstance()
		vtgateProcess.SysVarSetEnabled = true
		if err := vtgateProcess.Setup(); err != nil {
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestDBDDLPluginSync(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := exec(t, conn, `create database aaa`)
	require.EqualValues(t, 1, qr.RowsAffected)

	keyspace := &cluster.Keyspace{
		Name: "aaa",
	}
	require.NoError(t,
		clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false),
		"new database creation failed")

	exec(t, conn, `use aaa`)

	exec(t, conn, `create table t (id bigint primary key)`)
	exec(t, conn, `insert into t(id) values (1),(2),(3),(4),(5)`)
	assertMatches(t, conn, "select count(*) from t", `[[INT64(5)]]`)

	exec(t, conn, `drop database aaa`)

	execAssertError(t, conn, "select count(*) from t", `some error`)
}

func TestDBDDLPluginASync(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := exec(t, conn, `create database aaa`)
	require.EqualValues(t, 1, qr.RowsAffected)

	go func() {
		keyspace := &cluster.Keyspace{
			Name: "aaa",
		}
		require.NoError(t,
			clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false),
			"new database creation failed")
	}()

	exec(t, conn, `use aaa`)

	exec(t, conn, `create table t (id bigint primary key)`)
	exec(t, conn, `insert into t(id) values (1),(2),(3),(4),(5)`)
	assertMatches(t, conn, "select count(*) from t", `[[INT64(5)]]`)

	exec(t, conn, `drop database aaa`)

	execAssertError(t, conn, "select count(*) from t", `some error`)
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	return qr
}

func execAssertError(t *testing.T, conn *mysql.Conn, query string, errorString string) {
	t.Helper()
	_, err := conn.ExecuteFetch(query, 1000, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), errorString)
}

func assertMatches(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := exec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	diff := cmp.Diff(expected, got)
	if diff != "" {
		t.Errorf("Query: %s (-want +got):\n%s", query, diff)
	}
}
