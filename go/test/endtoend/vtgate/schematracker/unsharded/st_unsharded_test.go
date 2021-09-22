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

package unsharded

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
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
	sqlSchema       = `
		create table main (
			id bigint,
			val varchar(128),
			primary key(id)
		) Engine=InnoDB;
`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
		}
		clusterInstance.VtTabletExtraArgs = []string{"-queryserver-config-schema-change-signal", "-queryserver-config-schema-change-signal-interval", "0.1"}
		err = clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = []string{"-schema_change_signal", "-vschema_ddl_authorized_users", "%", "-planner_version", "Gen4CompareV3"}
		err = clusterInstance.StartVtgate()
		if err != nil {
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

func TestNewUnshardedTable(t *testing.T) {
	defer cluster.PanicHandler(t)

	// create a sql connection
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// ensuring our initial table "main" is in the schema
	qr := exec(t, conn, "SHOW VSCHEMA TABLES")
	got := fmt.Sprintf("%v", qr.Rows)
	want := `[[VARCHAR("dual")] [VARCHAR("main")]]`
	require.Equal(t, want, got)

	// create a new table which is not part of the VSchema
	exec(t, conn, `create table new_table_tracked(id bigint, name varchar(100), primary key(id)) Engine=InnoDB`)

	// waiting for the vttablet's schema_reload interval to kick in
	assertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		`[[VARCHAR("dual")] [VARCHAR("main")] [VARCHAR("new_table_tracked")]]`,
		100*time.Millisecond,
		3*time.Second,
		"new_table_tracked not in vschema tables")

	assertMatches(t, conn, "select id from new_table_tracked", `[]`)              // select
	assertMatches(t, conn, "select id from new_table_tracked where id = 5", `[]`) // select
	// DML on new table
	// insert initial data ,update and delete for the new table
	exec(t, conn, `insert into new_table_tracked(id) values(0),(1)`)
	exec(t, conn, `update new_table_tracked set name = "newName1"`)
	exec(t, conn, "delete from new_table_tracked where id = 0")
	assertMatches(t, conn, `select * from new_table_tracked`, `[[INT64(1) VARCHAR("newName1")]]`)

	exec(t, conn, `drop table new_table_tracked`)

	// waiting for the vttablet's schema_reload interval to kick in
	assertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		`[[VARCHAR("dual")] [VARCHAR("main")]]`,
		100*time.Millisecond,
		3*time.Second,
		"new_table_tracked not in vschema tables")
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

func assertMatchesWithTimeout(t *testing.T, conn *mysql.Conn, query, expected string, r time.Duration, d time.Duration, failureMsg string) {
	t.Helper()
	timeout := time.After(d)
	diff := "actual and expectation does not match"
	for len(diff) > 0 {
		select {
		case <-timeout:
			require.Fail(t, failureMsg, diff)
		case <-time.After(r):
			qr := exec(t, conn, query)
			diff = cmp.Diff(expected,
				fmt.Sprintf("%v", qr.Rows))
		}

	}
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err, "for query: "+query)
	return qr
}
