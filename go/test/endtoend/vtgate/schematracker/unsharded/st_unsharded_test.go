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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

var (
	keyspaceName  = "ks"
	sidecarDBName = "_vt_schema_tracker_metadata" // custom sidecar database name for testing
	sqlSchema     = `
		create table main (
			id bigint,
			val varchar(128),
			primary key(id)
		) Engine=InnoDB;
`
)

func setup(t *testing.T) mysql.ConnParams {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(keyspaceName).
			WithSchema(sqlSchema).
			WithSidecarDBName(sidecarDBName),
		vitesst.WithVTTabletArgs("--queryserver-config-schema-change-signal"),
		vitesst.WithVTGateArgs(
			"--schema-change-signal",
			"--vschema-ddl-authorized-users", "%",
		),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	return cluster.VTParams(ctx, "")
}

func TestNewUnshardedTable(t *testing.T) {
	// create a sql connection
	ctx := t.Context()
	vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	expected := `[[VARCHAR("main")]]`

	// ensuring our initial table "main" is in the schema
	vitesst.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		expected,
		100*time.Millisecond,
		30*time.Second,
		"initial table list not complete")

	// create a new table which is not part of the VSchema
	vitesst.Exec(t, conn, `create table new_table_tracked(id bigint, name varchar(100), primary key(id)) Engine=InnoDB`)

	expected = `[[VARCHAR("main")] [VARCHAR("new_table_tracked")]]`

	// waiting for the vttablet's schema_reload interval to kick in
	vitesst.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		expected,
		100*time.Millisecond,
		30*time.Second,
		"new_table_tracked not in vschema tables")

	vitesst.AssertMatchesWithTimeout(t, conn,
		"select id from new_table_tracked", `[]`,
		100*time.Millisecond,
		60*time.Second, // longer timeout as it's the first query after setup
		"could not query new_table_tracked through vtgate")
	vitesst.AssertMatchesWithTimeout(t, conn,
		"select id from new_table_tracked where id = 5", `[]`,
		100*time.Millisecond,
		30*time.Second,
		"could not query new_table_tracked through vtgate")

	// DML on new table
	// insert initial data ,update and delete for the new table
	vitesst.Exec(t, conn, `insert into new_table_tracked(id) values(0),(1)`)
	vitesst.Exec(t, conn, `update new_table_tracked set name = "newName1"`)
	vitesst.Exec(t, conn, "delete from new_table_tracked where id = 0")
	vitesst.AssertMatchesWithTimeout(t, conn,
		`select * from new_table_tracked`, `[[INT64(1) VARCHAR("newName1")]]`,
		100*time.Millisecond,
		30*time.Second,
		"could not query expected row in new_table_tracked through vtgate")

	vitesst.Exec(t, conn, `drop table new_table_tracked`)

	// waiting for the vttablet's schema_reload interval to kick in
	expected = `[[VARCHAR("main")]]`
	vitesst.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		expected,
		100*time.Millisecond,
		30*time.Second,
		"new_table_tracked not in vschema tables")
}

// TestCaseSensitiveSchemaTracking tests that schema tracking is case-sensitive.
// This test only works on Linux (and not on Windows and Mac) since it has a case-sensitive file system, so it allows
// creating two tables having the same name differing only in casing, but other operating systems don't.
// More information at https://dev.mysql.com/doc/refman/8.0/en/identifier-case-sensitivity.html#:~:text=Table%20names%20are%20stored%20in,lowercase%20on%20storage%20and%20lookup.
func TestCaseSensitiveSchemaTracking(t *testing.T) {
	// create a sql connection
	ctx := t.Context()
	vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// ensuring our initial table "main" is in the schema
	vitesst.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		`[[VARCHAR("main")]]`,
		100*time.Millisecond,
		30*time.Second,
		"initial tables not found in vschema")

	// Now we create two tables with the same name differing only in casing t1 and T1.
	// For both of them we'll have different schema's and verify that we can read the data after schema tracking kicks in.
	vitesst.Exec(t, conn, `create table t1(id bigint, primary key(id)) Engine=InnoDB`)
	vitesst.Exec(t, conn, `create table T1(col bigint, col2 bigint, primary key(col)) Engine=InnoDB`)

	// Wait for schema tracking to be caught up
	vitesst.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		`[[VARCHAR("T1")] [VARCHAR("main")] [VARCHAR("t1")]]`,
		100*time.Millisecond,
		30*time.Second,
		"schema tracking didn't track both the tables")

	// Run DMLs
	vitesst.Exec(t, conn, `insert into t1(id) values(0),(1)`)
	vitesst.Exec(t, conn, `insert into T1(col, col2) values(0,0),(1,1)`)

	// Verify the tables are queryable
	vitesst.AssertMatchesWithTimeout(t, conn,
		`select * from t1`, `[[INT64(0)] [INT64(1)]]`,
		100*time.Millisecond,
		30*time.Second,
		"could not query expected rows in t1 through vtgate")
	vitesst.AssertMatchesWithTimeout(t, conn,
		`select * from T1`, `[[INT64(0) INT64(0)] [INT64(1) INT64(1)]]`,
		100*time.Millisecond,
		30*time.Second,
		"could not query expected rows in T1 through vtgate")
}
