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

package sharded

import (
	"context"
	_ "embed"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

var (
	KeyspaceName  = "ks"
	sidecarDBName = "_vt_schema_tracker_metadata" // custom sidecar database name for testing
	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
)

func setup(t *testing.T) (*vitesst.Cluster, mysql.ConnParams) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(KeyspaceName).
			WithShards(2).
			WithSchema(SchemaSQL).
			WithVSchema(VSchema).
			WithSidecarDBName(sidecarDBName),
		vitesst.WithVTGateArgs(
			"--schema-change-signal",
			"--vschema-ddl-authorized-users", "%",
			"--enable-views",
		),
		vitesst.WithVTTabletArgs(
			"--queryserver-config-schema-change-signal",
			"--queryserver-enable-views",
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

	return cluster, cluster.VTParams(ctx, "")
}

func TestNewTable(t *testing.T) {
	ctx := t.Context()
	_, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	shard1Params := vtParams
	shard1Params.DbName += ":-80@primary"
	connShard1, err := mysql.Connect(ctx, &shard1Params)
	require.NoError(t, err)
	defer connShard1.Close()

	shard2Params := vtParams
	shard2Params.DbName += ":80-@primary"
	connShard2, err := mysql.Connect(ctx, &shard2Params)
	require.NoError(t, err)
	defer connShard2.Close()

	_ = vitesst.Exec(t, conn, "create table test_table (id bigint, name varchar(100))")

	vitesst.AssertMatchesWithTimeout(t, conn,
		"select * from test_table", `[]`,
		100*time.Millisecond,
		60*time.Second, // longer timeout as this is the first query after setup
		"could not query test_table through vtgate")
	vitesst.AssertMatchesWithTimeout(t, connShard1,
		"select * from test_table", `[]`,
		100*time.Millisecond,
		30*time.Second,
		"could not query test_table on "+shard1Params.DbName)
	vitesst.AssertMatchesWithTimeout(t, connShard2,
		"select * from test_table", `[]`,
		100*time.Millisecond,
		30*time.Second,
		"could not query test_table on "+shard2Params.DbName)

	vitesst.Exec(t, conn, "drop table test_table")

	time.Sleep(2 * time.Second)
}

func TestAmbiguousColumnJoin(t *testing.T) {
	ctx := t.Context()
	_, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	// this query only works if we know which table the testId belongs to. The vschema does not contain
	// this info, so we are testing that the schema tracker has added column info to the vschema
	_, err = conn.ExecuteFetch(`select testId from t8 join t2`, 1000, true)
	require.NoError(t, err)
}

func TestInitAndUpdate(t *testing.T) {
	ctx := t.Context()
	_, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	expected := `[[VARCHAR("t2")] [VARCHAR("t2_id4_idx")] [VARCHAR("t8")]]`
	vitesst.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		expected,
		100*time.Millisecond,
		30*time.Second,
		"initial table list not complete")

	vitesst.AssertMatches(t, conn,
		"SHOW VSCHEMA KEYSPACES",
		`[[VARCHAR("ks") VARCHAR("true") VARCHAR("unmanaged") VARCHAR("")]]`)

	// Init
	_ = vitesst.Exec(t, conn, "create table test_sc (id bigint primary key)")
	expected = `[[VARCHAR("t2")] [VARCHAR("t2_id4_idx")] [VARCHAR("t8")] [VARCHAR("test_sc")]]`
	vitesst.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		expected,
		100*time.Millisecond,
		30*time.Second,
		"test_sc not in vschema tables")

	// Tables Update via health check.
	_ = vitesst.Exec(t, conn, "create table test_sc1 (id bigint primary key)")
	expected = `[[VARCHAR("t2")] [VARCHAR("t2_id4_idx")] [VARCHAR("t8")] [VARCHAR("test_sc")] [VARCHAR("test_sc1")]]`
	vitesst.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		expected,
		100*time.Millisecond,
		30*time.Second,
		"test_sc1 not in vschema tables")

	_ = vitesst.Exec(t, conn, "drop table test_sc, test_sc1")
	expected = `[[VARCHAR("t2")] [VARCHAR("t2_id4_idx")] [VARCHAR("t8")]]`
	vitesst.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		expected,
		100*time.Millisecond,
		30*time.Second,
		"test_sc and test_sc_1 should not be in vschema tables")
}

func TestDMLOnNewTable(t *testing.T) {
	ctx := t.Context()
	_, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// create a new table which is not part of the VSchema
	vitesst.Exec(t, conn, `create table new_table_tracked(id bigint, name varchar(100), primary key(id)) Engine=InnoDB`)

	expected := `[[VARCHAR("new_table_tracked")] [VARCHAR("t2")] [VARCHAR("t2_id4_idx")] [VARCHAR("t8")]]`
	// wait for vttablet's schema reload interval to pass
	vitesst.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		expected,
		100*time.Millisecond,
		30*time.Second,
		"test_sc not in vschema tables")

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
	// insert initial data ,update and delete will fail since we have not added a primary vindex
	errorMessage := "table 'new_table_tracked' does not have a primary vindex (errno 1173) (sqlstate 42000)"
	vitesst.AssertContainsError(t, conn, `insert into new_table_tracked(id) values(0),(1)`, errorMessage)
	vitesst.AssertContainsError(t, conn, `update new_table_tracked set name = "newName1"`, errorMessage)
	vitesst.AssertContainsError(t, conn, "delete from new_table_tracked", errorMessage)

	vitesst.Exec(t, conn, `select name from new_table_tracked join t8`)

	// add a primary vindex for the table
	vitesst.Exec(t, conn, "alter vschema on ks.new_table_tracked add vindex hash(id) using hash")
	time.Sleep(1 * time.Second)
	vitesst.Exec(t, conn, `insert into new_table_tracked(id) values(0),(1)`)
	vitesst.Exec(t, conn, `insert into t8(id8) values(2)`)
	defer vitesst.Exec(t, conn, `delete from t8`)
	vitesst.AssertMatchesWithTimeout(t, conn,
		"select count(*) from new_table_tracked join t8", `[[INT64(2)]]`,
		100*time.Millisecond,
		30*time.Second,
		"did not get expected number of rows when joining new_table_tracked with t8")
	vitesst.AssertMatchesNoOrder(t, conn, `select id from new_table_tracked join t8`, `[[INT64(0)] [INT64(1)]]`)
}

// TestNewView validates that view tracking works as expected.
func TestNewView(t *testing.T) {
	ctx := t.Context()
	_, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// insert some data
	_ = vitesst.Exec(t, conn, "insert into t2 (id3, id4) values (1, 10), (2, 20), (3, 30)")
	defer vitesst.Exec(t, conn, "delete from t2")

	selQuery := "select sum(id4) from t2 where id4 > 10"

	// create a view
	_ = vitesst.Exec(t, conn, "create view test_view as "+selQuery)

	// executing the query directly
	qr := vitesst.Exec(t, conn, selQuery)
	// selecting it through the view.
	vitesst.AssertMatchesWithTimeout(t, conn, "select * from test_view", fmt.Sprintf("%v", qr.Rows), 100*time.Millisecond, 30*time.Second, "test_view not in vschema tables")
}

// TestViewAndTable validates that new column added in table is present in the view definition
func TestViewAndTable(t *testing.T) {
	ctx := t.Context()
	cluster, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// add a new column to the table t8
	_ = vitesst.Exec(t, conn, "alter table t8 add column new_col varchar(50)")
	err = vitesst.WaitForColumn(t, cluster.VTGate(), KeyspaceName, "t8", "new_col")
	require.NoError(t, err)

	// insert some data
	_ = vitesst.Exec(t, conn, "insert into t8(id8, new_col) values (1, 'V')")
	defer vitesst.Exec(t, conn, "delete from t8")

	// create a view with t8, having the new column.
	_ = vitesst.Exec(t, conn, "create view t8_view as select * from t8")

	// executing the view query, with the new column in the select field.
	vitesst.AssertMatchesWithTimeout(t, conn, "select new_col from t8_view", `[[VARCHAR("V")]]`, 100*time.Millisecond, 30*time.Second, "t8_view not in vschema tables")

	// add another column to the table t8
	_ = vitesst.Exec(t, conn, "alter table t8 add column additional_col bigint")
	err = vitesst.WaitForColumn(t, cluster.VTGate(), KeyspaceName, "t8", "additional_col")
	require.NoError(t, err)

	// executing the query on view
	qr := vitesst.Exec(t, conn, "select * from t8_view")
	// validate that field name should not have additional_col
	assert.NotContains(t, fmt.Sprintf("%v", qr.Fields), "additional_col")
}
