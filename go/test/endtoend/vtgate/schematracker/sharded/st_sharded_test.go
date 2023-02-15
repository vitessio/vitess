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
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "ks"
	Cell            = "test"
	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(Cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      KeyspaceName,
			SchemaSQL: SchemaSQL,
			VSchema:   VSchema,
		}
		clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal",
			"--vschema_ddl_authorized_users", "%",
			"--schema_change_signal_user", "userData1"}
		clusterInstance.VtGatePlannerVersion = planbuilder.Gen4
		clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal",
			"--queryserver-config-schema-change-signal-interval", "0.1",
			"--queryserver-config-strict-table-acl",
			"--queryserver-config-acl-exempt-acl", "userData1",
			"--table-acl-config", "dummy.json"}

		vtgateVer, err := cluster.GetMajorVersion("vtgate")
		if err != nil {
			return 1
		}
		vttabletVer, err := cluster.GetMajorVersion("vttablet")
		if err != nil {
			return 1
		}
		if vtgateVer >= 16 && vttabletVer >= 16 {
			clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "--enable-views")
			clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs, "--queryserver-enable-views")
		}

		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false)
		if err != nil {
			return 1
		}

		// Start vtgate
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

func TestNewTable(t *testing.T) {
	ctx := context.Background()
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

	_ = utils.Exec(t, conn, "create table test_table (id bigint, name varchar(100))")

	time.Sleep(2 * time.Second)

	utils.AssertMatches(t, conn, "select * from test_table", `[]`)
	utils.AssertMatches(t, connShard1, "select * from test_table", `[]`)
	utils.AssertMatches(t, connShard2, "select * from test_table", `[]`)

	utils.Exec(t, conn, "drop table test_table")

	time.Sleep(2 * time.Second)
}

func TestAmbiguousColumnJoin(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	// this query only works if we know which table the testId belongs to. The vschema does not contain
	// this info, so we are testing that the schema tracker has added column info to the vschema
	_, err = conn.ExecuteFetch(`select testId from t8 join t2`, 1000, true)
	require.NoError(t, err)
}

func TestInitAndUpdate(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		`[[VARCHAR("t2")] [VARCHAR("t2_id4_idx")] [VARCHAR("t8")]]`,
		100*time.Millisecond,
		3*time.Second,
		"initial table list not complete")

	// Init
	_ = utils.Exec(t, conn, "create table test_sc (id bigint primary key)")
	utils.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		`[[VARCHAR("t2")] [VARCHAR("t2_id4_idx")] [VARCHAR("t8")] [VARCHAR("test_sc")]]`,
		100*time.Millisecond,
		3*time.Second,
		"test_sc not in vschema tables")

	// Tables Update via health check.
	_ = utils.Exec(t, conn, "create table test_sc1 (id bigint primary key)")
	utils.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		`[[VARCHAR("t2")] [VARCHAR("t2_id4_idx")] [VARCHAR("t8")] [VARCHAR("test_sc")] [VARCHAR("test_sc1")]]`,
		100*time.Millisecond,
		3*time.Second,
		"test_sc1 not in vschema tables")

	_ = utils.Exec(t, conn, "drop table test_sc, test_sc1")
	utils.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		`[[VARCHAR("t2")] [VARCHAR("t2_id4_idx")] [VARCHAR("t8")]]`,
		100*time.Millisecond,
		3*time.Second,
		"test_sc and test_sc_1 should not be in vschema tables")

}

func TestDMLOnNewTable(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// create a new table which is not part of the VSchema
	utils.Exec(t, conn, `create table new_table_tracked(id bigint, name varchar(100), primary key(id)) Engine=InnoDB`)

	// wait for vttablet's schema reload interval to pass
	utils.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		`[[VARCHAR("new_table_tracked")] [VARCHAR("t2")] [VARCHAR("t2_id4_idx")] [VARCHAR("t8")]]`,
		100*time.Millisecond,
		3*time.Second,
		"test_sc not in vschema tables")

	utils.AssertMatches(t, conn, "select id from new_table_tracked", `[]`)              // select
	utils.AssertMatches(t, conn, "select id from new_table_tracked where id = 5", `[]`) // select
	// DML on new table
	// insert initial data ,update and delete will fail since we have not added a primary vindex
	errorMessage := "table 'new_table_tracked' does not have a primary vindex (errno 1173) (sqlstate 42000)"
	utils.AssertContainsError(t, conn, `insert into new_table_tracked(id) values(0),(1)`, errorMessage)
	utils.AssertContainsError(t, conn, `update new_table_tracked set name = "newName1"`, errorMessage)
	utils.AssertContainsError(t, conn, "delete from new_table_tracked", errorMessage)

	utils.Exec(t, conn, `select name from new_table_tracked join t8`)

	// add a primary vindex for the table
	utils.Exec(t, conn, "alter vschema on ks.new_table_tracked add vindex hash(id) using hash")
	time.Sleep(1 * time.Second)
	utils.Exec(t, conn, `insert into new_table_tracked(id) values(0),(1)`)
	utils.Exec(t, conn, `insert into t8(id8) values(2)`)
	defer utils.Exec(t, conn, `delete from t8`)
	utils.AssertMatchesNoOrder(t, conn, `select id from new_table_tracked join t8`, `[[INT64(0)] [INT64(1)]]`)
}

// TestNewView validates that view tracking works as expected.
func TestNewView(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 16, "vtgate")
	utils.SkipIfBinaryIsBelowVersion(t, 16, "vttablet")

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// insert some data
	_ = utils.Exec(t, conn, "insert into t2 (id3, id4) values (1, 10), (2, 20), (3, 30)")
	defer utils.Exec(t, conn, "delete from t2")

	selQuery := "select sum(id4) from t2 where id4 > 10"

	// create a view
	_ = utils.Exec(t, conn, "create view test_view as "+selQuery)

	// executing the query directly
	qr := utils.Exec(t, conn, selQuery)
	// selecting it through the view.
	utils.AssertMatchesWithTimeout(t, conn, "select * from test_view", fmt.Sprintf("%v", qr.Rows), 100*time.Millisecond, 10*time.Second, "test_view not in vschema tables")
}

// TestViewAndTable validates that new column added in table is present in the view definition
func TestViewAndTable(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 16, "vtgate")
	utils.SkipIfBinaryIsBelowVersion(t, 16, "vttablet")

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// add a new column to the table t8
	_ = utils.Exec(t, conn, "alter table t8 add column new_col varchar(50)")
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, KeyspaceName, "t8", "new_col")
	require.NoError(t, err)

	// insert some data
	_ = utils.Exec(t, conn, "insert into t8(id8, new_col) values (1, 'V')")
	defer utils.Exec(t, conn, "delete from t8")

	// create a view with t8, having the new column.
	_ = utils.Exec(t, conn, "create view t8_view as select * from t8")

	// executing the view query, with the new column in the select field.
	utils.AssertMatchesWithTimeout(t, conn, "select new_col from t8_view", `[[VARCHAR("V")]]`, 100*time.Millisecond, 5*time.Second, "t8_view not in vschema tables")

	// add another column to the table t8
	_ = utils.Exec(t, conn, "alter table t8 add column additional_col bigint")
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, KeyspaceName, "t8", "additional_col")
	require.NoError(t, err)

	// executing the query on view
	qr := utils.Exec(t, conn, "select * from t8_view")
	// validate that field name should not have additional_col
	assert.NotContains(t, fmt.Sprintf("%v", qr.Fields), "additional_col")
}
