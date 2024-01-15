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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	sidecarDBName   = "_vt_schema_tracker_metadata" // custom sidecar database name for testing
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

		vtgateVer, err := cluster.GetMajorVersion("vtgate")
		if err != nil {
			return 1
		}
		vttabletVer, err := cluster.GetMajorVersion("vttablet")
		if err != nil {
			return 1
		}

		// For upgrade/downgrade tests.
		if vtgateVer < 17 || vttabletVer < 17 {
			// Then only the default sidecarDBName is supported.
			sidecarDBName = sidecar.DefaultName
		}

		// Start topo server
		err = clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:          keyspaceName,
			SchemaSQL:     sqlSchema,
			SidecarDBName: sidecarDBName,
		}
		clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal"}
		err = clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal", "--vschema_ddl_authorized_users", "%"}
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		err = clusterInstance.WaitForVTGateAndVTTablets(5 * time.Minute)
		if err != nil {
			fmt.Println(err)
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

	expected := `[[VARCHAR("main")]]`

	// ensuring our initial table "main" is in the schema
	utils.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		expected,
		100*time.Millisecond,
		30*time.Second,
		"initial table list not complete")

	// create a new table which is not part of the VSchema
	utils.Exec(t, conn, `create table new_table_tracked(id bigint, name varchar(100), primary key(id)) Engine=InnoDB`)

	expected = `[[VARCHAR("main")] [VARCHAR("new_table_tracked")]]`

	// waiting for the vttablet's schema_reload interval to kick in
	utils.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		expected,
		100*time.Millisecond,
		30*time.Second,
		"new_table_tracked not in vschema tables")

	utils.AssertMatchesWithTimeout(t, conn,
		"select id from new_table_tracked", `[]`,
		100*time.Millisecond,
		60*time.Second, // longer timeout as it's the first query after setup
		"could not query new_table_tracked through vtgate")
	utils.AssertMatchesWithTimeout(t, conn,
		"select id from new_table_tracked where id = 5", `[]`,
		100*time.Millisecond,
		30*time.Second,
		"could not query new_table_tracked through vtgate")

	// DML on new table
	// insert initial data ,update and delete for the new table
	utils.Exec(t, conn, `insert into new_table_tracked(id) values(0),(1)`)
	utils.Exec(t, conn, `update new_table_tracked set name = "newName1"`)
	utils.Exec(t, conn, "delete from new_table_tracked where id = 0")
	utils.AssertMatchesWithTimeout(t, conn,
		`select * from new_table_tracked`, `[[INT64(1) VARCHAR("newName1")]]`,
		100*time.Millisecond,
		30*time.Second,
		"could not query expected row in new_table_tracked through vtgate")

	utils.Exec(t, conn, `drop table new_table_tracked`)

	// waiting for the vttablet's schema_reload interval to kick in
	expected = `[[VARCHAR("main")]]`
	utils.AssertMatchesWithTimeout(t, conn,
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
	utils.SkipIfBinaryIsBelowVersion(t, 19, "vttablet")
	defer cluster.PanicHandler(t)

	// create a sql connection
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// ensuring our initial table "main" is in the schema
	utils.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		`[[VARCHAR("main")]]`,
		100*time.Millisecond,
		30*time.Second,
		"initial tables not found in vschema")

	// Now we create two tables with the same name differing only in casing t1 and T1.
	// For both of them we'll have different schema's and verify that we can read the data after schema tracking kicks in.
	utils.Exec(t, conn, `create table t1(id bigint, primary key(id)) Engine=InnoDB`)
	utils.Exec(t, conn, `create table T1(col bigint, col2 bigint, primary key(col)) Engine=InnoDB`)

	// Wait for schema tracking to be caught up
	utils.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		`[[VARCHAR("T1")] [VARCHAR("main")] [VARCHAR("t1")]]`,
		100*time.Millisecond,
		30*time.Second,
		"schema tracking didn't track both the tables")

	// Run DMLs
	utils.Exec(t, conn, `insert into t1(id) values(0),(1)`)
	utils.Exec(t, conn, `insert into T1(col, col2) values(0,0),(1,1)`)

	// Verify the tables are queryable
	utils.AssertMatchesWithTimeout(t, conn,
		`select * from t1`, `[[INT64(0)] [INT64(1)]]`,
		100*time.Millisecond,
		30*time.Second,
		"could not query expected rows in t1 through vtgate")
	utils.AssertMatchesWithTimeout(t, conn,
		`select * from T1`, `[[INT64(0) INT64(0)] [INT64(1) INT64(1)]]`,
		100*time.Millisecond,
		30*time.Second,
		"could not query expected rows in T1 through vtgate")
}
