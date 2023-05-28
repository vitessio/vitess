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

	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/sidecardb"

	"github.com/stretchr/testify/require"

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
			sidecarDBName = sidecardb.DefaultName
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
		clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal", "--queryserver-config-schema-change-signal-interval", "0.1"}
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

	vtgateVersion, err := cluster.GetMajorVersion("vtgate")
	require.NoError(t, err)
	expected := `[[VARCHAR("dual")] [VARCHAR("main")]]`
	if vtgateVersion >= 17 {
		expected = `[[VARCHAR("main")]]`
	}

	// ensuring our initial table "main" is in the schema
	utils.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		expected,
		100*time.Millisecond,
		30*time.Second,
		"initial table list not complete")

	// create a new table which is not part of the VSchema
	utils.Exec(t, conn, `create table new_table_tracked(id bigint, name varchar(100), primary key(id)) Engine=InnoDB`)

	expected = `[[VARCHAR("dual")] [VARCHAR("main")] [VARCHAR("new_table_tracked")]]`
	if vtgateVersion >= 17 {
		expected = `[[VARCHAR("main")] [VARCHAR("new_table_tracked")]]`
	}

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
	expected = `[[VARCHAR("dual")] [VARCHAR("main")]]`
	if vtgateVersion >= 17 {
		expected = `[[VARCHAR("main")]]`
	}
	utils.AssertMatchesWithTimeout(t, conn,
		"SHOW VSCHEMA TABLES",
		expected,
		100*time.Millisecond,
		30*time.Second,
		"new_table_tracked not in vschema tables")
}
