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

package singleton

import (
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/schema"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	shards          []cluster.Shard
	vtParams        mysql.ConnParams

	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	tableName             = `onlineddl_test`
	createTableWrapper    = `CREATE TABLE onlineddl_test(%s)`
	dropTableStatement    = `
		DROP TABLE onlineddl_test
	`
	ddlStrategy = "online -declarative -allow-zero-in-date"
)

type testCase struct {
	name       string
	fromSchema string
	toSchema   string
	// expectProblems              bool
	removedUniqueKeyNames       string
	droppedNoDefaultColumnNames string
	expandedColumnNames         string
}

var testCases = []testCase{
	{
		name:       "identical schemas",
		fromSchema: `id int primary key, i1 int not null default 0`,
		toSchema:   `id int primary key, i2 int not null default 0`,
	},
	{
		name:       "different schemas, nothing to note",
		fromSchema: `id int primary key, i1 int not null default 0, unique key i1_uidx(i1)`,
		toSchema:   `id int primary key, i1 int not null default 0, i2 int not null default 0, unique key i1_uidx(i1)`,
	},
	{
		name:                  "removed non-nullable unique key",
		fromSchema:            `id int primary key, i1 int not null default 0, unique key i1_uidx(i1)`,
		toSchema:              `id int primary key, i2 int not null default 0`,
		removedUniqueKeyNames: `i1_uidx`,
	},
	{
		name:                  "removed nullable unique key",
		fromSchema:            `id int primary key, i1 int default null, unique key i1_uidx(i1)`,
		toSchema:              `id int primary key, i2 int default null`,
		removedUniqueKeyNames: `i1_uidx`,
	},
	{
		name:                  "expanding unique key removes unique constraint",
		fromSchema:            `id int primary key, i1 int default null, unique key i1_uidx(i1)`,
		toSchema:              `id int primary key, i1 int default null, unique key i1_uidx(i1, id)`,
		removedUniqueKeyNames: `i1_uidx`,
	},
	{
		name:                  "reducing unique key does not remove unique constraint",
		fromSchema:            `id int primary key, i1 int default null, unique key i1_uidx(i1, id)`,
		toSchema:              `id int primary key, i1 int default null, unique key i1_uidx(i1)`,
		removedUniqueKeyNames: ``,
	},
	{
		name:                        "remove column without default",
		fromSchema:                  `id int primary key, i1 int not null`,
		toSchema:                    `id int primary key, i2 int not null default 0`,
		droppedNoDefaultColumnNames: `i1`,
	},
	{
		name:                "expanded: nullable",
		fromSchema:          `id int primary key, i1 int not null, i2 int default null`,
		toSchema:            `id int primary key, i1 int default null, i2 int not null`,
		expandedColumnNames: `i1`,
	},
	{
		name:                "expanded: longer text",
		fromSchema:          `id int primary key, i1 int default null, v1 varchar(40) not null, v2 varchar(5), v3 varchar(3)`,
		toSchema:            `id int primary key, i1 int not null, v1 varchar(100) not null, v2 char(3), v3 char(5)`,
		expandedColumnNames: `v1,v3`,
	},
	{
		name:                "expanded: int numeric precision and scale",
		fromSchema:          `id int primary key, i1 int, i2 tinyint, i3 mediumint, i4 bigint`,
		toSchema:            `id int primary key, i1 int, i2 mediumint, i3 int, i4 tinyint`,
		expandedColumnNames: `i2,i3`,
	},
	{
		name:                "expanded: floating point",
		fromSchema:          `id int primary key, i1 int, n2 bigint, n3 bigint, n4 float, n5 double`,
		toSchema:            `id int primary key, i1 int, n2 float, n3 double, n4 double, n5 float`,
		expandedColumnNames: `n2,n3,n4`,
	},
	{
		name:                "expanded: decimal numeric precision and scale",
		fromSchema:          `id int primary key, i1 int, d1 decimal(10,2), d2 decimal (10,2), d3 decimal (10,2)`,
		toSchema:            `id int primary key, i1 int, d1 decimal(11,2), d2 decimal (9,1), d3 decimal (10,3)`,
		expandedColumnNames: `d1,d3`,
	},
	{
		name:                "expanded: signed, unsigned",
		fromSchema:          `id int primary key, i1 bigint signed, i2 int unsigned, i3 bigint unsigned`,
		toSchema:            `id int primary key, i1 int signed, i2 int signed, i3 int signed`,
		expandedColumnNames: `i2,i3`,
	},
	{
		name:                "expanded: signed, unsigned: range",
		fromSchema:          `id int primary key, i1 int signed, i2 bigint signed, i3 int signed`,
		toSchema:            `id int primary key, i1 int unsigned, i2 int unsigned, i3 bigint unsigned`,
		expandedColumnNames: `i1,i3`,
	},
	{
		name:                "expanded: datetime precision",
		fromSchema:          `id int primary key, dt1 datetime, ts1 timestamp, ti1 time, dt2 datetime(3), dt3 datetime(6), ts2 timestamp(3)`,
		toSchema:            `id int primary key, dt1 datetime(3), ts1 timestamp(6), ti1 time(3), dt2 datetime(6), dt3 datetime(3), ts2 timestamp`,
		expandedColumnNames: `dt1,ts1,ti1,dt2`,
	},
	{
		name:                "expanded: strange data type changes",
		fromSchema:          `id int primary key, dt1 datetime, ts1 timestamp, i1 int, d1 date, e1 enum('a', 'b')`,
		toSchema:            `id int primary key, dt1 char(32), ts1 varchar(32), i1 tinytext, d1 char(2), e1 varchar(2)`,
		expandedColumnNames: `dt1,ts1,i1,d1,e1`,
	},
	{
		name:                "expanded: temporal types",
		fromSchema:          `id int primary key, t1 time, t2 timestamp, t3 date, t4 datetime, t5 time, t6 date`,
		toSchema:            `id int primary key, t1 datetime, t2 datetime, t3 timestamp, t4 timestamp, t5 timestamp, t6 datetime`,
		expandedColumnNames: `t1,t2,t3,t5,t6`,
	},
	{
		name:                "expanded: character sets",
		fromSchema:          `id int primary key, c1 char(3) charset utf8, c2 char(3) charset utf8mb4, c3 char(3) charset ascii, c4 char(3) charset utf8mb4, c5 char(3) charset utf8, c6 char(3) charset latin1`,
		toSchema:            `id int primary key, c1 char(3) charset utf8mb4, c2 char(3) charset utf8, c3 char(3) charset utf8, c4 char(3) charset ascii, c5 char(3) charset utf8, c6 char(3) charset utf8mb4`,
		expandedColumnNames: `c1,c3,c6`,
	},
	{
		name:                "expanded: enum",
		fromSchema:          `id int primary key, e1 enum('a', 'b'), e2 enum('a', 'b'), e3 enum('a', 'b'), e4 enum('a', 'b'), e5 enum('a', 'b'), e6 enum('a', 'b'), e7 enum('a', 'b'), e8 enum('a', 'b')`,
		toSchema:            `id int primary key, e1 enum('a', 'b'), e2 enum('a'), e3 enum('a', 'b', 'c'), e4 enum('a', 'x'), e5 enum('a', 'x', 'b'), e6 enum('b'), e7 varchar(1), e8 tinyint`,
		expandedColumnNames: `e3,e4,e5,e6,e7,e8`,
	},
	{
		name:                "expanded: set",
		fromSchema:          `id int primary key, e1 set('a', 'b'), e2 set('a', 'b'), e3 set('a', 'b'), e4 set('a', 'b'), e5 set('a', 'b'), e6 set('a', 'b'), e7 set('a', 'b'), e8 set('a', 'b')`,
		toSchema:            `id int primary key, e1 set('a', 'b'), e2 set('a'), e3 set('a', 'b', 'c'), e4 set('a', 'x'), e5 set('a', 'x', 'b'), e6 set('b'), e7 varchar(1), e8 tinyint`,
		expandedColumnNames: `e3,e4,e5,e6,e7,e8`,
	},
}

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		schemaChangeDirectory = path.Join("/tmp", fmt.Sprintf("schema_change_dir_%d", clusterInstance.GetAndReserveTabletUID()))
		defer os.RemoveAll(schemaChangeDirectory)
		defer clusterInstance.Teardown()

		if _, err := os.Stat(schemaChangeDirectory); os.IsNotExist(err) {
			_ = os.Mkdir(schemaChangeDirectory, 0700)
		}

		clusterInstance.VtctldExtraArgs = []string{
			"--schema_change_dir", schemaChangeDirectory,
			"--schema_change_controller", "local",
			"--schema_change_check_interval", "1"}

		clusterInstance.VtTabletExtraArgs = []string{
			"--enable-lag-throttler",
			"--throttle_threshold", "1s",
			"--heartbeat_enable",
			"--heartbeat_interval", "250ms",
			"--heartbeat_on_demand_duration", "5s",
			"--watch_replication_stream",
		}
		clusterInstance.VtGateExtraArgs = []string{}

		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name: keyspaceName,
		}

		// No need for replicas in this stress test
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"1"}, 0, false); err != nil {
			return 1, err
		}

		vtgateInstance := clusterInstance.NewVtgateInstance()
		// Start vtgate
		if err := vtgateInstance.Setup(); err != nil {
			return 1, err
		}
		// ensure it is torn down during cluster TearDown
		clusterInstance.VtgateProcess = *vtgateInstance
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}

func removeBackticks(s string) string {
	return strings.Replace(s, "`", "", -1)
}

func TestSchemaChange(t *testing.T) {
	defer cluster.PanicHandler(t)
	shards = clusterInstance.Keyspaces[0].Shards
	require.Equal(t, 1, len(shards))

	for _, testcase := range testCases {
		t.Run(testcase.name, func(t *testing.T) {

			t.Run("ensure table dropped", func(t *testing.T) {
				uuid := testOnlineDDLStatement(t, dropTableStatement, ddlStrategy, "vtgate", "", "", false)
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
				checkTable(t, tableName, false)
			})

			t.Run("create from-table", func(t *testing.T) {
				fromStatement := fmt.Sprintf(createTableWrapper, testcase.fromSchema)
				uuid := testOnlineDDLStatement(t, fromStatement, ddlStrategy, "vtgate", "", "", false)
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
				checkTable(t, tableName, true)
			})
			var uuid string
			t.Run("run migration", func(t *testing.T) {
				toStatement := fmt.Sprintf(createTableWrapper, testcase.toSchema)
				uuid = testOnlineDDLStatement(t, toStatement, ddlStrategy, "vtgate", "", "", false)
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
				checkTable(t, tableName, true)
			})
			t.Run("check migration", func(t *testing.T) {
				rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
				require.NotNil(t, rs)
				for _, row := range rs.Named().Rows {
					removedUniqueKeyNames := row.AsString("removed_unique_key_names", "")
					droppedNoDefaultColumnNames := row.AsString("dropped_no_default_column_names", "")
					expandedColumnNames := row.AsString("expanded_column_names", "")

					assert.Equal(t, testcase.removedUniqueKeyNames, removeBackticks(removedUniqueKeyNames))
					assert.Equal(t, testcase.droppedNoDefaultColumnNames, removeBackticks(droppedNoDefaultColumnNames))
					assert.Equal(t, testcase.expandedColumnNames, removeBackticks(expandedColumnNames))
				}
			})
		})
	}
}

// testOnlineDDLStatement runs an online DDL, ALTER statement
func testOnlineDDLStatement(t *testing.T, alterStatement string, ddlStrategy string, executeStrategy string, expectHint string, expectError string, skipWait bool) (uuid string) {
	strategySetting, err := schema.ParseDDLStrategy(ddlStrategy)
	require.NoError(t, err)

	if executeStrategy == "vtgate" {
		result := onlineddl.VtgateExecDDL(t, &vtParams, ddlStrategy, alterStatement, expectError)
		if result != nil {
			row := result.Named().Row()
			if row != nil {
				uuid = row.AsString("uuid", "")
			}
		}
	} else {
		output, err := clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, alterStatement, cluster.VtctlClientParams{DDLStrategy: ddlStrategy, SkipPreflight: true})
		if expectError == "" {
			assert.NoError(t, err)
			uuid = output
		} else {
			assert.Error(t, err)
			assert.Contains(t, output, expectError)
		}
	}
	uuid = strings.TrimSpace(uuid)
	fmt.Println("# Generated UUID (for debug purposes):")
	fmt.Printf("<%s>\n", uuid)

	if !strategySetting.Strategy.IsDirect() && !skipWait {
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
	}

	if expectError == "" && expectHint != "" {
		checkMigratedTable(t, tableName, expectHint)
	}
	return uuid
}

// checkTable checks the number of tables in the first two shards.
func checkTable(t *testing.T, showTableName string, expectExists bool) bool {
	expectCount := 0
	if expectExists {
		expectCount = 1
	}
	for i := range clusterInstance.Keyspaces[0].Shards {
		if !checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], showTableName, expectCount) {
			return false
		}
	}
	return true
}

// checkTablesCount checks the number of tables in the given tablet
func checkTablesCount(t *testing.T, tablet *cluster.Vttablet, showTableName string, expectCount int) bool {
	query := fmt.Sprintf(`show tables like '%%%s%%';`, showTableName)
	queryResult, err := tablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	require.Nil(t, err)
	return assert.Equal(t, expectCount, len(queryResult.Rows))
}

// checkMigratedTables checks the CREATE STATEMENT of a table after migration
func checkMigratedTable(t *testing.T, tableName, expectHint string) {
	for i := range clusterInstance.Keyspaces[0].Shards {
		createStatement := getCreateTableStatement(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], tableName)
		assert.Contains(t, createStatement, expectHint)
	}
}

// getCreateTableStatement returns the CREATE TABLE statement for a given table
func getCreateTableStatement(t *testing.T, tablet *cluster.Vttablet, tableName string) (statement string) {
	queryResult, err := tablet.VttabletProcess.QueryTablet(fmt.Sprintf("show create table %s;", tableName), keyspaceName, true)
	require.Nil(t, err)

	assert.Equal(t, len(queryResult.Rows), 1)
	assert.Equal(t, len(queryResult.Rows[0]), 2) // table name, create statement
	statement = queryResult.Rows[0][1].ToString()
	return statement
}
