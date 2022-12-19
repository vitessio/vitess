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

package fk

import (
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/textutil"
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

	createStatements = []string{
		`
			CREATE TABLE parent_table (
					id INT NOT NULL,
					parent_hint_col INT NOT NULL DEFAULT 0,
					PRIMARY KEY (id)
			)
		`,
		`
			CREATE TABLE child_table (
				id INT NOT NULL auto_increment,
				parent_id INT,
				child_hint_col INT NOT NULL DEFAULT 0,
				PRIMARY KEY (id),
				KEY parent_id_idx (parent_id),
				CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES parent_table(id) ON DELETE CASCADE
			)
		`,
		`
			CREATE TABLE child_nofk_table (
				id INT NOT NULL auto_increment,
				parent_id INT,
				child_hint_col INT NOT NULL DEFAULT 0,
				PRIMARY KEY (id),
				KEY parent_id_idx (parent_id)
			)
		`,
	}
	insertStatements = []string{
		"insert into parent_table (id) values(43)",
		"insert into child_table (id, parent_id) values(1,43)",
		"insert into child_table (id, parent_id) values(2,43)",
		"insert into child_table (id, parent_id) values(3,43)",
		"insert into child_table (id, parent_id) values(4,43)",
	}
	ddlStrategy        = "online --allow-zero-in-date"
	ddlStrategyAllowFK = ddlStrategy + " --unsafe-allow-foreign-keys"
)

type testCase struct {
	name             string
	sql              string
	allowForeignKeys bool
	expectHint       string
}

var testCases = []testCase{
	{
		name:             "modify parent, not allowed",
		sql:              "alter table parent_table engine=innodb",
		allowForeignKeys: false,
	},
	{
		name:             "modify child, not allowed",
		sql:              "alter table child_table engine=innodb",
		allowForeignKeys: false,
	},
	{
		name:             "add foreign key to child, not allowed",
		sql:              "alter table child_table add CONSTRAINT another_fk FOREIGN KEY (parent_id) REFERENCES parent_table(id) ON DELETE CASCADE",
		allowForeignKeys: false,
	},
	{
		name:             "add foreign key to table which wasn't a child before, not allowed",
		sql:              "alter table child_nofk_table add CONSTRAINT new_fk FOREIGN KEY (parent_id) REFERENCES parent_table(id) ON DELETE CASCADE",
		allowForeignKeys: false,
	},
	{
		// on vanilla MySQL, this migration ends with the child_table referencing the old, original table, and not to the new table now called parent_table.
		// This is a fundamental foreign key limitation, see https://vitess.io/blog/2021-06-15-online-ddl-why-no-fk/
		// However, this tests is still valid in the sense that it lets us modify the parent table in the first place.
		name:             "modify parent, trivial",
		sql:              "alter table parent_table engine=innodb",
		allowForeignKeys: true,
		expectHint:       "parent_hint_col",
	},
	{
		// on vanilla MySQL, this migration ends with two tables, the original and the new child_table, both referencing parent_table. This has
		// the unwanted property of then limiting actions on the parent_table based on what rows exist or do not exist on the now stale old
		// child table.
		// This is a fundamental foreign key limitation, see https://vitess.io/blog/2021-06-15-online-ddl-why-no-fk/
		// However, this tests is still valid in the sense that it lets us modify the child table in the first place.
		// A valid use case: using FOREIGN_KEY_CHECKS=0  at all times.
		name:             "modify child, trivial",
		sql:              "alter table child_table engine=innodb",
		allowForeignKeys: true,
		expectHint:       "REFERENCES `parent_table`",
	},
	{
		// on vanilla MySQL, this migration ends with two tables, the original and the new child_table, both referencing parent_table. This has
		// the unwanted property of then limiting actions on the parent_table based on what rows exist or do not exist on the now stale old
		// child table.
		// This is a fundamental foreign key limitation, see https://vitess.io/blog/2021-06-15-online-ddl-why-no-fk/
		// However, this tests is still valid in the sense that it lets us modify the child table in the first place.
		// A valid use case: using FOREIGN_KEY_CHECKS=0  at all times.
		name:             "add foreign key to child",
		sql:              "alter table child_table add CONSTRAINT another_fk FOREIGN KEY (parent_id) REFERENCES parent_table(id) ON DELETE CASCADE",
		allowForeignKeys: true,
		expectHint:       "another_fk",
	},
	{
		name:             "add foreign key to table which wasn't a child before",
		sql:              "alter table child_nofk_table add CONSTRAINT new_fk FOREIGN KEY (parent_id) REFERENCES parent_table(id) ON DELETE CASCADE",
		allowForeignKeys: true,
		expectHint:       "new_fk",
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
			t.Run("create tables", func(t *testing.T) {
				for _, statement := range createStatements {
					t.Run(statement, func(t *testing.T) {
						uuid, _ := testOnlineDDLStatement(t, statement, ddlStrategyAllowFK, "", false)
						onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
					})
				}
			})
			t.Run("populate tables", func(t *testing.T) {
				for _, statement := range insertStatements {
					t.Run(statement, func(t *testing.T) {
						onlineddl.VtgateExecQuery(t, &vtParams, statement, "")
					})
				}
			})
			var artifacts []string
			t.Run("run migration", func(t *testing.T) {
				var uuid string
				if testcase.allowForeignKeys {
					uuid, artifacts = testOnlineDDLStatement(t, testcase.sql, ddlStrategyAllowFK, testcase.expectHint, false)
					onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
				} else {
					uuid, artifacts = testOnlineDDLStatement(t, testcase.sql, ddlStrategy, "", true)
					if uuid != "" {
						onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
					}
				}
			})
			t.Run("cleanup", func(t *testing.T) {
				artifacts = append(artifacts, "child_table", "child_nofk_table", "parent_table")
				// brute force drop all tables. In MySQL 8.0 you can do a single `DROP TABLE ... <list of all tables>`
				// which auto-resovled order. But in 5.7 you can't.
				droppedTables := map[string]bool{}
				for range artifacts {
					for _, artifact := range artifacts {
						if droppedTables[artifact] {
							continue
						}
						statement := fmt.Sprintf("DROP TABLE IF EXISTS %s", artifact)
						_, err := clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, statement, cluster.VtctlClientParams{DDLStrategy: "direct", SkipPreflight: true})
						if err == nil {
							droppedTables[artifact] = true
						}
					}
				}
				statement := fmt.Sprintf("DROP TABLE IF EXISTS %s", strings.Join(artifacts, ","))
				t.Run(statement, func(t *testing.T) {
					_, _ = testOnlineDDLStatement(t, statement, "direct", "", false)
				})
			})
		})
	}
}

// testOnlineDDLStatement runs an online DDL, ALTER statement
func testOnlineDDLStatement(t *testing.T, sql string, ddlStrategy string, expectHint string, expectError bool) (uuid string, artifacts []string) {
	strategySetting, err := schema.ParseDDLStrategy(ddlStrategy)
	require.NoError(t, err)

	output, err := clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, sql, cluster.VtctlClientParams{DDLStrategy: ddlStrategy, SkipPreflight: true})
	if expectError && err != nil {
		return
	}
	assert.NoError(t, err)

	uuid = output
	uuid = strings.TrimSpace(uuid)
	fmt.Println("# Generated UUID (for debug purposes):")
	fmt.Printf("<%s>\n", uuid)

	if !strategySetting.Strategy.IsDirect() {
		if !expectError {
			require.NotEmpty(t, uuid)
		}
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
	}

	if uuid == "" {
		return
	}

	rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
	require.NotNil(t, rs)
	row := rs.Named().Row()
	require.NotNil(t, row)

	if expectHint != "" {
		tableName := row.AsString("mysql_table", "")
		checkMigratedTable(t, tableName, expectHint)
	}
	artifacts = textutil.SplitDelimitedList(row.AsString("artifacts", ""))
	return uuid, artifacts
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
