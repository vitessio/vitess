/*
Copyright 2019 The Vitess Authors.

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

package onlineddl

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/schema"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	clusterInstance       *cluster.LocalProcessCluster
	vtParams              mysql.ConnParams
	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	totalTableCount       = 4
	ddlStrategyUnchanged  = "-"
	createTable           = `
		CREATE TABLE %s (
		id BIGINT(20) not NULL,
		msg varchar(64),
		PRIMARY KEY (id)
		) ENGINE=InnoDB;`
	// To verify non online-DDL behavior
	alterTableNormalStatement = `
		ALTER TABLE %s
		ADD COLUMN non_online INT UNSIGNED NOT NULL`
	// A trivial statement which must succeed and does not change the schema
	alterTableTrivialStatement = `
		ALTER TABLE %s
		ENGINE=InnoDB`
	// The following statement is valid
	alterTableSuccessfulStatement = `
		ALTER TABLE %s
		MODIFY id BIGINT UNSIGNED NOT NULL,
		ADD COLUMN ghost_col INT NOT NULL,
		ADD INDEX idx_msg(msg)`
	// The following statement will fail because gh-ost requires some shared unique key
	alterTableFailedStatement = `
		ALTER TABLE %s
		DROP PRIMARY KEY,
		DROP COLUMN ghost_col`
	// We will run this query with "gh-ost --max-load=Threads_running=1"
	alterTableThrottlingStatement = `
		ALTER TABLE %s
		DROP COLUMN ghost_col`
)

func fullWordUUIDRegexp(uuid, searchWord string) *regexp.Regexp {
	return regexp.MustCompile(uuid + `.*?\b` + searchWord + `\b`)
}
func fullWordRegexp(searchWord string) *regexp.Regexp {
	return regexp.MustCompile(`.*?\b` + searchWord + `\b`)
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
			"-schema_change_dir", schemaChangeDirectory,
			"-schema_change_controller", "local",
			"-schema_change_check_interval", "1"}

		clusterInstance.VtTabletExtraArgs = []string{
			"-migration_check_interval", "5s",
		}
		clusterInstance.VtGateExtraArgs = []string{
			"-ddl_strategy", "gh-ost",
		}

		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name: keyspaceName,
		}

		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 2, true); err != nil {
			return 1, err
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"1"}, 1, false); err != nil {
			return 1, err
		}

		vtgateInstance := clusterInstance.NewVtgateInstance()
		// set the gateway we want to use
		vtgateInstance.GatewayImplementation = "tabletgateway"
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

func TestSchemaChange(t *testing.T) {
	defer cluster.PanicHandler(t)
	assert.Equal(t, 2, len(clusterInstance.Keyspaces[0].Shards))
	testWithInitialSchema(t)
	{
		_ = testAlterTable(t, alterTableNormalStatement, string(schema.DDLStrategyNormal), "vtctl", "non_online")
	}
	{
		uuid := testAlterTable(t, alterTableSuccessfulStatement, ddlStrategyUnchanged, "vtgate", "ghost_col")
		checkRecentMigrations(t, uuid, schema.OnlineDDLStatusComplete)
		checkCancelMigration(t, uuid, false)
		checkRetryMigration(t, uuid, false)
	}
	{
		uuid := testAlterTable(t, alterTableTrivialStatement, "gh-ost", "vtctl", "ghost_col")
		checkRecentMigrations(t, uuid, schema.OnlineDDLStatusComplete)
		checkCancelMigration(t, uuid, false)
		checkRetryMigration(t, uuid, false)
	}
	{
		uuid := testAlterTable(t, alterTableThrottlingStatement, "gh-ost --max-load=Threads_running=1", "vtgate", "ghost_col")
		checkRecentMigrations(t, uuid, schema.OnlineDDLStatusRunning)
		checkCancelMigration(t, uuid, true)
		time.Sleep(2 * time.Second)
		checkRecentMigrations(t, uuid, schema.OnlineDDLStatusFailed)
	}
	{
		uuid := testAlterTable(t, alterTableFailedStatement, "gh-ost", "vtgate", "ghost_col")
		checkRecentMigrations(t, uuid, schema.OnlineDDLStatusFailed)
		checkCancelMigration(t, uuid, false)
		checkRetryMigration(t, uuid, true)
	}
}

func testWithInitialSchema(t *testing.T) {
	// Create 4 tables
	var sqlQuery = "" //nolint
	for i := 0; i < totalTableCount; i++ {
		sqlQuery = fmt.Sprintf(createTable, fmt.Sprintf("vt_onlineddl_test_%02d", i))
		err := clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, sqlQuery)
		require.Nil(t, err)
	}

	// Check if 4 tables are created
	checkTables(t, totalTableCount)
}

// testAlterTable runs an online DDL, ALTER statement
func testAlterTable(t *testing.T, alterStatement string, ddlStrategy string, executeStrategy string, expectColumn string) (uuid string) {
	tableName := fmt.Sprintf("vt_onlineddl_test_%02d", 3)
	sqlQuery := fmt.Sprintf(alterStatement, tableName)
	if executeStrategy == "vtgate" {
		row := vtgateExec(t, ddlStrategy, sqlQuery, "").Named().Row()
		if row != nil {
			uuid = row.AsString("uuid", "")
		}
	} else {
		var err error
		ddlStrategyArg := ""
		if ddlStrategy != ddlStrategyUnchanged {
			ddlStrategyArg = ddlStrategy
		}
		uuid, err = clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, sqlQuery, ddlStrategyArg)
		assert.NoError(t, err)
	}
	uuid = strings.TrimSpace(uuid)
	fmt.Println("# Generated UUID (for debug purposes):")
	fmt.Printf("<%s>\n", uuid)

	if ddlStrategy != string(schema.DDLStrategyNormal) {
		time.Sleep(time.Second * 20)
	}

	checkMigratedTable(t, tableName, expectColumn)
	return uuid
}

// checkTables checks the number of tables in the first two shards.
func checkTables(t *testing.T, count int) {
	for i := range clusterInstance.Keyspaces[0].Shards {
		checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], count)
	}
}

// checkTablesCount checks the number of tables in the given tablet
func checkTablesCount(t *testing.T, tablet *cluster.Vttablet, count int) {
	queryResult, err := tablet.VttabletProcess.QueryTablet("show tables;", keyspaceName, true)
	require.Nil(t, err)
	assert.Equal(t, len(queryResult.Rows), count)
}

// checkRecentMigrations checks 'OnlineDDL <keyspace> show recent' output. Example to such output:
// +------------------+-------+--------------+----------------------+--------------------------------------+----------+---------------------+---------------------+------------------+
// |      Tablet      | shard | mysql_schema |     mysql_table      |            migration_uuid            | strategy |  started_timestamp  | completed_timestamp | migration_status |
// +------------------+-------+--------------+----------------------+--------------------------------------+----------+---------------------+---------------------+------------------+
// | zone1-0000003880 |     0 | vt_ks        | vt_onlineddl_test_03 | a0638f6b_ec7b_11ea_9bf8_000d3a9b8a9a | gh-ost   | 2020-09-01 17:50:40 | 2020-09-01 17:50:41 | complete         |
// | zone1-0000003884 |     1 | vt_ks        | vt_onlineddl_test_03 | a0638f6b_ec7b_11ea_9bf8_000d3a9b8a9a | gh-ost   | 2020-09-01 17:50:40 | 2020-09-01 17:50:41 | complete         |
// +------------------+-------+--------------+----------------------+--------------------------------------+----------+---------------------+---------------------+------------------+

func checkRecentMigrations(t *testing.T, uuid string, expectStatus schema.OnlineDDLStatus) {
	result, err := clusterInstance.VtctlclientProcess.OnlineDDLShowRecent(keyspaceName)
	assert.NoError(t, err)
	fmt.Println("# 'vtctlclient OnlineDDL show recent' output (for debug purposes):")
	fmt.Println(result)
	assert.Equal(t, len(clusterInstance.Keyspaces[0].Shards), strings.Count(result, uuid))
	// We ensure "full word" regexp becuase some column names may conflict
	expectStatusRegexp := fullWordUUIDRegexp(uuid, string(expectStatus))
	m := expectStatusRegexp.FindAllString(result, -1)
	assert.Equal(t, len(clusterInstance.Keyspaces[0].Shards), len(m))
}

// checkCancelMigration attempts to cancel a migration, and expects rejection
func checkCancelMigration(t *testing.T, uuid string, expectCancelPossible bool) {
	result, err := clusterInstance.VtctlclientProcess.OnlineDDLCancelMigration(keyspaceName, uuid)
	fmt.Println("# 'vtctlclient OnlineDDL cancel <uuid>' output (for debug purposes):")
	fmt.Println(result)
	assert.NoError(t, err)

	var r *regexp.Regexp
	if expectCancelPossible {
		r = fullWordRegexp("1")
	} else {
		r = fullWordRegexp("0")
	}
	m := r.FindAllString(result, -1)
	assert.Equal(t, len(clusterInstance.Keyspaces[0].Shards), len(m))
}

// checkRetryMigration attempts to retry a migration, and expects rejection
func checkRetryMigration(t *testing.T, uuid string, expectRetryPossible bool) {
	result, err := clusterInstance.VtctlclientProcess.OnlineDDLRetryMigration(keyspaceName, uuid)
	fmt.Println("# 'vtctlclient OnlineDDL retry <uuid>' output (for debug purposes):")
	fmt.Println(result)
	assert.NoError(t, err)

	var r *regexp.Regexp
	if expectRetryPossible {
		r = fullWordRegexp("1")
	} else {
		r = fullWordRegexp("0")
	}
	m := r.FindAllString(result, -1)
	assert.Equal(t, len(clusterInstance.Keyspaces[0].Shards), len(m))
}

// checkMigratedTables checks the CREATE STATEMENT of a table after migration
func checkMigratedTable(t *testing.T, tableName, expectColumn string) {
	for i := range clusterInstance.Keyspaces[0].Shards {
		createStatement := getCreateTableStatement(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], tableName)
		assert.Contains(t, createStatement, expectColumn)
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

func vtgateExec(t *testing.T, ddlStrategy string, query string, expectError string) *sqltypes.Result {
	t.Helper()

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	if ddlStrategy != ddlStrategyUnchanged {
		setSession := fmt.Sprintf("set @@ddl_strategy='%s'", ddlStrategy)
		_, err := conn.ExecuteFetch(setSession, 1000, true)
		assert.NoError(t, err)
	}
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if expectError == "" {
		require.NoError(t, err)
	} else {
		require.Error(t, err, "error should not be nil")
		assert.Contains(t, err.Error(), expectError, "Unexpected error")
	}
	return qr
}
