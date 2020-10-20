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
	"flag"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/schema"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance       *cluster.LocalProcessCluster
	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	totalTableCount       = 4
	createTable           = `
		CREATE TABLE %s (
		id BIGINT(20) not NULL,
		msg varchar(64),
		PRIMARY KEY (id)
		) ENGINE=InnoDB;`
	// The following statement is valid
	alterTableSuccessfulStatament = `
		ALTER WITH 'gh-ost' TABLE %s
		MODIFY id BIGINT UNSIGNED NOT NULL,
		ADD COLUMN ghost_col INT NOT NULL,
		ADD INDEX idx_msg(msg)`
	// The following statement will fail because gh-ost requires some shared unique key
	alterTableFailedStatament = `
		ALTER WITH 'gh-ost' TABLE %s
		DROP PRIMARY KEY,
		DROP COLUMN ghost_col`
	alterTableThrottlingStatament = `
		ALTER WITH 'gh-ost' '--max-load=Threads_running=1' TABLE %s
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
		uuid := testAlterTable(t, alterTableSuccessfulStatament)
		checkRecentMigrations(t, uuid, schema.OnlineDDLStatusComplete)
		checkCancelMigration(t, uuid, false)
		checkRetryMigration(t, uuid, false)
	}
	{
		uuid := testAlterTable(t, alterTableThrottlingStatament)
		checkRecentMigrations(t, uuid, schema.OnlineDDLStatusRunning)
		checkCancelMigration(t, uuid, true)
		time.Sleep(2 * time.Second)
		checkRecentMigrations(t, uuid, schema.OnlineDDLStatusFailed)
	}
	{
		uuid := testAlterTable(t, alterTableFailedStatament)
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
func testAlterTable(t *testing.T, alterStatement string) (uuid string) {
	tableName := fmt.Sprintf("vt_onlineddl_test_%02d", 3)
	sqlQuery := fmt.Sprintf(alterStatement, tableName)
	uuid, err := clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, sqlQuery)
	require.Nil(t, err)
	uuid = strings.TrimSpace(uuid)
	require.NotEmpty(t, uuid)
	// Migration is asynchronous. Give it some time.
	time.Sleep(time.Second * 20)
	checkMigratedTable(t, tableName)
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
func checkMigratedTable(t *testing.T, tableName string) {
	expect := "ghost_col"
	for i := range clusterInstance.Keyspaces[0].Shards {
		createStatement := getCreateTableStatement(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], tableName)
		assert.Contains(t, createStatement, expect)
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
