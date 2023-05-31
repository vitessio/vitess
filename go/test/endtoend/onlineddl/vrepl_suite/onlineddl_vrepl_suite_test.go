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

package vreplsuite

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
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/test/endtoend/throttler"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	clusterInstance         *cluster.LocalProcessCluster
	vtParams                mysql.ConnParams
	evaluatedMysqlParams    *mysql.ConnParams
	ddlStrategy             = "vitess -vreplication-test-suite"
	waitForMigrationTimeout = 20 * time.Second

	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	tableName             = `onlineddl_test`
	beforeTableName       = `onlineddl_test_before`
	afterTableName        = `onlineddl_test_after`
	eventName             = `onlineddl_test`
)

const (
	testDataPath   = "testdata"
	defaultSQLMode = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
)

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
			"--schema_change_check_interval", "1s",
		}

		clusterInstance.VtTabletExtraArgs = []string{
			"--heartbeat_interval", "250ms",
			"--heartbeat_on_demand_duration", "5s",
			"--migration_check_interval", "5s",
			"--watch_replication_stream",
		}

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

func TestSchemaChange(t *testing.T) {
	defer cluster.PanicHandler(t)

	shards := clusterInstance.Keyspaces[0].Shards
	require.Equal(t, 1, len(shards))

	throttler.EnableLagThrottlerAndWaitForStatus(t, clusterInstance, time.Second)

	files, err := os.ReadDir(testDataPath)
	require.NoError(t, err)
	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		// this is a test!
		t.Run(f.Name(), func(t *testing.T) {
			testSingle(t, f.Name())
		})
	}
}

func readTestFile(t *testing.T, testName string, fileName string) (content string, exists bool) {
	filePath := path.Join(testDataPath, testName, fileName)
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return "", false
	}
	require.NoError(t, err)
	b, err := os.ReadFile(filePath)
	require.NoError(t, err)
	return strings.TrimSpace(string(b)), true
}

// testSingle is the main testing function for a single test in the suite.
// It prepares the grounds, creates the test data, runs a migration, expects results/error, cleans up.
func testSingle(t *testing.T, testName string) {
	if ignoreVersions, exists := readTestFile(t, testName, "ignore_versions"); exists {
		// ignoreVersions is a regexp
		re, err := regexp.Compile(ignoreVersions)
		require.NoError(t, err)

		rs := mysqlExec(t, "select @@version as ver", "")
		row := rs.Named().Row()
		require.NotNil(t, row)
		mysqlVersion := row["ver"].ToString()

		if re.MatchString(mysqlVersion) {
			t.Skipf("Skipping test due to ignore_versions=%s", ignoreVersions)
			return
		}
	}

	sqlMode := defaultSQLMode
	if overrideSQLMode, exists := readTestFile(t, testName, "sql_mode"); exists {
		sqlMode = overrideSQLMode
	}
	sqlModeQuery := fmt.Sprintf("set @@global.sql_mode='%s'", sqlMode)
	_ = mysqlExec(t, sqlModeQuery, "")
	_ = mysqlExec(t, "set @@global.event_scheduler=1", "")

	_ = mysqlExec(t, fmt.Sprintf("drop table if exists %s_child, %s, %s_parent, %s, %s;", tableName, tableName, tableName, beforeTableName, afterTableName), "")
	_ = mysqlExec(t, fmt.Sprintf("drop event if exists %s", eventName), "")

	{
		// create
		f := "create.sql"
		_, exists := readTestFile(t, testName, f)
		require.True(t, exists)
		onlineddl.MysqlClientExecFile(t, mysqlParams(), testDataPath, testName, f)
		// ensure test table has been created:
		getCreateTableStatement(t, tableName)
	}
	defer func() {
		// destroy
		f := "destroy.sql"
		if _, exists := readTestFile(t, testName, f); exists {
			onlineddl.MysqlClientExecFile(t, mysqlParams(), testDataPath, testName, f)
		}
	}()

	var expectQueryFailure string
	if content, exists := readTestFile(t, testName, "expect_query_failure"); exists {
		// VTGate failure is expected!
		expectQueryFailure = content
	}

	singleDDLStrategy := ddlStrategy
	if extra, exists := readTestFile(t, testName, "ddl_strategy"); exists {
		singleDDLStrategy = fmt.Sprintf("%s %s", singleDDLStrategy, extra)
	}

	var migrationMessage string
	var migrationStatus string
	// Run test
	alterClause := "engine=innodb"
	if content, exists := readTestFile(t, testName, "alter"); exists {
		alterClause = content
	}
	alterStatement := fmt.Sprintf("alter table %s %s", tableName, alterClause)
	// Run the DDL!
	uuid := testOnlineDDLStatement(t, alterStatement, singleDDLStrategy, expectQueryFailure)

	if expectQueryFailure != "" {
		// Nothing further to do. Migration isn't actually running
		return
	}
	assert.NotEmpty(t, uuid)

	defer func() {
		query, err := sqlparser.ParseAndBind("alter vitess_migration %a cancel",
			sqltypes.StringBindVariable(uuid),
		)
		require.NoError(t, err)
		onlineddl.VtgateExecQuery(t, &vtParams, query, "")
	}()
	row := waitForMigration(t, uuid, waitForMigrationTimeout)
	// migration is complete
	{
		migrationStatus = row["migration_status"].ToString()
		migrationMessage = row["message"].ToString()
	}

	if expectedErrorMessage, exists := readTestFile(t, testName, "expect_failure"); exists {
		// Failure is expected!
		assert.Contains(t, []string{string(schema.OnlineDDLStatusFailed), string(schema.OnlineDDLStatusCancelled)}, migrationStatus)
		require.Contains(t, migrationMessage, expectedErrorMessage, "expected error message (%s) to contain (%s)", migrationMessage, expectedErrorMessage)
		// no need to proceed to checksum or anything further
		return
	}
	// We do not expect failure.
	require.Equal(t, string(schema.OnlineDDLStatusComplete), migrationStatus, migrationMessage)

	if content, exists := readTestFile(t, testName, "expect_table_structure"); exists {
		createStatement := getCreateTableStatement(t, afterTableName)
		assert.Regexpf(t, content, createStatement, "expected SHOW CREATE TABLE to match text in 'expect_table_structure' file")
	}

	{
		// checksum
		beforeColumns := "*"
		if content, exists := readTestFile(t, testName, "before_columns"); exists {
			beforeColumns = content
		}
		afterColumns := "*"
		if content, exists := readTestFile(t, testName, "after_columns"); exists {
			afterColumns = content
		}
		orderBy := ""
		if content, exists := readTestFile(t, testName, "order_by"); exists {
			orderBy = fmt.Sprintf("order by %s", content)
		}
		selectBefore := fmt.Sprintf("select %s from %s %s", beforeColumns, beforeTableName, orderBy)
		selectAfter := fmt.Sprintf("select %s from %s %s", afterColumns, afterTableName, orderBy)

		selectBeforeFile := onlineddl.CreateTempScript(t, selectBefore)
		defer os.Remove(selectBeforeFile)
		beforeOutput := onlineddl.MysqlClientExecFile(t, mysqlParams(), testDataPath, "", selectBeforeFile)

		selectAfterFile := onlineddl.CreateTempScript(t, selectAfter)
		defer os.Remove(selectAfterFile)
		afterOutput := onlineddl.MysqlClientExecFile(t, mysqlParams(), testDataPath, "", selectAfterFile)

		require.Equal(t, beforeOutput, afterOutput, "results mismatch: (%s) and (%s)", selectBefore, selectAfter)
	}
}

// testOnlineDDLStatement runs an online DDL, ALTER statement
func testOnlineDDLStatement(t *testing.T, alterStatement string, ddlStrategy string, expectError string) (uuid string) {
	qr := onlineddl.VtgateExecDDL(t, &vtParams, ddlStrategy, alterStatement, expectError)
	if qr != nil {
		row := qr.Named().Row()
		require.NotNil(t, row)
		uuid = row.AsString("uuid", "")
	}
	uuid = strings.TrimSpace(uuid)
	return uuid
}

func readMigration(t *testing.T, uuid string) sqltypes.RowNamedValues {
	rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
	require.NotNil(t, rs)
	row := rs.Named().Row()
	require.NotNil(t, row)
	return row
}

func waitForMigration(t *testing.T, uuid string, timeout time.Duration) sqltypes.RowNamedValues {
	var status string
	sleepDuration := time.Second
	for timeout > 0 {
		row := readMigration(t, uuid)
		status = row["migration_status"].ToString()
		switch status {
		case string(schema.OnlineDDLStatusComplete), string(schema.OnlineDDLStatusFailed), string(schema.OnlineDDLStatusCancelled):
			// migration is complete, either successful or not
			return row
		}
		time.Sleep(sleepDuration)
		timeout = timeout - sleepDuration
	}
	require.NoError(t, fmt.Errorf("timeout in waitForMigration(%s). status is: %s", uuid, status))
	return nil
}

func getTablet() *cluster.Vttablet {
	return clusterInstance.Keyspaces[0].Shards[0].Vttablets[0]
}

func mysqlParams() *mysql.ConnParams {
	if evaluatedMysqlParams != nil {
		return evaluatedMysqlParams
	}
	evaluatedMysqlParams = &mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", getTablet().TabletUID), "/mysql.sock"),
		DbName:     fmt.Sprintf("vt_%s", keyspaceName),
	}
	return evaluatedMysqlParams
}

// VtgateExecDDL executes a DDL query with given strategy
func mysqlExec(t *testing.T, sql string, expectError string) *sqltypes.Result {
	t.Helper()

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, mysqlParams())
	require.Nil(t, err)
	defer conn.Close()

	qr, err := conn.ExecuteFetch(sql, 100000, true)
	if expectError == "" {
		require.NoError(t, err)
	} else {
		require.Error(t, err, "error should not be nil")
		require.Contains(t, err.Error(), expectError, "Unexpected error")
	}
	return qr
}

// getCreateTableStatement returns the CREATE TABLE statement for a given table
func getCreateTableStatement(t *testing.T, tableName string) (statement string) {
	queryResult, err := getTablet().VttabletProcess.QueryTablet(fmt.Sprintf("show create table %s", tableName), keyspaceName, true)
	require.Nil(t, err)

	assert.Equal(t, len(queryResult.Rows), 1)
	assert.Equal(t, len(queryResult.Rows[0]), 2) // table name, create statement
	statement = queryResult.Rows[0][1].ToString()
	return statement
}
