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
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/config"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	clusterInstance         *vitesst.Cluster
	primaryTablet           *vitesst.Tablet
	vtParams                mysql.ConnParams
	ddlStrategy             = "vitess -vreplication-test-suite"
	waitForMigrationTimeout = 20 * time.Second

	keyspaceName          = "ks"
	schemaChangeDirectory = "/vt/files"
	tableName             = `onlineddl_test`
	beforeTableName       = `onlineddl_test_before`
	afterTableName        = `onlineddl_test_after`
	eventName             = `onlineddl_test`

	testsFilter = ""
)

const (
	testDataPath           = "testdata"
	testFilterEnvVar       = "ONLINEDDL_SUITE_TEST_FILTER"
	throttlerConfigTimeout = 60 * time.Second
)

// Use $VREPL_SUITE_TEST_FILTER environment variable to filter tests by name.
func setup(t *testing.T) {
	t.Helper()
	testsFilter = os.Getenv(testFilterEnvVar)
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithVTCtldArgs(
			"--schema-change-dir", schemaChangeDirectory,
			"--schema-change-controller", "local",
			"--schema-change-check-interval", "1s",
		),
		vitesst.WithVTTabletArgs(
			"--heartbeat-interval", "250ms",
			"--heartbeat-on-demand-duration", "5s",
			"--migration-check-interval", "5s",
		),
		// No need for replicas in this stress test
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames("1"),
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

	clusterInstance = cluster
	primaryTablet = cluster.Keyspace(keyspaceName).Shards()[0].Primary()
	vtParams = cluster.VTParams(ctx, "")
}

func TestVreplSuiteSchemaChanges(t *testing.T) {
	setup(t)
	shards := clusterInstance.Keyspace(keyspaceName).Shards()
	require.Equal(t, 1, len(shards))

	enableLagThrottlerAndWaitForStatus(t)

	fkOnlineDDLPossible := false
	t.Run("check 'rename_table_preserve_foreign_key' variable", func(t *testing.T) {
		// Online DDL is not possible on vanilla MySQL 8.0 for reasons described in https://vitess.io/blog/2021-06-15-online-ddl-why-no-fk/.
		// However, Online DDL is made possible in via these changes:
		// - https://github.com/planetscale/mysql-server/commit/bb777e3e86387571c044fb4a2beb4f8c60462ced
		// - https://github.com/planetscale/mysql-server/commit/c2f1344a6863518d749f2eb01a4c74ca08a5b889
		// as part of https://github.com/planetscale/mysql-server/releases/tag/8.0.34-ps3.
		// Said changes introduce a new global/session boolean variable named 'rename_table_preserve_foreign_key'. It defaults 'false'/0 for backwards compatibility.
		// When enabled, a `RENAME TABLE` to a FK parent "pins" the children's foreign keys to the table name rather than the table pointer. Which means after the RENAME,
		// the children will point to the newly instated table rather than the original, renamed table.
		// (Note: this applies to a particular type of RENAME where we swap tables, see the above blog post).
		// For FK children, the MySQL changes simply ignore any Vitess-internal table.
		//
		// In this stress test, we enable Online DDL if the variable 'rename_table_preserve_foreign_key' is present. The Online DDL mechanism will in turn
		// query for this variable, and manipulate it, when starting the migration and when cutting over.
		rs, err := shards[0].Primary().QueryTabletWithDB(t.Context(), "show global variables like 'rename_table_preserve_foreign_key'", "")
		require.NoError(t, err)
		fkOnlineDDLPossible = len(rs.Rows) > 0
		t.Logf("MySQL support for 'rename_table_preserve_foreign_key': %v", fkOnlineDDLPossible)
	})

	files, err := os.ReadDir(testDataPath)
	require.NoError(t, err)
	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		// this is a test!
		t.Run(f.Name(), func(t *testing.T) {
			testSingle(t, f.Name(), fkOnlineDDLPossible)
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
func testSingle(t *testing.T, testName string, fkOnlineDDLPossible bool) {
	if !strings.Contains(testName, testsFilter) {
		t.Skipf("Skipping test %s due to filter: %s=%s", testName, testFilterEnvVar, testsFilter)
		return
	}
	if _, exists := readTestFile(t, testName, "require_rename_table_preserve_foreign_key"); exists {
		if !fkOnlineDDLPossible {
			t.Skipf("Skipping test due to require_rename_table_preserve_foreign_key")
			return
		}
	}

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

	sqlMode := config.DefaultSQLMode
	if overrideSQLMode, exists := readTestFile(t, testName, "sql_mode"); exists {
		sqlMode = overrideSQLMode
	}
	sqlModeQuery := fmt.Sprintf("set @@global.sql_mode='%s'", sqlMode)
	_ = mysqlExec(t, sqlModeQuery, "")
	_ = mysqlExec(t, "set @@global.event_scheduler=1", "")

	_ = mysqlExec(t, fmt.Sprintf("drop table if exists %s_child, %s, %s_parent, %s, %s;", tableName, tableName, tableName, beforeTableName, afterTableName), "")
	_ = mysqlExec(t, "drop event if exists "+eventName, "")

	{
		// create
		f := "create.sql"
		_, exists := readTestFile(t, testName, f)
		require.True(t, exists)
		mysqlExecFile(t, testName, f)
		// ensure test table has been created:
		getCreateTableStatement(t, tableName)
	}
	defer func() {
		// destroy
		f := "destroy.sql"
		if _, exists := readTestFile(t, testName, f); exists {
			mysqlExecFile(t, testName, f)
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
		query, err := sqlparser.ParseAndBind(
			"alter vitess_migration %a cancel",
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
			orderBy = "order by " + content
		}
		selectBefore := fmt.Sprintf("select %s from %s %s", beforeColumns, beforeTableName, orderBy)
		selectAfter := fmt.Sprintf("select %s from %s %s", afterColumns, afterTableName, orderBy)

		beforeOutput := mysqlExecOutput(t, selectBefore)
		afterOutput := mysqlExecOutput(t, selectAfter)

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

func getTablet() *vitesst.Tablet {
	return primaryTablet
}

// mysqlConn opens a dba connection to the tablet's mysqld, on the keyspace's database
func mysqlConn(t *testing.T) *mysql.Conn {
	t.Helper()

	conn, err := vitesst.GetMySQLConn(t.Context(), getTablet(), "vt_"+keyspaceName)
	require.NoError(t, err)
	return conn
}

// VtgateExecDDL executes a DDL query with given strategy
func mysqlExec(t *testing.T, sql string, expectError string) *sqltypes.Result {
	t.Helper()

	conn := mysqlConn(t)
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

// mysqlExecOutput runs a query on the tablet's mysqld and returns its result rows rendered as
// tab separated lines, with `NULL` standing for a NULL value.
func mysqlExecOutput(t *testing.T, sql string) (output string) {
	t.Helper()

	conn := mysqlConn(t)
	defer conn.Close()

	qr, err := conn.ExecuteFetch(sql, mysql.FETCH_ALL_ROWS, true)
	require.NoError(t, err)
	return renderRows(qr)
}

// mysqlExecFile runs all statements of a test's SQL file on the tablet's mysqld
func mysqlExecFile(t *testing.T, testName string, fileName string) {
	t.Helper()

	content, exists := readTestFile(t, testName, fileName)
	require.True(t, exists)

	conn := mysqlConn(t)
	defer conn.Close()

	for _, statement := range splitSQLScript(content) {
		_, err := conn.ExecuteFetch(statement, mysql.FETCH_ALL_ROWS, true)
		require.NoError(t, err, "executing statement: %s", statement)
	}
}

// renderRows formats a query result as tab separated lines, one line per row,
// with `NULL` standing for a NULL value.
func renderRows(qr *sqltypes.Result) string {
	var b strings.Builder
	for _, row := range qr.Rows {
		for i, value := range row {
			if i > 0 {
				b.WriteByte('\t')
			}
			if value.IsNull() {
				b.WriteString("NULL")
			} else {
				b.WriteString(value.ToString())
			}
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// splitSQLScript breaks a SQL script into its individual statements. It honors the `delimiter`
// directive, which the scripts use to declare a routine or event body whose statements are
// themselves terminated by a semicolon.
func splitSQLScript(script string) (statements []string) {
	const delimiterDirective = "delimiter "

	var statement strings.Builder
	delimiter := ";"

	flush := func() {
		if s := strings.TrimSpace(statement.String()); s != "" {
			statements = append(statements, s)
		}
		statement.Reset()
	}

	for i := 0; i < len(script); {
		rest := script[i:]

		atLineStart := i == 0 || script[i-1] == '\n'
		if atLineStart && strings.TrimSpace(statement.String()) == "" && strings.HasPrefix(strings.ToLower(rest), delimiterDirective) {
			line := rest
			if end := strings.IndexByte(line, '\n'); end >= 0 {
				line = line[:end]
			}
			delimiter = strings.TrimSpace(line[len(delimiterDirective):])
			statement.Reset()
			i += len(line)
			continue
		}

		if strings.HasPrefix(rest, delimiter) {
			flush()
			i += len(delimiter)
			continue
		}

		switch {
		case rest[0] == '\'', rest[0] == '"', rest[0] == '`':
			end := endOfQuoted(script, i)
			statement.WriteString(script[i:end])
			i = end
		case rest[0] == '#', strings.HasPrefix(rest, "--") && (len(rest) == 2 || rest[2] == ' ' || rest[2] == '\t' || rest[2] == '\n'):
			i += endOfLineComment(rest)
		case strings.HasPrefix(rest, "/*"):
			i += endOfBlockComment(rest)
		default:
			statement.WriteByte(rest[0])
			i++
		}
	}
	flush()
	return statements
}

// endOfQuoted returns the index just past the quoted literal or identifier that starts at `start`
func endOfQuoted(script string, start int) int {
	quote := script[start]
	for i := start + 1; i < len(script); i++ {
		switch script[i] {
		case '\\':
			i++
		case quote:
			if i+1 < len(script) && script[i+1] == quote {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(script)
}

// endOfLineComment returns the length of the line comment that starts at the beginning of `rest`
func endOfLineComment(rest string) int {
	if end := strings.IndexByte(rest, '\n'); end >= 0 {
		return end + 1
	}
	return len(rest)
}

// endOfBlockComment returns the length of the block comment that starts at the beginning of `rest`
func endOfBlockComment(rest string) int {
	if end := strings.Index(rest[2:], "*/"); end >= 0 {
		return 2 + end + 2
	}
	return len(rest)
}

// getCreateTableStatement returns the CREATE TABLE statement for a given table
func getCreateTableStatement(t *testing.T, tableName string) (statement string) {
	queryResult, err := getTablet().QueryTablet(t.Context(), "show create table "+tableName)
	require.Nil(t, err)

	assert.Equal(t, len(queryResult.Rows), 1)
	assert.Equal(t, len(queryResult.Rows[0]), 2) // table name, create statement
	statement = queryResult.Rows[0][1].ToString()
	return statement
}

// updateThrottlerConfig runs vtctldclient UpdateThrottlerConfig. It retries the command until it
// succeeds or times out, as the SrvKeyspace record may not yet exist for a keyspace that is still
// initializing before it becomes serving.
func updateThrottlerConfig(ctx context.Context, args ...string) (result string, err error) {
	args = append([]string{"UpdateThrottlerConfig"}, args...)
	args = append(args, "--custom-query", "")
	args = append(args, keyspaceName)

	ctx, cancel := context.WithTimeout(ctx, throttlerConfigTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		result, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, args...)
		if err == nil {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timed out waiting for UpdateThrottlerConfig to succeed after %v; last seen value: %+v, error: %v", throttlerConfigTimeout, result, err)
		case <-ticker.C:
		}
	}
}

// getThrottlerStatus runs vtctldclient GetThrottlerStatus.
func getThrottlerStatus(ctx context.Context, tablet *vitesst.Tablet) (*tabletmanagerdatapb.GetThrottlerStatusResponse, error) {
	output, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "GetThrottlerStatus", tablet.Alias())
	if err != nil {
		return nil, err
	}
	var resp vtctldatapb.GetThrottlerStatusResponse
	if err := protojson.Unmarshal([]byte(output), &resp); err != nil {
		return nil, err
	}
	return resp.Status, nil
}

// tabletNotServing indicates whether the tablet reports itself as not serving, in which case
// its throttler is not open and its throttler status is not indicative.
func tabletNotServing(ctx context.Context, tablet *vitesst.Tablet) bool {
	_, body, err := tablet.MakeAPICall(ctx, "/debug/status_details")
	if err != nil {
		return false
	}
	class := strings.ToLower(gjson.Get(body, "0.Class").String())
	value := strings.ToLower(gjson.Get(body, "0.Value").String())
	return class == "unhappy" && strings.Contains(value, "not serving")
}

// waitForThrottlerStatusEnabled waits for a tablet to report its throttler status as
// enabled/disabled until the specified timeout.
func waitForThrottlerStatusEnabled(t *testing.T, tablet *vitesst.Tablet, enabled bool, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		// If the tablet is Not Serving due to e.g. being involved in a
		// Reshard where its QueryService is explicitly disabled, then
		// we should not fail the test as the throttler will not be Open.
		if tabletNotServing(ctx, tablet) {
			log.Info(fmt.Sprintf("tablet %s is Not Serving, so ignoring throttler status as the throttler will not be Opened", tablet.Alias()))
			return
		}

		status, err := getThrottlerStatus(ctx, tablet)
		good := func() bool {
			if err != nil {
				log.Error(fmt.Sprintf("GetThrottlerStatus failed: %v", err))
				return false
			}
			if status.IsEnabled != enabled {
				return false
			}
			if status.IsEnabled && len(status.MetricsHealth) == 0 {
				// throttler is enabled, but no metrics collected yet. Wait for something to be collected.
				return false
			}
			return true
		}
		if good() {
			return
		}
		select {
		case <-ctx.Done():
			assert.Fail(t, "timeout", "waiting for the %s tablet's throttler status enabled to be %t with the correct config after %v; last seen status: %+v",
				tablet.Alias(), enabled, timeout, status)
			return
		case <-ticker.C:
		}
	}
}

// enableLagThrottlerAndWaitForStatus enables the throttler, configured to use the standard
// replication lag metric. The function waits until the throttler is confirmed to be running
// on all tablets.
func enableLagThrottlerAndWaitForStatus(t *testing.T) {
	_, err := updateThrottlerConfig(t.Context(), "--enable")
	require.NoError(t, err)

	for _, tablet := range clusterInstance.Tablets() {
		waitForThrottlerStatusEnabled(t, tablet, true, time.Minute)
	}
}
