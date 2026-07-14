/*
Copyright 2022 The Vitess Authors.

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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/test/vitesst"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/sqlparser"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	clusterInstance *vitesst.Cluster
	primaryTablet   *vitesst.Tablet
	vtParams        mysql.ConnParams

	keyspaceName          = "ks"
	schemaChangeDirectory = "/vt/files"
	tableName             = `onlineddl_test`
	eventName             = `onlineddl_test`
)

const (
	testDataPath           = "../../onlineddl/vrepl_suite/testdata"
	sqlModeAllowsZeroDate  = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
	throttlerConfigTimeout = 60 * time.Second
)

type testTableSchema struct {
	testName    string
	tableSchema string
}

var (
	fromTestTableSchemas []*testTableSchema
	toTestTableSchemas   []*testTableSchema
	autoIncrementRegexp  = regexp.MustCompile(`(?i) auto_increment[\s]*[=]?[\s]*([0-9]+)`)
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

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
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}

		cleanup, err := cluster.Start(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer func() {
			if err := cleanup(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "cluster teardown:", err)
			}
		}()

		clusterInstance = cluster
		primaryTablet = cluster.Keyspace(keyspaceName).Shards()[0].Primary()
		vtParams = cluster.VTParams(ctx, "")

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestSchemadiffSchemaChanges(t *testing.T) {
	shards := clusterInstance.Keyspace(keyspaceName).Shards()
	require.Equal(t, 1, len(shards))

	enableLagThrottlerAndWaitForStatus(t)

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
	if _, exists := readTestFile(t, testName, "expect_query_failure"); exists {
		// VTGate failure is expected!
		// irrelevant to this suite.
		// We only want to test actual migrations
		t.Skip("expect_query_failure found. Irrelevant to this suite")
		return
	}
	if _, exists := readTestFile(t, testName, "expect_failure"); exists {
		// irrelevant to this suite.
		// We only want to test actual migrations
		t.Skip("expect_failure found. Irrelevant to this suite")
		return
	}
	if _, exists := readTestFile(t, testName, "skip_schemadiff"); exists {
		// irrelevant to this suite.
		t.Skip("skip_schemadiff found. Irrelevant to this suite")
		return
	}

	sqlModeQuery := fmt.Sprintf("set @@global.sql_mode='%s'", sqlModeAllowsZeroDate)
	_ = mysqlExec(t, sqlModeQuery, "")
	_ = mysqlExec(t, "set @@global.event_scheduler=0", "")

	_ = mysqlExec(t, "drop table if exists "+tableName, "")
	_ = mysqlExec(t, "drop event if exists "+eventName, "")

	var fromCreateTable string
	var toCreateTable string
	{
		// create
		f := "create.sql"
		_, exists := readTestFile(t, testName, f)
		require.True(t, exists)
		mysqlExecFile(t, testName, f)
		// ensure test table has been created:
		// read the create statement
		fromCreateTable = getCreateTableStatement(t, tableName)
		require.NotEmpty(t, fromCreateTable)
	}
	defer func() {
		// destroy
		f := "destroy.sql"
		if _, exists := readTestFile(t, testName, f); exists {
			mysqlExecFile(t, testName, f)
		}
	}()

	// Run test
	alterClause := "engine=innodb"
	if content, exists := readTestFile(t, testName, "alter"); exists {
		alterClause = content
	}
	alterStatement := fmt.Sprintf("alter table %s %s", tableName, alterClause)
	// Run the DDL!
	onlineddl.VtgateExecQuery(t, &vtParams, alterStatement, "")
	// migration is complete
	// read the table structure of modified table:
	toCreateTable = getCreateTableStatement(t, tableName)
	require.NotEmpty(t, toCreateTable)

	if content, exists := readTestFile(t, testName, "expect_table_structure"); exists {
		switch {
		case strings.HasPrefix(testName, "autoinc"):
			// In schemadiff_vrepl test, we run a direct ALTER TABLE. This is as opposed to
			// vrepl_suite runnign a vreplication Online DDL. This matters, because AUTO_INCREMENT
			// values in the resulting table are different between the two approaches!
			// So for schemadiff_vrepl tests we ignore any AUTO_INCREMENT requirements,
			// they're just not interesting for this test.
		default:
			assert.Regexpf(t, content, toCreateTable, "expected SHOW CREATE TABLE to match text in 'expect_table_structure' file")
		}
	}

	fromTestTableSchemas = append(fromTestTableSchemas, &testTableSchema{
		testName:    testName,
		tableSchema: fromCreateTable,
	})
	toTestTableSchemas = append(toTestTableSchemas, &testTableSchema{
		testName:    testName,
		tableSchema: toCreateTable,
	})

	hints := &schemadiff.DiffHints{}
	if strings.Contains(alterClause, "AUTO_INCREMENT") {
		hints.AutoIncrementStrategy = schemadiff.AutoIncrementApplyAlways
	}
	t.Run("validate diff", func(t *testing.T) {
		_, allowSchemadiffNormalization := readTestFile(t, testName, "allow_schemadiff_normalization")
		validateDiff(t, fromCreateTable, toCreateTable, allowSchemadiffNormalization, hints)
	})
}

// func TestRandomSchemaChanges(t *testing.T) {

// 	hints := &schemadiff.DiffHints{AutoIncrementStrategy: schemadiff.AutoIncrementIgnore}
// 	// count := 20
// 	// for i := 0; i < count; i++ {
// 	// 	fromTestTableSchema := fromTestTableSchemas[rand.IntN(len(fromTestTableSchemas))]
// 	// 	toTestTableSchema := toTestTableSchemas[rand.IntN(len(toTestTableSchemas))]
// 	// 	testName := fmt.Sprintf("%s/%s", fromTestTableSchema.testName, toTestTableSchema.testName)
// 	// 	t.Run(testName, func(t *testing.T) {
// 	// 		validateDiff(t, fromTestTableSchema.tableSchema, toTestTableSchema.tableSchema, hints)
// 	// 	})
// 	// }
// 	for i := range rand.Perm(len(fromTestTableSchemas)) {
// 		fromTestTableSchema := fromTestTableSchemas[i]
// 		for j := range rand.Perm(len(toTestTableSchemas)) {
// 			toTestTableSchema := toTestTableSchemas[j]
// 			testName := fmt.Sprintf("%s:%s", fromTestTableSchema.testName, toTestTableSchema.testName)
// 			t.Run(testName, func(t *testing.T) {
// 				validateDiff(t, fromTestTableSchema.tableSchema, toTestTableSchema.tableSchema, hints)
// 			})
// 		}
// 	}
// }

func TestIgnoreAutoIncrementRegexp(t *testing.T) {
	// validate the validation function we use in our tests...
	tt := []struct {
		statement string
		expect    string
	}{
		{
			statement: "CREATE TABLE t(id int auto_increment primary key)",
			expect:    "CREATE TABLE t(id int auto_increment primary key)",
		},
		{
			statement: "CREATE TABLE t(id int auto_increment primary key) auto_increment=3",
			expect:    "CREATE TABLE t(id int auto_increment primary key)",
		},
		{
			statement: "CREATE TABLE t(id int auto_increment primary key) AUTO_INCREMENT=3 default charset=utf8",
			expect:    "CREATE TABLE t(id int auto_increment primary key) default charset=utf8",
		},
		{
			statement: "CREATE TABLE t(id int auto_increment primary key) default charset=utf8 auto_increment=3",
			expect:    "CREATE TABLE t(id int auto_increment primary key) default charset=utf8",
		},
		{
			statement: "CREATE TABLE t(id int auto_increment primary key) default charset=utf8 auto_increment=3 engine=innodb",
			expect:    "CREATE TABLE t(id int auto_increment primary key) default charset=utf8 engine=innodb",
		},
	}
	for _, tc := range tt {
		t.Run(tc.statement, func(t *testing.T) {
			ignored := ignoreAutoIncrement(t, tc.statement)
			assert.Equal(t, tc.expect, ignored)
		})
	}
}

func ignoreAutoIncrement(t *testing.T, createTable string) string {
	result := autoIncrementRegexp.ReplaceAllString(createTable, "")
	// sanity:
	require.Contains(t, result, "CREATE TABLE")
	require.Contains(t, result, ")")
	return result
}

func validateDiff(t *testing.T, fromCreateTable string, toCreateTable string, allowSchemadiffNormalization bool, hints *schemadiff.DiffHints) {
	// turn the "from" and "to" create statement strings (which we just read via SHOW CREATE TABLE into sqlparser.CreateTable statement)
	env := schemadiff.NewTestEnv()
	fromStmt, err := env.Parser().ParseStrictDDL(fromCreateTable)
	require.NoError(t, err)
	fromCreateTableStatement, ok := fromStmt.(*sqlparser.CreateTable)
	require.True(t, ok)

	toStmt, err := env.Parser().ParseStrictDDL(toCreateTable)
	require.NoError(t, err)
	toCreateTableStatement, ok := toStmt.(*sqlparser.CreateTable)
	require.True(t, ok)

	// The actual diff logic here!
	diff, err := schemadiff.DiffTables(env, fromCreateTableStatement, toCreateTableStatement, hints)
	assert.NoError(t, err)

	// The diff can be empty or there can be an actual ALTER TABLE statement
	diffedAlterQuery := ""
	if diff != nil && !diff.IsEmpty() {
		diffedAlterQuery = diff.CanonicalStatementString()
	}

	// Validate the diff! The way we do it is:
	// Recreate the original table
	// Alter the table directly using our evaluated diff (if empty we do nothing)
	// Review the resulted table structure (via SHOW CREATE TABLE)
	// Expect it to be identical to the structure generated by the suite earlier on (toCreateTable)
	_ = mysqlExec(t, "drop table if exists "+tableName, "")
	onlineddl.VtgateExecQuery(t, &vtParams, fromCreateTable, "")
	if diffedAlterQuery != "" {
		onlineddl.VtgateExecQuery(t, &vtParams, diffedAlterQuery, "")
	}
	resultCreateTable := getCreateTableStatement(t, tableName)
	if hints.AutoIncrementStrategy == schemadiff.AutoIncrementIgnore {
		toCreateTable = ignoreAutoIncrement(t, toCreateTable)
		resultCreateTable = ignoreAutoIncrement(t, resultCreateTable)
	}

	// Next, the big test: does the result table, applied by schemadiff's evaluated ALTER, look exactly like
	// the table generated by the test's own ALTER statement?

	// But wait, there's caveats.
	if toCreateTable != resultCreateTable {
		// schemadiff's ALTER statement can normalize away CHARACTER SET and COLLATION definitions:
		// when altering a column's CHARTSET&COLLATION into the table's values, schemadiff just strips the
		// CHARSET and COLLATION clauses out of the `MODIFY COLUMN ...` statement. This is valid.
		// However, MySQL outputs two different SHOW CREATE TABLE statements, even though the table
		// structure is identical. And so we accept that there can be a normalization issue.
		if allowSchemadiffNormalization {
			{
				stmt, err := env.Parser().ParseStrictDDL(toCreateTable)
				require.NoError(t, err)
				createTableStatement, ok := stmt.(*sqlparser.CreateTable)
				require.True(t, ok)
				c, err := schemadiff.NewCreateTableEntity(env, createTableStatement)
				require.NoError(t, err)
				toCreateTable = c.Create().CanonicalStatementString()
			}
			{
				stmt, err := env.Parser().ParseStrictDDL(resultCreateTable)
				require.NoError(t, err)
				createTableStatement, ok := stmt.(*sqlparser.CreateTable)
				require.True(t, ok)
				c, err := schemadiff.NewCreateTableEntity(env, createTableStatement)
				require.NoError(t, err)
				resultCreateTable = c.Create().CanonicalStatementString()
			}
		}
	}

	// The actual validation test here:
	assert.Equal(t, toCreateTable, resultCreateTable, "mismatched table structure. ALTER query was: %s", diffedAlterQuery)

	// Also, let's see that our diff agrees there's no change:
	resultStmt, err := env.Parser().ParseStrictDDL(resultCreateTable)
	require.NoError(t, err)
	resultCreateTableStatement, ok := resultStmt.(*sqlparser.CreateTable)
	require.True(t, ok)

	resultDiff, err := schemadiff.DiffTables(env, toCreateTableStatement, resultCreateTableStatement, hints)
	assert.NoError(t, err)
	assert.Nil(t, resultDiff)
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
