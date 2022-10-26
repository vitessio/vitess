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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/sqlparser"
)

var (
	clusterInstance      *cluster.LocalProcessCluster
	vtParams             mysql.ConnParams
	evaluatedMysqlParams *mysql.ConnParams

	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	tableName             = `onlineddl_test`
	eventName             = `onlineddl_test`
)

const (
	testDataPath   = "../../onlineddl/vrepl_suite/testdata"
	defaultSQLMode = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
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
			"--schema_change_check_interval", "1",
		}

		clusterInstance.VtTabletExtraArgs = []string{
			"--enable-lag-throttler",
			"--throttle_threshold", "1s",
			"--heartbeat_enable",
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

	sqlModeQuery := fmt.Sprintf("set @@global.sql_mode='%s'", defaultSQLMode)
	_ = mysqlExec(t, sqlModeQuery, "")
	_ = mysqlExec(t, "set @@global.event_scheduler=0", "")

	_ = mysqlExec(t, fmt.Sprintf("drop table if exists %s", tableName), "")
	_ = mysqlExec(t, fmt.Sprintf("drop event if exists %s", eventName), "")

	var fromCreateTable string
	var toCreateTable string
	{
		// create
		f := "create.sql"
		_, exists := readTestFile(t, testName, f)
		require.True(t, exists)
		onlineddl.MysqlClientExecFile(t, mysqlParams(), testDataPath, testName, f)
		// ensure test table has been created:
		// read the create statement
		fromCreateTable = getCreateTableStatement(t, tableName)
		require.NotEmpty(t, fromCreateTable)
	}
	defer func() {
		// destroy
		f := "destroy.sql"
		if _, exists := readTestFile(t, testName, f); exists {
			onlineddl.MysqlClientExecFile(t, mysqlParams(), testDataPath, testName, f)
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
// 	defer cluster.PanicHandler(t)

// 	hints := &schemadiff.DiffHints{AutoIncrementStrategy: schemadiff.AutoIncrementIgnore}
// 	// count := 20
// 	// for i := 0; i < count; i++ {
// 	// 	fromTestTableSchema := fromTestTableSchemas[rand.Intn(len(fromTestTableSchemas))]
// 	// 	toTestTableSchema := toTestTableSchemas[rand.Intn(len(toTestTableSchemas))]
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
	fromStmt, err := sqlparser.ParseStrictDDL(fromCreateTable)
	require.NoError(t, err)
	fromCreateTableStatement, ok := fromStmt.(*sqlparser.CreateTable)
	require.True(t, ok)

	toStmt, err := sqlparser.ParseStrictDDL(toCreateTable)
	require.NoError(t, err)
	toCreateTableStatement, ok := toStmt.(*sqlparser.CreateTable)
	require.True(t, ok)

	// The actual diff logic here!
	diff, err := schemadiff.DiffTables(fromCreateTableStatement, toCreateTableStatement, hints)
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
	_ = mysqlExec(t, fmt.Sprintf("drop table if exists %s", tableName), "")
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
				stmt, err := sqlparser.ParseStrictDDL(toCreateTable)
				require.NoError(t, err)
				createTableStatement, ok := stmt.(*sqlparser.CreateTable)
				require.True(t, ok)
				c, err := schemadiff.NewCreateTableEntity(createTableStatement)
				require.NoError(t, err)
				toCreateTable = c.Create().CanonicalStatementString()
			}
			{
				stmt, err := sqlparser.ParseStrictDDL(resultCreateTable)
				require.NoError(t, err)
				createTableStatement, ok := stmt.(*sqlparser.CreateTable)
				require.True(t, ok)
				c, err := schemadiff.NewCreateTableEntity(createTableStatement)
				require.NoError(t, err)
				resultCreateTable = c.Create().CanonicalStatementString()
			}
		}
	}

	// The actual validation test here:
	assert.Equal(t, toCreateTable, resultCreateTable, "mismatched table structure. ALTER query was: %s", diffedAlterQuery)

	// Also, let's see that our diff agrees there's no change:
	resultStmt, err := sqlparser.ParseStrictDDL(resultCreateTable)
	require.NoError(t, err)
	resultCreateTableStatement, ok := resultStmt.(*sqlparser.CreateTable)
	require.True(t, ok)

	resultDiff, err := schemadiff.DiffTables(toCreateTableStatement, resultCreateTableStatement, hints)
	assert.NoError(t, err)
	assert.Nil(t, resultDiff)
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
