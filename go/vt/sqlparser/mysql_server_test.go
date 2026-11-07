/*
Copyright 2026 The Vitess Authors.

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

// This test is in its own package, because the MySQL client that it uses
// imports sqlparser.
package sqlparser_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/sqlparser/internal/mysqltest"
)

// mysqlServersEnv names the environment variable that holds the Unix sockets
// of two MySQL servers that TestMySQLTestSuiteDDLOnMySQL may drop every
// database of, separated by a comma.
const mysqlServersEnv = "VT_MYSQL_COMPARE_SOCKETS"

// replayedStatement matches the statements besides DDL that the test replays
// on both servers, so that the DDL of a test file runs on the tables, the data
// and the session settings that the file sets up. It leaves out anything that
// can reach beyond the test's own databases or session.
var replayedStatement = regexp.MustCompile(`(?is)^(insert|replace|drop\s+(temporary\s+)?(table|tables|view|index)|drop\s+(database|schema)|rename\s+table|truncate|use|create\s+(or\s+replace\s+)?(algorithm\s*=\s*\w+\s+)?view|set\s+(names|character\s+set|charset|sql_mode|@@sql_mode|session|@@session|@@local|local|foreign_key_checks|@@foreign_key_checks|default_storage_engine|@@default_storage_engine|explicit_defaults_for_timestamp|@@explicit_defaults_for_timestamp|character_set_\w+|collation_\w+|@@character_set_\w+|@@collation_\w+|big_tables|default_collation_for_utf8mb4|@@default_collation_for_utf8mb4))\b`)

// unsafeStatement matches the statements that the test does not run, even
// when they are DDL: those that name a system schema, change a global
// setting, read or write files, or wait.
var unsafeStatement = regexp.MustCompile("(?is)\\b(mysql|performance_schema|sys|information_schema)\\s*\\.|\\bglobal\\b|\\bpersist\\b|\\bpassword\\b|\\b(database|schema)\\s+(if\\s+exists\\s+)?`?(mysql|sys|performance_schema)\\b|load_file|into\\s+(out|dump)file|\\bsleep\\s*\\(|get_lock|data\\s+directory|index\\s+directory")

// nondeterministicFunction matches the functions whose result differs from
// one call to the next, so that the rows that CREATE TABLE ... SELECT copies
// with them differ between the servers.
var nondeterministicFunction = regexp.MustCompile(`(?i)\b(uuid|uuid_short|rand|now|sysdate|curtime|curdate|current_time|current_date|current_timestamp|localtime|localtimestamp|unix_timestamp|utc_date|utc_time|utc_timestamp|connection_id|last_insert_id|found_rows|row_count|random_bytes)\s*\(`)

// spatialSynonym matches the names of spatial functions that MySQL takes for
// another name of the same function. The parser gives each function one
// name, and MySQL shows the name as written in a definition, so the test
// shows both under that name.
var spatialSynonym = regexp.MustCompile(`\bst_(?:(geomcollfromtxt)|(geomfrom|geomcollfrom|mpointfrom|linefrom|mlinefrom|polyfrom|mpolyfrom)(text|wkb)|as(wkt|wkb))\(`)

var spatialNames = map[string]string{
	"geomfrom":     "geometryfrom",
	"geomcollfrom": "geometrycollectionfrom",
	"mpointfrom":   "multipointfrom",
	"linefrom":     "linestringfrom",
	"mlinefrom":    "multilinestringfrom",
	"polyfrom":     "polygonfrom",
	"mpolyfrom":    "multipolygonfrom",
	"wkt":          "text",
	"wkb":          "binary",
}

func normalizeSpatialSynonyms(definition string) string {
	return spatialSynonym.ReplaceAllStringFunc(definition, func(name string) string {
		match := spatialSynonym.FindStringSubmatch(name)
		switch {
		case match[1] != "":
			return "st_geometrycollectionfromtext("
		case match[4] != "":
			return "st_as" + spatialNames[match[4]] + "("
		}
		return "st_" + spatialNames[match[2]] + match[3] + "("
	})
}

// mysqlServer is one of the two servers that the test compares.
type mysqlServer struct {
	params *mysql.ConnParams
	conn   *mysql.Conn
}

func (s *mysqlServer) connect(t *testing.T) {
	if s.conn != nil {
		s.conn.Close()
	}
	// Some tests crash MySQL. A server that restarts after a crash takes a
	// while to come back.
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		conn, err := mysql.Connect(ctx, s.params)
		if err != nil {
			return false
		}
		s.conn = conn
		return true
	}, 2*time.Minute, time.Second)
}

func (s *mysqlServer) query(sql string) (string, error) {
	result, err := s.conn.ExecuteFetch(sql, 100000, false)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	for _, row := range result.Rows {
		for i, value := range row {
			if i > 0 {
				out.WriteByte('\t')
			}
			out.WriteString(value.ToString())
		}
		out.WriteByte('\n')
	}
	return out.String(), nil
}

// reset drops every database but the system ones, creates the database test
// and starts a new session in it, as mysqltest does for each test file.
func (s *mysqlServer) reset(t *testing.T) {
	require.Eventually(t, func() bool {
		s.connect(t)
		databases, err := s.query("select schema_name from information_schema.schemata where schema_name not in ('mysql', 'sys', 'performance_schema', 'information_schema')")
		if err != nil {
			return false
		}
		if _, err := s.query("set foreign_key_checks = 0"); err != nil {
			return false
		}
		for _, database := range strings.Fields(databases) {
			if _, err := s.query("drop database `" + database + "`"); err != nil {
				return false
			}
		}
		if _, err := s.query("create database test"); err != nil {
			return false
		}
		s.connect(t)
		_, err = s.query("use test")
		return err == nil
	}, 5*time.Minute, time.Second)
}

func errorCode(err error) string {
	if err == nil {
		return "no error"
	}
	var sqlErr *sqlerror.SQLError
	if errors.As(err, &sqlErr) {
		return fmt.Sprintf("error %d", sqlErr.Num)
	}
	return err.Error()
}

func lostConnection(err error) bool {
	var sqlErr *sqlerror.SQLError
	return errors.As(err, &sqlErr) && (sqlErr.Num == sqlerror.CRServerGone || sqlErr.Num == sqlerror.CRServerLost)
}

// definitions returns the statements that show what a DDL statement that
// succeeded defined.
func definitions(stmt sqlparser.Statement) []string {
	showTable := func(name sqlparser.TableName) string {
		if name.Qualifier.IsEmpty() {
			return "show create table " + sqlparser.String(name.Name)
		}
		return "show create table " + sqlparser.String(name)
	}
	switch stmt := stmt.(type) {
	case *sqlparser.CreateTable:
		shows := []string{showTable(stmt.Table)}
		if stmt.Select != nil {
			// The rows that CREATE TABLE ... SELECT copies, unless they
			// differ from one run to the next.
			if !nondeterministicFunction.MatchString(sqlparser.String(stmt.Select)) {
				shows = append(shows, "checksum table "+sqlparser.String(stmt.Table))
			}
		}
		return shows
	case *sqlparser.AlterTable:
		for _, option := range stmt.AlterOptions {
			if rename, ok := option.(*sqlparser.RenameTableName); ok {
				return []string{showTable(rename.Table)}
			}
		}
		return []string{showTable(stmt.Table)}
	case *sqlparser.CreateDatabase:
		return []string{"show create database " + sqlparser.String(stmt.DBName)}
	case *sqlparser.AlterDatabase:
		if stmt.DBName.IsEmpty() {
			return []string{"select @@character_set_database, @@collation_database"}
		}
		return []string{"show create database " + sqlparser.String(stmt.DBName)}
	}
	return nil
}

// TestMySQLTestSuiteDDLOnMySQL runs each test file of MySQL's own test suite
// on two MySQL servers. The first server gets each DDL statement as written,
// and the second gets the statement that Vitess formats from it, which is what
// Vitess sends MySQL. The test checks that MySQL accepts or rejects both the
// same way, and that both leave the same definitions behind. The statements
// that set up a DDL statement, such as INSERT and SET, run on both servers as
// written; the others are left out.
//
// The test runs only when VT_MYSQL_SERVER_SOURCE points at a checkout of
// github.com/mysql/mysql-server and VT_MYSQL_COMPARE_SOCKETS holds the Unix
// sockets of two MySQL servers of the same version, which the test drops
// every database of. Some tests in the suite crash MySQL, so the servers
// should restart after a crash, from an empty data directory.
func TestMySQLTestSuiteDDLOnMySQL(t *testing.T) {
	source := os.Getenv(mysqltest.SourceEnv)
	if source == "" {
		t.Skipf("%s is not set", mysqltest.SourceEnv)
	}
	sockets := strings.Split(os.Getenv(mysqlServersEnv), ",")
	if len(sockets) != 2 {
		t.Skipf("%s does not hold two sockets", mysqlServersEnv)
	}
	asWritten := &mysqlServer{params: &mysql.ConnParams{UnixSocket: sockets[0], Uname: "root"}}
	formatted := &mysqlServer{params: &mysql.ConnParams{UnixSocket: sockets[1], Uname: "root"}}
	paths, names, err := mysqltest.Files(source, true)
	require.NoError(t, err)
	parser := sqlparser.NewTestParser()
	compared := 0
	var lenient []string
	for i, path := range paths {
		statements, err := mysqltest.ReadFile(path, names[i])
		require.NoError(t, err)
		if !hasDDL(statements) {
			continue
		}
		if i%100 == 0 {
			t.Logf("%d of %d files, %d statements compared", i, len(paths), compared)
		}
		asWritten.reset(t)
		formatted.reset(t)
		for _, statement := range statements {
			if !compareStatement(t, parser, asWritten, formatted, statement, &lenient) {
				// The servers no longer hold the same tables, so the rest of
				// the file would differ too.
				break
			}
			compared++
		}
	}
	t.Logf("compared %d statements", compared)
	t.Logf("%d statements that MySQL rejects as syntax errors run as formatted:\n%s", len(lenient), strings.Join(lenient, "\n"))
}

func hasDDL(statements []mysqltest.Statement) bool {
	for _, statement := range statements {
		if mysqltest.IsDDL(statement.SQL) {
			return true
		}
	}
	return false
}

// compareStatement runs statement on both servers and checks that they agree.
// It reports false when the servers no longer hold the same tables, because a
// server crashed or the statement ran differently. A statement that MySQL rejects as a
// syntax error but that Vitess accepts is added to lenient instead: Vitess
// accepts some words unquoted that MySQL reserves, and some spellings that
// MySQL does not, and it formats what it understood, so the formatted
// statement means what Vitess planned.
func compareStatement(t *testing.T, parser *sqlparser.Parser, asWritten, formatted *mysqlServer, statement mysqltest.Statement, lenient *[]string) bool {
	sql := statement.SQL
	if unsafeStatement.MatchString(sql) {
		return true
	}
	isDDL := mysqltest.IsDDL(sql)
	if !isDDL && !replayedStatement.MatchString(sql) {
		return true
	}
	where := fmt.Sprintf("%s:%d: %s", statement.File, statement.Line, sql)
	formattedSQL := sql
	var tree sqlparser.Statement
	if isDDL && !statement.UnsupportedSession {
		parsed, err := parser.Parse(sql)
		if err == nil {
			tree = parsed
			formattedSQL = sqlparser.String(tree)
		}
	}
	_, errAsWritten := asWritten.query(sql)
	_, errFormatted := formatted.query(formattedSQL)
	if lostConnection(errAsWritten) || lostConnection(errFormatted) {
		// A crash on one server only is a difference too.
		assert.Equal(t, lostConnection(errAsWritten), lostConnection(errFormatted), "%s\nonly one server crashed on\n%s", where, formattedSQL)
		return false
	}
	if tree == nil {
		// Vitess rejects the statement, or the statement is not DDL. It
		// ran as written on both servers, so that they stay the same.
		return true
	}
	var sqlErr *sqlerror.SQLError
	if errors.As(errAsWritten, &sqlErr) && sqlErr.Num == sqlerror.ERParseError && errorCode(errFormatted) != errorCode(errAsWritten) {
		*lenient = append(*lenient, fmt.Sprintf("%s\n\tformatted: %s\n\t%s", where, formattedSQL, errorCode(errFormatted)))
		return errFormatted != nil
	}
	if !assert.Equal(t, errorCode(errAsWritten), errorCode(errFormatted), "%s\nMySQL ran the formatted statement differently:\n%s\nas written: %v\nformatted: %v", where, formattedSQL, errAsWritten, errFormatted) {
		return false
	}
	if errAsWritten != nil {
		return true
	}
	same := true
	for _, show := range definitions(tree) {
		outAsWritten, errAsWritten := asWritten.query(show)
		outFormatted, errFormatted := formatted.query(show)
		same = assert.Equal(t, errorCode(errAsWritten), errorCode(errFormatted), "%s\n%s", where, show) && same
		same = assert.Equal(t, normalizeSpatialSynonyms(outAsWritten), normalizeSpatialSynonyms(outFormatted), "%s\nMySQL defined the formatted statement differently:\n%s", where, formattedSQL) && same
	}
	return same
}
