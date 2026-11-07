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

package sqlparser

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser/internal/mysqltest"
)

// TestMySQLTestSuiteDDL parses the DDL statements of MySQL's own test suite.
// A statement that MySQL accepts must parse, and its formatted form must parse
// back to the same tree, because Vitess sends the formatted form to MySQL.
//
// A statement that MySQL is expected to reject with a different error may be
// valid syntax, so it must round-trip if it parses, but it may fail to parse.
//
// A statement that MySQL rejects as a syntax error is only reported when
// Vitess accepts it. Vitess accepts some words unquoted that MySQL reserves,
// such as GET, and it does not repeat the checks that MySQL makes in its
// parser but that are not about syntax, such as the number of partitions. It
// formats what it parsed, quoting such a word, so MySQL runs the statement
// that Vitess planned, rather than another one.
//
// The test runs only when VT_MYSQL_SERVER_SOURCE points at a checkout of
// github.com/mysql/mysql-server, for example the 8.4 branch.
func TestMySQLTestSuiteDDL(t *testing.T) {
	source := os.Getenv(mysqltest.SourceEnv)
	if source == "" {
		t.Skipf("%s is not set", mysqltest.SourceEnv)
	}
	paths, names, err := mysqltest.Files(source, false)
	require.NoError(t, err)
	var statements []mysqltest.Statement
	for i, path := range paths {
		fileStatements, err := mysqltest.ReadFile(path, names[i])
		require.NoError(t, err)
		statements = append(statements, fileStatements...)
	}
	require.NotEmpty(t, statements, "no statements found under %s", source)

	parser := NewTestParser()
	checked := 0
	var accepted []string
	for _, statement := range statements {
		// mysqltest sends the text up to its delimiter as one query, which can
		// hold several statements when the delimiter is not ';'. An expected
		// error then belongs to one of them, but not to a known one.
		pieces, err := parser.SplitStatementToPieces(statement.SQL)
		if err != nil || len(pieces) > 1 && statement.ExpectedErrors != "" {
			continue
		}
		for _, piece := range pieces {
			statement.SQL = piece
			if statement.SQLModeDependent || !mysqltest.IsDDL(statement.SQL) {
				continue
			}
			checked++
			if mysqltest.IsParseError(statement.ExpectedErrors) {
				if _, err := parser.Parse(statement.SQL); err == nil {
					accepted = append(accepted, fmt.Sprintf("%s:%d: %s", statement.File, statement.Line, statement.SQL))
				}
				continue
			}
			checkMySQLTestStatement(t, parser, statement)
		}
	}
	t.Logf("checked %d DDL statements of %d statements", checked, len(statements))
	t.Logf("%d statements that MySQL rejects as syntax errors parse:\n%s", len(accepted), strings.Join(accepted, "\n"))
}

func checkMySQLTestStatement(t *testing.T, parser *Parser, statement mysqltest.Statement) {
	tree, err := parser.Parse(statement.SQL)
	if err != nil {
		if statement.ExpectedErrors == "" {
			assert.NoError(t, err, "%s:%d: MySQL accepts this statement\n%s", statement.File, statement.Line, statement.SQL)
		}
		return
	}
	formatted := String(tree)
	reparsed, err := parser.Parse(formatted)
	if !assert.NoError(t, err, "%s:%d: the formatted statement does not parse\n%s\n%s", statement.File, statement.Line, statement.SQL, formatted) {
		return
	}
	assert.Equal(t, formatted, String(reparsed), "%s:%d: the formatted statement does not round-trip\n%s", statement.File, statement.Line, statement.SQL)
}
