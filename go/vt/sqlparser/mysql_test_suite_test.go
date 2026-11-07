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
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mysqlSourceEnv names the environment variable that points at a checkout of
// github.com/mysql/mysql-server. The MySQL test suite is GPL licensed, so it is
// read from a local checkout instead of being copied into this repository.
const mysqlSourceEnv = "VT_MYSQL_SERVER_SOURCE"

// mysqlTestStatement is one SQL statement taken from a mysqltest file.
type mysqlTestStatement struct {
	file string
	line int
	sql  string
	// expectedErrors is the argument of the `--error` command that precedes
	// the statement, or empty when MySQL is expected to accept it.
	expectedErrors string
	// ansiQuotes is set when the file enabled ANSI_QUOTES before the
	// statement, which makes a double-quoted string an identifier. The parser
	// does not know the session's sql_mode, so such a statement cannot be
	// judged.
	ansiQuotes bool
}

// mysqlTestCommand matches the mysqltest commands that can be written
// without the `--` prefix. A statement that starts with one of them is a
// command to the test runner, not SQL.
var mysqlTestCommand = regexp.MustCompile(`(?i)^\s*(let|eval|connect|connection|disconnect|source|echo|if|while|end|sleep|replace_result|replace_regex|replace_column|exec|perl|send|send_eval|reap|disable_\w+|enable_\w+|query_vertical|sorted_result|error|inc|dec|die|skip|exit|vertical_results|horizontal_results|real_sleep|save_master_pos|sync_slave_with_master|copy_file|remove_file|write_file|append_file|cat_file|file_exists|mkdir|rmdir|list_files\w*|chmod|move_file|shutdown_server|character_set|result_format|lowercase_result|expr|assert|output|change_user|ping|query|send_quit|start_timer|end_timer)\b`)

var (
	mysqlTestErrorCommand     = regexp.MustCompile(`(?i)^--\s*error\s+(.*)$`)
	mysqlTestDelimiterCommand = regexp.MustCompile(`(?i)^(?:--\s*)?delimiter\s+(\S+)\s*$`)
	mysqlTestSQLMode          = regexp.MustCompile(`(?i)^set\s+(?:@@)?(?:session\s+|global\s+|local\s+)?(?:@@)?(?:session\.|global\.|local\.)?sql_mode\s*=\s*(.*)$`)
)

// mysqlTestReader splits a mysqltest file into its SQL statements, the way
// mysqltest does: a statement ends at the first delimiter outside a quoted
// string, so one line can hold several statements and one statement can span
// several lines.
type mysqlTestReader struct {
	name       string
	statements []mysqlTestStatement
	buffer     strings.Builder
	delimiter  string
	// quote is the quote character of the string that is open at the end of
	// the buffer, or zero when no string is open.
	quote          byte
	firstLine      int
	expectedErrors string
	ansiQuotes     bool
}

// readMySQLTestFile returns the SQL statements of a mysqltest file. It drops
// the statements that refer to mysqltest variables, because their text is only
// known when the test runs.
func readMySQLTestFile(path, name string) ([]mysqlTestStatement, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	reader := &mysqlTestReader{name: name, delimiter: ";"}
	for index, line := range strings.Split(string(content), "\n") {
		reader.readLine(index+1, line)
	}
	return reader.statements, nil
}

func (reader *mysqlTestReader) readLine(lineNumber int, line string) {
	for line != "" {
		if reader.buffer.Len() == 0 {
			if reader.readCommand(line) {
				return
			}
			reader.firstLine = lineNumber
		}
		end := reader.scanToDelimiter(line)
		if end < 0 {
			reader.buffer.WriteString(line)
			reader.buffer.WriteByte('\n')
			return
		}
		reader.buffer.WriteString(line[:end])
		reader.endStatement()
		line = line[end+len(reader.delimiter):]
	}
}

// readCommand handles a line that starts outside of any statement. It reports
// whether the line is a comment or a mysqltest command rather than the start
// of a statement.
func (reader *mysqlTestReader) readCommand(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return true
	}
	if match := mysqlTestDelimiterCommand.FindStringSubmatch(trimmed); match != nil {
		// Without the `--` prefix, the command itself ends with the
		// delimiter that is in force.
		delimiter := match[1]
		if !strings.HasPrefix(trimmed, "--") && len(delimiter) > len(reader.delimiter) {
			delimiter = strings.TrimSuffix(delimiter, reader.delimiter)
		}
		reader.delimiter = delimiter
		return true
	}
	if strings.HasPrefix(trimmed, "--") {
		if match := mysqlTestErrorCommand.FindStringSubmatch(trimmed); match != nil {
			reader.expectedErrors = match[1]
		}
		return true
	}
	return false
}

// scanToDelimiter returns the index of the first delimiter in line that is
// outside a quoted string, or -1 when the statement continues on the next
// line. It keeps track of a string that is still open at the end of the line.
func (reader *mysqlTestReader) scanToDelimiter(line string) int {
	for index := 0; index < len(line); index++ {
		character := line[index]
		if reader.quote != 0 {
			if character == '\\' && reader.quote != '`' {
				index++
				continue
			}
			if character == reader.quote {
				reader.quote = 0
			}
			continue
		}
		if strings.HasPrefix(line[index:], reader.delimiter) {
			return index
		}
		switch character {
		case '\'', '"', '`':
			reader.quote = character
		case '#':
			return -1
		}
	}
	return -1
}

func (reader *mysqlTestReader) endStatement() {
	sql := strings.TrimSpace(reader.buffer.String())
	reader.buffer.Reset()
	reader.quote = 0
	expectedErrors := reader.expectedErrors
	reader.expectedErrors = ""
	if sql == "" || mysqlTestCommand.MatchString(sql) || strings.Contains(sql, "$") {
		return
	}
	if match := mysqlTestSQLMode.FindStringSubmatch(sql); match != nil {
		reader.ansiQuotes = strings.Contains(strings.ToUpper(match[1]), "ANSI")
		return
	}
	reader.statements = append(reader.statements, mysqlTestStatement{
		file:           reader.name,
		line:           reader.firstLine,
		sql:            sql,
		expectedErrors: expectedErrors,
		ansiQuotes:     reader.ansiQuotes,
	})
}

// mysqlDDLStatement matches the statements whose syntax Vitess must parse in
// full: a DDL statement that the parser does not understand is rejected, so
// every form that MySQL accepts has to be in the grammar.
var mysqlDDLStatement = regexp.MustCompile(`(?is)^(create\s+(/\*.*?\*/\s*)?(temporary\s+)?table|alter\s+(/\*.*?\*/\s*)?table|create\s+(/\*.*?\*/\s*)?(unique\s+|fulltext\s+|spatial\s+)?index|create\s+(/\*.*?\*/\s*)?(database|schema)|alter\s+(/\*.*?\*/\s*)?(database|schema))\b`)

// isMySQLParseError reports whether the expected errors of a statement say
// that MySQL rejects its syntax. A list of alternatives that also allows a
// different error does not say that, because the statement may parse.
func isMySQLParseError(expectedErrors string) bool {
	if expectedErrors == "" {
		return false
	}
	for _, code := range strings.Split(expectedErrors, ",") {
		code = strings.TrimSpace(code)
		if code != "ER_PARSE_ERROR" && code != "1064" {
			return false
		}
	}
	return true
}

// TestMySQLTestSuiteDDL parses the DDL statements of MySQL's own test suite.
// A statement that MySQL accepts must parse, and its formatted form must parse
// back to the same tree, because Vitess sends the formatted form to MySQL. A
// statement that MySQL rejects as a syntax error must be rejected too, so that
// Vitess does not send MySQL a statement other than the one the client wrote.
//
// A statement that MySQL is expected to reject with a different error may be
// valid syntax, so it must round-trip if it parses, but it may fail to parse.
//
// The test runs only when VT_MYSQL_SERVER_SOURCE points at a checkout of
// github.com/mysql/mysql-server, for example the 8.4 branch.
func TestMySQLTestSuiteDDL(t *testing.T) {
	source := os.Getenv(mysqlSourceEnv)
	if source == "" {
		t.Skipf("%s is not set", mysqlSourceEnv)
	}
	root := filepath.Join(source, "mysql-test")
	var statements []mysqlTestStatement
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !(strings.HasSuffix(path, ".test") || strings.HasSuffix(path, ".inc")) {
			return nil
		}
		name, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		fileStatements, err := readMySQLTestFile(path, name)
		if err != nil {
			return err
		}
		statements = append(statements, fileStatements...)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, statements, "no statements found under %s", root)

	parser := NewTestParser()
	checked := 0
	for _, statement := range statements {
		// mysqltest sends the text up to its delimiter as one query, which can
		// hold several statements when the delimiter is not ';'. An expected
		// error then belongs to one of them, but not to a known one.
		pieces, err := parser.SplitStatementToPieces(statement.sql)
		if err != nil || len(pieces) > 1 && statement.expectedErrors != "" {
			continue
		}
		for _, piece := range pieces {
			statement.sql = piece
			if statement.ansiQuotes || !mysqlDDLStatement.MatchString(statement.sql) {
				continue
			}
			checked++
			checkMySQLTestStatement(t, parser, statement)
		}
	}
	t.Logf("checked %d DDL statements of %d statements", checked, len(statements))
}

func checkMySQLTestStatement(t *testing.T, parser *Parser, statement mysqlTestStatement) {
	tree, err := parser.Parse(statement.sql)
	if isMySQLParseError(statement.expectedErrors) {
		assert.Error(t, err, "%s:%d: MySQL rejects this statement as a syntax error\n%s", statement.file, statement.line, statement.sql)
		return
	}
	if err != nil {
		if statement.expectedErrors == "" {
			assert.NoError(t, err, "%s:%d: MySQL accepts this statement\n%s", statement.file, statement.line, statement.sql)
		}
		return
	}
	if ddl, ok := tree.(DDLStatement); ok && !ddl.IsFullyParsed() {
		_, err := parser.ParseStrictDDL(statement.sql)
		if statement.expectedErrors == "" {
			assert.NoError(t, err, "%s:%d: MySQL accepts this statement\n%s", statement.file, statement.line, statement.sql)
		}
		return
	}
	if ddl, ok := tree.(DBDDLStatement); ok && !ddl.IsFullyParsed() {
		_, err := parser.ParseStrictDDL(statement.sql)
		if statement.expectedErrors == "" {
			assert.NoError(t, err, "%s:%d: MySQL accepts this statement\n%s", statement.file, statement.line, statement.sql)
		}
		return
	}
	formatted := String(tree)
	reparsed, err := parser.Parse(formatted)
	if !assert.NoError(t, err, "%s:%d: the formatted statement does not parse\n%s\n%s", statement.file, statement.line, statement.sql, formatted) {
		return
	}
	assert.Equal(t, formatted, String(reparsed), "%s:%d: the formatted statement does not round-trip\n%s", statement.file, statement.line, statement.sql)
}
