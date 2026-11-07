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

// Package mysqltest reads the SQL statements of the test files of MySQL's own
// test suite, which the sqlparser tests check the parser against. The suite is
// GPL licensed, so it is read from a local checkout of
// github.com/mysql/mysql-server rather than copied into this repository.
package mysqltest

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// SourceEnv names the environment variable that points at a checkout of
// github.com/mysql/mysql-server.
const SourceEnv = "VT_MYSQL_SERVER_SOURCE"

// Files returns the paths of the test files under source, a checkout of
// github.com/mysql/mysql-server, and the names that they are reported by. It
// includes the files that tests source, unless tests is set.
func Files(source string, tests bool) (paths, names []string, err error) {
	root := filepath.Join(source, "mysql-test")
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".test") && (tests || !strings.HasSuffix(path, ".inc")) {
			return nil
		}
		name, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		paths = append(paths, path)
		names = append(names, name)
		return nil
	})
	sort.Sort(byName{paths, names})
	return paths, names, err
}

type byName struct{ paths, names []string }

func (b byName) Len() int           { return len(b.names) }
func (b byName) Less(i, j int) bool { return b.names[i] < b.names[j] }
func (b byName) Swap(i, j int) {
	b.paths[i], b.paths[j] = b.paths[j], b.paths[i]
	b.names[i], b.names[j] = b.names[j], b.names[i]
}

// IsDDL reports whether sql is a statement whose syntax Vitess must parse in
// full: CREATE TABLE, ALTER TABLE, CREATE INDEX, CREATE DATABASE or ALTER
// DATABASE.
func IsDDL(sql string) bool {
	return ddlStatement.MatchString(sql)
}

// Statement is one SQL statement taken from a mysqltest file.
type Statement struct {
	File string
	Line int
	SQL  string
	// ExpectedErrors is the argument of the `--error` command that precedes
	// the statement, or empty when MySQL is expected to accept it.
	ExpectedErrors string
	// SQLModeDependent is set when the file enabled ANSI_QUOTES or
	// IGNORE_SPACE before the statement. They change how MySQL reads a
	// double-quoted string and a function name followed by a space, and the
	// parser does not know the session's sql_mode, so such a statement cannot
	// be judged.
	SQLModeDependent bool
}

// testCommand matches the mysqltest commands that can be written
// without the `--` prefix. A statement that starts with one of them is a
// command to the test runner, not SQL.
var testCommand = regexp.MustCompile(`(?i)^\s*(let|eval|connect|connection|disconnect|source|echo|if|while|end|sleep|replace_result|replace_regex|replace_column|exec|perl|send|send_eval|reap|disable_\w+|enable_\w+|query_vertical|sorted_result|error|inc|dec|die|skip|exit|vertical_results|horizontal_results|real_sleep|save_master_pos|sync_slave_with_master|copy_file|remove_file|write_file|append_file|cat_file|file_exists|mkdir|rmdir|list_files\w*|chmod|move_file|shutdown_server|character_set|result_format|lowercase_result|expr|assert|output|change_user|ping|query|send_quit|start_timer|end_timer)\b`)

var (
	errorCommand     = regexp.MustCompile(`(?i)^--\s*error\s+(.*)$`)
	delimiterCommand = regexp.MustCompile(`(?i)^(?:--\s*)?delimiter\s+(\S+)\s*$`)
	sqlModeCommand   = regexp.MustCompile(`(?i)^set\s+(?:@@)?(?:session\s+|global\s+|local\s+)?(?:@@)?(?:session\.|global\.|local\.)?sql_mode\s*=\s*(.*)$`)
)

// reader splits a mysqltest file into its SQL statements, the way
// mysqltest does: a statement ends at the first delimiter outside a quoted
// string, so one line can hold several statements and one statement can span
// several lines.
type reader struct {
	name       string
	statements []Statement
	buffer     strings.Builder
	delimiter  string
	// quote is the quote character of the string that is open at the end of
	// the buffer, or zero when no string is open.
	quote            byte
	firstLine        int
	expectedErrors   string
	sqlModeDependent bool
}

// ReadFile returns the SQL statements of a mysqltest file. It drops
// the statements that refer to mysqltest variables, because their text is only
// known when the test runs.
func ReadFile(path, name string) ([]Statement, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	reader := &reader{name: name, delimiter: ";"}
	for index, line := range strings.Split(string(content), "\n") {
		reader.readLine(index+1, line)
	}
	return reader.statements, nil
}

func (reader *reader) readLine(lineNumber int, line string) {
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
func (reader *reader) readCommand(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return true
	}
	if match := delimiterCommand.FindStringSubmatch(trimmed); match != nil {
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
		if match := errorCommand.FindStringSubmatch(trimmed); match != nil {
			reader.expectedErrors = match[1]
		}
		return true
	}
	return false
}

// scanToDelimiter returns the index of the first delimiter in line that is
// outside a quoted string, or -1 when the statement continues on the next
// line. It keeps track of a string that is still open at the end of the line.
func (reader *reader) scanToDelimiter(line string) int {
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

func (reader *reader) endStatement() {
	sql := strings.TrimSpace(reader.buffer.String())
	reader.buffer.Reset()
	reader.quote = 0
	expectedErrors := reader.expectedErrors
	reader.expectedErrors = ""
	if sql == "" || testCommand.MatchString(sql) || strings.Contains(sql, "$") {
		return
	}
	if match := sqlModeCommand.FindStringSubmatch(sql); match != nil {
		mode := strings.ToUpper(match[1])
		reader.sqlModeDependent = strings.Contains(mode, "ANSI") || strings.Contains(mode, "IGNORE_SPACE")
	}
	reader.statements = append(reader.statements, Statement{
		File:             reader.name,
		Line:             reader.firstLine,
		SQL:              sql,
		ExpectedErrors:   expectedErrors,
		SQLModeDependent: reader.sqlModeDependent,
	})
}

// ddlStatement matches the statements whose syntax Vitess must parse in
// full: a DDL statement that the parser does not understand is rejected, so
// every form that MySQL accepts has to be in the grammar.
var ddlStatement = regexp.MustCompile(`(?is)^(create\s+(/\*.*?\*/\s*)?(temporary\s+)?table|alter\s+(/\*.*?\*/\s*)?table|create\s+(/\*.*?\*/\s*)?(unique\s+|fulltext\s+|spatial\s+)?index|create\s+(/\*.*?\*/\s*)?(database|schema)|alter\s+(/\*.*?\*/\s*)?(database|schema))\b`)

// IsParseError reports whether the expected errors of a statement say
// that MySQL rejects its syntax. A list of alternatives that also allows a
// different error does not say that, because the statement may parse.
func IsParseError(expectedErrors string) bool {
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
