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

package schema

import (
	"fmt"
	"regexp"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// NormalizedDDLQuery contains a query which is online-ddl -normalized
type NormalizedDDLQuery struct {
	SQL       string
	TableName sqlparser.TableName
}

var (
	// ALTER TABLE
	alterTableBasicPattern               = `(?s)(?i)\balter\s+table\s+`
	alterTableExplicitSchemaTableRegexps = []*regexp.Regexp{
		// ALTER TABLE `scm`.`tbl` something
		regexp.MustCompile(alterTableBasicPattern + "`" + `([^` + "`" + `]+)` + "`" + `[.]` + "`" + `([^` + "`" + `]+)` + "`" + `\s+(.*$)`),
		// ALTER TABLE `scm`.tbl something
		regexp.MustCompile(alterTableBasicPattern + "`" + `([^` + "`" + `]+)` + "`" + `[.]([\S]+)\s+(.*$)`),
		// ALTER TABLE scm.`tbl` something
		regexp.MustCompile(alterTableBasicPattern + `([\S]+)[.]` + "`" + `([^` + "`" + `]+)` + "`" + `\s+(.*$)`),
		// ALTER TABLE scm.tbl something
		regexp.MustCompile(alterTableBasicPattern + `([\S]+)[.]([\S]+)\s+(.*$)`),
	}
	alterTableExplicitTableRegexps = []*regexp.Regexp{
		// ALTER TABLE `tbl` something
		regexp.MustCompile(alterTableBasicPattern + "`" + `([^` + "`" + `]+)` + "`" + `\s+(.*$)`),
		// ALTER TABLE tbl something
		regexp.MustCompile(alterTableBasicPattern + `([\S]+)\s+(.*$)`),
	}
	createTableRegexp     = regexp.MustCompile(`(?s)(?i)(CREATE\s+TABLE\s+)` + "`" + `([^` + "`" + `]+)` + "`" + `(\s*[(].*$)`)
	revertStatementRegexp = regexp.MustCompile(`(?i)^revert\s+([\S]*)$`)
)

// ReplaceTableNameInCreateTableStatement returns a modified CREATE TABLE statement, such that the table name is replaced with given name.
// This intentionally string-replacement based, and not sqlparser.String() based, because the return statement has to be formatted _precisely_,
// up to MySQL version nuances, like the original statement. That's in favor of tengo table comparison.
// We expect a well formatted, no-qualifier statement in the form:
// CREATE TABLE `some_table` ...
func ReplaceTableNameInCreateTableStatement(createStatement string, replacementName string) (modifiedStatement string, err error) {
	submatch := createTableRegexp.FindStringSubmatch(createStatement)
	if len(submatch) == 0 {
		return createStatement, fmt.Errorf("could not parse statement: %s", createStatement)
	}
	return fmt.Sprintf("%s`%s`%s", submatch[1], replacementName, submatch[3]), nil
}

// ParseAlterTableOptions parses a ALTER ... TABLE... statement into:
// - explicit schema and table, if available
// - alter options (anything that follows ALTER ... TABLE)
func ParseAlterTableOptions(alterStatement string) (explicitSchema, explicitTable, alterOptions string) {
	alterOptions = strings.TrimSpace(alterStatement)
	for _, alterTableRegexp := range alterTableExplicitSchemaTableRegexps {
		if submatch := alterTableRegexp.FindStringSubmatch(alterOptions); len(submatch) > 0 {
			explicitSchema = submatch[1]
			explicitTable = submatch[2]
			alterOptions = submatch[3]
			return explicitSchema, explicitTable, alterOptions
		}
	}
	for _, alterTableRegexp := range alterTableExplicitTableRegexps {
		if submatch := alterTableRegexp.FindStringSubmatch(alterOptions); len(submatch) > 0 {
			explicitTable = submatch[1]
			alterOptions = submatch[2]
			return explicitSchema, explicitTable, alterOptions
		}
	}
	return explicitSchema, explicitTable, alterOptions
}

// legacyParseRevertUUID expects a query like "revert 4e5dcf80_354b_11eb_82cd_f875a4d24e90" and returns the UUID value.
func legacyParseRevertUUID(sql string) (uuid string, err error) {
	submatch := revertStatementRegexp.FindStringSubmatch(sql)
	if len(submatch) == 0 {
		return "", fmt.Errorf("Not a Revert DDL: '%s'", sql)
	}
	uuid = submatch[1]
	if !IsOnlineDDLUUID(uuid) {
		return "", fmt.Errorf("Not an online DDL UUID: '%s'", uuid)
	}
	return uuid, nil
}
