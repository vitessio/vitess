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
)

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

// NormalizeOnlineDDL normalizes a given query for OnlineDDL, possibly exploding it into multiple distinct queries
func NormalizeOnlineDDL(sql string) (normalized []*NormalizedDDLQuery, err error) {
	ddlStmt, action, err := ParseOnlineDDLStatement(sql)
	if err != nil {
		return normalized, err
	}
	switch action {
	case sqlparser.DropDDLAction:
		tables := ddlStmt.GetFromTables()
		for _, table := range tables {
			ddlStmt.SetFromTables([]sqlparser.TableName{table})
			normalized = append(normalized, &NormalizedDDLQuery{SQL: sqlparser.String(ddlStmt), TableName: table})
		}
		return normalized, nil
	}
	if ddlStmt.IsFullyParsed() {
		sql = sqlparser.String(ddlStmt)
	}
	n := &NormalizedDDLQuery{SQL: sql, TableName: ddlStmt.GetTable()}
	return []*NormalizedDDLQuery{n}, nil
}
