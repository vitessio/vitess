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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
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

// parseEnumOrSetTokens parses the comma delimited part of an enum/set column definition and
// returns the (unquoted) text values
// Expected input: `'x-small','small','medium','large','x-large'`
// Unexpected input: `enum('x-small','small','medium','large','x-large')`
func parseEnumOrSetTokens(env *vtenv.Environment, enumOrSetValues string) ([]string, error) {
	// sqlparser cannot directly parse enum/set values, so we create a dummy query to parse it.
	dummyQuery := fmt.Sprintf("alter table t add column e enum(%s)", enumOrSetValues)
	ddlStmt, err := env.Parser().ParseStrictDDL(dummyQuery)
	if err != nil {
		return nil, err
	}
	unexpectedError := func() error {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected error parsing enum values: %v", enumOrSetValues)
	}
	alterTable, ok := ddlStmt.(*sqlparser.AlterTable)
	if !ok {
		return nil, unexpectedError()
	}
	if len(alterTable.AlterOptions) != 1 {
		return nil, unexpectedError()
	}
	addColumn, ok := alterTable.AlterOptions[0].(*sqlparser.AddColumns)
	if !ok {
		return nil, unexpectedError()
	}
	if len(addColumn.Columns) != 1 {
		return nil, unexpectedError()
	}
	enumValues := addColumn.Columns[0].Type.EnumValues
	decodedEnumValues := make([]string, len(enumValues))
	for i := range enumValues {
		val, err := sqltypes.DecodeStringSQL(enumValues[i])
		if err != nil {
			return nil, err
		}
		decodedEnumValues[i] = val
	}
	return decodedEnumValues, nil
}

// ParseEnumOrSetTokensMap parses the comma delimited part of an enum column definition
// and returns a map where [1] is the first token, and [<n>] is the last.
func ParseEnumOrSetTokensMap(env *vtenv.Environment, enumOrSetValues string) (map[int]string, error) {
	tokens, err := parseEnumOrSetTokens(env, enumOrSetValues)
	if err != nil {
		return nil, err
	}
	tokensMap := map[int]string{}
	for i, token := range tokens {
		// SET and ENUM values are 1 indexed.
		tokensMap[i+1] = token
	}
	return tokensMap, nil
}
