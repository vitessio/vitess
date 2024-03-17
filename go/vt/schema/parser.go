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
	"strconv"
	"strings"

	"vitess.io/vitess/go/sqltypes"
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
	revertStatementRegexp = regexp.MustCompile(`(?i)^revert\s+([\S]*)$`)

	enumValuesRegexp = regexp.MustCompile("(?i)^enum[(](.*)[)]$")
	setValuesRegexp  = regexp.MustCompile("(?i)^set[(](.*)[)]$")
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

// ParseEnumValues parses the comma delimited part of an enum column definition
func ParseEnumValues(enumColumnType string) string {
	if submatch := enumValuesRegexp.FindStringSubmatch(enumColumnType); len(submatch) > 0 {
		return submatch[1]
	}
	return enumColumnType
}

// ParseSetValues parses the comma delimited part of a set column definition
func ParseSetValues(setColumnType string) string {
	if submatch := setValuesRegexp.FindStringSubmatch(setColumnType); len(submatch) > 0 {
		return submatch[1]
	}
	return setColumnType
}

// parseEnumOrSetTokens parses the comma delimited part of an enum/set column definition and
// returns the (unquoted) text values
// Expected input: `'x-small','small','medium','large','x-large'`
// Unexpected input: `enum('x-small','small','medium','large','x-large')`
func parseEnumOrSetTokens(enumOrSetValues string) []string {
	// We need to track both the start of the current value and current
	// position, since there might be quoted quotes inside the value
	// which we need to handle.
	start := 0
	pos := 1
	var tokens []string
	for {
		// If the input does not start with a quote, it's not a valid enum/set definition
		if enumOrSetValues[start] != '\'' {
			return nil
		}
		i := strings.IndexByte(enumOrSetValues[pos:], '\'')
		// If there's no closing quote, we have invalid input
		if i < 0 {
			return nil
		}
		// We're at the end here of the last quoted value,
		// so we add the last token and return them.
		if i == len(enumOrSetValues[pos:])-1 {
			tok, err := sqltypes.DecodeStringSQL(enumOrSetValues[start:])
			if err != nil {
				return nil
			}
			tokens = append(tokens, tok)
			return tokens
		}
		// MySQL double quotes things as escape value, so if we see another
		// single quote, we skip the character and remove it from the input.
		if enumOrSetValues[pos+i+1] == '\'' {
			pos = pos + i + 2
			continue
		}
		// Next value needs to be a comma as a separator, otherwise
		// the data is invalid so we return nil.
		if enumOrSetValues[pos+i+1] != ',' {
			return nil
		}
		// If we're at the end of the input here, it's invalid
		// since we have a trailing comma which is not what MySQL
		// returns.
		if pos+i+1 == len(enumOrSetValues) {
			return nil
		}

		tok, err := sqltypes.DecodeStringSQL(enumOrSetValues[start : pos+i+1])
		if err != nil {
			return nil
		}

		tokens = append(tokens, tok)
		// We add 2 to the position to skip the closing quote & comma
		start = pos + i + 2
		pos = start + 1
	}
}

// ParseEnumOrSetTokensMap parses the comma delimited part of an enum column definition
// and returns a map where ["1"] is the first token, and ["<n>"] is th elast token
func ParseEnumOrSetTokensMap(enumOrSetValues string) map[string]string {
	tokens := parseEnumOrSetTokens(enumOrSetValues)
	tokensMap := map[string]string{}
	for i, token := range tokens {
		tokensMap[strconv.Itoa(i+1)] = token
	}
	return tokensMap
}
