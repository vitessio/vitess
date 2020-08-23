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
)

var (
	// ALTER TABLE
	// ALTER WITH_GHOST TABLE
	// ALTER WITH_GHOST LAG_'--max-lag-millis=2.5 --throttle-http=...' TABLE
	// ALTER WITH_PT TABLE
	alterTableBasicPattern               = `(?i)\balter\s+(with\s+|\s+|).*?table\s+`
	alterTableExplicitSchemaTableRegexps = []*regexp.Regexp{
		// ALTER TABLE `scm`.`tbl` something
		// ALTER WITH_GHOST TABLE `scm`.`tbl` something
		// ALTER WITH_PT TABLE `scm`.`tbl` something
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
// - alter roptions (anything that follows ALTER ... TABLE)
func ParseAlterTableOptions(alterStatement string) (explicitSchema, explicitTable, alterOptions string) {
	alterOptions = strings.TrimSpace(alterStatement)
	for _, alterTableRegexp := range alterTableExplicitSchemaTableRegexps {
		if submatch := alterTableRegexp.FindStringSubmatch(alterOptions); len(submatch) > 0 {
			explicitSchema = submatch[2]
			explicitTable = submatch[3]
			alterOptions = submatch[4]
			return explicitSchema, explicitTable, alterOptions
		}
	}
	for _, alterTableRegexp := range alterTableExplicitTableRegexps {
		if submatch := alterTableRegexp.FindStringSubmatch(alterOptions); len(submatch) > 0 {
			explicitTable = submatch[2]
			alterOptions = submatch[3]
			return explicitSchema, explicitTable, alterOptions
		}
	}
	return explicitSchema, explicitTable, alterOptions
}

// RemoveOnlineDDLHints removes a WITH_GHOST or WITH_PT hint, which is vitess-specific,
// from an ALTER TABLE statement
// e.g "ALTER WITH_GHOST TABLE my_table DROP COLUMN i" -> "ALTER TABLE `my_table` DROP COLUMN i"
func RemoveOnlineDDLHints(alterStatement string) (normalizedAlterStatement string) {
	explicitSchema, explicitTable, alterOptions := ParseAlterTableOptions(alterStatement)

	if explicitTable == "" {
		return alterOptions
	}
	if explicitSchema == "" {
		return fmt.Sprintf("ALTER TABLE `%s` %s", explicitTable, alterOptions)
	}
	return fmt.Sprintf("ALTER TABLE `%s`.`%s` %s", explicitSchema, explicitTable, alterOptions)
}
