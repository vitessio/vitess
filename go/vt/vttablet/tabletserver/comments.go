/*
Copyright 2017 Google Inc.

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

package tabletserver

import "github.com/youtube/vitess/go/vt/sqlparser"

const trailingComment = "_trailingComment"

// stripTrailing strips out trailing comments if any and puts them in a bind variable.
func stripTrailing(sql string, bindVariables map[string]interface{}) string {
	query, comments := sqlparser.SplitTrailingComments(sql)
	if comments != "" {
		bindVariables[trailingComment] = comments
		return query
	}
	return sql
}

// restoreTrailing undoes work done by stripTrailing
func restoreTrailing(sql []byte, bindVars map[string]interface{}) []byte {
	if ytcomment, ok := bindVars[trailingComment]; ok {
		sql = append(sql, ytcomment.(string)...)
	}
	return sql
}
