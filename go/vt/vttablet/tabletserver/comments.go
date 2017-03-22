// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
