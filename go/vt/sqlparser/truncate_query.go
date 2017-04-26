// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"flag"
)

var (
	// Maximum length for a query in a sqlerror string. 0 means unlimited.
	TruncateUILen  = flag.Int("sql-max-length-ui", 512, "truncate queries in debug UIs to the given length (default 512)")
	TruncateErrLen = flag.Int("sql-max-length-errors", 0, "truncate queries in error logs to the given length (default unlimited)")
)

func truncateQuery(query string, max int) string {
	sql, comments := SplitTrailingComments(query)

	if max == 0 || len(sql) <= max {
		return sql + comments
	}

	return sql[:max-12] + " [TRUNCATED] " + comments
}

// TruncateForUI is used when displaying queries on various Vitess status pages
// to keep the pages small enough to load and render properly
func TruncateForUI(query string) string {
	return truncateQuery(query, *TruncateUILen)
}

// TruncateForError is used when displaying queries as part of error logs
// to avoid overwhelming logging systems with potentially long queries and
// bind value data.
func TruncateForError(query string) string {
	return truncateQuery(query, *TruncateErrLen)
}
