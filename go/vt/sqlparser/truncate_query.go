package sqlparser

import (
	"flag"
)

var (
	// TruncateUILen truncate queries in debug UIs to the given length. 0 means unlimited.
	TruncateUILen = flag.Int("sql-max-length-ui", 512, "truncate queries in debug UIs to the given length (default 512)")

	// TruncateErrLen truncate queries in error logs to the given length. 0 means unlimited.
	TruncateErrLen = flag.Int("sql-max-length-errors", 0, "truncate queries in error logs to the given length (default unlimited)")
)

func truncateQuery(query string, max int) string {
	sql, comments := SplitMarginComments(query)

	if max == 0 || len(sql) <= max {
		return comments.Leading + sql + comments.Trailing
	}

	return comments.Leading + sql[:max-12] + " [TRUNCATED]" + comments.Trailing
}

// TruncateForUI is used when displaying queries on various Vitess status pages
// to keep the pages small enough to load and render properly
func TruncateForUI(query string) string {
	return truncateQuery(query, *TruncateUILen)
}

// TruncateForLog is used when displaying queries as part of error logs
// to avoid overwhelming logging systems with potentially long queries and
// bind value data.
func TruncateForLog(query string) string {
	return truncateQuery(query, *TruncateErrLen)
}
