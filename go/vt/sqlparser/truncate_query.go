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

package sqlparser

import (
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
)

var (
	// TruncateUILen truncate queries in debug UIs to the given length. 0 means unlimited.
	TruncateUILen int

	// TruncateErrLen truncate queries in error logs to the given length. 0 means unlimited.
	TruncateErrLen int
)

func registerQueryTruncationFlags(fs *pflag.FlagSet) {
	fs.IntVar(&TruncateUILen, "sql-max-length-ui", 512, "truncate queries in debug UIs to the given length (default 512)")
	fs.IntVar(&TruncateErrLen, "sql-max-length-errors", 0, "truncate queries in error logs to the given length (default unlimited)")
}

func init() {
	servenv.OnParse(registerQueryTruncationFlags)
}

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
	return truncateQuery(query, TruncateUILen)
}

// TruncateForLog is used when displaying queries as part of error logs
// to avoid overwhelming logging systems with potentially long queries and
// bind value data.
func TruncateForLog(query string) string {
	return truncateQuery(query, TruncateErrLen)
}
