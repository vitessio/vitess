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
	// truncateUILen truncate queries in debug UIs to the given length. 0 means unlimited.
	truncateUILen = 512

	// truncateErrLen truncate queries in error logs to the given length. 0 means unlimited.
	truncateErrLen = 0
)

const TruncationText = "[TRUNCATED]"

func registerQueryTruncationFlags(fs *pflag.FlagSet) {
	fs.IntVar(&truncateUILen, "sql-max-length-ui", truncateUILen, "truncate queries in debug UIs to the given length (default 512)")
	fs.IntVar(&truncateErrLen, "sql-max-length-errors", truncateErrLen, "truncate queries in error logs to the given length (default unlimited)")
}

func init() {
	for _, cmd := range []string{
		"vtgate",
		"vttablet",
		"vtcombo",
		"vtctld",
		"vtctl",
		"vtexplain",
		"vtbackup",
		"vttestserver",
		"vtbench",
	} {
		servenv.OnParseFor(cmd, registerQueryTruncationFlags)
	}
}

// GetTruncateErrLen is a function used to read the value of truncateErrLen
func GetTruncateErrLen() int {
	return truncateErrLen
}

// SetTruncateErrLen is a function used to override the value of truncateErrLen
// It is only meant to be used from tests and not from production code.
func SetTruncateErrLen(errLen int) {
	truncateErrLen = errLen
}

func truncateQuery(query string, max int) string {
	sql, comments := SplitMarginComments(query)

	if max == 0 || len(sql) <= max {
		return comments.Leading + sql + comments.Trailing
	}

	return comments.Leading + sql[:max-(len(TruncationText)+1)] + " " + TruncationText + comments.Trailing
}

// TruncateForUI is used when displaying queries on various Vitess status pages
// to keep the pages small enough to load and render properly
func TruncateForUI(query string) string {
	return truncateQuery(query, truncateUILen)
}

// TruncateForLog is used when displaying queries as part of error logs
// to avoid overwhelming logging systems with potentially long queries and
// bind value data.
func TruncateForLog(query string) string {
	return truncateQuery(query, truncateErrLen)
}
