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

const TruncationText = "[TRUNCATED]"

// GetTruncateErrLen is a function used to read the value of truncateErrLen
func (p *Parser) GetTruncateErrLen() int {
	return p.truncateErrLen
}

func TruncateQuery(query string, max int) string {
	sql, comments := SplitMarginComments(query)

	if max == 0 || len(sql) <= max || len(sql) < len(TruncationText) {
		return comments.Leading + sql + comments.Trailing
	}

	if max < len(TruncationText)+1 {
		max = len(TruncationText) + 1
	}

	return comments.Leading + sql[:max-(len(TruncationText)+1)] + " " + TruncationText + comments.Trailing
}

// TruncateForUI is used when displaying queries on various Vitess status pages
// to keep the pages small enough to load and render properly
func (p *Parser) TruncateForUI(query string) string {
	return TruncateQuery(query, p.truncateUILen)
}

// TruncateForLog is used when displaying queries as part of error logs
// to avoid overwhelming logging systems with potentially long queries and
// bind value data.
func (p *Parser) TruncateForLog(query string) string {
	return TruncateQuery(query, p.truncateErrLen)
}
