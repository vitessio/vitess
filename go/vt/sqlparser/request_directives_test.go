/*
Copyright 2026 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReplaceRequestDirectives checks which comment directive blocks are
// replaced with a marker block, what the marker block keeps, and that Restore
// gives back the statement's text.
func TestReplaceRequestDirectives(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		// want is the text of the statement with its markers, or empty if
		// nothing is replaced.
		want string
	}{{
		name: "no comments",
		sql:  "select * from t",
	}, {
		name: "comment that isn't a directive block",
		sql:  "select /* WORKLOAD_NAME=w1 */ * from t",
	}, {
		name: "block without a request-only directive",
		sql:  "select /*vt+ PLANNER=gen4 */ * from t",
	}, {
		name: "block with request-only directives",
		sql:  "select /*vt+ WORKLOAD_NAME=w1 PRIORITY=10 CONSOLIDATOR=enabled IGNORE_MAX_MEMORY_ROWS=1 IGNORE_MAX_PAYLOAD_SIZE=1 */ * from t",
		want: "select /*vt+ VTGATE_REQUEST_DIRECTIVES */ * from t",
	}, {
		name: "lower case request-only directive",
		sql:  "select /*vt+ workload_name=w1 */ * from t",
		want: "select /*vt+ VTGATE_REQUEST_DIRECTIVES */ * from t",
	}, {
		name: "block that also has a directive that affects the plan",
		sql:  "select /*vt+ WORKLOAD_NAME=w1 PLANNER=gen4 */ * from t",
		want: "select /*vt+ PLANNER=gen4 VTGATE_REQUEST_DIRECTIVES */ * from t",
	}, {
		name: "several blocks",
		sql:  "select /* leading */ /*vt+ WORKLOAD_NAME=w1 */ /*vt+ PLANNER=gen4 */ /*vt+ PRIORITY=10 */ * from t",
		want: "select /* leading */ /*vt+ VTGATE_REQUEST_DIRECTIVES */ /*vt+ PLANNER=gen4 */ /*vt+ VTGATE_REQUEST_DIRECTIVES_2 */ * from t",
	}, {
		name: "directive that runs into the end of the comment, which Directives ignores",
		sql:  "select /*vt+ WORKLOAD_NAME=w1*/ * from t",
	}, {
		name: "statement that already has the marker",
		sql:  "select /*vt+ WORKLOAD_NAME=w1 */ * from t where a = 'VTGATE_REQUEST_DIRECTIVES'",
	}, {
		name: "statement that has the marker behind an escape sequence",
		sql:  "select /*vt+ WORKLOAD_NAME=w1 */ * from t where a = '/*vt+ VTGATE_\\REQUEST_DIRECTIVES */'",
	}, {
		name: "block in a subquery, which VTGate doesn't read",
		sql:  "select * from t where a in (select /*vt+ WORKLOAD_NAME=w1 */ a from u)",
	}, {
		name: "union",
		sql:  "select /*vt+ WORKLOAD_NAME=w1 */ a from t union select /*vt+ WORKLOAD_NAME=w1 */ a from u",
		want: "select /*vt+ VTGATE_REQUEST_DIRECTIVES */ a from t union select /*vt+ WORKLOAD_NAME=w1 */ a from u",
	}, {
		name: "insert",
		sql:  "insert /*vt+ PRIORITY=10 */ into t(a) values (1)",
		want: "insert /*vt+ VTGATE_REQUEST_DIRECTIVES */ into t(a) values (1)",
	}, {
		name: "update",
		sql:  "update /*vt+ PRIORITY=10 */ t set a = 1",
		want: "update /*vt+ VTGATE_REQUEST_DIRECTIVES */ t set a = 1",
	}, {
		name: "delete",
		sql:  "delete /*vt+ PRIORITY=10 */ from t",
		want: "delete /*vt+ VTGATE_REQUEST_DIRECTIVES */ from t",
	}}

	parser := NewTestParser()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.Parse(tc.sql)
			require.NoError(t, err)
			original := String(stmt)

			rd := ReplaceRequestDirectives(stmt)
			if tc.want == "" {
				assert.Nil(t, rd)
				assert.Equal(t, original, String(stmt))
				return
			}
			require.NotNil(t, rd)
			assert.Equal(t, tc.want, String(stmt))
			assert.Equal(t, original, rd.Restore(String(stmt)))
		})
	}
}

// TestReplaceRequestDirectivesSharesText checks that statements that differ
// only in their request-only directives format to the same text once they
// are replaced, and that each one's own blocks are restored.
func TestReplaceRequestDirectivesSharesText(t *testing.T) {
	queries := []string{
		"select /*vt+ PLANNER=gen4 WORKLOAD_NAME=w1 */ /*vt+ PRIORITY=10 */ * from t",
		"select /*vt+ WORKLOAD_NAME=w2 PLANNER=gen4 */ /*vt+ CONSOLIDATOR=disabled PRIORITY=20 */ * from t",
	}
	parser := NewTestParser()
	var texts []string
	for _, sql := range queries {
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)
		original := String(stmt)
		rd := ReplaceRequestDirectives(stmt)
		require.NotNil(t, rd)
		texts = append(texts, String(stmt))
		assert.Equal(t, original, rd.Restore(String(stmt)))
	}
	assert.Equal(t, texts[0], texts[1])
}

// TestRequestDirectivesRestore checks that Restore leaves a query without a
// marker as it is, and replaces every copy of a marker.
func TestRequestDirectivesRestore(t *testing.T) {
	var none *RequestDirectives
	assert.Equal(t, "select /*vt+ VTGATE_REQUEST_DIRECTIVES */ 1", none.Restore("select /*vt+ VTGATE_REQUEST_DIRECTIVES */ 1"))

	stmt, err := NewTestParser().Parse("select /*vt+ WORKLOAD_NAME=w1 */ 1")
	require.NoError(t, err)
	rd := ReplaceRequestDirectives(stmt)
	require.NotNil(t, rd)
	assert.Equal(t, "select 1 from dual", rd.Restore("select 1 from dual"))
	assert.Equal(t,
		"select /*vt+ WORKLOAD_NAME=w1 */ 1 union select /*vt+ WORKLOAD_NAME=w1 */ 2",
		rd.Restore("select /*vt+ VTGATE_REQUEST_DIRECTIVES */ 1 union select /*vt+ VTGATE_REQUEST_DIRECTIVES */ 2"))
}
