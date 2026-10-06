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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLiteralEncodingContexts(t *testing.T) {
	parser := NewTestParser()
	for _, tc := range []struct {
		name        string
		query       string
		want        string
		expressions int
	}{
		{name: "expression", query: "select cast('é\\n' as char character set latin1) from t", expressions: 1},
		{name: "introducer and expression", query: "select _latin1 'é\\n', 'é\\n' from t", expressions: 1},
		{name: "national Unicode", query: "select N'é' from t", want: "select N'é' from t"},
		{name: "national quote", query: "select N'é''' from t", want: "select N'é''' from t"},
		{name: "national backslash", query: "select N'é\\\\' from t", want: "select _utf8mb3 '\\\xc3\\\xa9\\\\' from t"},
		{name: "national NUL", query: "select N'é\\0' from t", want: "select _utf8mb3 '\\\xc3\\\xa9\\0' from t"},
		{name: "national backspace", query: "select N'é\\b' from t", want: "select _utf8mb3 '\\\xc3\\\xa9\\b' from t"},
		{name: "national newline", query: "select N'é\\n' from t", want: "select _utf8mb3 '\\\xc3\\\xa9\\n' from t"},
		{name: "national carriage return", query: "select N'é\\r' from t", want: "select _utf8mb3 '\\\xc3\\\xa9\\r' from t"},
		{name: "national tab", query: "select N'é\\t' from t", want: "select _utf8mb3 '\\\xc3\\\xa9\\t' from t"},
		{name: "national ctrl-Z", query: "select N'é\\Z' from t", want: "select _utf8mb3 '\\\xc3\\\xa9\\Z' from t"},
		{name: "national percent passthrough", query: "select N'é\\%' from t", want: "select _utf8mb3 '\\\xc3\\\xa9\\%' from t"},
		{name: "national underscore passthrough", query: "select N'é\\_' from t", want: "select _utf8mb3 '\\\xc3\\\xa9\\_' from t"},
		{name: "national ASCII passthrough", query: "select N'a\\%' from t", want: "select N'a\\%' from t"},
		{name: "national nonempty prefix", query: "select N'aé\\n' from t", want: "select _utf8mb3 'a\\\xc3\\\xa9\\n' from t"},
		{name: "national quote then escape", query: "select N'é''é\\n' from t", want: "select _utf8mb3 'é''\\\xc3\\\xa9\\n' from t"},
		{name: "show like", query: "show tables like 'é\\n'"},
		{name: "set names", query: "set names 'é\\n'"},
		{name: "SQLSTATE", query: "create procedure p() begin declare c condition for sqlstate 'é\\n'; end"},
		{name: "separator", query: "select group_concat(c separator 'é\\n') from t"},
		{name: "prepare", query: "prepare s from 'select é\\n'"},
		{name: "enum and comments", query: "create table t (a enum('é\\n'), b varchar(10) comment 'é\\n') comment='é\\n'"},
		{name: "column attributes", query: "create table t (a int engine_attribute='é\\n' secondary_engine_attribute='é\\n')"},
		{name: "index comment", query: "create table t (a int, key k (a) comment 'é\\n')"},
		{name: "partition comment", query: "create table t (a int) partition by range(a) (partition p0 values less than (10) comment 'é\\n')"},
		{name: "table option", query: "alter table t comment='é\\n'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.ParseStrictDDL(tc.query)
			require.NoError(t, err)
			var formatted string
			for _, fast := range []bool{false, true} {
				t.Run(fmt.Sprintf("fast=%v", fast), func(t *testing.T) {
					buf := NewTrackedBuffer(nil)
					buf.fast = fast
					buf.WriteNode(stmt)
					got := buf.String()
					if tc.want != "" {
						assert.Equal(t, tc.want, got)
					} else {
						assert.Equal(t, tc.expressions, strings.Count(got, "'' "), got)
					}
					parsed, err := parser.ParseStrictDDL(got)
					require.NoError(t, err, got)
					assert.Equal(t, got, String(parsed))
					if fast {
						assert.Equal(t, formatted, got)
					} else {
						formatted = got
					}
				})
			}
			t.Run("uppercase", func(t *testing.T) {
				buf := NewTrackedBuffer(nil)
				buf.SetUpperCase(true)
				buf.WriteNode(stmt)
				parsed, err := parser.ParseStrictDDL(buf.String())
				require.NoError(t, err)
				reformatted := NewTrackedBuffer(nil)
				reformatted.SetUpperCase(true)
				reformatted.WriteNode(parsed)
				assert.Equal(t, buf.String(), reformatted.String())
			})
		})
	}
}
