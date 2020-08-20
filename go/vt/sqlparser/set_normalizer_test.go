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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeSetExpr(t *testing.T) {
	tests := []struct {
		in, expected, err string
	}{{
		in:       "@@session.x.foo = 42",
		expected: "session `x.foo` = 42",
	}, {
		in:       "@@session.foo = 42",
		expected: "session foo = 42",
	}, {
		in: "session foo = 42",
	}, {
		in: "global foo = 42",
	}, {
		in:       "@@global.foo = 42",
		expected: "global foo = 42",
	}, {
		in:  "global @foo = 42",
		err: "cannot mix scope and user defined variables",
	}, {
		in:  "global @@foo = 42",
		err: "cannot use scope and @@",
	}, {
		in:  "session @@foo = 42",
		err: "cannot use scope and @@",
	}, {
		in:       "foo = 42",
		expected: "session foo = 42",
	}, {
		in:       "@@vitess_metadata.foo = 42",
		expected: "vitess_metadata foo = 42",
	}, {
		in:       "@@x.foo = 42",
		expected: "session `x.foo` = 42",
	}, {
		in:       "@@session.`foo` = 1",
		expected: "session foo = 1",
	}, {
		in:       "@@global.`foo` = 1",
		expected: "global foo = 1",
	}, {
		in:       "@@vitess_metadata.`foo` = 1",
		expected: "vitess_metadata foo = 1",
		//}, { TODO: we should support local scope as well
		//	in:  "local foo = 42",
		//	expected: "session foo = 42",
	}}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if tt.expected == "" {
				tt.expected = tt.in
			}

			statement, err := Parse("set " + tt.in)
			require.NoError(t, err)
			rewriter := setNormalizer{}
			out, err := rewriter.normalizeSetExpr(statement.(*Set).Exprs[0])
			if tt.err != "" {
				require.EqualError(t, err, tt.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, String(out))
			}
		})
	}
}
