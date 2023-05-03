/*
Copyright 2022 The Vitess Authors.

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

func TestSimplifyExpression(in *testing.T) {
	tests := []struct {
		in       string
		expected string
	}{{
		in:       "not (not A = 3)",
		expected: "A = 3",
	}, {
		in:       "not (A = 3 and B = 2)",
		expected: "not A = 3 or not B = 2",
	}, {
		in:       "not (A = 3 or B = 2)",
		expected: "not A = 3 and not B = 2",
	}, {
		in:       "A xor B",
		expected: "(A or B) and not (A and B)",
	}, {
		in:       "(A and B) or C",
		expected: "(A or C) and (B or C)",
	}, {
		in:       "C or (A and B)",
		expected: "(C or A) and (C or B)",
	}, {
		in:       "A and A",
		expected: "A",
	}, {
		in:       "A OR A",
		expected: "A",
	}, {
		in:       "A OR (A AND B)",
		expected: "A",
	}, {
		in:       "A OR (B AND A)",
		expected: "A",
	}, {
		in:       "(A AND B) OR A",
		expected: "A",
	}, {
		in:       "(B AND A) OR A",
		expected: "A",
	}, {
		in:       "(A and B) and (B and A)",
		expected: "A and B",
	}, {
		in:       "(A or B) and A",
		expected: "A",
	}, {
		in:       "A and (A or B)",
		expected: "A",
	}, {
		in:       "(A and B) OR (A and C)",
		expected: "A and (B or C)",
	}, {
		in:       "(A and B) OR (C and A)",
		expected: "A and (B or C)",
	}, {
		in:       "(B and A) OR (A and C)",
		expected: "A and (B or C)",
	}, {
		in:       "(B and A) OR (C and A)",
		expected: "A and (B or C)",
	}}

	for _, tc := range tests {
		in.Run(tc.in, func(t *testing.T) {
			expr, err := ParseExpr(tc.in)
			require.NoError(t, err)

			expr, didRewrite := simplifyExpression(expr)
			assert.True(t, didRewrite.changed())
			assert.Equal(t, tc.expected, String(expr))
		})
	}
}

func TestRewritePredicate(in *testing.T) {
	tests := []struct {
		in       string
		expected string
	}{{
		in:       "A xor B",
		expected: "(A or B) and (not A or not B)",
	}, {
		in:       "(A and B) and (B and A) and (B and A) and (A and B)",
		expected: "A and B",
	}, {
		in:       "((A and B) OR (A and C) OR (A and D)) and E and F",
		expected: "A and (B or C or D) and E and F",
	}, {
		in:       "(A and B) OR (A and C)",
		expected: "A and (B or C)",
	}, {
		in:       "(A and B) OR (C and A)",
		expected: "A and (B or C)",
	}, {
		in:       "(B and A) OR (A and C)",
		expected: "A and (B or C)",
	}, {
		in:       "(A and B) or (A and C) or (A and D)",
		expected: "A and (B or C or D)",
	}, {
		in:       "(a=1 or a IN (1,2)) or (a = 2 or a = 3)",
		expected: "a in (1, 2, 3)",
	}, {
		in:       "A and (B or A)",
		expected: "A",
	}}

	for _, tc := range tests {
		in.Run(tc.in, func(t *testing.T) {
			expr, err := ParseExpr(tc.in)
			require.NoError(t, err)

			output := RewritePredicate(expr)
			assert.Equal(t, tc.expected, String(output))
		})
	}
}

func TestExtractINFromOR(in *testing.T) {
	tests := []struct {
		in       string
		expected string
	}{{
		in:       "(A and B) or (B and A)",
		expected: "<nil>",
	}, {
		in:       "(a = 5 and B) or A",
		expected: "<nil>",
	}, {
		in:       "a = 5 and B or a = 6 and C",
		expected: "a in (5, 6)",
	}, {
		in:       "(a = 5 and b = 1 or b = 2 and a = 6)",
		expected: "a in (5, 6) and b in (1, 2)",
	}, {
		in:       "(a in (1,5) and B or C and a = 6)",
		expected: "a in (1, 5, 6)",
	}, {
		in:       "(a in (1, 5) and B or C and a in (5, 7))",
		expected: "a in (1, 5, 7)",
	}}

	for _, tc := range tests {
		in.Run(tc.in, func(t *testing.T) {
			expr, err := ParseExpr(tc.in)
			require.NoError(t, err)

			output := ExtractINFromOR(expr.(*OrExpr))
			assert.Equal(t, tc.expected, String(AndExpressions(output...)))
		})
	}
}
