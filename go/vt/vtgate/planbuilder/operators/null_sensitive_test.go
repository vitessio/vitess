/*
Copyright 2023 The Vitess Authors.

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

package operators

import (
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/require"
)

func TestNotTrueWhenRootIsNULL(t *testing.T) {
	// This test checks whether the function NotTrueWhenRootIsNULL
	// returns true when the expression is not true when the root is NULL. The name kind of gives it away.

	testcases := []struct {
		expr     string
		expected bool
	}{
		// The following expressions are guaranteed to return false or NULL if the root is NULL.
		{expr: "root", expected: true},
		{expr: "root + 1", expected: true},
		{expr: "root = 42", expected: true},
		{expr: "root is true", expected: true},
		{expr: "root is false", expected: true},
		{expr: "root is not null", expected: true},
		{expr: "root between 100 and 200", expected: true},
		{expr: "root = 42 and somethingElse", expected: true},

		// The following expressions can return true even if the root is NULL
		{expr: "root is null"},
		{expr: "root is not false"},
		{expr: "root is not true"},
		{expr: "coalesce(root, 1)"},
		{expr: "root <=> 42"}}

	for _, testcase := range testcases {
		t.Run(testcase.expr, func(t *testing.T) {
			expr, err := sqlparser.ParseExpr(testcase.expr)
			require.NoError(t, err)
			res := notTrueWhenRootIsNULL(expr, func(node *sqlparser.ColName) bool {
				return node.Name.EqualString("root")
			})
			require.Equal(t, testcase.expected, res)
		})
	}
}
