/*
Copyright 2021 The Vitess Authors.

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

package abstract

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestName(t *testing.T) {
	testCases := []struct {
		query, expected string
	}{
		{
			query: "select intcol, count(*) from user group by 1",
			expected: `
{
  "Select": [
    "intcol",
    "aggr: count(*)"
  ],
  "Grouping": [
    "intcol"
  ],
  "OrderBy": []
}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.query, func(t *testing.T) {
			ast, err := sqlparser.Parse(tc.query)
			require.NoError(t, err)
			sel := ast.(*sqlparser.Select)
			qp, err := CreateQPFromSelect(sel)
			require.NoError(t, err)
			require.Equal(t, tc.expected[1:], qp.ToString())
		})
	}
}
