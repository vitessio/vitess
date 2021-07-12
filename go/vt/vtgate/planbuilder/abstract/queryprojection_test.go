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
			query: "select intcol, count(*) from user group by intcol",
			expected: `
{
  "Select": [
    "intcol",
    "aggr: count(*)"
  ],
  "Grouping": [
    "intcol"
  ],
  "OrderBy": [
    "intcol asc"
  ]
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
