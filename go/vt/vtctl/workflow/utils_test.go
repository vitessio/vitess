package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func getInKeyRange() *sqlparser.FuncExpr {
	subExprs := make(sqlparser.SelectExprs, 0, 2)
	subExprs = append(subExprs, &sqlparser.AliasedExpr{Expr: sqlparser.NewStrLiteral("vindex1")})
	subExprs = append(subExprs, &sqlparser.AliasedExpr{Expr: sqlparser.NewStrLiteral("-80")})
	inKeyRange := &sqlparser.FuncExpr{
		Name:  sqlparser.NewIdentifierCI("in_keyrange"),
		Exprs: subExprs,
	}
	return inKeyRange
}

func TestAdditionalFilter(t *testing.T) {
	// write a test for getAdditionalFilter() and addFilter() using a case driven approach
	// getAdditionalFilter() should return the filter that was added using addFilter()
	parser := sqlparser.NewTestParser()
	inKeyRange := getInKeyRange()
	type test struct {
		name        string
		filter      string
		expected    string
		useKeyRange bool
	}

	tests := []test{
		{
			name:        "empty filter",
			filter:      "",
			expected:    "select * from t",
			useKeyRange: false,
		},
		{
			name:        "empty filter sharded",
			filter:      "",
			expected:    "select * from t where in_keyrange('vindex1', '-80')",
			useKeyRange: true,
		},
		{
			name:        "equal filter",
			filter:      "tenant_id = 1",
			expected:    "select * from t where tenant_id = 1",
			useKeyRange: false,
		},
		{
			name:        "equal filter sharded",
			filter:      "country = 'USA'",
			expected:    "select * from t where in_keyrange('vindex1', '-80') and country = 'USA'",
			useKeyRange: true,
		},
		{
			name:        "complex filter",
			filter:      "(tenant_id = 1 or tenant_id = 2) and country in ('USA', 'UK')",
			expected:    "select * from t where (tenant_id = 1 or tenant_id = 2) and country in ('USA', 'UK')",
			useKeyRange: false,
		},
		{
			name:        "complex filter sharded",
			filter:      "(tenant_id = 1 or tenant_id = 2) and country in ('USA', 'UK')",
			expected:    "select * from t where in_keyrange('vindex1', '-80') and ((tenant_id = 1 or tenant_id = 2) and country in ('USA', 'UK'))",
			useKeyRange: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			query := "select * from t"
			stmt, err := parser.Parse(query)
			require.NoError(t, err)
			sel, ok := stmt.(*sqlparser.Select)
			require.True(t, ok)
			var where *sqlparser.Where
			if tc.filter != "" {
				where, err = getAdditionalFilter(parser, tc.filter)
				require.NoError(t, err)
				addFilter(sel, where.Expr)
			}
			if tc.useKeyRange {
				addFilter(sel, inKeyRange)
			}
			want := sqlparser.String(sel)
			require.Equal(t, tc.expected, want)
		})
	}
}
