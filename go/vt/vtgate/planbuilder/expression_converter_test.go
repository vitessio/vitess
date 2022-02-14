package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func e(in ...evalengine.Expr) []evalengine.Expr {
	return in
}

func TestConversion(t *testing.T) {
	type queriesWithExpectations struct {
		expressionsIn  string
		expressionsOut []evalengine.Expr
	}

	queries := []queriesWithExpectations{{
		expressionsIn:  "1",
		expressionsOut: e(evalengine.NewLiteralInt(1)),
	}, {
		expressionsIn:  "@@foo",
		expressionsOut: e(evalengine.NewColumn(0, collations.TypedCollation{})),
	}}

	for _, tc := range queries {
		t.Run(tc.expressionsIn, func(t *testing.T) {
			statement, err := sqlparser.Parse("select " + tc.expressionsIn)
			require.NoError(t, err)
			slct := statement.(*sqlparser.Select)
			exprs := extract(slct.SelectExprs)
			ec := &expressionConverter{}
			var result []evalengine.Expr
			for _, expr := range exprs {
				evalExpr, err := ec.convert(expr, false, false)
				require.NoError(t, err)
				result = append(result, evalExpr)
			}
			require.Equal(t, tc.expressionsOut, result)
		})
	}
}

func extract(in sqlparser.SelectExprs) []sqlparser.Expr {
	var result []sqlparser.Expr
	for _, expr := range in {
		result = append(result, expr.(*sqlparser.AliasedExpr).Expr)
	}
	return result
}
