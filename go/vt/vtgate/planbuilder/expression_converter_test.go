/*
Copyright 2020 The Vitess Authors.

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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"

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
		expressionsOut: e(evalengine.NewColumn(0)),
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
