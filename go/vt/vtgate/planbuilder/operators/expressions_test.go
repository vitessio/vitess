/*
Copyright 2025 The Vitess Authors.

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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func TestSplitComplexPredicateToLHS(t *testing.T) {
	ast, err := sqlparser.NewTestParser().ParseExpr("l.foo + r.bar - l.baz / r.tata = 0")
	require.NoError(t, err)
	lID := semantics.SingleTableSet(0)
	rID := semantics.SingleTableSet(1)
	ctx := plancontext.CreateEmptyPlanningContext()
	ctx.SemTable = semantics.EmptySemTable()
	// simple sem analysis using the column prefix
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		col, ok := node.(*sqlparser.ColName)
		if !ok {
			return true, nil
		}
		if col.Qualifier.Name.String() == "l" {
			ctx.SemTable.Recursive[col] = lID
		} else {
			ctx.SemTable.Recursive[col] = rID
		}
		return false, nil
	}, ast)

	valuesJoinCols := breakValuesJoinExpressionInLHS(ctx, ast, lID)
	nodes := slice.Map(valuesJoinCols.LHS, func(from sqlparser.Expr) string {
		return sqlparser.String(from)
	})

	assert.Equal(t, []string{"l.foo", "l.baz"}, nodes)
}
