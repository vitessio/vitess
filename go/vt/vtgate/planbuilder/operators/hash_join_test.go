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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestJoinPredicates(t *testing.T) {
	lcol := sqlparser.NewColName("lhs")
	rcol := sqlparser.NewColName("rhs")
	ctx := &plancontext.PlanningContext{SemTable: semantics.EmptySemTable()}
	lid := semantics.SingleTableSet(0)
	rid := semantics.SingleTableSet(1)
	ctx.SemTable.Recursive[lcol] = lid
	ctx.SemTable.Recursive[rcol] = rid
	lhs := &fakeOp{id: lid}
	rhs := &fakeOp{id: rid}
	hj := &HashJoin{
		LHS:      lhs,
		RHS:      rhs,
		LeftJoin: false,
		columns:  &hashJoinColumns{},
	}

	cmp := &sqlparser.ComparisonExpr{
		Operator: sqlparser.EqualOp,
		Left:     lcol,
		Right:    rcol,
	}
	hj.AddJoinPredicate(ctx, cmp)
	require.Len(t, hj.JoinComparisons, 1)
	hj.planOffsets(ctx)
	require.Len(t, hj.LHSKeys, 1)
	require.Len(t, hj.RHSKeys, 1)
}

func TestOffsetPlanning(t *testing.T) {
	lcol1, lcol2 := sqlparser.NewColName("lhs1"), sqlparser.NewColName("lhs2")
	rcol1, rcol2 := sqlparser.NewColName("rhs1"), sqlparser.NewColName("rhs2")
	ctx := &plancontext.PlanningContext{
		SemTable: semantics.EmptySemTable(),
		VSchema: &vschemawrapper.VSchemaWrapper{
			V:             &vindexes.VSchema{},
			SysVarEnabled: true,
			Env:           vtenv.NewTestEnv(),
		},
	}
	lid := semantics.SingleTableSet(0)
	rid := semantics.SingleTableSet(1)
	ctx.SemTable.Recursive[lcol1] = lid
	ctx.SemTable.Recursive[lcol2] = lid
	ctx.SemTable.Recursive[rcol1] = rid
	ctx.SemTable.Recursive[rcol2] = rid
	lhs := &fakeOp{id: lid}
	rhs := &fakeOp{id: rid}

	tests := []struct {
		expr               sqlparser.Expr
		expectedColOffsets []int
	}{{
		expr:               lcol1,
		expectedColOffsets: []int{-1},
	}, {
		expr:               rcol1,
		expectedColOffsets: []int{1},
	}, {
		expr:               sqlparser.AndExpressions(lcol1, lcol2),
		expectedColOffsets: []int{-1, -2},
	}, {
		expr:               sqlparser.AndExpressions(lcol1, rcol1, lcol2, rcol2),
		expectedColOffsets: []int{-1, 1, -2, 2},
	}}

	for _, test := range tests {
		t.Run(sqlparser.String(test.expr), func(t *testing.T) {
			hj := &HashJoin{
				LHS:      lhs,
				RHS:      rhs,
				LeftJoin: false,
				columns:  &hashJoinColumns{},
			}
			hj.AddColumn(ctx, true, false, aeWrap(test.expr))
			hj.planOffsets(ctx)
			assert.Equal(t, test.expectedColOffsets, hj.ColumnOffsets)
		})
	}
}
