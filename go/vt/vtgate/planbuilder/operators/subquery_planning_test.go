/*
Copyright 2026 The Vitess Authors.

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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// TestRewriteOriginalPushedToRHSKeepsColumnsWithoutDependencies checks that a column that the semantic
// table knows nothing about is not mistaken for one that the left side of the join feeds.
func TestRewriteOriginalPushedToRHSKeepsColumnsWithoutDependencies(t *testing.T) {
	fed := sqlparser.NewColName("fed")
	unknown := sqlparser.NewColName("unknown")
	ctx := &plancontext.PlanningContext{
		SemTable:          semantics.EmptySemTable(),
		ReservedVars:      sqlparser.NewReservedVars("vtg", sqlparser.BindVars{}),
		ReservedArguments: map[sqlparser.Expr]string{},
		VSchema: &vschemawrapper.VSchemaWrapper{
			V:   &vindexes.VSchema{},
			Env: vtenv.NewTestEnv(),
		},
	}
	t0, t1, t2 := semantics.SingleTableSet(0), semantics.SingleTableSet(1), semantics.SingleTableSet(2)
	ctx.SemTable.Recursive[fed] = t0

	outer := &ApplyJoin{
		binaryOperator: newBinaryOp(&fakeOp{id: t0.Merge(t1)}, &fakeOp{id: t2}),
		JoinColumns:    &applyJoinColumns{},
		JoinPredicates: &applyJoinColumns{},
	}
	expr := sqlparser.AndExpressions(fed, unknown)

	rewritten, _ := rewriteOriginalPushedToRHS(ctx, expr, outer)

	require.Equal(t, ":fed and `unknown`", sqlparser.String(rewritten))
}
