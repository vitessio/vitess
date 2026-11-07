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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// environmentOnlyVSchema stubs the one plancontext.VSchema method extraction
// touches.
type environmentOnlyVSchema struct {
	plancontext.VSchema
}

func (environmentOnlyVSchema) Environment() *vtenv.Environment {
	return vtenv.NewTestEnv()
}

// TestExtractInfoSchemaRoutingPredicateListArgReplay pins that re-extracting an
// already-rewritten `table_name IN ::list` node is idempotent.
func TestExtractInfoSchemaRoutingPredicateListArgReplay(t *testing.T) {
	ctx := &plancontext.PlanningContext{
		ReservedVars:      sqlparser.NewReservedVars("vtg", sqlparser.BindVars{"tables": {}}),
		ReservedArguments: map[sqlparser.Expr]string{},
		SemTable:          semantics.EmptySemTable(),
		VSchema:           environmentOnlyVSchema{},
	}
	cmp := &sqlparser.ComparisonExpr{
		Operator: sqlparser.InOp,
		Left:     sqlparser.NewColName("table_name"),
		Right:    sqlparser.ListArg("tables"),
	}

	isSchema, bvName, out := extractInfoSchemaRoutingPredicate(ctx, cmp)
	require.False(t, isSchema)
	require.Equal(t, sqlparser.ListArg("tables"), out)
	require.NotEqual(t, "tables", bvName, "the predicate must be re-pointed at a dedicated variable")
	require.Equal(t, sqlparser.ListArg(bvName), cmp.Right)

	isSchema2, bvName2, out2 := extractInfoSchemaRoutingPredicate(ctx, cmp)
	require.False(t, isSchema2)
	assert.Equal(t, bvName, bvName2, "replay must reuse the dedicated variable, not reserve another")
	assert.Equal(t, sqlparser.ListArg("tables"), out2, "replay must recover the client's original list")
	assert.Equal(t, sqlparser.ListArg(bvName), cmp.Right, "replay must not mutate the predicate again")
}
