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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// environmentOnlyVSchema stubs the one plancontext.VSchema method extraction
// touches.
type environmentOnlyVSchema struct {
	plancontext.VSchema
}

func (environmentOnlyVSchema) Environment() *vtenv.Environment {
	return vtenv.NewTestEnv()
}

type infoSchemaRoutingVSchema struct {
	environmentOnlyVSchema
}

func (infoSchemaRoutingVSchema) AnyKeyspace() (*vindexes.Keyspace, error) {
	return &vindexes.Keyspace{Name: "commerce"}, nil
}

func (infoSchemaRoutingVSchema) ConnCollation() collations.ID {
	return collations.CollationUtf8mb4ID
}

// TestInfoSchemaRoutingResetPreservesRoutingValues pins that resetRoutingLogic
// replays the original predicates, not the rewritten ones: the routing values
// and the tablet predicates must be unchanged after repeated resets.
func TestInfoSchemaRoutingResetPreservesRoutingValues(t *testing.T) {
	tests := []struct {
		name       string
		predicates []string
		schemas    []string
		tables     map[string]string
	}{
		{
			name:       "schema equality",
			predicates: []string{"table_schema = 'commerce'"},
			schemas:    []string{"'commerce'"},
		},
		{
			name:       "reversed schema equality",
			predicates: []string{"'commerce' = table_schema"},
			schemas:    []string{"'commerce'"},
		},
		{
			name:       "schema bind variable",
			predicates: []string{"table_schema = :schema"},
			schemas:    []string{":schema"},
		},
		{
			name:       "schema single value IN",
			predicates: []string{"table_schema in ('commerce')"},
			schemas:    []string{"'commerce'"},
		},
		{
			name:       "schema tuple IN",
			predicates: []string{"table_schema in ('commerce', 'customer')"},
			schemas:    []string{"('commerce', 'customer')"},
		},
		{
			name:       "schema list argument",
			predicates: []string{"table_schema in ::schemas"},
			schemas:    []string{"::schemas"},
		},
		{
			name:       "table equality",
			predicates: []string{"table_name = 'orders'"},
			tables:     map[string]string{"table_name": "'orders'"},
		},
		{
			name:       "conflicting table predicates",
			predicates: []string{"table_name = 'orders'", "table_name = 'customers'"},
			tables:     map[string]string{"table_name": "'orders'"},
		},
		{
			name:       "schema and table predicates",
			predicates: []string{"table_schema = 'commerce'", "table_name = :table"},
			schemas:    []string{"'commerce'"},
			tables:     map[string]string{"table_name": ":table"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &plancontext.PlanningContext{
				ReservedVars:      sqlparser.NewReservedVars("vtg", sqlparser.BindVars{"schema": {}, "schemas": {}, "table": {}}),
				ReservedArguments: map[sqlparser.Expr]string{},
				SemTable:          semantics.EmptySemTable(),
				VSchema:           infoSchemaRoutingVSchema{},
			}
			isr := &InfoSchemaRouting{}
			var predicates []sqlparser.Expr
			var rewritten []string
			for _, query := range tt.predicates {
				expr, err := sqlparser.NewTestParser().ParseExpr(query)
				require.NoError(t, err)
				require.Same(t, isr, UpdateRoutingLogic(ctx, expr, isr))
				predicates = append(predicates, expr)
				rewritten = append(rewritten, sqlparser.String(expr))
			}

			checkRouting := func() {
				t.Helper()
				var schemas []string
				for _, expr := range isr.SysTableTableSchema {
					schemas = append(schemas, sqlparser.String(expr))
				}
				assert.Equal(t, tt.schemas, schemas)
				assert.Len(t, isr.SysTableTableName, len(tt.tables))
				for name, value := range tt.tables {
					assert.Equal(t, value, sqlparser.String(isr.SysTableTableName[name]))
				}
			}
			checkRouting()
			for range 2 {
				require.Same(t, isr, isr.resetRoutingLogic(ctx))
				checkRouting()
				for i, expr := range predicates {
					assert.Equal(t, rewritten[i], sqlparser.String(expr), "reset must preserve the tablet predicate")
				}
			}
		})
	}
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
