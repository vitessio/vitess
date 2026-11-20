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
	"slices"
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
			name:       "table list argument",
			predicates: []string{"table_name in ::tables"},
			tables:     map[string]string{"__tables": "::tables"},
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
				ReservedVars:      sqlparser.NewReservedVars("vtg", sqlparser.BindVars{"schema": {}, "schemas": {}, "table": {}, "tables": {}}),
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

func newInfoSchemaHistoryTestContext() *plancontext.PlanningContext {
	return &plancontext.PlanningContext{
		ReservedVars:      sqlparser.NewReservedVars("vtg", sqlparser.BindVars{}),
		ReservedArguments: map[sqlparser.Expr]string{},
		SemTable:          semantics.EmptySemTable(),
		VSchema:           infoSchemaRoutingVSchema{},
	}
}

func newInfoSchemaHistoryTestRoute(t *testing.T, ctx *plancontext.PlanningContext, id int, predicates []string) *Route {
	t.Helper()
	r := &Route{
		unaryOperator: newUnaryOp(&Table{QTable: &QueryTable{ID: semantics.SingleTableSet(id)}}),
		Routing:       &InfoSchemaRouting{},
	}
	for _, predicate := range predicates {
		expr, err := sqlparser.NewTestParser().ParseExpr(predicate)
		require.NoError(t, err)
		err = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			if col, ok := node.(*sqlparser.ColName); ok {
				ctx.SemTable.Direct[col] = semantics.SingleTableSet(id)
				ctx.SemTable.Recursive[col] = semantics.SingleTableSet(id)
			}
			return true, nil
		}, expr)
		require.NoError(t, err)
		r.AddPredicate(ctx, expr)
	}
	return r
}

func infoSchemaHistoryStrings(exprs []sqlparser.Expr) []string {
	result := make([]string, len(exprs))
	for i, expr := range exprs {
		result[i] = sqlparser.String(expr)
	}
	return result
}

// Cloning must preserve pristine predicates with independent comparison roots,
// since resetting one route rewrites its saved predicates in place.
func TestInfoSchemaRoutingClonePreservesHistory(t *testing.T) {
	ctx := newInfoSchemaHistoryTestContext()
	route := newInfoSchemaHistoryTestRoute(t, ctx, 0, []string{"table_schema = 'commerce'", "table_name in ::tables"})
	original := route.Routing.(*InfoSchemaRouting)
	cloned := Clone(route).Routing.(*InfoSchemaRouting)
	require.Len(t, cloned.seenPredicates, len(original.seenPredicates))
	for i, expr := range original.seenPredicates {
		require.NotSame(t, expr, cloned.seenPredicates[i])
		// Child identity retains the semantic information used during replay.
		require.Same(t, expr.(*sqlparser.ComparisonExpr).Left, cloned.seenPredicates[i].(*sqlparser.ComparisonExpr).Left)
	}
	pristine := infoSchemaHistoryStrings(original.seenPredicates)
	for range 2 {
		require.Same(t, cloned, cloned.resetRoutingLogic(ctx))
		assert.Equal(t, infoSchemaHistoryStrings(original.SysTableTableSchema), infoSchemaHistoryStrings(cloned.SysTableTableSchema))
		assert.Equal(t, original.SysTableTableName, cloned.SysTableTableName)
		assert.Equal(t, pristine, infoSchemaHistoryStrings(original.seenPredicates))
	}
	require.Same(t, original, original.resetRoutingLogic(ctx))
	assert.Equal(t, pristine, infoSchemaHistoryStrings(original.seenPredicates))
	assert.Equal(t, pristine, infoSchemaHistoryStrings(cloned.seenPredicates))
}

// Exercise the merge rewrite that actually resets the selected routing, rather
// than calling extraction directly on a hand-rewritten predicate.
func TestInfoSchemaRoutingMergePreservesHistory(t *testing.T) {
	tests := []struct {
		name        string
		left, right []string
		schemas     []string
		tables      map[string]string
	}{
		{
			name:    "empty left routing",
			left:    []string{"t.column_name = 'id'"},
			right:   []string{"c.table_schema = 'commerce'", "c.table_name = 'orders'"},
			schemas: []string{"'commerce'"},
			tables:  map[string]string{"c_table_name": "'orders'"},
		},
		{
			name:    "empty right routing",
			left:    []string{"t.table_schema = 'commerce'", "t.table_name = 'orders'"},
			right:   []string{"c.column_name = 'id'"},
			schemas: []string{"'commerce'"},
			tables:  map[string]string{"t_table_name": "'orders'"},
		},
		{
			name:  "table names only",
			left:  []string{"t.table_name = 'orders'"},
			right: []string{"c.table_name = 'customers'"},
			tables: map[string]string{
				"t_table_name": "'orders'",
				"c_table_name": "'customers'",
			},
		},
		{
			name:    "matching schemas",
			left:    []string{"t.table_schema = 'commerce'", "t.table_name = 'orders'"},
			right:   []string{"c.table_schema = 'commerce'", "c.table_name = 'customers'"},
			schemas: []string{"'commerce'"},
			tables: map[string]string{
				"t_table_name": "'orders'",
				"c_table_name": "'customers'",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := newInfoSchemaHistoryTestContext()
			left := newInfoSchemaHistoryTestRoute(t, ctx, 0, tt.left)
			right := newInfoSchemaHistoryTestRoute(t, ctx, 1, tt.right)
			// Apply joins may be built from cloned routes during planning.
			left = Clone(left)
			right = Clone(right)
			history := slices.Concat(left.Routing.(*InfoSchemaRouting).seenPredicates, right.Routing.(*InfoSchemaRouting).seenPredicates)
			pristine := infoSchemaHistoryStrings(history)
			join := NewApplyJoin(ctx, left, right, nil, sqlparser.NormalJoinType, false)
			op, result := tryMergeApplyJoin(join, ctx)
			require.NotNil(t, result)
			merged, ok := op.(*Route)
			require.True(t, ok)
			routing := merged.Routing.(*InfoSchemaRouting)
			for range 2 {
				assert.Equal(t, pristine, infoSchemaHistoryStrings(history), "reset must not mutate the input histories")
				assert.Equal(t, pristine, infoSchemaHistoryStrings(routing.seenPredicates))
				var schemas []string
				for _, expr := range routing.SysTableTableSchema {
					schemas = append(schemas, sqlparser.String(expr))
				}
				assert.Equal(t, tt.schemas, schemas)
				tables := map[string]string{}
				for name, expr := range routing.SysTableTableName {
					tables[name] = sqlparser.String(expr)
				}
				assert.Equal(t, tt.tables, tables)
				require.Same(t, routing, routing.resetRoutingLogic(ctx))
			}
		})
	}
}
