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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type mockVSchema struct {
	plancontext.VSchema
	preventCrossKeyspaceReads map[string]bool
	tables                    map[string]*vindexes.BaseTable
}

func (m *mockVSchema) AllowCrossKeyspaceReads(keyspace string) (bool, error) {
	if m.preventCrossKeyspaceReads == nil {
		return true, nil
	}
	return !m.preventCrossKeyspaceReads[keyspace], nil
}

func (m *mockVSchema) FindTableOrVindex(tab sqlparser.TableName) (*vindexes.BaseTable, vindexes.Vindex, string, topodatapb.TabletType, key.ShardDestination, error) {
	tbl, found := m.tables[tab.Name.String()]
	if !found {
		return nil, nil, "", topodatapb.TabletType_PRIMARY, nil, vterrors.VT05004(tab.Name.String())
	}
	return tbl, nil, tbl.Keyspace.Name, topodatapb.TabletType_PRIMARY, nil, nil
}

func (m *mockVSchema) Environment() *vtenv.Environment {
	return vtenv.NewTestEnv()
}

func (m *mockVSchema) ConnCollation() collations.ID {
	return collations.CollationUtf8mb4ID
}

func TestCheckCrossKeyspaceJoin(t *testing.T) {
	ks1 := &vindexes.Keyspace{Name: "ks1"}
	ks2 := &vindexes.Keyspace{Name: "ks2"}

	makeRoute := func(ks *vindexes.Keyspace) *Route {
		return &Route{Routing: &NoneRouting{keyspace: ks}}
	}
	plainTable := func(ks *vindexes.Keyspace) *Table {
		return &Table{
			QTable: &QueryTable{Table: sqlparser.NewTableName("t")},
			VTable: &vindexes.BaseTable{Name: sqlparser.NewIdentifierCS("t"), Keyspace: ks},
		}
	}
	refTable := func(ks *vindexes.Keyspace, copyKs string) *Table {
		return &Table{
			QTable: &QueryTable{Table: sqlparser.NewTableName("ref")},
			VTable: &vindexes.BaseTable{
				Name:     sqlparser.NewIdentifierCS("ref"),
				Keyspace: ks,
				ReferencedBy: map[string]*vindexes.BaseTable{
					copyKs: {Name: sqlparser.NewIdentifierCS("refcopy")},
				},
			},
		}
	}
	sourcedTable := func(ks *vindexes.Keyspace, sourceKs string) *Table {
		return &Table{
			QTable: &QueryTable{Table: sqlparser.NewTableName("ref")},
			VTable: &vindexes.BaseTable{
				Name:     sqlparser.NewIdentifierCS("ref"),
				Keyspace: ks,
				Source: &vindexes.Source{
					TableName: sqlparser.TableName{
						Qualifier: sqlparser.NewIdentifierCS(sourceKs),
						Name:      sqlparser.NewIdentifierCS("src"),
					},
				},
			},
		}
	}
	noneRouteOver := func(ks *vindexes.Keyspace, src Operator) *Route {
		return &Route{unaryOperator: newUnaryOp(src), Routing: &NoneRouting{keyspace: ks}}
	}

	tests := []struct {
		name        string
		lhs         Operator
		rhs         Operator
		vschema     *mockVSchema
		stmt        sqlparser.Statement
		expectPanic bool
	}{
		{
			name:    "non-route operators",
			lhs:     &Projection{},
			rhs:     &Projection{},
			vschema: &mockVSchema{},
		},
		{
			name:    "lhs non-route",
			lhs:     &Projection{},
			rhs:     makeRoute(ks1),
			vschema: &mockVSchema{},
		},
		{
			name:    "same keyspace",
			lhs:     makeRoute(ks1),
			rhs:     makeRoute(ks1),
			vschema: &mockVSchema{},
		},
		{
			name:    "nil lhs keyspace",
			lhs:     makeRoute(nil),
			rhs:     makeRoute(ks1),
			vschema: &mockVSchema{},
		},
		{
			name:    "nil rhs keyspace",
			lhs:     makeRoute(ks1),
			rhs:     makeRoute(nil),
			vschema: &mockVSchema{},
		},
		{
			name: "cross-keyspace allowed",
			lhs:  makeRoute(ks1),
			rhs:  makeRoute(ks2),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": false, "ks2": false},
			},
		},
		{
			name: "cross-keyspace denied on lhs",
			lhs:  makeRoute(ks1),
			rhs:  makeRoute(ks2),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": true},
			},
			expectPanic: true,
		},
		{
			name: "cross-keyspace denied on rhs",
			lhs:  makeRoute(ks1),
			rhs:  makeRoute(ks2),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks2": true},
			},
			expectPanic: true,
		},
		{
			name: "cross-keyspace denied but directive allows",
			lhs:  makeRoute(ks1),
			rhs:  makeRoute(ks2),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": true},
			},
			stmt: func() sqlparser.Statement {
				stmt, err := sqlparser.NewTestParser().Parse(
					fmt.Sprintf("select /*vt+ %s */ 1", sqlparser.DirectiveAllowCrossKeyspaceReads),
				)
				require.NoError(t, err)
				return stmt
			}(),
		},
		{
			name: "cross-keyspace denied but lhs has alternate in rhs keyspace",
			lhs: &Route{Routing: &AnyShardRouting{
				keyspace: ks1,
				Alternates: map[*vindexes.Keyspace]*Route{
					ks2: makeRoute(ks2),
				},
			}},
			rhs: makeRoute(ks2),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": true, "ks2": true},
			},
		},
		{
			name: "cross-keyspace denied but rhs has alternate in lhs keyspace",
			lhs:  makeRoute(ks1),
			rhs: &Route{Routing: &AnyShardRouting{
				keyspace: ks2,
				Alternates: map[*vindexes.Keyspace]*Route{
					ks1: makeRoute(ks1),
				},
			}},
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": true, "ks2": true},
			},
		},
		{
			name: "wrapped alternate in rhs keyspace denied (merge cant use wrapped alternates)",
			lhs: &Projection{
				unaryOperator: newUnaryOp(&Route{Routing: &AnyShardRouting{
					keyspace: ks1,
					Alternates: map[*vindexes.Keyspace]*Route{
						ks2: makeRoute(ks2),
					},
				}}),
			},
			rhs: makeRoute(ks2),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": true, "ks2": true},
			},
			expectPanic: true,
		},
		{
			name: "composite same-keyspace lhs, cross-keyspace denied",
			lhs:  &Join{binaryOperator: binaryOperator{LHS: makeRoute(ks1), RHS: makeRoute(ks1)}},
			rhs:  makeRoute(ks2),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": true},
			},
			expectPanic: true,
		},
		{
			name: "composite mixed-keyspace lhs, rhs denied",
			lhs:  &Join{binaryOperator: binaryOperator{LHS: makeRoute(ks1), RHS: makeRoute(ks2)}},
			rhs:  makeRoute(&vindexes.Keyspace{Name: "ks3"}),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks3": true},
			},
			expectPanic: true,
		},
		{
			name: "non-route wrapping route, cross-keyspace denied",
			lhs: &Projection{
				unaryOperator: newUnaryOp(makeRoute(ks1)),
			},
			rhs: makeRoute(ks2),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": true},
			},
			expectPanic: true,
		},
		{
			name: "non-route wrapping route, cross-keyspace allowed",
			lhs: &Projection{
				unaryOperator: newUnaryOp(makeRoute(ks1)),
			},
			rhs: makeRoute(ks2),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": false, "ks2": false},
			},
		},
		{
			name: "inferred none keyspace does not create a cross-keyspace pair",
			lhs:  &Route{Routing: &NoneRouting{keyspace: ks1, inferredKeyspace: true}},
			rhs:  makeRoute(ks2),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": true, "ks2": true},
			},
		},
		{
			name: "inferred none keyspace wrapped in a non-route is also skipped",
			lhs: &Projection{
				unaryOperator: newUnaryOp(&Route{Routing: &NoneRouting{keyspace: ks1, inferredKeyspace: true}}),
			},
			rhs: makeRoute(ks2),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": true, "ks2": true},
			},
		},
		{
			name: "cross-keyspace denied but the lhs none branch's table has a reference copy on the rhs",
			lhs:  noneRouteOver(ks1, refTable(ks1, "ks2")),
			rhs:  makeRoute(ks2),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": true, "ks2": true},
				tables: map[string]*vindexes.BaseTable{
					"refcopy": {Name: sqlparser.NewIdentifierCS("refcopy"), Keyspace: ks2},
				},
			},
		},
		{
			name: "cross-keyspace denied but the rhs none branch's table is sourced from the lhs keyspace",
			lhs:  makeRoute(ks1),
			rhs:  noneRouteOver(ks2, sourcedTable(ks2, "ks1")),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": true, "ks2": true},
				tables: map[string]*vindexes.BaseTable{
					"src": {Name: sqlparser.NewIdentifierCS("src"), Keyspace: ks1},
				},
			},
		},
		{
			name: "cross-keyspace denied but the rhs none branch's table has an unqualified source resolving to the lhs keyspace",
			lhs:  makeRoute(ks1),
			rhs: noneRouteOver(ks2, &Table{
				QTable: &QueryTable{Table: sqlparser.NewTableName("ref")},
				VTable: &vindexes.BaseTable{
					Name:     sqlparser.NewIdentifierCS("ref"),
					Keyspace: ks2,
					Source: &vindexes.Source{
						TableName: sqlparser.TableName{Name: sqlparser.NewIdentifierCS("src")},
					},
				},
			}),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": true, "ks2": true},
				tables: map[string]*vindexes.BaseTable{
					"src": {Name: sqlparser.NewIdentifierCS("src"), Keyspace: ks1},
				},
			},
		},
		{
			name: "a none branch that absorbed a table without a copy on the rhs stays denied",
			lhs: noneRouteOver(ks1, &Join{binaryOperator: binaryOperator{
				LHS: refTable(ks1, "ks2"),
				RHS: plainTable(ks1),
			}}),
			rhs: makeRoute(ks2),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": true, "ks2": true},
				tables: map[string]*vindexes.BaseTable{
					"refcopy": {Name: sqlparser.NewIdentifierCS("refcopy"), Keyspace: ks2},
				},
			},
			expectPanic: true,
		},
		{
			name: "an inferred none keyspace over a real table stays accountable",
			lhs: &Route{
				unaryOperator: newUnaryOp(plainTable(ks1)),
				Routing:       &NoneRouting{keyspace: ks1, inferredKeyspace: true},
			},
			rhs: makeRoute(ks2),
			vschema: &mockVSchema{
				preventCrossKeyspaceReads: map[string]bool{"ks1": true, "ks2": true},
			},
			expectPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &plancontext.PlanningContext{
				SemTable:  &semantics.SemTable{},
				VSchema:   tt.vschema,
				Statement: tt.stmt,
			}
			if tt.expectPanic {
				assert.Panics(t, func() {
					checkCrossKeyspaceOp(ctx, tt.lhs, tt.rhs, "JOIN")
				})
			} else {
				assert.NotPanics(t, func() {
					checkCrossKeyspaceOp(ctx, tt.lhs, tt.rhs, "JOIN")
				})
			}
		})
	}
}

func TestOperatorKeyspaces(t *testing.T) {
	ks1 := &vindexes.Keyspace{Name: "ks1"}
	ks2 := &vindexes.Keyspace{Name: "ks2"}

	makeRoute := func(ks *vindexes.Keyspace) *Route {
		return &Route{Routing: &NoneRouting{keyspace: ks}}
	}

	tests := []struct {
		name     string
		op       Operator
		expected []*vindexes.Keyspace
	}{
		{
			name:     "route operator",
			op:       makeRoute(ks1),
			expected: []*vindexes.Keyspace{ks1},
		},
		{
			name:     "route with nil keyspace",
			op:       makeRoute(nil),
			expected: nil,
		},
		{
			name:     "projection wrapping route",
			op:       &Projection{unaryOperator: newUnaryOp(makeRoute(ks1))},
			expected: []*vindexes.Keyspace{ks1},
		},
		{
			name:     "deeply nested single-input operators",
			op:       &Projection{unaryOperator: newUnaryOp(&Projection{unaryOperator: newUnaryOp(makeRoute(ks1))})},
			expected: []*vindexes.Keyspace{ks1},
		},
		{
			name:     "multi-input same keyspace returns single keyspace",
			op:       &Join{binaryOperator: binaryOperator{LHS: makeRoute(ks1), RHS: makeRoute(ks1)}},
			expected: []*vindexes.Keyspace{ks1},
		},
		{
			name:     "multi-input different keyspaces returns both",
			op:       &Join{binaryOperator: binaryOperator{LHS: makeRoute(ks1), RHS: makeRoute(ks2)}},
			expected: []*vindexes.Keyspace{ks1, ks2},
		},
		{
			name:     "non-route with no inputs",
			op:       &Projection{},
			expected: nil,
		},
		{
			name:     "nil operator",
			op:       nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, operatorKeyspaces(tt.op))
		})
	}
}

func TestRealTableKeyspaces(t *testing.T) {
	ks1 := &vindexes.Keyspace{Name: "ks1"}
	ks2 := &vindexes.Keyspace{Name: "ks2"}

	makeTable := func(ks *vindexes.Keyspace) *Table {
		return &Table{
			QTable: &QueryTable{Table: sqlparser.NewTableName("t")},
			VTable: &vindexes.BaseTable{Name: sqlparser.NewIdentifierCS("t"), Keyspace: ks},
		}
	}
	makeInfSchemaTable := func(ks *vindexes.Keyspace) *Table {
		return &Table{
			QTable: &QueryTable{Table: sqlparser.NewTableName("tables"), IsInfSchema: true},
			VTable: &vindexes.BaseTable{Name: sqlparser.NewIdentifierCS("tables"), Keyspace: ks},
		}
	}

	tests := []struct {
		name     string
		op       Operator
		expected []*vindexes.Keyspace
	}{
		{
			name:     "real table",
			op:       makeTable(ks1),
			expected: []*vindexes.Keyspace{ks1},
		},
		{
			name:     "virtual dual table has no keyspace",
			op:       &Table{},
			expected: nil,
		},
		{
			name:     "table nested under a projection",
			op:       &Projection{unaryOperator: newUnaryOp(makeTable(ks1))},
			expected: []*vindexes.Keyspace{ks1},
		},
		{
			name:     "same keyspace tables are deduplicated",
			op:       &Join{binaryOperator: binaryOperator{LHS: makeTable(ks1), RHS: makeTable(ks1)}},
			expected: []*vindexes.Keyspace{ks1},
		},
		{
			name:     "tables from different keyspaces",
			op:       &Join{binaryOperator: binaryOperator{LHS: makeTable(ks1), RHS: makeTable(ks2)}},
			expected: []*vindexes.Keyspace{ks1, ks2},
		},
		{
			name:     "real table joined with a virtual dual",
			op:       &Join{binaryOperator: binaryOperator{LHS: makeTable(ks1), RHS: &Table{}}},
			expected: []*vindexes.Keyspace{ks1},
		},
		{
			name:     "information_schema table with a synthetic vtable contributes nothing",
			op:       makeInfSchemaTable(ks1),
			expected: nil,
		},
		{
			name:     "information_schema table joined with a real table",
			op:       &Join{binaryOperator: binaryOperator{LHS: makeInfSchemaTable(ks1), RHS: makeTable(ks2)}},
			expected: []*vindexes.Keyspace{ks2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, realTableKeyspaces(tt.op))
		})
	}
}

func TestUpdateRoutingLogicKeepsNoneRouting(t *testing.T) {
	ks := &vindexes.Keyspace{Name: "ks1"}
	ctx := &plancontext.PlanningContext{
		SemTable: &semantics.SemTable{},
		VSchema:  &mockVSchema{},
	}
	orig := &NoneRouting{keyspace: ks, inferredKeyspace: true}
	falseCmp := &sqlparser.ComparisonExpr{
		Operator: sqlparser.EqualOp,
		Left:     sqlparser.NewIntLiteral("1"),
		Right:    sqlparser.NewIntLiteral("2"),
	}

	got := UpdateRoutingLogic(ctx, falseCmp, orig)

	assert.Same(t, orig, got)
}

func TestHasInfoSchemaTables(t *testing.T) {
	ks1 := &vindexes.Keyspace{Name: "ks1"}

	makeTable := func() *Table {
		return &Table{
			QTable: &QueryTable{Table: sqlparser.NewTableName("t")},
			VTable: &vindexes.BaseTable{Name: sqlparser.NewIdentifierCS("t"), Keyspace: ks1},
		}
	}
	makeInfSchemaTable := func() *Table {
		return &Table{
			QTable: &QueryTable{Table: sqlparser.NewTableName("tables"), IsInfSchema: true},
			VTable: &vindexes.BaseTable{Name: sqlparser.NewIdentifierCS("tables"), Keyspace: ks1},
		}
	}

	tests := []struct {
		name     string
		op       Operator
		expected bool
	}{
		{
			name:     "real table",
			op:       makeTable(),
			expected: false,
		},
		{
			name:     "information_schema table",
			op:       makeInfSchemaTable(),
			expected: true,
		},
		{
			name:     "virtual dual table",
			op:       &Table{},
			expected: false,
		},
		{
			name:     "information_schema table nested under a projection",
			op:       &Projection{unaryOperator: newUnaryOp(makeInfSchemaTable())},
			expected: true,
		},
		{
			name:     "information_schema table joined with a real table",
			op:       &Join{binaryOperator: binaryOperator{LHS: makeTable(), RHS: makeInfSchemaTable()}},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, hasInfoSchemaTables(tt.op))
		})
	}
}
