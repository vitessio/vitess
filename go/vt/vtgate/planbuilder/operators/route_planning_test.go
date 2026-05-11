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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type mockVSchema struct {
	plancontext.VSchema
	preventCrossKeyspaceReads map[string]bool
}

func (m *mockVSchema) AllowCrossKeyspaceReads(keyspace string) (bool, error) {
	if m.preventCrossKeyspaceReads == nil {
		return true, nil
	}
	return !m.preventCrossKeyspaceReads[keyspace], nil
}

func TestCheckCrossKeyspaceJoin(t *testing.T) {
	ks1 := &vindexes.Keyspace{Name: "ks1"}
	ks2 := &vindexes.Keyspace{Name: "ks2"}

	makeRoute := func(ks *vindexes.Keyspace) *Route {
		return &Route{Routing: &NoneRouting{keyspace: ks}}
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
