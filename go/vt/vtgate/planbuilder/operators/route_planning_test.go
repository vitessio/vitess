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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type mockVSchema struct {
	plancontext.VSchema
	noCrossKeyspaceJoins map[string]bool
}

func (m *mockVSchema) AllowCrossKeyspaceJoins(keyspace string) (bool, error) {
	if m.noCrossKeyspaceJoins == nil {
		return true, nil
	}
	return !m.noCrossKeyspaceJoins[keyspace], nil
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
				noCrossKeyspaceJoins: map[string]bool{"ks1": false, "ks2": false},
			},
		},
		{
			name: "cross-keyspace denied on lhs",
			lhs:  makeRoute(ks1),
			rhs:  makeRoute(ks2),
			vschema: &mockVSchema{
				noCrossKeyspaceJoins: map[string]bool{"ks1": true},
			},
			expectPanic: true,
		},
		{
			name: "cross-keyspace denied on rhs",
			lhs:  makeRoute(ks1),
			rhs:  makeRoute(ks2),
			vschema: &mockVSchema{
				noCrossKeyspaceJoins: map[string]bool{"ks2": true},
			},
			expectPanic: true,
		},
		{
			name: "cross-keyspace denied but directive allows",
			lhs:  makeRoute(ks1),
			rhs:  makeRoute(ks2),
			vschema: &mockVSchema{
				noCrossKeyspaceJoins: map[string]bool{"ks1": true},
			},
			stmt: func() sqlparser.Statement {
				stmt, _ := sqlparser.NewTestParser().Parse(
					fmt.Sprintf("select /*vt+ %s */ 1", sqlparser.DirectiveAllowCrossKeyspaceJoins),
				)
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
				noCrossKeyspaceJoins: map[string]bool{"ks1": true, "ks2": true},
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
				noCrossKeyspaceJoins: map[string]bool{"ks1": true, "ks2": true},
			},
		},
		{
			name: "wrapped alternate in rhs keyspace still allowed",
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
				noCrossKeyspaceJoins: map[string]bool{"ks1": true, "ks2": true},
			},
		},
		{
			name: "non-route wrapping route, cross-keyspace denied",
			lhs: &Projection{
				unaryOperator: newUnaryOp(makeRoute(ks1)),
			},
			rhs: makeRoute(ks2),
			vschema: &mockVSchema{
				noCrossKeyspaceJoins: map[string]bool{"ks1": true},
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
				noCrossKeyspaceJoins: map[string]bool{"ks1": false, "ks2": false},
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
					checkCrossKeyspaceOp(ctx, tt.lhs, tt.rhs, "join")
				})
			} else {
				assert.NotPanics(t, func() {
					checkCrossKeyspaceOp(ctx, tt.lhs, tt.rhs, "join")
				})
			}
		})
	}
}

func TestOperatorKeyspace(t *testing.T) {
	ks1 := &vindexes.Keyspace{Name: "ks1"}

	makeRoute := func(ks *vindexes.Keyspace) *Route {
		return &Route{Routing: &NoneRouting{keyspace: ks}}
	}

	tests := []struct {
		name     string
		op       Operator
		expected *vindexes.Keyspace
	}{
		{
			name:     "route operator",
			op:       makeRoute(ks1),
			expected: ks1,
		},
		{
			name:     "route with nil keyspace",
			op:       makeRoute(nil),
			expected: nil,
		},
		{
			name:     "projection wrapping route",
			op:       &Projection{unaryOperator: newUnaryOp(makeRoute(ks1))},
			expected: ks1,
		},
		{
			name:     "deeply nested single-input operators",
			op:       &Projection{unaryOperator: newUnaryOp(&Projection{unaryOperator: newUnaryOp(makeRoute(ks1))})},
			expected: ks1,
		},
		{
			name:     "multi-input operator returns nil",
			op:       &Join{binaryOperator: binaryOperator{LHS: makeRoute(ks1), RHS: makeRoute(ks1)}},
			expected: nil,
		},
		{
			name:     "non-route with no inputs",
			op:       &Projection{},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, operatorKeyspace(tt.op))
		})
	}
}
