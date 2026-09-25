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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

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
			name: "synthetic dual reference table has no keyspace",
			op: &Table{
				QTable: &QueryTable{Table: sqlparser.NewTableName("dual")},
				VTable: &vindexes.BaseTable{Name: sqlparser.NewIdentifierCS("dual"), Keyspace: ks1, Type: vindexes.TypeReference},
			},
			expected: nil,
		},
		{
			name:     "table nested under a projection",
			op:       &Projection{unaryOperator: newUnaryOp(makeTable(ks1))},
			expected: []*vindexes.Keyspace{ks1},
		},
		{
			name:     "same keyspace tables are deduplicated",
			op:       &Join{binaryOperator: newBinaryOp(makeTable(ks1), makeTable(ks1))},
			expected: []*vindexes.Keyspace{ks1},
		},
		{
			name:     "tables from different keyspaces",
			op:       &Join{binaryOperator: newBinaryOp(makeTable(ks1), makeTable(ks2))},
			expected: []*vindexes.Keyspace{ks1, ks2},
		},
		{
			name:     "real table joined with a virtual dual",
			op:       &Join{binaryOperator: newBinaryOp(makeTable(ks1), &Table{})},
			expected: []*vindexes.Keyspace{ks1},
		},
		{
			name:     "information_schema table with a synthetic vtable contributes nothing",
			op:       makeInfSchemaTable(ks1),
			expected: nil,
		},
		{
			name:     "information_schema table joined with a real table",
			op:       &Join{binaryOperator: newBinaryOp(makeInfSchemaTable(ks1), makeTable(ks2))},
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
	}
	orig := &NoneRouting{keyspace: ks}
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
			op:       &Join{binaryOperator: newBinaryOp(makeTable(), makeInfSchemaTable())},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, hasInfoSchemaTables(tt.op))
		})
	}
}
