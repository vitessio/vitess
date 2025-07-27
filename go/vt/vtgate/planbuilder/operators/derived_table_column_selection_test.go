/*
Copyright 2024 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// TestDerivedTableColumnSelection tests the fix for the derived table column selection bug
func TestDerivedTableColumnSelection(t *testing.T) {
	// Test the specific scenario from the bug report:
	// select team_id from (select team_id, phone from contacts group by 1, 2) t group by 1
	// The outer query asks for 1 column but inner produces 2 columns

	// Create test columns
	col1 := &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("team_id")}
	col2 := &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("phone")}

	// Test Aggregator column truncation for derived tables
	tableId := semantics.SingleTableSet(0)
	aggregator := &Aggregator{
		Columns: []*sqlparser.AliasedExpr{col1, col2},
		DT: &DerivedTable{
			TableID: tableId,
			Alias:   "t",
		},
	}

	// Before fix: would return 2 columns
	// After fix: should respect ResultColumns and return only 1 column
	aggregator.setTruncateColumnCount(1)

	// Test the derived table path in GetColumns
	columns := aggregator.GetColumns(nil)
	assert.Len(t, columns, 1, "Should return only 1 column when ResultColumns is set to 1")
	assert.Equal(t, "team_id", columns[0].Expr.(*sqlparser.ColName).Name.String())
}

// TestRouteColumnTruncation tests that Route respects ResultColumns field
func TestRouteColumnTruncation(t *testing.T) {
	// Create test columns
	col1 := &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("team_id")}
	col2 := &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("phone")}

	mockSource := &mockOperator{
		columns: []*sqlparser.AliasedExpr{col1, col2},
	}

	route := &Route{
		unaryOperator: newUnaryOp(mockSource),
	}

	// Test with ResultColumns set to 1 - should truncate to 1 column
	route.setTruncateColumnCount(1)
	columns := route.GetColumns(nil)
	assert.Len(t, columns, 1, "Should return only 1 column when ResultColumns is set to 1")
	assert.Equal(t, "team_id", columns[0].Expr.(*sqlparser.ColName).Name.String())
}

// TestTruncateColumnCountInterface tests the columnTruncator interface
func TestTruncateColumnCountInterface(t *testing.T) {
	// Test that Aggregator implements columnTruncator interface
	var aggregator Operator = &Aggregator{}

	type columnTruncator interface {
		setTruncateColumnCount(offset int)
	}

	truncator, ok := aggregator.(columnTruncator)
	require.True(t, ok, "Aggregator should implement columnTruncator interface")

	// Should not panic
	truncator.setTruncateColumnCount(1)

	// Test that Route implements columnTruncator interface
	var route Operator = &Route{}
	truncator, ok = route.(columnTruncator)
	require.True(t, ok, "Route should implement columnTruncator interface")

	// Should not panic
	truncator.setTruncateColumnCount(1)
}

// mockOperator is a simple mock for testing
type mockOperator struct {
	noColumns
	noPredicates
	columns []*sqlparser.AliasedExpr
}

func (m *mockOperator) Clone([]Operator) Operator { return m }
func (m *mockOperator) Inputs() []Operator        { return nil }
func (m *mockOperator) SetInputs([]Operator)      {}
func (m *mockOperator) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return m.columns
}
func (m *mockOperator) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	result := make([]sqlparser.SelectExpr, len(m.columns))
	for i, col := range m.columns {
		result[i] = col
	}
	return result
}
func (m *mockOperator) ShortDescription() string                           { return "mock" }
func (m *mockOperator) GetOrdering(*plancontext.PlanningContext) []OrderBy { return nil }
