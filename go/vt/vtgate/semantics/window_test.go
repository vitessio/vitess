/*
Copyright 2025 The Vitess Authors.

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

package semantics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestWindowFunctionBinding(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		shouldErr bool
		err       string
	}{
		{
			name:      "Named Window Binding Success",
			query:     "select avg(price) over w from products window w as (partition by category)",
			shouldErr: false,
		},
		{
			name:      "Named Window Binding Failure - Missing Window",
			query:     "select avg(price) over w from products",
			shouldErr: true,
			err:       "VT03025: Incorrect arguments to w",
		},
		{
			name:      "Anonymous Window Success",
			query:     "select avg(price) over (partition by category) from products",
			shouldErr: false,
		},
		{
			name:      "Scope Visibility Success - Inner Query",
			query:     "select * from (select avg(price) over w from products window w as (partition by category)) as t",
			shouldErr: false,
		},
		{
			name:      "Scope Visibility Failure - Window defined in inner query, used in outer",
			query:     "select avg(price) over w from (select * from products window w as (partition by category)) as t",
			shouldErr: true,
			err:       "VT03025: Incorrect arguments to w",
		},
		{
			name:      "Multiple Windows",
			query:     "select avg(price) over w1, rank() over w2 from products window w1 as (partition by category), window w2 as (order by price)",
			shouldErr: false,
		},
		{
			name:      "Window defined but not used",
			query:     "select * from products window w as (partition by category)",
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse, err := sqlparser.NewTestParser().Parse(tt.query)
			require.NoError(t, err)

			_, err = Analyze(parse, "d", fakeSchemaInfo())
			if tt.shouldErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestWindowQuerySignature(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		isWindow bool
	}{
		{
			name:     "Simple Window",
			query:    "select avg(price) over w from products window w as (partition by category)",
			isWindow: true,
		},
		{
			name:     "Anonymous Window",
			query:    "select avg(price) over (partition by category) from products",
			isWindow: true,
		},
		{
			name:     "No Window",
			query:    "select avg(price) from products group by category",
			isWindow: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, tt.query, "d")
			require.NotNil(t, stmt)
			require.NotNil(t, semTable)
			assert.Equal(t, tt.isWindow, semTable.QuerySignature.WindowFunc, "QuerySignature.WindowFunc mismatch")
		})
	}
}

func TestWindowColumnDependencies(t *testing.T) {
	// This test verifies that columns used in the WINDOW clause are correctly bound to the table.
	query := "select avg(price) over w from products window w as (partition by category order by price)"
	stmt, semTable := parseAndAnalyze(t, query, "d")

	sel, ok := stmt.(*sqlparser.Select)
	require.True(t, ok)
	require.Len(t, sel.Windows, 1)

	windowDef := sel.Windows[0].Windows[0]
	require.Equal(t, "w", windowDef.Name.String())

	// Check Partition By column 'category'
	partitionExpr := windowDef.WindowSpec.PartitionClause[0]
	colName, ok := partitionExpr.(*sqlparser.ColName)
	require.True(t, ok)

	deps := semTable.RecursiveDeps(colName)
	assert.Equal(t, TS0, deps, "Dependency for 'category' in PARTITION BY should be table 0 (products)")

	// Check Order By column 'price'
	orderExpr := windowDef.WindowSpec.OrderClause[0].Expr
	colNameOrder, ok := orderExpr.(*sqlparser.ColName)
	require.True(t, ok)

	depsOrder := semTable.RecursiveDeps(colNameOrder)
	assert.Equal(t, TS0, depsOrder, "Dependency for 'price' in ORDER BY should be table 0 (products)")
}
