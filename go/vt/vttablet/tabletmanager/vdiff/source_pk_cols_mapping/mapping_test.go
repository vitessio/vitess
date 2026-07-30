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

// Package source_pk_cols_mapping contains standalone tests for the PK-to-SELECT
// index mapping logic used by VDiff's getSourcePKCols. These tests require only
// the SQL parser and can run without MySQL, topo, or any Vitess infrastructure.
package source_pk_cols_mapping

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

// mapPKColsToSelectIndices replicates the core mapping logic from
// getSourcePKCols in table_differ.go. Given a source query and PK column
// names, it returns each PK column's index in the SELECT expression list.
func mapPKColsToSelectIndices(parser *sqlparser.Parser, sourceQuery string, pkColumns []string) ([]int, error) {
	statement, err := parser.Parse(sourceQuery)
	if err != nil {
		return nil, err
	}
	sourceSelect, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, errors.New("unexpected statement type")
	}

	result := make([]int, 0, len(pkColumns))
	for _, pkc := range pkColumns {
		found := false
		// Pass 1: match by underlying ColName (direct source column match).
		for i, selExpr := range sourceSelect.SelectExprs.Exprs {
			aliasedExpr, ok := selExpr.(*sqlparser.AliasedExpr)
			if !ok {
				continue
			}
			switch ct := aliasedExpr.Expr.(type) {
			case *sqlparser.ColName:
				if strings.EqualFold(pkc, ct.Name.String()) {
					result = append(result, i)
					found = true
				}
			case *sqlparser.FuncExpr:
				if strings.EqualFold(pkc, aliasedExpr.As.String()) {
					result = append(result, i)
					found = true
				}
			}
			if found {
				break
			}
		}
		// Pass 2: fallback to alias match for cross-table filters.
		if !found {
			for i, selExpr := range sourceSelect.SelectExprs.Exprs {
				aliasedExpr, ok := selExpr.(*sqlparser.AliasedExpr)
				if !ok {
					continue
				}
				if !aliasedExpr.As.IsEmpty() && strings.EqualFold(pkc, aliasedExpr.As.String()) {
					result = append(result, i)
					found = true
					break
				}
			}
		}
		if !found {
			return nil, fmt.Errorf("PK column %s not found in SELECT list", pkc)
		}
	}
	return result, nil
}

func TestMapSourcePKToSelectIndices(t *testing.T) {
	testCases := []struct {
		name        string
		sourceQuery string
		pkColumns   []string
		wantIndices []int
		wantErr     bool
	}{
		{
			name:        "natural order single PK",
			sourceQuery: "select c1, c2 from t1 order by c1 asc",
			pkColumns:   []string{"c1"},
			wantIndices: []int{0},
		},
		{
			name:        "reordered columns single PK",
			sourceQuery: "select c2, c1 from t1 order by c1 asc",
			pkColumns:   []string{"c1"},
			wantIndices: []int{1},
		},
		{
			name:        "composite PK natural order",
			sourceQuery: "select c1, c2 from multipk order by c1 asc, c2 asc",
			pkColumns:   []string{"c1", "c2"},
			wantIndices: []int{0, 1},
		},
		{
			name:        "composite PK columns reordered in select",
			sourceQuery: "select c2, c1 from multipk order by c1 asc, c2 asc",
			pkColumns:   []string{"c1", "c2"},
			wantIndices: []int{1, 0},
		},
		{
			name:        "composite PK 4 columns fully reversed",
			sourceQuery: "select d, c, b, a from t order by b asc, d asc",
			pkColumns:   []string{"b", "d"},
			wantIndices: []int{2, 0},
		},
		{
			name:        "alias matches PK (cross-table MoveTables filter)",
			sourceQuery: "select c0 as c1, c2 from t2 order by c1 asc",
			pkColumns:   []string{"c1"},
			wantIndices: []int{0},
		},
		{
			name:        "expression with alias matches PK",
			sourceQuery: "select c2, a + b as textcol from pktext order by textcol asc",
			pkColumns:   []string{"textcol"},
			wantIndices: []int{1},
		},
		{
			name:        "function expression with alias",
			sourceQuery: "select c1, c2, count(*) as c3, sum(c4) as c4 from t group by c1 order by c1 asc",
			pkColumns:   []string{"c1"},
			wantIndices: []int{0},
		},
		{
			name:        "PK at last position",
			sourceQuery: "select a, b, c, id from t order by id asc",
			pkColumns:   []string{"id"},
			wantIndices: []int{3},
		},
		{
			name:        "case insensitive match",
			sourceQuery: "select ID, Name from t order by ID asc",
			pkColumns:   []string{"id"},
			wantIndices: []int{0},
		},
		{
			name:        "PK not found in select list",
			sourceQuery: "select a, b from t order by a asc",
			pkColumns:   []string{"missing_col"},
			wantErr:     true,
		},
		{
			name:        "in_keyrange filter preserves column positions",
			sourceQuery: "select c1, c2 from t1 where in_keyrange('-80') order by c1 asc",
			pkColumns:   []string{"c1"},
			wantIndices: []int{0},
		},
		{
			name:        "three PKs scattered across wide select",
			sourceQuery: "select a, b, c, d, e, f from t order by b asc, d asc, f asc",
			pkColumns:   []string{"b", "d", "f"},
			wantIndices: []int{1, 3, 5},
		},
		{
			name:        "last PK resume scenario - c1 is PK but at index 1 in select",
			sourceQuery: "select c2, c1 from t1 where c1 > 100 order by c1 asc",
			pkColumns:   []string{"c1"},
			wantIndices: []int{1},
		},
		{
			name:        "multi-alias cross-table filter",
			sourceQuery: "select src_a as id, src_b as name, src_c as value from source_t order by id asc",
			pkColumns:   []string{"id", "name"},
			wantIndices: []int{0, 1},
		},
		{
			name:        "column swap alias does not shadow real PK",
			sourceQuery: "select b as a, a as b from t order by a asc",
			pkColumns:   []string{"a"},
			wantIndices: []int{1},
		},
		{
			name:        "column swap composite PK prefers ColName over alias",
			sourceQuery: "select b as a, a as b, c from t order by a asc, b asc",
			pkColumns:   []string{"a", "b"},
			wantIndices: []int{1, 0},
		},
	}

	parser := sqlparser.NewTestParser()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			indices, err := mapPKColsToSelectIndices(parser, tc.sourceQuery, tc.pkColumns)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantIndices, indices)
		})
	}
}
