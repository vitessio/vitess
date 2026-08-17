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

package vdiff

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// TestSourcePKColsOrdering verifies that getSourcePKCols maps PK columns
// against the source query's SELECT expression order (the actual row layout),
// not against td.table.Columns (column ordinal position).
//
// Before the fix, the function mapped PK columns against td.table.Columns,
// producing indices in ordinal order. When the source query reorders columns
// (e.g., "select c2, c1 from t1"), lastPKFromRow would index into the wrong
// position, corrupting the resume checkpoint.
func TestSourcePKColsOrdering(t *testing.T) {
	tvde := newTestVDiffEnv(t)
	defer tvde.close()

	ct := tvde.createController(t, 1)

	testCases := []struct {
		name string
		// sourceTable is the physical table the source query reads from
		// (the table in the FROM clause). getSourcePKCols resolves the source
		// schema from this name, which can differ from the target table name.
		sourceTable       string
		columns           []string
		primaryKeyColumns []string
		sourceQuery       string
		wantSourcePkCols  []int
	}{
		{
			name:              "columns in natural order",
			sourceTable:       "t",
			columns:           []string{"c1", "c2"},
			primaryKeyColumns: []string{"c1"},
			sourceQuery:       "select c1, c2 from t order by c1 asc",
			wantSourcePkCols:  []int{0},
		},
		{
			name:              "columns reordered in select",
			sourceTable:       "t",
			columns:           []string{"c1", "c2"},
			primaryKeyColumns: []string{"c1"},
			sourceQuery:       "select c2, c1 from t order by c1 asc",
			wantSourcePkCols:  []int{1},
		},
		{
			name:              "composite pk reordered in select",
			sourceTable:       "t",
			columns:           []string{"a", "b", "c"},
			primaryKeyColumns: []string{"c", "a"},
			sourceQuery:       "select a, b, c from t order by c asc, a asc",
			wantSourcePkCols:  []int{2, 0},
		},
		{
			name:              "composite pk with select reorder",
			sourceTable:       "t",
			columns:           []string{"a", "b", "c", "d"},
			primaryKeyColumns: []string{"b", "d"},
			sourceQuery:       "select d, c, b, a from t order by b asc, d asc",
			wantSourcePkCols:  []int{2, 0},
		},
		{
			// Cross-table MoveTables filter: the source table is t2 and its
			// physical PK is c0, renamed to the target column c1 in the SELECT.
			// The source PK c0 is matched via its underlying ColName.
			name:              "renamed physical source column resolves from source table",
			sourceTable:       "t2",
			columns:           []string{"c0", "c2"},
			primaryKeyColumns: []string{"c0"},
			sourceQuery:       "select c0 as c1, c2 from t2 order by c1 asc",
			wantSourcePkCols:  []int{0},
		},
		{
			name:              "pk matches ordinal order",
			sourceTable:       "t",
			columns:           []string{"a", "b", "c"},
			primaryKeyColumns: []string{"a", "b"},
			sourceQuery:       "select a, b, c from t order by a asc, b asc",
			wantSourcePkCols:  []int{0, 1},
		},
		{
			name:              "column swap alias does not shadow real pk",
			sourceTable:       "t",
			columns:           []string{"a", "b"},
			primaryKeyColumns: []string{"a"},
			sourceQuery:       "select b as a, a as b from t order by a asc",
			wantSourcePkCols:  []int{1},
		},
		{
			name:              "column swap composite pk prefers colname over alias",
			sourceTable:       "t",
			columns:           []string{"a", "b", "c"},
			primaryKeyColumns: []string{"a", "b"},
			sourceQuery:       "select b as a, a as b, c from t order by a asc, b asc",
			wantSourcePkCols:  []int{1, 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			types := make([]string, len(tc.columns))
			for i := range types {
				types[i] = "varbinary"
			}
			fieldTypes := strings.Join(types, "|")
			fields := strings.Join(tc.columns, "|")

			// The source schema is keyed by the physical source table name so
			// that getSourcePKCols resolves it from the query's FROM clause.
			sourceTable := &tabletmanagerdatapb.TableDefinition{
				Name:              tc.sourceTable,
				Columns:           tc.columns,
				PrimaryKeyColumns: tc.primaryKeyColumns,
				Fields:            sqltypes.MakeTestFields(fields, fieldTypes),
			}

			tvde.tmc.schema = &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{sourceTable},
			}

			td := &tableDiffer{
				wd: &workflowDiffer{
					ct: ct,
				},
				table: sourceTable,
				tablePlan: &tablePlan{
					table:       sourceTable,
					sourceQuery: tc.sourceQuery,
				},
			}

			err := td.getSourcePKCols()
			require.NoError(t, err)

			assert.Equal(t, tc.wantSourcePkCols, td.tablePlan.sourcePkCols,
				"sourcePkCols should reflect PK column positions in the source SELECT expression list")
		})
	}
}

// TestSourcePKSelectIndices tests the extracted sourcePKSelectIndices function
// directly with parsed queries, without needing topo/tablet infrastructure.
func TestSourcePKSelectIndices(t *testing.T) {
	testCases := []struct {
		name        string
		sourceQuery string
		pkColumns   []string
		wantIndices []int
		// wantErr is for the present-but-non-physical case (a PK name is
		// projected only via a computed/unrelated expression): fail closed.
		wantErr bool
		// wantNotProjected is for the entirely-absent case (a PK column is not
		// projected at all): no error, allProjected == false, and no partial
		// index slice is produced.
		wantNotProjected bool
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
			// Cross-table MoveTables filter: the physical source column c0 is
			// renamed to the target column c1. The source PK is the physical
			// column c0, which we match via the underlying ColName.
			name:        "renamed physical source column (cross-table MoveTables filter)",
			sourceQuery: "select c0 as c1, c2 from t2 order by c1 asc",
			pkColumns:   []string{"c0"},
			wantIndices: []int{0},
		},
		{
			// A CONVERT(col USING charset) rename must unwrap to the inner
			// physical source column, mirroring the row streamer planner.
			name:        "convert using rename unwraps to source column",
			sourceQuery: "select convert(c1 using utf8mb4) as c2, c3 from t order by c2 asc",
			pkColumns:   []string{"c1"},
			wantIndices: []int{0},
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
			// A PK column entirely absent from the SELECT list is a valid
			// subset-projection filter: no error, and allProjected is false so
			// the caller does not build a partial source key.
			name:             "PK entirely absent from select list is not projected",
			sourceQuery:      "select a, b from t order by a asc",
			pkColumns:        []string{"missing_col"},
			wantNotProjected: true,
		},
		{
			// Mirrors the customer CI case: composite source PK (cid, typ) with
			// a filter that projects only cid. typ is entirely absent, so the
			// whole key is treated as not projected (never a partial [0] slice).
			name:             "composite PK with one column absent is not projected",
			sourceQuery:      "select cid, name from customer order by cid asc",
			pkColumns:        []string{"cid", "typ"},
			wantNotProjected: true,
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
			// The source PKs are the physical columns src_a and src_b, matched
			// via their underlying ColNames even though they are renamed.
			name:        "multi-column cross-table filter matches physical columns",
			sourceQuery: "select src_a as id, src_b as name, src_c as value from source_t order by id asc",
			pkColumns:   []string{"src_a", "src_b"},
			wantIndices: []int{0, 1},
		},
		{
			// A computed alias must not satisfy a source PK lookup: the row
			// streamer resumes using the physical PK value, so persisting a
			// derived value would skip/repeat rows. Fail closed instead.
			name:        "computed alias does not satisfy source PK (fails closed)",
			sourceQuery: "select a + b as id, c from t order by id asc",
			pkColumns:   []string{"id"},
			wantErr:     true,
		},
		{
			// An alias mapping an unrelated physical column to the source PK
			// name must not match; only the real physical PK column may.
			name:        "unrelated column aliased to source PK name fails closed",
			sourceQuery: "select other_col as id, c from t order by id asc",
			pkColumns:   []string{"id"},
			wantErr:     true,
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
			statement, err := parser.Parse(tc.sourceQuery)
			require.NoError(t, err)
			sourceSelect, ok := statement.(*sqlparser.Select)
			require.True(t, ok)

			indices, allProjected, err := sourcePKSelectIndices(sourceSelect, tc.pkColumns)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.wantNotProjected {
				assert.False(t, allProjected)
				assert.Empty(t, indices, "must not return a partial index slice when a PK column is absent")
				return
			}
			assert.True(t, allProjected)
			assert.Equal(t, tc.wantIndices, indices)
		})
	}
}
