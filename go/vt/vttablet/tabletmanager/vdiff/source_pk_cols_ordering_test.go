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
		name              string
		columns           []string
		primaryKeyColumns []string
		sourceQuery       string
		wantSourcePkCols  []int
	}{
		{
			name:              "columns in natural order",
			columns:           []string{"c1", "c2"},
			primaryKeyColumns: []string{"c1"},
			sourceQuery:       "select c1, c2 from t order by c1 asc",
			wantSourcePkCols:  []int{0},
		},
		{
			name:              "columns reordered in select",
			columns:           []string{"c1", "c2"},
			primaryKeyColumns: []string{"c1"},
			sourceQuery:       "select c2, c1 from t order by c1 asc",
			wantSourcePkCols:  []int{1},
		},
		{
			name:              "composite pk reordered in select",
			columns:           []string{"a", "b", "c"},
			primaryKeyColumns: []string{"c", "a"},
			sourceQuery:       "select a, b, c from t order by c asc, a asc",
			wantSourcePkCols:  []int{2, 0},
		},
		{
			name:              "composite pk with select reorder",
			columns:           []string{"a", "b", "c", "d"},
			primaryKeyColumns: []string{"b", "d"},
			sourceQuery:       "select d, c, b, a from t order by b asc, d asc",
			wantSourcePkCols:  []int{2, 0},
		},
		{
			name:              "aliased column matches pk via alias",
			columns:           []string{"c1", "c2"},
			primaryKeyColumns: []string{"c1"},
			sourceQuery:       "select c0 as c1, c2 from t2 order by c1 asc",
			wantSourcePkCols:  []int{0},
		},
		{
			name:              "pk matches ordinal order",
			columns:           []string{"a", "b", "c"},
			primaryKeyColumns: []string{"a", "b"},
			sourceQuery:       "select a, b, c from t order by a asc, b asc",
			wantSourcePkCols:  []int{0, 1},
		},
		{
			name:              "column swap alias does not shadow real pk",
			columns:           []string{"a", "b"},
			primaryKeyColumns: []string{"a"},
			sourceQuery:       "select b as a, a as b from t order by a asc",
			wantSourcePkCols:  []int{1},
		},
		{
			name:              "column swap composite pk prefers colname over alias",
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

			table := &tabletmanagerdatapb.TableDefinition{
				Name:              tc.name,
				Columns:           tc.columns,
				PrimaryKeyColumns: tc.primaryKeyColumns,
				Fields:            sqltypes.MakeTestFields(fields, fieldTypes),
			}

			tvde.tmc.schema = &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{table},
			}

			td := &tableDiffer{
				wd: &workflowDiffer{
					ct: ct,
				},
				table: table,
				tablePlan: &tablePlan{
					table:       table,
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
