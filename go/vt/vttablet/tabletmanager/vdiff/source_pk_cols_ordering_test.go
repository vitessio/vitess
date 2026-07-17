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

package vdiff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// TestSourcePKColsOrdering verifies that getSourcePKCols preserves PK
// definition order (SEQ_IN_INDEX) rather than column ordinal position.
//
// Before the fix, the function put PrimaryKeyColumns into a map (losing order)
// then scanned td.table.Columns in ORDINAL_POSITION order. This produced
// sourcePkCols in ordinal order which corrupted lastpk on VDiff resume.
func TestSourcePKColsOrdering(t *testing.T) {
	tvde := newTestVDiffEnv(t)
	defer tvde.close()

	ct := tvde.createController(t, 1)

	testCases := []struct {
		name              string
		columns           []string
		primaryKeyColumns []string
		wantSourcePkCols  []int
	}{
		{
			name:              "pk_order_differs_from_ordinal",
			columns:           []string{"a", "b", "c", "d", "e"},
			primaryKeyColumns: []string{"c", "a"},
			wantSourcePkCols:  []int{2, 0},
		},
		{
			name:              "three_col_pk_not_in_ordinal_order",
			columns:           []string{"a", "b", "c", "d", "e", "f"},
			primaryKeyColumns: []string{"b", "d", "a"},
			wantSourcePkCols:  []int{1, 3, 0},
		},
		{
			name:              "two_col_pk_reversed",
			columns:           []string{"a", "b", "c"},
			primaryKeyColumns: []string{"b", "a"},
			wantSourcePkCols:  []int{1, 0},
		},
		{
			name:              "pk_matches_ordinal",
			columns:           []string{"a", "b", "c"},
			primaryKeyColumns: []string{"a", "b"},
			wantSourcePkCols:  []int{0, 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fieldTypes := ""
			for i := range tc.columns {
				if i > 0 {
					fieldTypes += "|"
				}
				fieldTypes += "varbinary"
			}

			fields := ""
			for i, col := range tc.columns {
				if i > 0 {
					fields += "|"
				}
				fields += col
			}

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
					table: table,
				},
			}

			err := td.getSourcePKCols()
			require.NoError(t, err)

			assert.Equal(t, tc.wantSourcePkCols, td.tablePlan.sourcePkCols,
				"sourcePkCols should reflect PK definition order (SEQ_IN_INDEX), not column ordinal position")
		})
	}
}
