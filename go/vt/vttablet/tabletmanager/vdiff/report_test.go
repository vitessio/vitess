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

package vdiff

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestGenRowDiff(t *testing.T) {
	vdenv := newTestVDiffEnv(t)
	defer vdenv.close()

	testCases := []struct {
		name          string
		schema        *tabletmanagerdatapb.SchemaDefinition
		query         string
		tablePlan     *tablePlan
		row           []sqltypes.Value
		reportOptions *tabletmanagerdatapb.VDiffReportOptions
		want          *RowDiff
	}{
		{
			name: "defaults",
			schema: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					{
						Name:              "t1",
						Columns:           []string{"c1", "c2", "c3", "c4", "c5"},
						PrimaryKeyColumns: []string{"c1", "c5"},
						Fields:            sqltypes.MakeTestFields("c1|c2|c3|c4|c5", "int64|int64|varchar|varchar|int64"),
					},
				},
			},
			query: "select c1,c2,c3,c4,c5 from t1",
			tablePlan: &tablePlan{
				selectPks: []int{0, 4},
			},
			row: []sqltypes.Value{
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(2),
				sqltypes.NewVarChar("hi3"),
				sqltypes.NewVarChar("hi4"),
				sqltypes.NewInt64(5),
			},
			reportOptions: &tabletmanagerdatapb.VDiffReportOptions{},
			want: &RowDiff{
				Row: map[string]string{ // The two PK cols should be first
					"c1": "1", "c5": "5", "c2": "2", "c3": "hi3", "c4": "hi4",
				},
			},
		},
		{
			name: "only PKs",
			schema: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					{
						Name:              "t1",
						Columns:           []string{"c1", "c2"},
						PrimaryKeyColumns: []string{"c1"},
						Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
					},
				},
			},
			query: "select c1,c2 from t1",
			tablePlan: &tablePlan{
				selectPks: []int{0},
			},
			row: []sqltypes.Value{
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(2),
			},
			reportOptions: &tabletmanagerdatapb.VDiffReportOptions{
				OnlyPks: true,
			},
			want: &RowDiff{
				Row: map[string]string{
					"c1": "1",
				},
			},
		},
		{
			name: "debug query",
			schema: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					{
						Name:              "t1",
						Columns:           []string{"c1", "c2"},
						PrimaryKeyColumns: []string{"c1"},
						Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
					},
				},
			},
			query: "select c1,c2 from t1",
			tablePlan: &tablePlan{
				selectPks: []int{0},
			},
			row: []sqltypes.Value{
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(2),
			},
			reportOptions: &tabletmanagerdatapb.VDiffReportOptions{
				DebugQuery: true,
			},
			want: &RowDiff{
				Row: map[string]string{
					"c1": "1",
					"c2": "2",
				},
				Query: "select c1, c2 from t1 where c1=1;",
			},
		},
		{
			name: "column truncation",
			schema: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					{
						Name:              "t1",
						Columns:           []string{"c1", "c2"},
						PrimaryKeyColumns: []string{"c1"},
						Fields:            sqltypes.MakeTestFields("c1|c2", "varchar|varchar"),
					},
				},
			},
			query: "select c1,c2 from t1",
			tablePlan: &tablePlan{
				selectPks: []int{0},
			},
			row: []sqltypes.Value{
				sqltypes.NewVarChar(strings.Repeat("a", 100)),
				sqltypes.NewVarChar(strings.Repeat("b", 100)),
			},
			reportOptions: &tabletmanagerdatapb.VDiffReportOptions{
				RowDiffColumnTruncateAt: 5,
			},
			want: &RowDiff{
				Row: map[string]string{
					"c1": strings.Repeat("a", 100), // PK fields are not truncated
					"c2": strings.Repeat("b", 5) + truncatedNotation,
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotNil(t, tc.tablePlan)
			require.NotNil(t, tc.row)
			require.NotNil(t, tc.reportOptions)

			vdenv.tmc.schema = tc.schema
			ct := vdenv.createController(t, 1)
			wd, err := newWorkflowDiffer(ct, vdenv.opts, collations.MySQL8())
			require.NoError(t, err)
			td := &tableDiffer{
				wd:          wd,
				sourceQuery: tc.query,
				tablePlan:   tc.tablePlan,
			}

			got, err := td.genRowDiff(tc.query, tc.row, tc.reportOptions)
			require.NoError(t, err)
			require.EqualValues(t, tc.want, got)
		})
	}
}
