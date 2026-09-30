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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestUpdateTableProgress(t *testing.T) {
	wd := &workflowDiffer{
		ct: &controller{
			id:                 1,
			TableDiffRowCounts: stats.NewCountersWithSingleLabel("", "", "Rows"),
		},
		opts: &tabletmanagerdatapb.VDiffOptions{
			CoreOptions: &tabletmanagerdatapb.VDiffCoreOptions{
				MaxDiffSeconds: 100,
			},
		},
	}
	table := &tabletmanagerdatapb.TableDefinition{
		Name: "test",
	}
	dr := &DiffReport{
		TableName:     table.Name,
		ProcessedRows: 1e9,
	}
	queryTemplate := `update _vt.vdiff_table set rows_compared = 1000000000, lastpk = '%s', report = '{"TableName":"test","ProcessedRows":1000000000,"MatchingRows":0,"MismatchedRows":0,"ExtraRowsSource":0,"ExtraRowsTarget":0}' where vdiff_id = 1 and table_name = 'test'`

	testCases := []struct {
		name           string
		fields         []*querypb.Field
		pkCols         []int
		sourcePkCols   []int
		lastRow        []sqltypes.Value
		expectedLastPK string
		wantErr        bool
	}{
		{
			name: "identical PKs",
			fields: []*querypb.Field{
				{
					Name: "a", Type: sqltypes.Int64,
				},
				{
					Name: "b", Type: sqltypes.Int64,
				},
			},
			pkCols:         []int{0, 1},
			sourcePkCols:   []int{0, 1},
			lastRow:        []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)},
			expectedLastPK: `target:{fields:{name:"a" type:INT64} fields:{name:"b" type:INT64} rows:{lengths:1 lengths:1 values:"12"}}`,
		},
		{
			name: "more PK cols on target",
			fields: []*querypb.Field{
				{
					Name: "a", Type: sqltypes.Int64,
				},
				{
					Name: "b", Type: sqltypes.Int64,
				},
			},
			pkCols:         []int{0, 1},
			sourcePkCols:   []int{0},
			lastRow:        []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)},
			expectedLastPK: `target:{fields:{name:"a" type:INT64} fields:{name:"b" type:INT64} rows:{lengths:1 lengths:1 values:"12"}} source:{fields:{name:"a" type:INT64} rows:{lengths:1 values:"1"}}`,
		},
		{
			name: "more PK cols on source",
			fields: []*querypb.Field{
				{
					Name: "a", Type: sqltypes.Int64,
				},
				{
					Name: "b", Type: sqltypes.Int64,
				},
			},
			pkCols:         []int{0},
			sourcePkCols:   []int{0, 1},
			lastRow:        []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)},
			expectedLastPK: `target:{fields:{name:"a" type:INT64} rows:{lengths:1 values:"1"}} source:{fields:{name:"a" type:INT64} fields:{name:"b" type:INT64} rows:{lengths:1 lengths:1 values:"12"}}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dbc := binlogplayer.NewMockDBClient(t)
			dbc.ExpectRequest(fmt.Sprintf(queryTemplate, tc.expectedLastPK), &sqltypes.Result{}, nil)
			// compareCols maps each SELECT position to its column name, as
			// buildTablePlan populates it in production.
			compareCols := make([]compareColInfo, len(tc.fields))
			for i, f := range tc.fields {
				compareCols[i] = compareColInfo{colIndex: i, colName: f.Name}
			}
			td := &tableDiffer{
				wd:    wd,
				table: table,
				tablePlan: &tablePlan{
					pkCols:       tc.pkCols,
					sourcePkCols: tc.sourcePkCols,
					compareCols:  compareCols,
					table: &tabletmanagerdatapb.TableDefinition{
						Fields: tc.fields,
					},
				},
			}
			if err := td.updateTableProgress(dbc, dr, tc.lastRow); (err != nil) != tc.wantErr {
				require.FailNow(t, "tableDiffer.updateTableProgress() error = %v, wantErr %v",
					err, tc.wantErr)
			}
		})
	}
}

func TestGetSourcePKCols_TableDroppedOnSource(t *testing.T) {
	tvde := newTestVDiffEnv(t)
	defer tvde.close()

	ct := tvde.createController(t, 1)

	table := &tabletmanagerdatapb.TableDefinition{
		Name:              "dropped_table",
		Columns:           []string{"c1", "c2"},
		PrimaryKeyColumns: []string{"c1"},
		Fields:            sqltypes.MakeTestFields("c1|c2", "int64|varchar"),
	}

	tvde.tmc.schema = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{},
	}

	td := &tableDiffer{
		wd: &workflowDiffer{
			ct: ct,
		},
		table: table,
		tablePlan: &tablePlan{
			table:       table,
			sourceQuery: "select c1, c2 from dropped_table order by c1 asc",
		},
	}

	err := td.getSourcePKCols()
	require.NoError(t, err)
	require.Nil(t, td.tablePlan.sourcePkCols)
}

// TestGetSourcePKCols_ComputedAliasRejected verifies that when a source PK
// column is projected only via a non-physical expression (a computed value
// aliased to the PK name) rather than as a physical column, getSourcePKCols
// rejects the plan. The row streamer orders the source by the physical PK column
// while VDiff would compare the aliased value, so the merge input would not be
// sorted and could produce false diffs even on the first pass.
func TestGetSourcePKCols_ComputedAliasRejected(t *testing.T) {
	tvde := newTestVDiffEnv(t)
	defer tvde.close()

	ct := tvde.createController(t, 1)

	// The source table's physical PK is "textcol", but the source query only
	// projects a computed expression "a + b" aliased to "textcol".
	sourceTable := &tabletmanagerdatapb.TableDefinition{
		Name:              "pktext",
		Columns:           []string{"textcol", "c2"},
		PrimaryKeyColumns: []string{"textcol"},
		Fields:            sqltypes.MakeTestFields("textcol|c2", "varchar|int64"),
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
			sourceQuery: "select c2, a + b as textcol from pktext order by textcol asc",
			// textcol is the comparison key, projected at SELECT index 1 as the
			// computed expression "a + b".
			comparePKs: []compareColInfo{{colIndex: 1, colName: "textcol", isPK: true}},
		},
	}

	err := td.getSourcePKCols()
	require.Error(t, err)
	require.False(t, td.tablePlan.sourceCheckpointUnavailable)
	require.Empty(t, td.tablePlan.sourcePkCols)
}

// TestGetSourcePKCols_ReorderedComparisonKeyRejected verifies that a plan whose
// source PK is fully projected is still rejected when the comparison key is not
// an order-preserving prefix of the physical source PK. Here the source PK is
// (a, b) but the filter projects "select b as target_a, a as target_b", so VDiff
// compares in (a=physical b, b=physical a) order while the row streamer emits
// rows ordered by the physical PK (a, b). Without this check the first pass would
// report false differences.
func TestGetSourcePKCols_ReorderedComparisonKeyRejected(t *testing.T) {
	tvde := newTestVDiffEnv(t)
	defer tvde.close()

	ct := tvde.createController(t, 1)

	sourceTable := &tabletmanagerdatapb.TableDefinition{
		Name:              "t",
		Columns:           []string{"a", "b"},
		PrimaryKeyColumns: []string{"a", "b"},
		Fields:            sqltypes.MakeTestFields("a|b", "int64|int64"),
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
			sourceQuery: "select b as target_a, a as target_b from t order by target_a asc, target_b asc",
			// The comparison key (target_a, target_b) maps to physical (b, a), which
			// is not a prefix of the physical source PK (a, b).
			comparePKs: []compareColInfo{
				{colIndex: 0, colName: "target_a", isPK: true},
				{colIndex: 1, colName: "target_b", isPK: true},
			},
		},
	}

	err := td.getSourcePKCols()
	require.Error(t, err)
	require.False(t, td.tablePlan.sourceCheckpointUnavailable)
	require.Empty(t, td.tablePlan.sourcePkCols)
}

// TestGetSourcePKCols_SubsetProjectionUnavailable mirrors the customer
// materialize CI regression: the source table has a composite PK (cid, typ) but
// the filter projects only a subset that omits the trailing PK column typ
// ("select cid, name from customer"). getSourcePKCols must NOT fail closed for
// this valid subset-projection filter, and it must NOT build a partial source
// key. Instead it flags the source checkpoint as unavailable so that no
// resumable checkpoint is persisted and the whole table restarts on resume.
func TestGetSourcePKCols_SubsetProjectionUnavailable(t *testing.T) {
	tvde := newTestVDiffEnv(t)
	defer tvde.close()

	ct := tvde.createController(t, 1)

	sourceTable := &tabletmanagerdatapb.TableDefinition{
		Name:              "customer",
		Columns:           []string{"cid", "name", "typ"},
		PrimaryKeyColumns: []string{"cid", "typ"},
		Fields:            sqltypes.MakeTestFields("cid|name|typ", "int64|varchar|varchar"),
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
			sourceQuery: "select cid, name from customer order by cid asc",
			// The target has cid as its (single) PK at SELECT index 0. cid is a
			// prefix of the physical source PK (cid, typ), so the merge is valid.
			pkCols:     []int{0},
			comparePKs: []compareColInfo{{colIndex: 0, colName: "cid", isPK: true}},
		},
	}

	err := td.getSourcePKCols()
	require.NoError(t, err)
	// The source checkpoint is flagged unavailable and no partial source key is
	// built.
	require.True(t, td.tablePlan.sourceCheckpointUnavailable)
	require.Empty(t, td.tablePlan.sourcePkCols)
}

// TestGetSourcePKCols_FreshReorderedLayoutCheckpoints verifies that a fresh VDiff
// (no persisted checkpoint) on a table whose source query reorders the PK columns
// still checkpoints correctly with the SELECT-position mapping. Here the composite
// PK (c, a) is projected as b, c, a, so the corrected sourcePkCols are [1, 2] and
// lastPKFromRow pairs each PK value with the right column. The upgrade guard only
// affects a loaded checkpoint, so a fresh run is unaffected.
func TestGetSourcePKCols_FreshReorderedLayoutCheckpoints(t *testing.T) {
	tvde := newTestVDiffEnv(t)
	defer tvde.close()

	ct := tvde.createController(t, 1)

	sourceTable := &tabletmanagerdatapb.TableDefinition{
		Name:              "t",
		Columns:           []string{"a", "b", "c"},
		PrimaryKeyColumns: []string{"c", "a"},
		Fields:            sqltypes.MakeTestFields("a|b|c", "int64|int64|int64"),
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
			sourceQuery: "select b, c, a from t order by c asc, a asc",
			compareCols: []compareColInfo{
				{colIndex: 0, colName: "b"},
				{colIndex: 1, colName: "c"},
				{colIndex: 2, colName: "a"},
			},
			pkCols: []int{2, 1},
		},
	}

	require.NoError(t, td.getSourcePKCols())
	require.False(t, td.tablePlan.sourceCheckpointUnavailable, "a fresh reordered layout must still be checkpointed")
	require.Equal(t, []int{1, 2}, td.tablePlan.sourcePkCols)

	// A streamed row [b=10, c=20, a=30] serializes the source checkpoint as
	// PK (c, a) = (20, 30) using SELECT indices [1, 2].
	row := []sqltypes.Value{sqltypes.NewInt64(10), sqltypes.NewInt64(20), sqltypes.NewInt64(30)}
	lastPK := td.lastPKFromRow(row)
	require.NotNil(t, lastPK.Source)
	sourceResult := sqltypes.Proto3ToResult(lastPK.Source)
	require.Len(t, sourceResult.Rows, 1)
	require.Equal(t, "20", sourceResult.Rows[0][0].ToString(), "first source PK value should be column c")
	require.Equal(t, "30", sourceResult.Rows[0][1].ToString(), "second source PK value should be column a")
}

// TestGetSourcePKCols_DiscardsLegacyOrderedCheckpoint verifies the upgrade guard.
// A pre-fix checkpoint cannot be told apart from a current one by field names (a
// single reordered PK keeps its name; only the value differs), so any layout whose
// corrected mapping differs from the old column-ordinal mapping is treated as
// un-checkpointable: a loaded checkpoint is discarded and both streams restart. A
// layout whose mapping is unchanged keeps its checkpoint.
func TestGetSourcePKCols_DiscardsLegacyOrderedCheckpoint(t *testing.T) {
	tvde := newTestVDiffEnv(t)
	defer tvde.close()

	ct := tvde.createController(t, 1)

	t.Run("reordered single-PK layout discards a name-matching legacy checkpoint", func(t *testing.T) {
		// Columns (id, payload), PK id, filter "select payload, id": the corrected
		// index is 1 but the pre-fix code used the DDL index 0. The old checkpoint's
		// field is still named "id" (matching the corrected name) while its value
		// came from SELECT position 0 (payload) -- so a name check would wrongly
		// accept it. The mapping guard discards it.
		sourceTable := &tabletmanagerdatapb.TableDefinition{
			Name:              "t",
			Columns:           []string{"id", "payload"},
			PrimaryKeyColumns: []string{"id"},
			Fields:            sqltypes.MakeTestFields("id|payload", "int64|varchar"),
		}
		tvde.tmc.schema = &tabletmanagerdatapb.SchemaDefinition{
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{sourceTable},
		}
		td := &tableDiffer{
			wd:    &workflowDiffer{ct: ct},
			table: sourceTable,
			tablePlan: &tablePlan{
				table:       sourceTable,
				sourceQuery: "select payload, id from t order by id asc",
				compareCols: []compareColInfo{{colIndex: 0, colName: "payload"}, {colIndex: 1, colName: "id"}},
				pkCols:      []int{1},
			},
		}
		td.lastSourcePK = &querypb.QueryResult{Fields: sqltypes.MakeTestFields("id", "int64")}
		td.lastTargetPK = td.lastSourcePK

		require.NoError(t, td.getSourcePKCols())
		require.True(t, td.tablePlan.sourceCheckpointUnavailable)
		require.Nil(t, td.lastSourcePK, "a name-matching legacy checkpoint on a reordered layout must be discarded")
		require.Nil(t, td.lastTargetPK, "both streams must restart")
	})

	t.Run("unchanged layout keeps its checkpoint", func(t *testing.T) {
		// Columns (id, payload), PK id, filter "select id, payload": the corrected
		// mapping matches the old one, so the checkpoint is safe to reuse.
		sourceTable := &tabletmanagerdatapb.TableDefinition{
			Name:              "t2",
			Columns:           []string{"id", "payload"},
			PrimaryKeyColumns: []string{"id"},
			Fields:            sqltypes.MakeTestFields("id|payload", "int64|varchar"),
		}
		tvde.tmc.schema = &tabletmanagerdatapb.SchemaDefinition{
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{sourceTable},
		}
		td := &tableDiffer{
			wd:    &workflowDiffer{ct: ct},
			table: sourceTable,
			tablePlan: &tablePlan{
				table:       sourceTable,
				sourceQuery: "select id, payload from t2 order by id asc",
				compareCols: []compareColInfo{{colIndex: 0, colName: "id"}, {colIndex: 1, colName: "payload"}},
				pkCols:      []int{0},
			},
		}
		td.lastSourcePK = &querypb.QueryResult{Fields: sqltypes.MakeTestFields("id", "int64")}
		td.lastTargetPK = td.lastSourcePK

		require.NoError(t, td.getSourcePKCols())
		require.False(t, td.tablePlan.sourceCheckpointUnavailable)
		require.Equal(t, []int{0}, td.tablePlan.sourcePkCols)
		require.NotNil(t, td.lastSourcePK, "an unchanged-layout checkpoint must be kept")
	})
}

// TestGetSourcePKCols_SubsetProjectionPersistReloadResume is the persist/reload/
// resume regression test for the subset-projection case (source PK (cid, typ),
// target PK cid, filter "select cid, name from customer"). It exercises the full
// lifecycle where the failure would otherwise occur:
//
//  1. Persist: updateTableProgress runs after rows have been processed.
//  2. Reload: getTableLastPK reads the persisted state back on resume.
//
// Because the source checkpoint is unavailable for this filter, no lastpk may be
// persisted: a target-only checkpoint would resume the target mid-table while
// the source restarts from the beginning, so the merge loop would report every
// earlier source row as ExtraRowsSource. The fix persists no lastpk (an explicit
// "source checkpoint unavailable" state), so getTableLastPK returns nil on resume
// and the whole table restarts from the beginning for both the source and target
// streams.
func TestGetSourcePKCols_SubsetProjectionPersistReloadResume(t *testing.T) {
	tvde := newTestVDiffEnv(t)
	defer tvde.close()

	ct := tvde.createController(t, 1)

	sourceTable := &tabletmanagerdatapb.TableDefinition{
		Name:              "customer",
		Columns:           []string{"cid", "name", "typ"},
		PrimaryKeyColumns: []string{"cid", "typ"},
		Fields:            sqltypes.MakeTestFields("cid|name|typ", "int64|varchar|varchar"),
	}
	tvde.tmc.schema = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{sourceTable},
	}

	wd := &workflowDiffer{ct: ct}
	td := &tableDiffer{
		wd:    wd,
		table: sourceTable,
		tablePlan: &tablePlan{
			table:       sourceTable,
			sourceQuery: "select cid, name from customer order by cid asc",
			// The target has cid as its (single) PK at SELECT index 0. cid is a
			// prefix of the physical source PK (cid, typ), so the merge is valid.
			pkCols:     []int{0},
			comparePKs: []compareColInfo{{colIndex: 0, colName: "cid", isPK: true}},
		},
	}

	require.NoError(t, td.getSourcePKCols())
	require.True(t, td.tablePlan.sourceCheckpointUnavailable)

	// --- Persist: even with a processed row, updateTableProgress must explicitly
	// clear lastpk (to NULL) for a source-checkpoint-unavailable table, so no
	// resumable (and unsafe) checkpoint remains, including any stale value from
	// before this fix.
	persistClient := binlogplayer.NewMockDBClient(t)
	persistClient.ExpectRequestRE(
		`^update _vt\.vdiff_table set rows_compared = 100, lastpk = null, report = '.*' where vdiff_id = 1 and table_name = 'customer'$`,
		&sqltypes.Result{}, nil)
	dr := &DiffReport{TableName: sourceTable.Name, ProcessedRows: 100}
	row := []sqltypes.Value{sqltypes.NewInt64(42), sqltypes.NewVarChar("acme")}
	require.NoError(t, td.updateTableProgress(persistClient, dr, row))
	// The in-memory retry PKs must remain unset so a same-process
	// max-diff-duration restart also restarts both streams from the beginning.
	require.Nil(t, td.lastSourcePK)
	require.Nil(t, td.lastTargetPK)

	// --- Reload: with no lastpk persisted, getTableLastPK returns nil, so on
	// resume both td.lastSourcePK and td.lastTargetPK stay nil and the whole
	// table restarts from the beginning for both streams.
	reloadClient := binlogplayer.NewMockDBClient(t)
	getQuery, err := sqlparser.ParseAndBind(sqlGetVDiffTable,
		sqltypes.Int64BindVariable(ct.id),
		sqltypes.StringBindVariable(sourceTable.Name),
	)
	require.NoError(t, err)
	reloadClient.ExpectRequest(getQuery, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("lastpk|mismatch|report", "varbinary|int64|varbinary"),
		"|0|", // empty lastpk
	), nil)

	reloaded, err := wd.getTableLastPK(reloadClient, sourceTable.Name)
	require.NoError(t, err)
	require.Nil(t, reloaded, "no lastpk persisted, so resume must restart the whole table for both streams")
}
