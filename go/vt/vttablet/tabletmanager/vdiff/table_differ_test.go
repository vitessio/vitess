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
	"google.golang.org/protobuf/encoding/prototext"

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
			td := &tableDiffer{
				wd:    wd,
				table: table,
				tablePlan: &tablePlan{
					pkCols:       tc.pkCols,
					sourcePkCols: tc.sourcePkCols,
					table: &tabletmanagerdatapb.TableDefinition{
						Fields: tc.fields,
					},
				},
			}
			if err := td.updateTableProgress(dbc, dr, tc.lastRow); (err != nil) != tc.wantErr {
				require.FailNowf(t, "tableDiffer.updateTableProgress() error =", "%v, wantErr %v",
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

// TestGetSourcePKCols_ComputedAliasFailsClosed verifies that when a source PK
// column is not projected as a physical column in the source query (only a
// computed value aliased to the PK name is present), getSourcePKCols fails
// closed rather than persisting the derived value as the source checkpoint.
// The row streamer resumes using the physical source PK, so accepting the
// computed value would skip or repeat rows on resume.
func TestGetSourcePKCols_ComputedAliasFailsClosed(t *testing.T) {
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
		},
	}

	err := td.getSourcePKCols()
	require.Error(t, err)
	require.ErrorContains(t, err, "source PK column textcol is projected only via a non-physical expression")
	require.Nil(t, td.tablePlan.sourcePkCols)
}

// TestGetSourcePKCols_SubsetProjectionNoSourceKey mirrors the customer
// materialize CI regression: the source table has a composite PK (cid, typ) but
// the filter projects only a subset that omits the trailing PK column typ
// ("select cid, name from customer"). getSourcePKCols must NOT fail closed for
// this valid subset-projection filter, and it must NOT build a partial source
// key. Instead it records an explicit "source checkpoint unavailable" state by
// setting an empty sourcePkCols, so that lastPKFromRow emits an explicit empty
// source checkpoint (non-nil, zero PK values) rather than a nil Source. A nil
// Source is overloaded to mean "same as the target", which on resume would
// substitute the (wrong-length) target key for the source and fail the row
// streamer's length check. The explicit empty checkpoint instead makes the
// source restart safely from the beginning on resume.
func TestGetSourcePKCols_SubsetProjectionNoSourceKey(t *testing.T) {
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
			// The target has cid as its (single) PK at SELECT index 0.
			pkCols: []int{0},
		},
	}

	err := td.getSourcePKCols()
	require.NoError(t, err)
	// sourcePkCols is set to an explicit empty (non-nil) slice, which differs
	// from the target pkCols so that lastPKFromRow builds a distinct but empty
	// source checkpoint.
	require.Equal(t, []int{}, td.tablePlan.sourcePkCols)

	// The persisted source checkpoint must be an explicit empty QueryResult
	// (non-nil, zero PK values), NOT nil. A nil Source would be substituted with
	// the (single-column) target key on resume and fail the source row streamer's
	// length check against the two-column source PK.
	row := []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewVarChar("acme")}
	lastPK := td.lastPKFromRow(row)
	require.NotNil(t, lastPK.Target)
	require.NotNil(t, lastPK.Source, "must persist an explicit empty source checkpoint, not a nil (same-as-target) Source")
	sourceResult := sqltypes.Proto3ToResult(lastPK.Source)
	require.Len(t, sourceResult.Rows, 1)
	require.Empty(t, sourceResult.Rows[0], "explicit empty source checkpoint must carry zero PK values")
}

// TestGetSourcePKCols_ResumeCheckpointReorderedPK is a resume regression test.
// For a composite-PK table whose source query reorders the PK columns
// (SELECT layout differs from PK definition order), it confirms that the
// sourcePkCols indices point at the correct SELECT positions so that the
// persisted source lastpk pairs each PK value with the right column. Before
// the fix, the indices were in column-ordinal order and lastPKFromRow built a
// corrupted source checkpoint, causing false ExtraRowsSource on every resume.
func TestGetSourcePKCols_ResumeCheckpointReorderedPK(t *testing.T) {
	tvde := newTestVDiffEnv(t)
	defer tvde.close()

	ct := tvde.createController(t, 1)

	// Physical source table "t" has columns a,b,c with composite PK (c, a).
	// The source query projects them in a different order: b, c, a.
	// So source PK "c" is at SELECT index 1 and "a" is at SELECT index 2.
	// Fields are in DDL order (a,b,c), matching what GetSchema returns.
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
			// Target PK differs from source PK order so that lastPKFromRow
			// persists a distinct Source checkpoint.
			pkCols: []int{2, 1},
		},
	}

	err := td.getSourcePKCols()
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, td.tablePlan.sourcePkCols)

	// A streamed row [b=10, c=20, a=30] must serialize the source checkpoint
	// as PK (c, a) = (20, 30), i.e. taking SELECT indices [1, 2], not the
	// ordinal-order indices [2, 0] that the buggy code would have produced.
	row := []sqltypes.Value{sqltypes.NewInt64(10), sqltypes.NewInt64(20), sqltypes.NewInt64(30)}
	lastPK := td.lastPKFromRow(row)
	require.NotNil(t, lastPK.Source)
	sourceResult := sqltypes.Proto3ToResult(lastPK.Source)
	require.Len(t, sourceResult.Rows, 1)
	require.Equal(t, "20", sourceResult.Rows[0][0].ToString(), "first source PK value should be column c")
	require.Equal(t, "30", sourceResult.Rows[0][1].ToString(), "second source PK value should be column a")
	// Note: the checkpoint field-type metadata is still looked up via colIndex
	// against table.Fields (DDL order), which is the pre-existing known
	// limitation documented in the PR; it affects the target pkCols path
	// equally and is out of scope here. This test pins the value indices, which
	// is what this change fixes.
}

// TestGetSourcePKCols_SubsetProjectionPersistReloadResume is the persist/reload/
// resume regression test for the subset-projection case (source PK (cid, typ),
// target PK cid, filter "select cid, name from customer"). It exercises the full
// lifecycle where the failure actually occurs:
//
//  1. Persist: lastPKFromRow builds the checkpoint saved during the copy phase.
//  2. Reload: getTableLastPK reads it back on resume, applying the nil-Source
//     substitution rule.
//  3. Resume: the source stream decodes the reloaded checkpoint exactly as
//     TabletServer.VStreamRows does.
//
// The fix records an explicit empty source checkpoint (non-nil, zero PK values)
// rather than a nil Source. A nil Source would be substituted with the
// single-column target key on reload and, when handed to the source row streamer
// whose table has a two-column PK, would fail the length check in buildSelect
// (rowstreamer.go) before streaming any rows. The explicit empty checkpoint
// instead decodes to a zero-length lastpk, so the source restarts safely from
// the beginning while the target resumes from its own checkpoint.
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
			// The target has cid as its (single) PK at SELECT index 0.
			pkCols: []int{0},
		},
	}

	require.NoError(t, td.getSourcePKCols())
	require.Equal(t, []int{}, td.tablePlan.sourcePkCols)

	// --- Persist: the checkpoint lastPKFromRow saves during the copy phase. ---
	row := []sqltypes.Value{sqltypes.NewInt64(42), sqltypes.NewVarChar("acme")}
	lastPK := td.lastPKFromRow(row)
	require.NotNil(t, lastPK.Source, "subset-projection must persist an explicit empty source checkpoint")
	lastPKTxt, err := prototext.Marshal(lastPK)
	require.NoError(t, err)

	// --- Reload: getTableLastPK reads the persisted checkpoint on resume. ---
	dbClient := binlogplayer.NewMockDBClient(t)
	getQuery, err := sqlparser.ParseAndBind(sqlGetVDiffTable,
		sqltypes.Int64BindVariable(ct.id),
		sqltypes.StringBindVariable(sourceTable.Name),
	)
	require.NoError(t, err)
	dbClient.ExpectRequest(getQuery, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("lastpk|mismatch|report", "varbinary|int64|varbinary"),
		fmt.Sprintf("%s|0|", string(lastPKTxt)),
	), nil)

	reloaded, err := wd.getTableLastPK(dbClient, sourceTable.Name)
	require.NoError(t, err)
	require.NotNil(t, reloaded)
	// The explicit empty source checkpoint is non-nil, so getTableLastPK must NOT
	// substitute it with the target key.
	require.NotNil(t, reloaded.Source)
	require.NotNil(t, reloaded.Target)

	// --- Resume: decode the reloaded source checkpoint exactly as
	// TabletServer.VStreamRows does before handing it to the row streamer. An
	// explicit empty checkpoint has a single row with zero PK values, which the
	// row streamer treats as "no lastpk" and restarts the source from the
	// beginning, avoiding the length-mismatch failure against the two-column
	// source PK.
	sourceResume := sqltypes.Proto3ToResult(reloaded.Source)
	require.Len(t, sourceResume.Rows, 1)
	require.Empty(t, sourceResume.Rows[0],
		"resumed source lastpk must carry zero PK values so the source restarts from the beginning")

	// The target checkpoint is preserved with its single PK value, so only the
	// source restarts while the target resumes from where it left off.
	targetResume := sqltypes.Proto3ToResult(reloaded.Target)
	require.Len(t, targetResume.Rows, 1)
	require.Len(t, targetResume.Rows[0], 1)
	require.Equal(t, "42", targetResume.Rows[0][0].ToString())
}
