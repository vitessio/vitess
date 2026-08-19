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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"

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

var drainTestFields = sqltypes.MakeTestFields("c1|c2", "int64|varchar")

func drainTestRow(id int64, val string) []sqltypes.Value {
	return []sqltypes.Value{sqltypes.NewInt64(id), sqltypes.NewVarChar(val)}
}

// newDrainTestDiffer builds a minimal tableDiffer, backed by the given mock
// db client and source/target row streams, that can execute diff().
func newDrainTestDiffer(dbc binlogplayer.DBClient, source, target engine.Primitive) *tableDiffer {
	ct := &controller{
		id:                    1,
		uuid:                  "62b5b1de-561b-4b3b-a138-2be9d0f46f78",
		vde:                   &Engine{parser: sqlparser.NewTestParser()},
		done:                  make(chan struct{}),
		dbClientFactory:       func() binlogplayer.DBClient { return dbc },
		TableDiffRowCounts:    stats.NewCountersWithSingleLabel("", "", "Rows"),
		TableDiffPhaseTimings: stats.NewTimings("", "", ""),
	}
	wd := &workflowDiffer{
		ct:           ct,
		collationEnv: collations.MySQL8(),
		opts: &tabletmanagerdatapb.VDiffOptions{
			CoreOptions:   &tabletmanagerdatapb.VDiffCoreOptions{},
			ReportOptions: &tabletmanagerdatapb.VDiffReportOptions{},
		},
	}
	table := &tabletmanagerdatapb.TableDefinition{
		Name:   "t1",
		Fields: drainTestFields,
	}
	return &tableDiffer{
		wd:    wd,
		table: table,
		tablePlan: &tablePlan{
			table:        table,
			sourceQuery:  "select c1, c2 from t1",
			targetQuery:  "select c1, c2 from t1",
			pkCols:       []int{0},
			sourcePkCols: []int{0},
			selectPks:    []int{0},
			comparePKs: []compareColInfo{
				{colIndex: 0, isPK: true, colName: "c1"},
			},
			compareCols: []compareColInfo{
				{colIndex: 0, isPK: true, colName: "c1"},
				{colIndex: 1, isPK: false, colName: "c2"},
			},
		},
		sourcePrimitive: source,
		targetPrimitive: target,
	}
}

// TestDiffDrainedRowSampling tests that when one side's rows are exhausted
// first, the remaining rows drained from the other side are each saved as an
// extra-row sample (up to max-extra-rows-to-compare) so that
// reconcileExtraRows can later match them against the extra rows found on the
// exhausted side. Previously only the first drained row was given a sample,
// so any remaining drained rows could never be reconciled and were falsely
// reported as extra rows even when the data matched.
func TestDiffDrainedRowSampling(t *testing.T) {
	fields := drainTestFields
	makeRow := drainTestRow
	// Both sides contain the same four rows, but the streams disagree on
	// the order (e.g. because of differing PK collations between the source
	// and target): rows 3 and 4 arrive first on one side, so rows 1 and 2
	// are drained from it after the other side is exhausted.
	inOrderRows := [][]sqltypes.Value{makeRow(1, "a"), makeRow(2, "b"), makeRow(3, "c"), makeRow(4, "d")}
	outOfOrderRows := [][]sqltypes.Value{makeRow(3, "c"), makeRow(4, "d"), makeRow(1, "a"), makeRow(2, "b")}

	testCases := []struct {
		name                  string
		sourceRows            [][]sqltypes.Value
		targetRows            [][]sqltypes.Value
		maxExtraRowsToCompare int64
		wantExtraRowsSource   int64
		wantExtraRowsTarget   int64
		wantSourceSamples     int
		wantTargetSamples     int
		// Wanted values after reconciling the extra rows.
		wantReconciledExtraRowsSource int64
		wantReconciledExtraRowsTarget int64
		wantReconciledMatchingRows    int64
	}{
		{
			name:                          "drain target rows",
			sourceRows:                    inOrderRows,
			targetRows:                    outOfOrderRows,
			maxExtraRowsToCompare:         1000,
			wantExtraRowsSource:           2,
			wantExtraRowsTarget:           2,
			wantSourceSamples:             2,
			wantTargetSamples:             2,
			wantReconciledExtraRowsSource: 0,
			wantReconciledExtraRowsTarget: 0,
			wantReconciledMatchingRows:    4,
		},
		{
			name:                          "drain source rows",
			sourceRows:                    outOfOrderRows,
			targetRows:                    inOrderRows,
			maxExtraRowsToCompare:         1000,
			wantExtraRowsSource:           2,
			wantExtraRowsTarget:           2,
			wantSourceSamples:             2,
			wantTargetSamples:             2,
			wantReconciledExtraRowsSource: 0,
			wantReconciledExtraRowsTarget: 0,
			wantReconciledMatchingRows:    4,
		},
		{
			name:                  "drained samples capped by max-extra-rows-to-compare",
			sourceRows:            inOrderRows,
			targetRows:            outOfOrderRows,
			maxExtraRowsToCompare: 1,
			wantExtraRowsSource:   2,
			wantExtraRowsTarget:   2,
			wantSourceSamples:     1,
			wantTargetSamples:     1,
			// Only the first extra row on each side has a sample, so only
			// that pair can be reconciled.
			wantReconciledExtraRowsSource: 1,
			wantReconciledExtraRowsTarget: 1,
			wantReconciledMatchingRows:    3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dbc := binlogplayer.NewMockDBClient(t)
			td := newDrainTestDiffer(dbc,
				engine.NewRowsPrimitive(tc.sourceRows, fields),
				engine.NewRowsPrimitive(tc.targetRows, fields),
			)
			wd := td.wd

			stateQuery, err := sqlparser.ParseAndBind(sqlGetVDiffTable,
				sqltypes.Int64BindVariable(td.wd.ct.id),
				sqltypes.StringBindVariable(td.table.Name),
			)
			require.NoError(t, err)
			dbc.ExpectRequest(stateQuery, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"lastpk|mismatch|report",
				"varbinary|int64|varbinary",
			), "|0|{}"), nil)
			dbc.ExpectRequestRE("update _vt.vdiff_table set rows_compared = .*", &sqltypes.Result{}, nil)

			coreOpts := &tabletmanagerdatapb.VDiffCoreOptions{
				MaxRows:               100,
				MaxExtraRowsToCompare: tc.maxExtraRowsToCompare,
			}
			reportOpts := &tabletmanagerdatapb.VDiffReportOptions{
				MaxSampleRows: 10,
			}
			dr, err := td.diff(t.Context(), coreOpts, reportOpts, nil)
			require.NoError(t, err)
			require.Equal(t, int64(6), dr.ProcessedRows)
			require.Equal(t, int64(2), dr.MatchingRows)
			require.Equal(t, int64(0), dr.MismatchedRows)
			require.Equal(t, tc.wantExtraRowsSource, dr.ExtraRowsSource)
			require.Equal(t, tc.wantExtraRowsTarget, dr.ExtraRowsTarget)
			require.Len(t, dr.ExtraRowsSourceDiffs, tc.wantSourceSamples)
			require.Len(t, dr.ExtraRowsTargetDiffs, tc.wantTargetSamples)

			require.NoError(t, wd.doReconcileExtraRows(dr, tc.maxExtraRowsToCompare, reportOpts.MaxSampleRows))
			require.Equal(t, tc.wantReconciledExtraRowsSource, dr.ExtraRowsSource)
			require.Equal(t, tc.wantReconciledExtraRowsTarget, dr.ExtraRowsTarget)
			require.Equal(t, tc.wantReconciledMatchingRows, dr.MatchingRows)
			require.Equal(t, int64(6), dr.ProcessedRows)
			require.Equal(t, int64(0), dr.MismatchedRows)
		})
	}
}

// erroringPrimitive is an engine.Primitive for testing that streams the given
// results and then fails with err, simulating a mid-stream error.
type erroringPrimitive struct {
	engine.Primitive
	results []*sqltypes.Result
	err     error
}

func (p *erroringPrimitive) TryStreamExecute(ctx context.Context, vcursor engine.VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	for _, r := range p.results {
		if err := callback(r); err != nil {
			return err
		}
	}
	return p.err
}

// TestDiffDrainStreamError tests that when the stream fails in the middle of
// draining the remaining rows of one side, none of the partially drained rows
// are merged into the diff report: the report persisted by the deferred
// progress update must reflect only the fully compared rows, so that resuming
// the diff does not count the drained rows a second time.
func TestDiffDrainStreamError(t *testing.T) {
	fields := drainTestFields
	makeRow := drainTestRow
	streamErr := errors.New("stream terminated unexpectedly")
	coreOpts := &tabletmanagerdatapb.VDiffCoreOptions{
		MaxRows:               100,
		MaxExtraRowsToCompare: 1000,
	}
	reportOpts := &tabletmanagerdatapb.VDiffReportOptions{
		MaxSampleRows: 10,
	}
	stateQuery, err := sqlparser.ParseAndBind(sqlGetVDiffTable,
		sqltypes.Int64BindVariable(1),
		sqltypes.StringBindVariable("t1"),
	)
	require.NoError(t, err)
	stateFields := sqltypes.MakeTestFields(
		"lastpk|mismatch|report",
		"varbinary|int64|varbinary",
	)

	// First diff attempt: the source has rows 1 and 2; the target stream
	// returns the matching rows 1 and 2 plus rows 3 and 4, and then fails.
	// Rows 3 and 4 are drained after the source is exhausted, but since the
	// drain ends in an error they must not be counted or sampled.
	dbc := binlogplayer.NewMockDBClient(t)
	td := newDrainTestDiffer(dbc,
		engine.NewRowsPrimitive([][]sqltypes.Value{makeRow(1, "a"), makeRow(2, "b")}, fields),
		&erroringPrimitive{
			Primitive: engine.NewRowsPrimitive(nil, fields),
			results: []*sqltypes.Result{{
				Fields: fields,
				Rows:   [][]sqltypes.Value{makeRow(1, "a"), makeRow(2, "b"), makeRow(3, "c"), makeRow(4, "d")},
			}},
			err: streamErr,
		},
	)
	dbc.ExpectRequest(stateQuery, sqltypes.MakeTestResult(stateFields, "|0|{}"), nil)
	// The deferred progress update must persist a report that contains only
	// the two fully compared rows: no drained extra-row counts or samples.
	wantReport, err := json.Marshal(&DiffReport{
		TableName:     "t1",
		ProcessedRows: 2,
		MatchingRows:  2,
	})
	require.NoError(t, err)
	dbc.ExpectRequestRE(`update _vt\.vdiff_table set rows_compared = 2, lastpk = '.*', report = '`+
		regexp.QuoteMeta(string(wantReport))+`' where vdiff_id = 1 and table_name = 't1'`,
		&sqltypes.Result{}, nil)

	_, err = td.diff(t.Context(), coreOpts, reportOpts, nil)
	require.ErrorIs(t, err, streamErr)

	// Resume the diff from the persisted state. The source has no rows past
	// the persisted lastpk; the target re-streams the two drained rows. Each
	// drained row must be counted exactly once across the two attempts.
	dbc2 := binlogplayer.NewMockDBClient(t)
	td2 := newDrainTestDiffer(dbc2,
		engine.NewRowsPrimitive(nil, fields),
		engine.NewRowsPrimitive([][]sqltypes.Value{makeRow(3, "c"), makeRow(4, "d")}, fields),
	)
	dbc2.ExpectRequest(stateQuery, sqltypes.MakeTestResult(stateFields, fmt.Sprintf("|0|%s", wantReport)), nil)
	dbc2.ExpectRequestRE(`update _vt\.vdiff_table set rows_compared = 4, report = .*`, &sqltypes.Result{}, nil)

	dr, err := td2.diff(t.Context(), coreOpts, reportOpts, nil)
	require.NoError(t, err)
	require.Equal(t, int64(4), dr.ProcessedRows)
	require.Equal(t, int64(2), dr.MatchingRows)
	require.Equal(t, int64(0), dr.ExtraRowsSource)
	require.Equal(t, int64(2), dr.ExtraRowsTarget)
	require.Len(t, dr.ExtraRowsTargetDiffs, 2)
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
			table: table,
		},
	}

	err := td.getSourcePKCols()
	require.NoError(t, err)
	require.Nil(t, td.tablePlan.sourcePkCols)
}
