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

type (
	// erroringPrimitive is an engine.Primitive for testing that streams the
	// given results and then fails with err, simulating a mid-stream error.
	erroringPrimitive struct {
		engine.Primitive
		results []*sqltypes.Result
		err     error
	}
)

func (p *erroringPrimitive) TryStreamExecute(ctx context.Context, vcursor engine.VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	for _, r := range p.results {
		if err := callback(r); err != nil {
			return err
		}
	}
	return p.err
}

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

// TestGenRowDiffMarksLosslessValues tests that a row sample is affirmatively
// marked lossless only when it contains complete values: a sample whose values
// were actually truncated (per row-diff-column-truncate-at) is not, since only
// lossless samples take part in extra-row reconciliation.
func TestGenRowDiffMarksLosslessValues(t *testing.T) {
	td := newDrainTestDiffer(binlogplayer.NewMockDBClient(t),
		engine.NewRowsPrimitive(nil, drainTestFields),
		engine.NewRowsPrimitive(nil, drainTestFields),
	)
	reportOpts := &tabletmanagerdatapb.VDiffReportOptions{
		MaxSampleRows:           10,
		RowDiffColumnTruncateAt: 8,
	}

	longValue := "this value is definitely long enough to get truncated"
	rd, err := td.genRowDiff(td.tablePlan.sourceQuery, drainTestRow(1, longValue), reportOpts)
	require.NoError(t, err)
	require.False(t, rd.LosslessValues)
	require.Equal(t, longValue[:8]+truncatedNotation, rd.Row["c2"])

	rd, err = td.genRowDiff(td.tablePlan.sourceQuery, drainTestRow(1, "short"), reportOpts)
	require.NoError(t, err)
	require.True(t, rd.LosslessValues)
	require.Equal(t, "short", rd.Row["c2"])

	// A PK-only sample omits the non-PK columns and is not lossless...
	onlyPksOpts := &tabletmanagerdatapb.VDiffReportOptions{
		MaxSampleRows: 10,
		OnlyPks:       true,
	}
	rd, err = td.genRowDiff(td.tablePlan.sourceQuery, drainTestRow(1, "short"), onlyPksOpts)
	require.NoError(t, err)
	require.False(t, rd.LosslessValues)
	require.Equal(t, map[string]string{"c1": "1"}, rd.Row)

	// ...unless the PK columns cover the entire projection.
	td.tablePlan.selectPks = []int{0, 1}
	rd, err = td.genRowDiff(td.tablePlan.sourceQuery, drainTestRow(1, "short"), onlyPksOpts)
	require.NoError(t, err)
	require.True(t, rd.LosslessValues)
	require.Equal(t, map[string]string{"c1": "1", "c2": "short"}, rd.Row)
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

// TestDiffSourceDrainStreamError is the source-side counterpart of
// TestDiffDrainStreamError: the target stream is exhausted first and the
// source stream fails while its remaining rows are being drained. It also
// covers the case where the drain is entered with a held, not-yet-processed
// source row (after an extra-target-row comparison): that row must not be
// persisted as lastpk, since its count and sample were discarded with the
// failed drain, and a resumed diff would otherwise skip it permanently.
func TestDiffSourceDrainStreamError(t *testing.T) {
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

	t.Run("drain error after matched rows", func(t *testing.T) {
		// Rows 1 and 2 match; rows 3 and 4 are drained from the source and
		// the drain fails. The persisted report must contain only the two
		// fully compared rows, with lastpk pointing at row 2.
		dbc := binlogplayer.NewMockDBClient(t)
		td := newDrainTestDiffer(dbc,
			&erroringPrimitive{
				Primitive: engine.NewRowsPrimitive(nil, fields),
				results: []*sqltypes.Result{{
					Fields: fields,
					Rows:   [][]sqltypes.Value{makeRow(1, "a"), makeRow(2, "b"), makeRow(3, "c"), makeRow(4, "d")},
				}},
				err: streamErr,
			},
			engine.NewRowsPrimitive([][]sqltypes.Value{makeRow(1, "a"), makeRow(2, "b")}, fields),
		)
		dbc.ExpectRequest(stateQuery, sqltypes.MakeTestResult(stateFields, "|0|{}"), nil)
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
	})

	t.Run("drain error with a held unprocessed source row", func(t *testing.T) {
		// The first comparison finds an extra target row (source row 2 vs
		// target row 1), holding source row 2 unprocessed. The target is then
		// exhausted and the source drain fails. Since source row 2's count and
		// sample are discarded with the failed drain, it must not be recorded
		// as lastpk: the persisted update must carry no position (the report
		// contains only the extra target row), so that a resumed diff streams
		// source row 2 again.
		dbc := binlogplayer.NewMockDBClient(t)
		td := newDrainTestDiffer(dbc,
			&erroringPrimitive{
				Primitive: engine.NewRowsPrimitive(nil, fields),
				results: []*sqltypes.Result{{
					Fields: fields,
					Rows:   [][]sqltypes.Value{makeRow(2, "b"), makeRow(3, "c")},
				}},
				err: streamErr,
			},
			engine.NewRowsPrimitive([][]sqltypes.Value{makeRow(1, "a")}, fields),
		)
		dbc.ExpectRequest(stateQuery, sqltypes.MakeTestResult(stateFields, "|0|{}"), nil)
		wantReport, err := json.Marshal(&DiffReport{
			TableName:            "t1",
			ProcessedRows:        1,
			ExtraRowsTarget:      1,
			ExtraRowsTargetDiffs: []*RowDiff{{Row: map[string]string{"c1": "1", "c2": "a"}, LosslessValues: true}},
		})
		require.NoError(t, err)
		dbc.ExpectRequestRE(`update _vt\.vdiff_table set rows_compared = 1, report = '`+
			regexp.QuoteMeta(string(wantReport))+`' where vdiff_id = 1 and table_name = 't1'`,
			&sqltypes.Result{}, nil)

		_, err = td.diff(t.Context(), coreOpts, reportOpts, nil)
		require.ErrorIs(t, err, streamErr)
	})
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

// TestGetSourcePKCols_ComputedAliasUnavailable verifies that when a source PK
// column is projected only via a non-physical expression (a computed value
// aliased to the PK name) rather than as a physical column, getSourcePKCols does
// not fail: it flags the source checkpoint as unavailable so no derived value is
// persisted and the whole table restarts on resume. The row streamer resumes on
// the physical source PK, so the diff still runs, just without mid-table resume.
func TestGetSourcePKCols_ComputedAliasUnavailable(t *testing.T) {
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
	require.NoError(t, err)
	require.True(t, td.tablePlan.sourceCheckpointUnavailable)
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
			// The target has cid as its (single) PK at SELECT index 0.
			pkCols: []int{0},
		},
	}

	err := td.getSourcePKCols()
	require.NoError(t, err)
	// The source checkpoint is flagged unavailable and no partial source key is
	// built.
	require.True(t, td.tablePlan.sourceCheckpointUnavailable)
	require.Empty(t, td.tablePlan.sourcePkCols)
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
			// The target has cid as its (single) PK at SELECT index 0.
			pkCols: []int{0},
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
