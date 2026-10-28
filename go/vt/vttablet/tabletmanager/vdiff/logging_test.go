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
	"log/slog"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// expectTableState expects the query that reads the table's vdiff state.
func expectTableState(t *testing.T, dbc *binlogplayer.MockDBClient, td *tableDiffer) {
	stateQuery, err := sqlparser.ParseAndBind(sqlGetVDiffTable,
		sqltypes.Int64BindVariable(td.wd.ct.id),
		sqltypes.StringBindVariable(td.table.Name),
	)
	require.NoError(t, err)
	dbc.ExpectRequest(stateQuery, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"lastpk|mismatch|report",
		"varbinary|int64|varbinary",
	), "|0|{}"), nil)
}

// TestDiffMismatchLog checks the attributes of the mismatch log.
func TestDiffMismatchLog(t *testing.T) {
	const sourceValue, targetValue = "zq7vdiffsourcevalue", "zq7vdifftargetvalue"
	// Four matching rows, three rows only on the source, two only on the
	// target, then the mismatch, so that every count differs.
	var sourceRows, targetRows [][]sqltypes.Value
	for id := int64(1); id <= 4; id++ {
		sourceRows = append(sourceRows, drainTestRow(id, "same"))
		targetRows = append(targetRows, drainTestRow(id, "same"))
	}
	for id := int64(5); id <= 7; id++ {
		sourceRows = append(sourceRows, drainTestRow(id, "source"))
	}
	for id := int64(8); id <= 9; id++ {
		targetRows = append(targetRows, drainTestRow(id, "target"))
	}
	sourceRows = append(sourceRows, drainTestRow(10, sourceValue))
	targetRows = append(targetRows, drainTestRow(10, targetValue))

	dbc := binlogplayer.NewMockDBClient(t)
	td := newDrainTestDiffer(dbc,
		engine.NewRowsPrimitive(sourceRows, drainTestFields),
		engine.NewRowsPrimitive(targetRows, drainTestFields),
	)
	td.wd.ct.workflow = "wf"
	expectTableState(t, dbc, td)
	dbc.ExpectRequestRE("update _vt.vdiff_table set mismatch = 1.*", &sqltypes.Result{}, nil)
	dbc.ExpectRequestRE("update _vt.vdiff_table set rows_compared = .*", &sqltypes.Result{}, nil)

	logs := logtest.Capture(t, slog.LevelInfo)
	dr, err := td.diff(t.Context(), &tabletmanagerdatapb.VDiffCoreOptions{MaxRows: 100},
		&tabletmanagerdatapb.VDiffReportOptions{MaxSampleRows: 10}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), dr.MismatchedRows)

	record := logs.Record(t, "Flagging mismatch in vdiff")
	assertReportCountKeys(t, record)
	assert.Equal(t, td.wd.ct.uuid, record["vdiff"])
	assert.Equal(t, td.wd.ct.workflow, record["workflow"])
	assert.Equal(t, td.table.Name, record["table"])
	assert.EqualValues(t, 10, record["processed_rows"])
	assert.EqualValues(t, 4, record["matching_rows"])
	assert.EqualValues(t, 1, record["mismatched_rows"])
	assert.EqualValues(t, 3, record["extra_rows_source"])
	assert.EqualValues(t, 2, record["extra_rows_target"])
	// The report's sample rows stay in the stored report.
	assert.NotContains(t, logs.String(), sourceValue)
	assert.NotContains(t, logs.String(), targetValue)
}

// assertReportCountKeys checks that record has exactly the table and the
// report's row counts besides the standard and controller keys, so the
// report itself isn't logged.
func assertReportCountKeys(t *testing.T, record map[string]any) {
	t.Helper()
	keys := slices.Sorted(maps.Keys(record))
	assert.Equal(t, []string{
		"extra_rows_source", "extra_rows_target", "level", "matching_rows", "mismatched_rows",
		"msg", "processed_rows", "table", "time", "vdiff", "workflow",
	}, keys)
}

// TestDiffRowLimitLog checks that the row limit log carries the configured
// limit.
func TestDiffRowLimitLog(t *testing.T) {
	rows := [][]sqltypes.Value{drainTestRow(1, "a"), drainTestRow(2, "b"), drainTestRow(3, "c")}
	dbc := binlogplayer.NewMockDBClient(t)
	td := newDrainTestDiffer(dbc,
		engine.NewRowsPrimitive(rows, drainTestFields),
		engine.NewRowsPrimitive(rows, drainTestFields),
	)
	expectTableState(t, dbc, td)
	dbc.ExpectRequestRE("update _vt.vdiff_table set rows_compared = .*", &sqltypes.Result{}, nil)

	logs := logtest.Capture(t, slog.LevelInfo)
	_, err := td.diff(t.Context(), &tabletmanagerdatapb.VDiffCoreOptions{MaxRows: 2},
		&tabletmanagerdatapb.VDiffReportOptions{MaxSampleRows: 10}, nil)
	require.NoError(t, err)

	record := logs.Record(t, "Stopping vdiff, specified row limit reached")
	assert.Equal(t, td.wd.ct.uuid, record["vdiff"])
	assert.Equal(t, td.table.Name, record["table"])
	assert.EqualValues(t, 2, record["row_limit"])
}
