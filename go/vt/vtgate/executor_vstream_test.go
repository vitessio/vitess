/*
Copyright 2022 The Vitess Authors.

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

package vtgate

import (
	"context"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vtgate/engine"

	querypb "vitess.io/vitess/go/vt/proto/query"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	_ "vitess.io/vitess/go/vt/vtgate/vindexes"
)

// TestVStreamSQLUnsharded tests the experimental 'vstream * from' vtgate olap query
func TestVStreamSQLUnsharded(t *testing.T) {
	t.Skip("this test is failing due to races") // FIXME
	executor, _, _, sbcLookup := createExecutorEnv()
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)
	send1 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid01"},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "t1", Fields: []*querypb.Field{
			{Type: sqltypes.Int64},
		}}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t1", RowChanges: []*binlogdatapb.RowChange{{
			After: sqltypes.RowToProto3([]sqltypes.Value{
				sqltypes.NewInt64(1),
			}),
		}}}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t1", RowChanges: []*binlogdatapb.RowChange{{
			After: sqltypes.RowToProto3([]sqltypes.Value{
				sqltypes.NewInt64(2),
			}),
		}}}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t1", RowChanges: []*binlogdatapb.RowChange{{
			Before: sqltypes.RowToProto3([]sqltypes.Value{
				sqltypes.NewInt64(2),
			}),
		}}}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t1", RowChanges: []*binlogdatapb.RowChange{{
			Before: sqltypes.RowToProto3([]sqltypes.Value{
				sqltypes.NewInt64(1),
			}),
			After: sqltypes.RowToProto3([]sqltypes.Value{
				sqltypes.NewInt64(4),
			}),
		}}}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	sbcLookup.AddVStreamEvents(send1, nil)

	sql := "vstream * from t1"

	results := make(chan *sqltypes.Result, 20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err := executor.StreamExecute(ctx, nil, "TestExecuteStream", NewAutocommitSession(&vtgatepb.Session{TargetString: KsTestUnsharded}), sql, nil, func(qr *sqltypes.Result) error {
			results <- qr
			return nil
		})
		require.NoError(t, err)
	}()
	timer := time.NewTimer(5 * time.Second)
	done := false
	numRows, numInserts, numUpdates, numDeletes := 0, 0, 0, 0
	expectedRows, expectedInserts, expectedUpdates, expectedDeletes := 4, 2, 1, 1
	fieldsValidated := false
	for {
		if done {
			break
		}
		select {
		case qr := <-results:
			if !fieldsValidated {
				require.Equal(t, 2, len(qr.Fields))
				fieldsValidated = true
			}
			for _, row := range qr.Rows {
				numRows++
				switch row[0].ToString() {
				case engine.RowChangeInsert:
					numInserts++
				case engine.RowChangeUpdate:
					numUpdates++
				case engine.RowChangeDelete:
					numDeletes++
				default:
					require.FailNowf(t, "", "Unknown row change indicator: %s", row[0].ToString())
				}
			}
			if numRows >= expectedRows {
				done = true
			}
		case <-timer.C:
			done = true
		}
	}
	require.Equal(t, expectedRows, numRows)
	require.Equal(t, expectedInserts, numInserts)
	require.Equal(t, expectedUpdates, numUpdates)
	require.Equal(t, expectedDeletes, numDeletes)
}
