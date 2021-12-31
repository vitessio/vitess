/*
Copyright 2019 The Vitess Authors.

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
	"testing"
	"time"

	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/vt/discovery"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"vitess.io/vitess/go/vt/vtgate/engine"

	querypb "vitess.io/vitess/go/vt/proto/query"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"

	"context"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	_ "vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"
)

func TestStreamSQLUnsharded(t *testing.T) {
	executor, _, _, _ := createLegacyExecutorEnv()
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "stream * from user_msgs"
	result, err := executorStreamMessages(executor, sql)
	require.NoError(t, err)
	wantResult := sandboxconn.StreamRowResult
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestStreamSQLSharded(t *testing.T) {
	// Special setup: Don't use createLegacyExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeLegacyHealthCheck()
	s := createSandbox("TestExecutor")
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestLegacyResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	for _, shard := range shards {
		_ = hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
	}
	executor := NewExecutor(context.Background(), serv, cell, resolver, false, false, testBufferSize, cache.DefaultConfig, nil, false)

	sql := "stream * from sharded_user_msgs"
	result, err := executorStreamMessages(executor, sql)
	require.NoError(t, err)
	wantResult := &sqltypes.Result{
		Fields: sandboxconn.SingleRowResult.Fields,
		Rows: [][]sqltypes.Value{
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
		},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestVStreamSQLUnsharded(t *testing.T) {
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
		err := executor.StreamExecute(
			ctx,
			"TestExecuteStream",
			NewAutocommitSession(&vtgatepb.Session{TargetString: KsTestUnsharded}),
			sql,
			nil,
			func(qr *sqltypes.Result) error {
				results <- qr
				return nil
			},
		)
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

func executorStreamMessages(executor *Executor, sql string) (qr *sqltypes.Result, err error) {
	results := make(chan *sqltypes.Result, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = executor.StreamExecute(
		ctx,
		"TestExecuteStream",
		NewSafeSession(primarySession),
		sql,
		nil,
		func(qr *sqltypes.Result) error {
			results <- qr
			return nil
		},
	)
	close(results)
	if err != nil {
		return nil, err
	}
	first := true
	for r := range results {
		if first {
			qr = &sqltypes.Result{Fields: r.Fields}
			first = false
		}
		qr.Rows = append(qr.Rows, r.Rows...)
	}
	return qr, nil
}
