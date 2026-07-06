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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
	"vitess.io/vitess/go/vt/vtgate/logstats"
	_ "vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"
)

func TestStreamSQLUnsharded(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)
	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	sql := "stream * from user_msgs"
	result, err := executorStreamMessages(executor, sql)
	require.NoError(t, err)
	wantResult := sandboxconn.StreamRowResult
	assert.Truef(t, result.Equal(wantResult), "result: %+v, want %+v", result, wantResult)
}

func TestStreamSQLSharded(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	u := createSandbox(KsTestUnsharded)
	s := createSandbox("TestExecutor")
	s.VSchema = executorVSchema
	u.VSchema = unshardedVSchema
	serv := newSandboxForCells(ctx, []string{cell})
	resolver := newTestResolver(ctx, hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	for _, shard := range shards {
		_ = hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
	}
	queryLogger := streamlog.New[*logstats.LogStats]("VTGate", queryLogBufferSize)
	plans := DefaultPlanCache()

	executor := NewExecutor(ctx, vtenv.NewTestEnv(), serv, cell, resolver, createExecutorConfig(), false, plans, nil, querypb.ExecuteOptions_Gen4, NewDynamicViperConfig())
	executor.SetQueryLogger(queryLogger)

	defer executor.Close()

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
	assert.Truef(t, result.Equal(wantResult), "result: %+v, want %+v", result, wantResult)
}

// TestStreamExecutePropagatesOKPacketData verifies that the streaming path reports
// the OK-packet data — affected rows, last insert id and session-state changes —
// to the client for row-returning statements that carry an OK packet (e.g. a CALL
// of a procedure that performs DML), matching the buffered Execute path.
func TestStreamExecutePropagatesOKPacketData(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)

	// An OK packet with affected rows but no fields or rows, like a CALL that
	// performs an INSERT.
	sbclookup.SetResults([]*sqltypes.Result{{
		RowsAffected:        1,
		InsertID:            99,
		InsertIDChanged:     true,
		SessionStateChanges: "8bb25b46-16bd-11ea-8ffa-98af65266957:8",
	}})

	session := econtext.NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded})
	got := &sqltypes.Result{}
	err := executor.StreamExecute(ctx, nil, "TestStreamExecutePropagatesOKPacketData", session,
		"select id from main1", nil, false, func(qr *sqltypes.Result) error {
			got.AppendResult(qr)
			if qr.SessionStateChanges != "" {
				got.SessionStateChanges = qr.SessionStateChanges
			}
			return nil
		})
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.RowsAffected, "streamed result must carry RowsAffected to the client")
	assert.EqualValues(t, 99, got.InsertID, "streamed result must carry InsertID to the client")
	assert.True(t, got.InsertIDChanged)
	assert.Equal(t, "8bb25b46-16bd-11ea-8ffa-98af65266957:8", got.SessionStateChanges,
		"streamed result must carry SessionStateChanges to the client")
}

// TestStreamExecuteOKPacketDataAfterResultSet verifies that OK-packet data arriving
// after a result set was already delivered to the client is still sent in a final
// packet instead of being dropped — e.g. a stream that first produces fields and
// then an affected-row count.
func TestStreamExecuteOKPacketDataAfterResultSet(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)

	// The first result carries only fields, so the metadata is sent to the client
	// immediately. The second carries OK-packet data with no rows, so the final
	// left-over result has no rows either and must still be sent.
	sbclookup.SetResults([]*sqltypes.Result{
		{Fields: sqltypes.MakeTestFields("id", "int64")},
		{RowsAffected: 1},
	})

	session := econtext.NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded})
	var rowsAffected uint64
	err := executor.StreamExecute(ctx, nil, "TestStreamExecuteOKPacketDataAfterResultSet", session,
		"select id from main1", nil, false, func(qr *sqltypes.Result) error {
			rowsAffected += qr.RowsAffected
			return nil
		})
	require.NoError(t, err)
	assert.EqualValues(t, 1, rowsAffected,
		"RowsAffected arriving after the result set was sent must still reach the client")
}

// TestStreamExecuteRecordsPlanStats verifies that the streaming path records
// per-plan execution statistics (exec count, rows returned) on the engine.Plan,
// matching the buffered Execute path.
func TestStreamExecuteRecordsPlanStats(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)

	sbclookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "2")})

	sql := "select id from main1"
	session := econtext.NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded})
	err := executor.StreamExecute(ctx, nil, "TestStreamExecuteRecordsPlanStats", session,
		sql, nil, false, func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)

	var plan *engine.Plan
	executor.ForEachPlan(func(p *engine.Plan) bool {
		if p.Original == sql {
			plan = p
			return false
		}
		return true
	})
	require.NotNil(t, plan, "plan not found in cache")

	execCount, _, _, _, rowsReturned, errCount := plan.Stats()
	assert.EqualValues(t, 1, execCount, "streamed query must record an execution in plan stats")
	assert.EqualValues(t, 2, rowsReturned, "streamed query must record rows returned in plan stats")
	assert.EqualValues(t, 0, errCount, "successful streamed query must not record an error in plan stats")
}

func executorStreamMessages(executor *Executor, sql string) (qr *sqltypes.Result, err error) {
	results := make(chan *sqltypes.Result, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	session := &vtgatepb.Session{TargetString: "@primary"}
	err = executor.StreamExecute(
		ctx,
		nil,
		"TestExecuteStream",
		econtext.NewSafeSession(session),
		sql,
		nil,
		false,
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
