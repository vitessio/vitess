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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtenv"
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
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
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

	executor := NewExecutor(ctx, vtenv.NewTestEnv(), serv, cell, resolver, false, false, testBufferSize, plans, nil, false, querypb.ExecuteOptions_Gen4, 0)
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
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
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
		NewSafeSession(session),
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
