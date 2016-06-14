// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/gateway"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// This file uses the sandbox_test framework.

var (
	retryCount = 3
)

func TestScatterConnExecute(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecute", func(hc discovery.HealthCheck, shards []string) (*sqltypes.Result, error) {
		stc := NewScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
		return stc.Execute(context.Background(), "query", nil, "TestScatterConnExecute", shards, topodatapb.TabletType_REPLICA, nil, false)
	})
}

func TestScatterConnExecuteMulti(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecuteMulti", func(hc discovery.HealthCheck, shards []string) (*sqltypes.Result, error) {
		stc := NewScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
		shardVars := make(map[string]map[string]interface{})
		for _, shard := range shards {
			shardVars[shard] = nil
		}
		return stc.ExecuteMulti(context.Background(), "query", "TestScatterConnExecuteMulti", shardVars, topodatapb.TabletType_REPLICA, nil, false)
	})
}

func TestScatterConnExecuteBatch(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecuteBatch", func(hc discovery.HealthCheck, shards []string) (*sqltypes.Result, error) {
		stc := NewScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
		queries := []*vtgatepb.BoundShardQuery{{
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace: "TestScatterConnExecuteBatch",
			Shards:   shards,
		}}
		scatterRequest, err := boundShardQueriesToScatterBatchRequest(queries)
		if err != nil {
			return nil, err
		}
		qrs, err := stc.ExecuteBatch(context.Background(), scatterRequest, topodatapb.TabletType_REPLICA, false, nil)
		if err != nil {
			return nil, err
		}
		return &qrs[0], err
	})
}

func TestScatterConnStreamExecute(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnStreamExecute", func(hc discovery.HealthCheck, shards []string) (*sqltypes.Result, error) {
		stc := NewScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
		qr := new(sqltypes.Result)
		err := stc.StreamExecute(context.Background(), "query", nil, "TestScatterConnStreamExecute", shards, topodatapb.TabletType_REPLICA, func(r *sqltypes.Result) error {
			appendResult(qr, r)
			return nil
		})
		return qr, err
	})
}

func TestScatterConnStreamExecuteMulti(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnStreamExecuteMulti", func(hc discovery.HealthCheck, shards []string) (*sqltypes.Result, error) {
		stc := NewScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
		qr := new(sqltypes.Result)
		shardVars := make(map[string]map[string]interface{})
		for _, shard := range shards {
			shardVars[shard] = nil
		}
		err := stc.StreamExecuteMulti(context.Background(), "query", "TestScatterConnStreamExecuteMulti", shardVars, topodatapb.TabletType_REPLICA, func(r *sqltypes.Result) error {
			appendResult(qr, r)
			return nil
		})
		return qr, err
	})
}

// verifyScatterConnError checks that a returned error has the expected message,
// type, and error code.
func verifyScatterConnError(t *testing.T, err error, wantErr string, wantCode vtrpcpb.ErrorCode) {
	if err == nil || err.Error() != wantErr {
		t.Errorf("wanted error: %s, got error: %v", wantErr, err)
	}
	if _, ok := err.(*ScatterConnError); !ok {
		t.Errorf("wanted error type *ScatterConnError, got error type: %v", reflect.TypeOf(err))
	}
	code := vterrors.RecoverVtErrorCode(err)
	if code != wantCode {
		t.Errorf("wanted error code: %s, got: %v", wantCode, code)
	}
}

// verifyErrorCode checks the error code for an error
func verifyErrorCode(t *testing.T, err error, wantCode vtrpcpb.ErrorCode) {
	code := vterrors.RecoverVtErrorCode(err)
	if err == nil || code != wantCode {
		t.Errorf("vterrors.RecoverVtErrorCode(%v) => %v, want %v", err, code, wantCode)
	}
}

func testScatterConnGeneric(t *testing.T, name string, f func(hc discovery.HealthCheck, shards []string) (*sqltypes.Result, error)) {
	hc := discovery.NewFakeHealthCheck()
	// no shard
	s := createSandbox(name)
	qr, err := f(hc, nil)
	if qr.RowsAffected != 0 {
		t.Errorf("want 0, got %v", qr.RowsAffected)
	}
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	// single shard
	s.Reset()
	sbc := hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc.MustFailServer = 1
	qr, err = f(hc, []string{"0"})
	want := fmt.Sprintf("shard, host: %v.0.replica, alias:<cell:\"aa\" > hostname:\"0\" port_map:<key:\"vt\" value:1 > keyspace:\"%s\" shard:\"0\" type:REPLICA , error: err", name, name)
	// Verify server error string.
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure that we tried only once.
	if execCount := sbc.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}

	// two shards
	s.Reset()
	hc.Reset()
	sbc0 := hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, name, "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc0.MustFailServer = 1
	sbc1.MustFailServer = 1
	_, err = f(hc, []string{"0", "1"})
	// Verify server errors are consolidated.
	want = fmt.Sprintf("shard, host: %v.0.replica, alias:<cell:\"aa\" > hostname:\"0\" port_map:<key:\"vt\" value:1 > keyspace:\"%v\" shard:\"0\" type:REPLICA , error: err\nshard, host: %v.1.replica, alias:<cell:\"aa\" > hostname:\"1\" port_map:<key:\"vt\" value:1 > keyspace:\"%v\" shard:\"1\" type:REPLICA , error: err", name, name, name, name)
	verifyScatterConnError(t, err, want, vtrpcpb.ErrorCode_BAD_INPUT)
	// Ensure that we tried only once.
	if execCount := sbc0.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}

	// two shards with different errors
	s.Reset()
	hc.Reset()
	sbc0 = hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "1", 1, name, "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc0.MustFailServer = 1
	sbc1.MustFailTxPool = 1
	_, err = f(hc, []string{"0", "1"})
	// Verify server errors are consolidated.
	want = fmt.Sprintf("shard, host: %v.0.replica, alias:<cell:\"aa\" > hostname:\"0\" port_map:<key:\"vt\" value:1 > keyspace:\"%v\" shard:\"0\" type:REPLICA , error: err\nshard, host: %v.1.replica, alias:<cell:\"aa\" > hostname:\"1\" port_map:<key:\"vt\" value:1 > keyspace:\"%v\" shard:\"1\" type:REPLICA , tx_pool_full: err", name, name, name, name)
	// We should only surface the higher priority error code
	verifyScatterConnError(t, err, want, vtrpcpb.ErrorCode_BAD_INPUT)
	// Ensure that we tried only once.
	if execCount := sbc0.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}

	// duplicate shards
	s.Reset()
	hc.Reset()
	sbc = hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	qr, err = f(hc, []string{"0", "0"})
	// Ensure that we executed only once.
	if execCount := sbc.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}

	// no errors
	s.Reset()
	hc.Reset()
	sbc0 = hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "1", 1, name, "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	qr, err = f(hc, []string{"0", "1"})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if execCount := sbc0.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
	if qr.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.RowsAffected)
	}
	if len(qr.Rows) != 2 {
		t.Errorf("want 2, got %v", len(qr.Rows))
	}
}

func TestMultiExecs(t *testing.T) {
	createSandbox("TestMultiExecs")
	hc := discovery.NewFakeHealthCheck()
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestMultiExecs", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestMultiExecs", "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	stc := NewScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	shardVars := map[string]map[string]interface{}{
		"0": {
			"bv0": 0,
		},
		"1": {
			"bv1": 1,
		},
	}
	_, _ = stc.ExecuteMulti(context.Background(), "query", "TestMultiExecs", shardVars, topodatapb.TabletType_REPLICA, nil, false)
	if !reflect.DeepEqual(sbc0.Queries[0].BindVariables, shardVars["0"]) {
		t.Errorf("got %+v, want %+v", sbc0.Queries[0].BindVariables, shardVars["0"])
	}
	if !reflect.DeepEqual(sbc1.Queries[0].BindVariables, shardVars["1"]) {
		t.Errorf("got %+v, want %+v", sbc0.Queries[0].BindVariables, shardVars["1"])
	}
	sbc0.Queries = nil
	sbc1.Queries = nil
	_ = stc.StreamExecuteMulti(context.Background(), "query", "TestMultiExecs", shardVars, topodatapb.TabletType_REPLICA, func(*sqltypes.Result) error {
		return nil
	})
	if !reflect.DeepEqual(sbc0.Queries[0].BindVariables, shardVars["0"]) {
		t.Errorf("got %+v, want %+v", sbc0.Queries[0].BindVariables, shardVars["0"])
	}
	if !reflect.DeepEqual(sbc1.Queries[0].BindVariables, shardVars["1"]) {
		t.Errorf("got %+v, want %+v", sbc0.Queries[0].BindVariables, shardVars["1"])
	}
}

func TestScatterConnStreamExecuteSendError(t *testing.T) {
	createSandbox("TestScatterConnStreamExecuteSendError")
	hc := discovery.NewFakeHealthCheck()
	hc.AddTestTablet("aa", "0", 1, "TestScatterConnStreamExecuteSendError", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	stc := NewScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	err := stc.StreamExecute(context.Background(), "query", nil, "TestScatterConnStreamExecuteSendError", []string{"0"}, topodatapb.TabletType_REPLICA, func(*sqltypes.Result) error {
		return fmt.Errorf("send error")
	})
	want := "send error"
	// Ensure that we handle send errors.
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
}

func TestScatterCommitRollbackIncorrectSession(t *testing.T) {
	createSandbox("TestScatterCommitRollbackIncorrectSession")
	hc := discovery.NewFakeHealthCheck()
	hc.AddTestTablet("aa", "0", 1, "TestScatterCommitRollbackIncorrectSession", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	stc := NewScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)

	// nil session
	err := stc.Commit(context.Background(), nil)
	verifyErrorCode(t, err, vtrpcpb.ErrorCode_BAD_INPUT)

	err = stc.Rollback(context.Background(), nil)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	// not in transaction
	session := NewSafeSession(&vtgatepb.Session{})
	err = stc.Commit(context.Background(), session)
	verifyErrorCode(t, err, vtrpcpb.ErrorCode_NOT_IN_TX)
}

func TestScatterConnCommitSuccess(t *testing.T) {
	createSandbox("TestScatterConnCommitSuccess")
	hc := discovery.NewFakeHealthCheck()
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestScatterConnCommitSuccess", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestScatterConnCommitSuccess", "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	stc := NewScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnCommitSuccess", []string{"0"}, topodatapb.TabletType_REPLICA, session, false)
	wantSession := vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestScatterConnCommitSuccess",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
			},
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(wantSession, *session.Session) {
		t.Errorf("want\n%+v, got\n%+v", wantSession, *session.Session)
	}
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnCommitSuccess", []string{"0", "1"}, topodatapb.TabletType_REPLICA, session, false)
	wantSession = vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestScatterConnCommitSuccess",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
			},
			TransactionId: 1,
		}, {
			Target: &querypb.Target{
				Keyspace:   "TestScatterConnCommitSuccess",
				Shard:      "1",
				TabletType: topodatapb.TabletType_REPLICA,
			},
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(wantSession, *session.Session) {
		t.Errorf("want\n%+v, got\n%+v", wantSession, *session.Session)
	}
	sbc0.MustFailServer = 1
	err := stc.Commit(context.Background(), session)
	if err == nil {
		t.Errorf("want error, got nil")
	}
	wantSession = vtgatepb.Session{}
	if !reflect.DeepEqual(wantSession, *session.Session) {
		t.Errorf("want\n%+v, got\n%+v", wantSession, *session.Session)
	}
	if commitCount := sbc0.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
	if rollbackCount := sbc1.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("want 1, got %d", rollbackCount)
	}
}

func TestScatterConnRollback(t *testing.T) {
	createSandbox("TestScatterConnRollback")
	hc := discovery.NewFakeHealthCheck()
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestScatterConnRollback", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestScatterConnRollback", "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	stc := NewScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0"}, topodatapb.TabletType_REPLICA, session, false)
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0", "1"}, topodatapb.TabletType_REPLICA, session, false)
	err := stc.Rollback(context.Background(), session)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantSession := vtgatepb.Session{}
	if !reflect.DeepEqual(wantSession, *session.Session) {
		t.Errorf("want\n%#v, got\n%#v", wantSession, *session.Session)
	}
	if rollbackCount := sbc0.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("want 1, got %d", rollbackCount)
	}
	if rollbackCount := sbc1.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("want 1, got %d", rollbackCount)
	}
}

func TestScatterConnError(t *testing.T) {
	err := &ScatterConnError{
		Retryable: false,
		Errs: []error{
			&gateway.ShardError{ErrorCode: vtrpcpb.ErrorCode_PERMISSION_DENIED, Err: &tabletconn.ServerError{Err: "tabletconn error"}},
			fmt.Errorf("generic error"),
			tabletconn.ConnClosed,
		},
	}

	errString := err.Error()
	wantErrString := "generic error\ntabletconn error\nvttablet: Connection Closed"

	if errString != wantErrString {
		t.Errorf("got: %v, want: %v", errString, wantErrString)
	}
}

func TestScatterConnQueryNotInTransaction(t *testing.T) {
	s := createSandbox("TestScatterConnQueryNotInTransaction")
	hc := discovery.NewFakeHealthCheck()

	// case 1: read query (not in transaction) followed by write query, not in the same shard.
	hc.Reset()
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestScatterConnQueryNotInTransaction", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestScatterConnQueryNotInTransaction", "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	stc := NewScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnQueryNotInTransaction", []string{"0"}, topodatapb.TabletType_REPLICA, session, true)
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnQueryNotInTransaction", []string{"1"}, topodatapb.TabletType_REPLICA, session, false)

	wantSession := vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestScatterConnQueryNotInTransaction",
				Shard:      "1",
				TabletType: topodatapb.TabletType_REPLICA,
			},
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(wantSession, *session.Session) {
		t.Errorf("want\n%+v\ngot\n%+v", wantSession, *session.Session)
	}
	stc.Commit(context.Background(), session)
	{
		execCount0 := sbc0.ExecCount.Get()
		execCount1 := sbc1.ExecCount.Get()
		if execCount0 != 1 || execCount1 != 1 {
			t.Errorf("want 1/1, got %d/%d", execCount0, execCount1)
		}
	}
	if commitCount := sbc0.CommitCount.Get(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}
	if commitCount := sbc1.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	// case 2: write query followed by read query (not in transaction), not in the same shard.
	s.Reset()
	hc.Reset()
	sbc0 = hc.AddTestTablet("aa", "0", 1, "TestScatterConnQueryNotInTransaction", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "1", 1, "TestScatterConnQueryNotInTransaction", "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	stc = NewScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	session = NewSafeSession(&vtgatepb.Session{InTransaction: true})
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnQueryNotInTransaction", []string{"0"}, topodatapb.TabletType_REPLICA, session, false)
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnQueryNotInTransaction", []string{"1"}, topodatapb.TabletType_REPLICA, session, true)

	wantSession = vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestScatterConnQueryNotInTransaction",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
			},
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(wantSession, *session.Session) {
		t.Errorf("want\n%+v\ngot\n%+v", wantSession, *session.Session)
	}
	stc.Commit(context.Background(), session)
	{
		execCount0 := sbc0.ExecCount.Get()
		execCount1 := sbc1.ExecCount.Get()
		if execCount0 != 1 || execCount1 != 1 {
			t.Errorf("want 1/1, got %d/%d", execCount0, execCount1)
		}
	}
	if commitCount := sbc0.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
	if commitCount := sbc1.CommitCount.Get(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}

	// case 3: write query followed by read query, in the same shard.
	s.Reset()
	hc.Reset()
	sbc0 = hc.AddTestTablet("aa", "0", 1, "TestScatterConnQueryNotInTransaction", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "1", 1, "TestScatterConnQueryNotInTransaction", "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	stc = NewScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	session = NewSafeSession(&vtgatepb.Session{InTransaction: true})
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnQueryNotInTransaction", []string{"0"}, topodatapb.TabletType_REPLICA, session, false)
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnQueryNotInTransaction", []string{"0", "1"}, topodatapb.TabletType_REPLICA, session, true)

	wantSession = vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestScatterConnQueryNotInTransaction",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
			},
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(wantSession, *session.Session) {
		t.Errorf("want\n%+v\ngot\n%+v", wantSession, *session.Session)
	}
	stc.Commit(context.Background(), session)
	{
		execCount0 := sbc0.ExecCount.Get()
		execCount1 := sbc1.ExecCount.Get()
		if execCount0 != 2 || execCount1 != 1 {
			t.Errorf("want 2/1, got %d/%d", execCount0, execCount1)
		}
	}
	if commitCount := sbc0.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
	if commitCount := sbc1.CommitCount.Get(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}
}

func TestAppendResult(t *testing.T) {
	qr := new(sqltypes.Result)
	innerqr1 := &sqltypes.Result{
		Fields: []*querypb.Field{},
		Rows:   [][]sqltypes.Value{},
	}
	innerqr2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "foo", Type: sqltypes.Int8},
		},
		RowsAffected: 1,
		InsertID:     1,
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeString([]byte("abcd"))},
		},
	}
	// test one empty result
	appendResult(qr, innerqr1)
	appendResult(qr, innerqr2)
	if len(qr.Fields) != 1 {
		t.Errorf("want 1, got %v", len(qr.Fields))
	}
	if qr.RowsAffected != 1 {
		t.Errorf("want 1, got %v", qr.RowsAffected)
	}
	if qr.InsertID != 1 {
		t.Errorf("want 1, got %v", qr.InsertID)
	}
	if len(qr.Rows) != 1 {
		t.Errorf("want 1, got %v", len(qr.Rows))
	}
	// test two valid results
	qr = new(sqltypes.Result)
	appendResult(qr, innerqr2)
	appendResult(qr, innerqr2)
	if len(qr.Fields) != 1 {
		t.Errorf("want 1, got %v", len(qr.Fields))
	}
	if qr.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.RowsAffected)
	}
	if qr.InsertID != 1 {
		t.Errorf("want 1, got %v", qr.InsertID)
	}
	if len(qr.Rows) != 2 {
		t.Errorf("want 2, got %v", len(qr.Rows))
	}
}

// MockShuffleQueryPartsRandomGenerator implements the ShuffleQueryPartsRandomGeneratorInterface
// and returns a canned set of responses given in 'intNResults' for successive calls to its Intn()
// method.
type mockShuffleQueryPartsRandomGenerator struct {
	intNResults []int
}

func (mockRandGen *mockShuffleQueryPartsRandomGenerator) Intn(unused int) int {
	if len(mockRandGen.intNResults) == 0 {
		panic("MockShuffleQueryPartsRandomGenerator exhausted.")
	}
	result := mockRandGen.intNResults[0]
	mockRandGen.intNResults = mockRandGen.intNResults[1:]
	return result
}

func TestShuffleQueryParts(t *testing.T) {
	mockRandGen := &mockShuffleQueryPartsRandomGenerator{
		intNResults: []int{1, 0},
	}
	oldGen := injectShuffleQueryPartsRandomGenerator(mockRandGen)
	queryPart1 := vtgatepb.SplitQueryResponse_Part{
		Query: &querypb.BoundQuery{Sql: "part_1"},
	}
	queryPart2 := vtgatepb.SplitQueryResponse_Part{
		Query: &querypb.BoundQuery{Sql: "part_2"},
	}
	queryPart3 := vtgatepb.SplitQueryResponse_Part{
		Query: &querypb.BoundQuery{Sql: "part_3"},
	}
	queryParts := []*vtgatepb.SplitQueryResponse_Part{&queryPart1, &queryPart2, &queryPart3}
	queryPartsExpectedOutput := []*vtgatepb.SplitQueryResponse_Part{
		&queryPart3, &queryPart1, &queryPart2,
	}
	shuffleQueryParts(queryParts)
	if !reflect.DeepEqual(queryPartsExpectedOutput, queryParts) {
		t.Errorf("want: %+v, got %+v", queryPartsExpectedOutput, queryParts)
	}

	// Return the generator to what it was to avoid disrupting other tests.
	injectShuffleQueryPartsRandomGenerator(oldGen)
}
