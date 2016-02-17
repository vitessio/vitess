// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// This file uses the sandbox_test framework.

func TestScatterConnExecute(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecute", func(shards []string) (*sqltypes.Result, error) {
		stc := NewScatterConn(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, nil, "")
		return stc.Execute(context.Background(), "query", nil, "TestScatterConnExecute", shards, topodatapb.TabletType_REPLICA, nil, false)
	})
}

func TestScatterConnExecuteMulti(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecuteMulti", func(shards []string) (*sqltypes.Result, error) {
		stc := NewScatterConn(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, nil, "")
		shardVars := make(map[string]map[string]interface{})
		for _, shard := range shards {
			shardVars[shard] = nil
		}
		return stc.ExecuteMulti(context.Background(), "query", "TestScatterConnExecuteMulti", shardVars, topodatapb.TabletType_REPLICA, nil, false)
	})
}

func TestScatterConnExecuteBatch(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecuteBatch", func(shards []string) (*sqltypes.Result, error) {
		stc := NewScatterConn(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, nil, "")
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
	testScatterConnGeneric(t, "TestScatterConnStreamExecute", func(shards []string) (*sqltypes.Result, error) {
		stc := NewScatterConn(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, nil, "")
		qr := new(sqltypes.Result)
		err := stc.StreamExecute(context.Background(), "query", nil, "TestScatterConnStreamExecute", shards, topodatapb.TabletType_REPLICA, func(r *sqltypes.Result) error {
			appendResult(qr, r)
			return nil
		})
		return qr, err
	})
}

func TestScatterConnStreamExecuteMulti(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnStreamExecuteMulti", func(shards []string) (*sqltypes.Result, error) {
		stc := NewScatterConn(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, nil, "")
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

func testScatterConnGeneric(t *testing.T, name string, f func(shards []string) (*sqltypes.Result, error)) {
	// no shard
	s := createSandbox(name)
	qr, err := f(nil)
	if qr.RowsAffected != 0 {
		t.Errorf("want 0, got %v", qr.RowsAffected)
	}
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	// single shard
	s.Reset()
	sbc := &sandboxConn{mustFailServer: 1}
	s.MapTestConn("0", sbc)
	qr, err = f([]string{"0"})
	want := fmt.Sprintf("shard, host: %v.0.replica, host:\"0\" port_map:<key:\"vt\" value:1 > , error: err", name)
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
	sbc0 := &sandboxConn{mustFailServer: 1}
	s.MapTestConn("0", sbc0)
	sbc1 := &sandboxConn{mustFailServer: 1}
	s.MapTestConn("1", sbc1)
	_, err = f([]string{"0", "1"})
	// Verify server errors are consolidated.
	want = fmt.Sprintf("shard, host: %v.0.replica, host:\"0\" port_map:<key:\"vt\" value:1 > , error: err\nshard, host: %v.1.replica, host:\"1\" port_map:<key:\"vt\" value:1 > , error: err", name, name)
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
	sbc0 = &sandboxConn{mustFailServer: 1}
	s.MapTestConn("0", sbc0)
	sbc1 = &sandboxConn{mustFailTxPool: 1}
	s.MapTestConn("1", sbc1)
	_, err = f([]string{"0", "1"})
	// Verify server errors are consolidated.
	want = fmt.Sprintf("shard, host: %v.0.replica, host:\"0\" port_map:<key:\"vt\" value:1 > , error: err\nshard, host: %v.1.replica, host:\"1\" port_map:<key:\"vt\" value:1 > , tx_pool_full: err", name, name)
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
	sbc = &sandboxConn{}
	s.MapTestConn("0", sbc)
	qr, err = f([]string{"0", "0"})
	// Ensure that we executed only once.
	if execCount := sbc.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}

	// no errors
	s.Reset()
	sbc0 = &sandboxConn{}
	s.MapTestConn("0", sbc0)
	sbc1 = &sandboxConn{}
	s.MapTestConn("1", sbc1)
	qr, err = f([]string{"0", "1"})
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
	s := createSandbox("TestMultiExecs")
	sbc0 := &sandboxConn{}
	s.MapTestConn("0", sbc0)
	sbc1 := &sandboxConn{}
	s.MapTestConn("1", sbc1)
	stc := NewScatterConn(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, nil, "")
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
	s := createSandbox("TestScatterConnStreamExecuteSendError")
	sbc := &sandboxConn{}
	s.MapTestConn("0", sbc)
	stc := NewScatterConn(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, nil, "")
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
	s := createSandbox("TestScatterCommitRollbackIncorrectSession")
	sbc0 := &sandboxConn{}
	s.MapTestConn("0", sbc0)
	stc := NewScatterConn(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, nil, "")

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
	s := createSandbox("TestScatterConnCommitSuccess")
	sbc0 := &sandboxConn{}
	s.MapTestConn("0", sbc0)
	sbc1 := &sandboxConn{}
	s.MapTestConn("1", sbc1)
	stc := NewScatterConn(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, nil, "")

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
	sbc0.mustFailServer = 1
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
	s := createSandbox("TestScatterConnRollback")
	sbc0 := &sandboxConn{}
	s.MapTestConn("0", sbc0)
	sbc1 := &sandboxConn{}
	s.MapTestConn("1", sbc1)
	stc := NewScatterConn(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, nil, "")

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

func TestScatterConnClose(t *testing.T) {
	s := createSandbox("TestScatterConnClose")
	sbc := &sandboxConn{}
	s.MapTestConn("0", sbc)
	stc := NewScatterConn(nil, topo.Server{}, new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour, nil, "")
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnClose", []string{"0"}, topodatapb.TabletType_REPLICA, nil, false)
	stc.Close()
	// retry for 10s as Close() is async.
	for i := 0; i < 10; i++ {
		if closeCount := sbc.CloseCount.Get(); closeCount == 1 {
			return
		}
		time.Sleep(1 * time.Second)
	}
	if closeCount := sbc.CloseCount.Get(); closeCount != 1 {
		t.Errorf("want 1, got %d", closeCount)
	}
}

func TestScatterConnError(t *testing.T) {
	err := &ScatterConnError{
		Code: 12,
		Errs: []error{
			&ShardConnError{Code: 10, Err: &tabletconn.ServerError{Err: "tabletconn error"}},
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

	// case 1: read query (not in transaction) followed by write query, not in the same shard.
	sbc0 := &sandboxConn{}
	s.MapTestConn("0", sbc0)
	sbc1 := &sandboxConn{}
	s.MapTestConn("1", sbc1)
	stc := NewScatterConn(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, nil, "")
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
		if execCount0 != 1 || execCount1 != 3 {
			t.Errorf("want 1/3, got %d/%d", execCount0, execCount1)
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
	sbc0 = &sandboxConn{}
	s.MapTestConn("0", sbc0)
	sbc1 = &sandboxConn{}
	s.MapTestConn("1", sbc1)
	stc = NewScatterConn(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, nil, "")
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
		if execCount0 != 3 || execCount1 != 1 {
			t.Errorf("want 3/1, got %d/%d", execCount0, execCount1)
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
	sbc0 = &sandboxConn{}
	s.MapTestConn("0", sbc0)
	sbc1 = &sandboxConn{}
	s.MapTestConn("1", sbc1)
	stc = NewScatterConn(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, nil, "")
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
		if execCount0 != 4 || execCount1 != 1 {
			t.Errorf("want 4/1, got %d/%d", execCount0, execCount1)
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
