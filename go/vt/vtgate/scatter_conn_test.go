// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"
)

// This file uses the sandbox_test framework.

func TestScatterConnExecute(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecute", func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		return stc.Execute(context.Background(), "query", nil, "TestScatterConnExecute", shards, "", nil, false)
	})
}

func TestScatterConnExecuteMulti(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecute", func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		shardVars := make(map[string]map[string]interface{})
		for _, shard := range shards {
			shardVars[shard] = nil
		}
		return stc.ExecuteMulti(context.Background(), "query", "TestScatterConnExecute", shardVars, "", nil, false)
	})
}

func TestScatterConnExecuteBatch(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecuteBatch", func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		queries := []proto.BoundShardQuery{{
			Sql:           "query",
			BindVariables: nil,
			Keyspace:      "TestScatterConnExecuteBatch",
			Shards:        shards,
		}}
		scatterRequest := boundShardQueriesToScatterBatchRequest(queries)
		qrs, err := stc.ExecuteBatch(context.Background(), scatterRequest, "", false, nil)
		if err != nil {
			return nil, err
		}
		return &qrs.List[0], err
	})
}

func TestScatterConnStreamExecute(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnStreamExecute", func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		qr := new(mproto.QueryResult)
		err := stc.StreamExecute(context.Background(), "query", nil, "TestScatterConnStreamExecute", shards, "", nil, func(r *mproto.QueryResult) error {
			appendResult(qr, r)
			return nil
		}, false)
		return qr, err
	})
}

func TestScatterConnStreamExecuteMulti(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnStreamExecute", func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		qr := new(mproto.QueryResult)
		shardVars := make(map[string]map[string]interface{})
		for _, shard := range shards {
			shardVars[shard] = nil
		}
		err := stc.StreamExecuteMulti(context.Background(), "query", "TestScatterConnStreamExecute", shardVars, "", nil, func(r *mproto.QueryResult) error {
			appendResult(qr, r)
			return nil
		}, false)
		return qr, err
	})
}

func testScatterConnGeneric(t *testing.T, name string, f func(shards []string) (*mproto.QueryResult, error)) {
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
	want := fmt.Sprintf("shard, host: %v.0., host:\"0\" port_map:<key:\"vt\" value:1 > , error: err", name)
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
	want1 := fmt.Sprintf("shard, host: %v.0., host:\"0\" port_map:<key:\"vt\" value:1 > , error: err\nshard, host: %v.1., host:\"1\" port_map:<key:\"vt\" value:1 > , error: err", name, name)
	want2 := fmt.Sprintf("shard, host: %v.1., host:\"1\" port_map:<key:\"vt\" value:1 > , error: err\nshard, host: %v.0., host:\"0\" port_map:<key:\"vt\" value:1 > , error: err", name, name)
	if err == nil || (err.Error() != want1 && err.Error() != want2) {
		t.Errorf("\nwant\n%s\ngot\n%v", want1, err)
	}
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
	stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
	shardVars := map[string]map[string]interface{}{
		"0": map[string]interface{}{
			"bv0": 0,
		},
		"1": map[string]interface{}{
			"bv1": 1,
		},
	}
	_, _ = stc.ExecuteMulti(context.Background(), "query", "TestMultiExecs", shardVars, "", nil, false)
	if !reflect.DeepEqual(sbc0.Queries[0].BindVariables, shardVars["0"]) {
		t.Errorf("got %+v, want %+v", sbc0.Queries[0].BindVariables, shardVars["0"])
	}
	if !reflect.DeepEqual(sbc1.Queries[0].BindVariables, shardVars["1"]) {
		t.Errorf("got %+v, want %+v", sbc0.Queries[0].BindVariables, shardVars["1"])
	}
	sbc0.Queries = nil
	sbc1.Queries = nil
	_ = stc.StreamExecuteMulti(context.Background(), "query", "TestMultiExecs", shardVars, "", nil, func(*mproto.QueryResult) error {
		return nil
	}, false)
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
	stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
	err := stc.StreamExecute(context.Background(), "query", nil, "TestScatterConnStreamExecuteSendError", []string{"0"}, "", nil, func(*mproto.QueryResult) error {
		return fmt.Errorf("send error")
	}, false)
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
	stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)

	// nil session
	err := stc.Commit(context.Background(), nil)
	if err == nil {
		t.Errorf("want error, got nil")
	}
	err = stc.Rollback(context.Background(), nil)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	// not in transaction
	session := NewSafeSession(&proto.Session{})
	err = stc.Commit(context.Background(), session)
	if err == nil {
		t.Errorf("want error, got nil")
	}
}

func TestScatterConnCommitSuccess(t *testing.T) {
	s := createSandbox("TestScatterConnCommitSuccess")
	sbc0 := &sandboxConn{}
	s.MapTestConn("0", sbc0)
	sbc1 := &sandboxConn{}
	s.MapTestConn("1", sbc1)
	stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&proto.Session{InTransaction: true})
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnCommitSuccess", []string{"0"}, "", session, false)
	wantSession := proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestScatterConnCommitSuccess",
			Shard:         "0",
			TabletType:    "",
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(wantSession, *session.Session) {
		t.Errorf("want\n%+v, got\n%+v", wantSession, *session.Session)
	}
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnCommitSuccess", []string{"0", "1"}, "", session, false)
	wantSession = proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestScatterConnCommitSuccess",
			Shard:         "0",
			TabletType:    "",
			TransactionId: 1,
		}, {
			Keyspace:      "TestScatterConnCommitSuccess",
			Shard:         "1",
			TabletType:    "",
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
	wantSession = proto.Session{}
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
	stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&proto.Session{InTransaction: true})
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0"}, "", session, false)
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0", "1"}, "", session, false)
	err := stc.Rollback(context.Background(), session)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantSession := proto.Session{}
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
	stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnClose", []string{"0"}, "", nil, false)
	stc.Close()
	time.Sleep(1)
	if closeCount := sbc.CloseCount.Get(); closeCount != 1 {
		t.Errorf("want 1, got %d (test may be flaky because connections are closed asynchronously)", closeCount)
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
	wantErrString := "tabletconn error\ngeneric error\nvttablet: Connection Closed"

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
	stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
	session := NewSafeSession(&proto.Session{InTransaction: true})
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnQueryNotInTransaction", []string{"0"}, "", session, true)
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnQueryNotInTransaction", []string{"1"}, "", session, false)

	wantSession := proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestScatterConnQueryNotInTransaction",
			Shard:         "1",
			TabletType:    "",
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
	stc = NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
	session = NewSafeSession(&proto.Session{InTransaction: true})
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnQueryNotInTransaction", []string{"0"}, "", session, false)
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnQueryNotInTransaction", []string{"1"}, "", session, true)

	wantSession = proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestScatterConnQueryNotInTransaction",
			Shard:         "0",
			TabletType:    "",
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
	stc = NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
	session = NewSafeSession(&proto.Session{InTransaction: true})
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnQueryNotInTransaction", []string{"0"}, "", session, false)
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnQueryNotInTransaction", []string{"0", "1"}, "", session, true)

	wantSession = proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestScatterConnQueryNotInTransaction",
			Shard:         "0",
			TabletType:    "",
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
	qr := new(mproto.QueryResult)
	innerqr1 := &mproto.QueryResult{
		Fields: []mproto.Field{},
		Rows:   [][]sqltypes.Value{},
	}
	innerqr2 := &mproto.QueryResult{
		Fields: []mproto.Field{
			{Name: "foo", Type: 1},
		},
		RowsAffected: 1,
		InsertId:     1,
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
	if qr.InsertId != 1 {
		t.Errorf("want 1, got %v", qr.InsertId)
	}
	if len(qr.Rows) != 1 {
		t.Errorf("want 1, got %v", len(qr.Rows))
	}
	// test two valid results
	qr = new(mproto.QueryResult)
	appendResult(qr, innerqr2)
	appendResult(qr, innerqr2)
	if len(qr.Fields) != 1 {
		t.Errorf("want 1, got %v", len(qr.Fields))
	}
	if qr.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.RowsAffected)
	}
	if qr.InsertId != 1 {
		t.Errorf("want 1, got %v", qr.InsertId)
	}
	if len(qr.Rows) != 2 {
		t.Errorf("want 2, got %v", len(qr.Rows))
	}
}
