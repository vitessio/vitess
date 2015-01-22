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
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"
)

// This file uses the sandbox_test framework.

func TestScatterConnExecute(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecute", func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 1*time.Millisecond)
		return stc.Execute(context.Background(), "query", nil, "TestScatterConnExecute", shards, "", nil)
	})
}

func TestScatterConnExecuteMulti(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecute", func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 1*time.Millisecond)
		shardVars := make(map[string]map[string]interface{})
		for _, shard := range shards {
			shardVars[shard] = nil
		}
		return stc.ExecuteMulti(context.Background(), "query", "TestScatterConnExecute", shardVars, "", nil)
	})
}

func TestScatterConnExecuteBatch(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecuteBatch", func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 1*time.Millisecond)
		queries := []tproto.BoundQuery{{"query", nil}}
		qrs, err := stc.ExecuteBatch(context.Background(), queries, "TestScatterConnExecuteBatch", shards, "", nil)
		if err != nil {
			return nil, err
		}
		return &qrs.List[0], err
	})
}

func TestScatterConnStreamExecute(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnStreamExecute", func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 1*time.Millisecond)
		qr := new(mproto.QueryResult)
		err := stc.StreamExecute(context.Background(), "query", nil, "TestScatterConnStreamExecute", shards, "", nil, func(r *mproto.QueryResult) error {
			appendResult(qr, r)
			return nil
		})
		return qr, err
	})
}

func TestScatterConnStreamExecuteMulti(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnStreamExecute", func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 1*time.Millisecond)
		qr := new(mproto.QueryResult)
		shardVars := make(map[string]map[string]interface{})
		for _, shard := range shards {
			shardVars[shard] = nil
		}
		err := stc.StreamExecuteMulti(context.Background(), "query", "TestScatterConnStreamExecute", shardVars, "", nil, func(r *mproto.QueryResult) error {
			appendResult(qr, r)
			return nil
		})
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
	want := fmt.Sprintf("shard, host: %v.0., {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}, error: err", name)
	// Verify server error string.
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure that we tried only once.
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
	}

	// two shards
	s.Reset()
	sbc0 := &sandboxConn{mustFailServer: 1}
	s.MapTestConn("0", sbc0)
	sbc1 := &sandboxConn{mustFailServer: 1}
	s.MapTestConn("1", sbc1)
	_, err = f([]string{"0", "1"})
	// Verify server errors are consolidated.
	want1 := fmt.Sprintf("shard, host: %v.0., {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}, error: err\nshard, host: %v.1., {Uid:0 Host:1 NamedPortMap:map[vt:1] Health:map[]}, error: err", name, name)
	want2 := fmt.Sprintf("shard, host: %v.1., {Uid:0 Host:1 NamedPortMap:map[vt:1] Health:map[]}, error: err\nshard, host: %v.0., {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}, error: err", name, name)
	if err == nil || (err.Error() != want1 && err.Error() != want2) {
		t.Errorf("\nwant\n%s\ngot\n%v", want1, err)
	}
	// Ensure that we tried only once.
	if sbc0.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc0.ExecCount)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc1.ExecCount)
	}

	// duplicate shards
	s.Reset()
	sbc = &sandboxConn{}
	s.MapTestConn("0", sbc)
	qr, err = f([]string{"0", "0"})
	// Ensure that we executed only once.
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
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
	if sbc0.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc0.ExecCount)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc1.ExecCount)
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
	stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 1*time.Millisecond)
	shardVars := map[string]map[string]interface{}{
		"0": map[string]interface{}{
			"bv0": 0,
		},
		"1": map[string]interface{}{
			"bv1": 1,
		},
	}
	_, _ = stc.ExecuteMulti(context.Background(), "query", "TestMultiExecs", shardVars, "", nil)
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
	stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 1*time.Millisecond)
	err := stc.StreamExecute(context.Background(), "query", nil, "TestScatterConnStreamExecuteSendError", []string{"0"}, "", nil, func(*mproto.QueryResult) error {
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
	stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 1*time.Millisecond)

	// nil session
	err := stc.Commit(context.Background(), nil)
	if err == nil {
		t.Errorf("want error, got nil")
	}
	err = stc.Rollback(context.Background(), nil)
	if err == nil {
		t.Errorf("want error, got nil")
	}
	// not in transaction
	session := NewSafeSession(&proto.Session{})
	err = stc.Commit(context.Background(), session)
	if err == nil {
		t.Errorf("want error, got nil")
	}
	err = stc.Rollback(context.Background(), session)
	if err == nil {
		t.Errorf("want error, got nil")
	}
	// no shard session
	session = NewSafeSession(&proto.Session{InTransaction: true})
	err = stc.Commit(context.Background(), session)
	if err == nil {
		t.Errorf("want error, got nil")
	}
	err = stc.Rollback(context.Background(), session)
	if err == nil {
		t.Errorf("want error, got nil")
	}
}

func TestScatterConnCommitSuccess(t *testing.T) {
	s := createSandbox("TestScatterConnCommitSuccess")
	sbc0 := &sandboxConn{}
	s.MapTestConn("0", sbc0)
	sbc1 := &sandboxConn{mustFailTxPool: 1}
	s.MapTestConn("1", sbc1)
	stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 1*time.Millisecond)

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&proto.Session{InTransaction: true})
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnCommitSuccess", []string{"0"}, "", session)
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
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnCommitSuccess", []string{"0", "1"}, "", session)
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
	if sbc0.CommitCount != 1 {
		t.Errorf("want 1, got %d", sbc0.CommitCount)
	}
	if sbc1.RollbackCount != 1 {
		t.Errorf("want 1, got %d", sbc1.RollbackCount)
	}
}

func TestScatterConnRollback(t *testing.T) {
	s := createSandbox("TestScatterConnRollback")
	sbc0 := &sandboxConn{}
	s.MapTestConn("0", sbc0)
	sbc1 := &sandboxConn{mustFailTxPool: 1}
	s.MapTestConn("1", sbc1)
	stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 1*time.Millisecond)

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&proto.Session{InTransaction: true})
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0"}, "", session)
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0", "1"}, "", session)
	err := stc.Rollback(context.Background(), session)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantSession := proto.Session{}
	if !reflect.DeepEqual(wantSession, *session.Session) {
		t.Errorf("want\n%#v, got\n%#v", wantSession, *session.Session)
	}
	if sbc0.RollbackCount != 1 {
		t.Errorf("want 1, got %d", sbc0.RollbackCount)
	}
	if sbc1.RollbackCount != 1 {
		t.Errorf("want 1, got %d", sbc1.RollbackCount)
	}
}

func TestScatterConnClose(t *testing.T) {
	s := createSandbox("TestScatterConnClose")
	sbc := &sandboxConn{}
	s.MapTestConn("0", sbc)
	stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 1*time.Millisecond)
	stc.Execute(context.Background(), "query1", nil, "TestScatterConnClose", []string{"0"}, "", nil)
	stc.Close()
	if sbc.CloseCount != 1 {
		t.Errorf("want 1, got %d", sbc.CloseCount)
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
