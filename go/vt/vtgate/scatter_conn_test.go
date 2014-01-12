// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"

	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

// This file uses the sandbox_test framework.

func TestScatterConnExecute(t *testing.T) {
	testScatterConnGeneric(t, func(shards []string) (*proto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "aa", 1*time.Millisecond, 3)
		return stc.Execute("query", nil, "", shards, "", nil)
	})
}

func TestScatterConnExecuteBatch(t *testing.T) {
	testScatterConnGeneric(t, func(shards []string) (*proto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "aa", 1*time.Millisecond, 3)
		queries := []tproto.BoundQuery{{"query", nil}}
		qrs, err := stc.ExecuteBatch(queries, "", shards, "", nil)
		if err != nil {
			return nil, err
		}
		return &proto.QueryResult{QueryResult: qrs.List[0]}, err
	})
}

func TestScatterConnStreamExecute(t *testing.T) {
	testScatterConnGeneric(t, func(shards []string) (*proto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "aa", 1*time.Millisecond, 3)
		qr := new(proto.QueryResult)
		err := stc.StreamExecute("query", nil, "", shards, "", nil, func(r interface{}) error {
			appendResult(&qr.QueryResult, &r.(*proto.QueryResult).QueryResult)
			return nil
		})
		return qr, err
	})
}

func testScatterConnGeneric(t *testing.T, f func(shards []string) (*proto.QueryResult, error)) {
	// no shard
	resetSandbox()
	qr, err := f(nil)
	if qr.RowsAffected != 0 {
		t.Errorf("want 0, got %v", qr.RowsAffected)
	}
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	// single shard
	resetSandbox()
	sbc := &sandboxConn{mustFailServer: 1}
	testConns[0] = sbc
	qr, err = f([]string{"0"})
	want := "error: err, shard: (.0.), host: 0"
	// Verify server error string.
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure that we tried only once.
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
	}

	// two shards
	resetSandbox()
	sbc0 := &sandboxConn{mustFailServer: 1}
	testConns[0] = sbc0
	sbc1 := &sandboxConn{mustFailServer: 1}
	testConns[1] = sbc1
	_, err = f([]string{"0", "1"})
	// Verify server errors are consolidated.
	want = "error: err, shard: (.0.), host: 0\nerror: err, shard: (.1.), host: 1"
	if err == nil || err.Error() != want {
		t.Errorf("\nwant\n%s\ngot\n%v", want, err)
	}
	// Ensure that we tried only once.
	if sbc0.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc0.ExecCount)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc1.ExecCount)
	}

	// duplicate shards
	resetSandbox()
	sbc = &sandboxConn{}
	testConns[0] = sbc
	qr, err = f([]string{"0", "0"})
	// Ensure that we executed only once.
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
	}

	// no errors
	resetSandbox()
	sbc0 = &sandboxConn{}
	testConns[0] = sbc0
	sbc1 = &sandboxConn{}
	testConns[1] = sbc1
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

func TestScatterConnStreamExecuteSendError(t *testing.T) {
	resetSandbox()
	sbc := &sandboxConn{}
	testConns[0] = sbc
	stc := NewScatterConn(new(sandboxTopo), "aa", 1*time.Millisecond, 3)
	err := stc.StreamExecute("query", nil, "", []string{"0"}, "", nil, func(interface{}) error {
		return fmt.Errorf("send error")
	})
	want := "send error"
	// Ensure that we handle send errors.
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
}

func TestScatterConnCommitSuccess(t *testing.T) {
	resetSandbox()
	sbc0 := &sandboxConn{}
	testConns[0] = sbc0
	sbc1 := &sandboxConn{mustFailTxPool: 1}
	testConns[1] = sbc1
	stc := NewScatterConn(new(sandboxTopo), "aa", 1*time.Millisecond, 3)

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&proto.Session{InTransaction: true})
	stc.Execute("query1", nil, "", []string{"0"}, "", session)
	wantSession := proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "",
			Shard:         "0",
			TabletType:    "",
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(wantSession, *session.Session) {
		t.Errorf("want\n%#v, got\n%#v", wantSession, *session.Session)
	}
	stc.Execute("query1", nil, "", []string{"0", "1"}, "", session)
	wantSession = proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "",
			Shard:         "0",
			TabletType:    "",
			TransactionId: 1,
		}, {
			Keyspace:      "",
			Shard:         "1",
			TabletType:    "",
			TransactionId: 2,
		}},
	}
	if !reflect.DeepEqual(wantSession, *session.Session) {
		t.Errorf("want\n%#v, got\n%#v", wantSession, *session.Session)
	}
	sbc0.mustFailServer = 1
	err := stc.Commit(session)
	// wait for rollback goroutines to complete.
	runtime.Gosched()
	if err == nil {
		t.Errorf("want error, got nil")
	}
	wantSession = proto.Session{}
	if !reflect.DeepEqual(wantSession, *session.Session) {
		t.Errorf("want\n%#v, got\n%#v", wantSession, *session.Session)
	}
	if sbc0.CommitCount != 1 {
		t.Errorf("want 1, got %d", sbc0.CommitCount)
	}
	if sbc1.RollbackCount != 1 {
		t.Errorf("want 1, got %d", sbc1.RollbackCount)
	}
}

func TestScatterConnRollback(t *testing.T) {
	resetSandbox()
	sbc0 := &sandboxConn{}
	testConns[0] = sbc0
	sbc1 := &sandboxConn{mustFailTxPool: 1}
	testConns[1] = sbc1
	stc := NewScatterConn(new(sandboxTopo), "aa", 1*time.Millisecond, 3)

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&proto.Session{InTransaction: true})
	stc.Execute("query1", nil, "", []string{"0"}, "", session)
	stc.Execute("query1", nil, "", []string{"0", "1"}, "", session)
	err := stc.Rollback(session)
	// wait for rollback goroutines to complete.
	runtime.Gosched()
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
	resetSandbox()
	sbc := &sandboxConn{}
	testConns[0] = sbc
	stc := NewScatterConn(new(sandboxTopo), "aa", 1*time.Millisecond, 3)
	stc.Execute("query1", nil, "", []string{"0"}, "", nil)
	stc.Close()
	// wait for Close goroutines to complete.
	runtime.Gosched()
	if sbc.CloseCount != 1 {
		t.Errorf("want 1, got %d", sbc.CommitCount)
	}
}
