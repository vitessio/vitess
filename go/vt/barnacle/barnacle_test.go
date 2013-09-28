// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"runtime"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/vt/barnacle/proto"
)

// This file uses the sandbox_test framework.

func init() {
	Init(NewBalancerMap(new(sandboxTopo), "aa", "vt"), "sandbox", 1*time.Second, 10)
}

func resetBarnacle() (sess proto.Session) {
	resetSandbox()
	*RpcBarnacle = Barnacle{
		balancerMap:    NewBalancerMap(new(sandboxTopo), "aa", "vt"),
		tabletProtocol: "sandbox",
		connections:    pools.NewNumbered(),
		retryDelay:     1 * time.Second,
		retryCount:     10,
	}
	RpcBarnacle.GetSessionId(&proto.SessionParams{TabletType: "master"}, &sess)
	return
}

func exec(sess proto.Session) (qr mproto.QueryResult, err error) {
	q := proto.Query{
		Sql:       "query",
		SessionId: sess.SessionId,
		Shards:    []string{"0"},
	}
	err = RpcBarnacle.Execute(nil, &q, &qr)
	return
}

func TestBarnacleGetSessionId(t *testing.T) {
	resetBarnacle()
	var sess proto.Session
	err := RpcBarnacle.GetSessionId(&proto.SessionParams{TabletType: "master"}, &sess)
	if err != nil {
		t.Errorf("%v", err)
	}
	if sess.SessionId == 0 {
		t.Errorf("want non-zero, got 0")
	}

	var next proto.Session
	err = RpcBarnacle.GetSessionId(&proto.SessionParams{TabletType: "master"}, &next)
	if err != nil {
		t.Errorf("%v", err)
	}
	if sess.SessionId == next.SessionId {
		t.Errorf("want unequal, got equal %d", sess.SessionId)
	}
}

func TestBarnacleSessionConflict(t *testing.T) {
	sess := resetBarnacle()
	sbc := &sandboxConn{mustDelay: 50 * time.Millisecond}
	testConns["0:1"] = sbc
	q := proto.Query{
		Sql:       "query",
		SessionId: sess.SessionId,
		Shards:    []string{"0"},
	}
	var qr mproto.QueryResult
	go RpcBarnacle.Execute(nil, &q, &qr)
	runtime.Gosched()
	want := "in use"
	var err error
	var noOutput string
	i := 0
	for {
		switch i {
		case 0:
			err = RpcBarnacle.Execute(nil, &q, nil)
		case 1:
			err = RpcBarnacle.StreamExecute(nil, &q, nil)
		case 2:
			err = RpcBarnacle.Begin(nil, &sess, &noOutput)
		case 3:
			err = RpcBarnacle.Commit(nil, &sess, &noOutput)
		case 4:
			err = RpcBarnacle.Rollback(nil, &sess, &noOutput)
		case 5:
			err = RpcBarnacle.CloseSession(nil, &sess, &noOutput)
		default:
			return
		}
		if err == nil || err.Error() != want {
			t.Errorf("case %d: want %s, got %v", i, want, err)
		}
		i++
	}
}

func TestBarnacleExecute(t *testing.T) {
	sess := resetBarnacle()
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	qr, err := exec(sess)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if qr.RowsAffected != 1 {
		t.Errorf("want 1, got %v", qr.RowsAffected)
	}
}

func TestBarnacleStreamExecute(t *testing.T) {
	sess := resetBarnacle()
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	q := proto.Query{
		Sql:       "query",
		SessionId: sess.SessionId,
		Shards:    []string{"0"},
	}
	var qr mproto.QueryResult
	err := RpcBarnacle.StreamExecute(nil, &q, func(r interface{}) error {
		qr = *(r.(*mproto.QueryResult))
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if qr.RowsAffected != 1 {
		t.Errorf("want 1, got %v", qr.RowsAffected)
	}
}

func TestBarnacleTx(t *testing.T) {
	sess := resetBarnacle()
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	var noOutput string
	for i := 0; i < 2; i++ {
		RpcBarnacle.Begin(nil, &sess, &noOutput)
		exec(sess)
		if sbc.BeginCount != i+1 {
			t.Errorf("want 1, got %v", sbc.BeginCount)
		}
		switch i {
		case 0:
			if sbc.CommitCount != 0 {
				t.Errorf("want 0, got %v", sbc.BeginCount)
			}
			RpcBarnacle.Commit(nil, &sess, &noOutput)
			if sbc.CommitCount != 1 {
				t.Errorf("want 1, got %v", sbc.BeginCount)
			}
		case 1:
			if sbc.RollbackCount != 0 {
				t.Errorf("want 0, got %v", sbc.RollbackCount)
			}
			RpcBarnacle.Rollback(nil, &sess, &noOutput)
			if sbc.RollbackCount != 1 {
				t.Errorf("want 1, got %v", sbc.RollbackCount)
			}
		}
	}
}

func TestBarnacleClose(t *testing.T) {
	sess := resetBarnacle()
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	exec(sess)
	if sbc.CloseCount != 0 {
		t.Errorf("want 0, got %v", sbc.CloseCount)
	}
	var noOutput string
	err := RpcBarnacle.CloseSession(nil, &sess, &noOutput)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if sbc.CloseCount != 1 {
		t.Errorf("want 1, got %v", sbc.CloseCount)
	}
	_, err = exec(sess)
	want := "not found"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
}
