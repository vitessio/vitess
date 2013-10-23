// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"runtime"
	"strings"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/pools"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

// This file uses the sandbox_test framework.

func init() {
	Init(NewBalancerMap(new(sandboxTopo), "aa", "vt"), "sandbox", 1*time.Second, 10)
}

// resetVTGate resets the internal state of RpcVTGate.
func resetVTGate() (sess proto.Session) {
	resetSandbox()
	*RpcVTGate = VTGate{
		balancerMap:    NewBalancerMap(new(sandboxTopo), "aa", "vt"),
		tabletProtocol: "sandbox",
		connections:    pools.NewNumbered(),
		retryDelay:     1 * time.Second,
		retryCount:     10,
	}
	RpcVTGate.GetSessionId(&proto.SessionParams{TabletType: "master"}, &sess)
	return
}

// exec is a convienence wrapper for executing a query.
func execShard(sess proto.Session) (qr mproto.QueryResult, err error) {
	q := proto.QueryShard{
		Sql:       "query",
		SessionId: sess.SessionId,
		Shards:    []string{"0"},
	}
	err = RpcVTGate.ExecuteShard(nil, &q, &qr)
	return
}

func TestVTGateGetSessionId(t *testing.T) {
	resetVTGate()
	var sess proto.Session
	err := RpcVTGate.GetSessionId(&proto.SessionParams{TabletType: "master"}, &sess)
	if err != nil {
		t.Errorf("%v", err)
	}
	if sess.SessionId == 0 {
		t.Errorf("want non-zero, got 0")
	}

	var next proto.Session
	err = RpcVTGate.GetSessionId(&proto.SessionParams{TabletType: "master"}, &next)
	if err != nil {
		t.Errorf("%v", err)
	}
	// Every GetSessionId should issue different numbers.
	if sess.SessionId == next.SessionId {
		t.Errorf("want unequal, got equal %d", sess.SessionId)
	}
}

func TestVTGateSessionConflict(t *testing.T) {
	sess := resetVTGate()
	sbc := &sandboxConn{mustDelay: 50 * time.Millisecond}
	testConns["0:1"] = sbc
	q := proto.QueryShard{
		Sql:       "query",
		SessionId: sess.SessionId,
		Shards:    []string{"0"},
	}
	bq := proto.BatchQueryShard{
		Queries: []tproto.BoundQuery{{
			"query",
			nil,
		}},
		SessionId: sess.SessionId,
		Shards:    []string{"0"},
	}
	var qr mproto.QueryResult
	go RpcVTGate.ExecuteShard(nil, &q, &qr)
	runtime.Gosched()
	want := "in use"
	var err error
	var noOutput string
	i := 0
	for {
		switch i {
		case 0:
			err = RpcVTGate.ExecuteShard(nil, &q, nil)
		case 1:
			err = RpcVTGate.ExecuteBatchShard(nil, &bq, nil)
		case 2:
			err = RpcVTGate.StreamExecuteShard(nil, &q, nil)
		case 3:
			err = RpcVTGate.Begin(nil, &sess, &noOutput)
		case 4:
			err = RpcVTGate.Commit(nil, &sess, &noOutput)
		case 5:
			err = RpcVTGate.Rollback(nil, &sess, &noOutput)
		default:
			return
		}
		// Trying to issue another command on a session that's
		// already busy should result in an error.
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("case %d: want %s, got %v", i, want, err)
		}
		i++
	}
}

func TestVTGateExecuteShard(t *testing.T) {
	sess := resetVTGate()
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	qr, err := execShard(sess)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if qr.RowsAffected != 1 {
		t.Errorf("want 1, got %v", qr.RowsAffected)
	}
}

func TestVTGateExecuteBatchShard(t *testing.T) {
	sess := resetVTGate()
	testConns["0:1"] = &sandboxConn{}
	testConns["1:1"] = &sandboxConn{}
	q := proto.BatchQueryShard{
		Queries: []tproto.BoundQuery{{
			"query",
			nil,
		}, {
			"query",
			nil,
		}},
		SessionId: sess.SessionId,
		Shards:    []string{"0", "1"},
	}
	qrs := new(tproto.QueryResultList)
	err := RpcVTGate.ExecuteBatchShard(nil, &q, qrs)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if len(qrs.List) != 2 {
		t.Errorf("want 2, got %v", len(qrs.List))
	}
	if qrs.List[0].RowsAffected != 2 {
		t.Errorf("want 2, got %v", qrs.List[0].RowsAffected)
	}
}

func TestVTGateStreamExecuteShard(t *testing.T) {
	sess := resetVTGate()
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	q := proto.QueryShard{
		Sql:       "query",
		SessionId: sess.SessionId,
		Shards:    []string{"0"},
	}
	var qr mproto.QueryResult
	err := RpcVTGate.StreamExecuteShard(nil, &q, func(r interface{}) error {
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

func TestVTGateTx(t *testing.T) {
	sess := resetVTGate()
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	var noOutput string
	for i := 0; i < 2; i++ {
		RpcVTGate.Begin(nil, &sess, &noOutput)
		execShard(sess)
		// Ensure low level Begin gets called.
		if sbc.BeginCount != i+1 {
			t.Errorf("want 1, got %v", sbc.BeginCount)
		}
		switch i {
		case 0:
			// Ensure low level Commit gets called.
			if sbc.CommitCount != 0 {
				t.Errorf("want 0, got %v", sbc.BeginCount)
			}
			RpcVTGate.Commit(nil, &sess, &noOutput)
			if sbc.CommitCount != 1 {
				t.Errorf("want 1, got %v", sbc.BeginCount)
			}
		case 1:
			// Ensure low level Rollback gets called.
			if sbc.RollbackCount != 0 {
				t.Errorf("want 0, got %v", sbc.RollbackCount)
			}
			RpcVTGate.Rollback(nil, &sess, &noOutput)
			if sbc.RollbackCount != 1 {
				t.Errorf("want 1, got %v", sbc.RollbackCount)
			}
		}
	}
}

func TestVTGateClose(t *testing.T) {
	sess := resetVTGate()
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	execShard(sess)
	// Ensure low level Close gets called.
	if sbc.CloseCount != 0 {
		t.Errorf("want 0, got %v", sbc.CloseCount)
	}
	var noOutput string
	err := RpcVTGate.CloseSession(nil, &sess, &noOutput)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if sbc.CloseCount != 1 {
		t.Errorf("want 1, got %v", sbc.CloseCount)
	}
	_, err = execShard(sess)
	want := "not found"
	// Reusing a closed session should result in an error.
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("want %s, got %v", want, err)
	}
}
