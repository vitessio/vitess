// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"reflect"
	"testing"
	"time"

	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

// This file uses the sandbox_test framework.

func init() {
	Init(new(sandboxTopo), "aa", 1*time.Second, 10, 1*time.Millisecond)
}

func TestVTGateExecuteShard(t *testing.T) {
	sandbox := createSandbox("TestVTGateExecuteShard")
	sbc := &sandboxConn{}
	sandbox.MapTestConn("0", sbc)
	q := proto.QueryShard{
		Sql:      "query",
		Keyspace: "TestVTGateExecuteShard",
		Shards:   []string{"0"},
	}
	qr := new(proto.QueryResult)
	err := RpcVTGate.ExecuteShard(nil, &q, qr)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantqr := new(proto.QueryResult)
	proto.PopulateQueryResult(singleRowResult, wantqr)
	if !reflect.DeepEqual(wantqr, qr) {
		t.Errorf("want \n%+v, got \n%+v", singleRowResult, qr)
	}
	if qr.Session != nil {
		t.Errorf("want nil, got %+v\n", qr.Session)
	}

	q.Session = new(proto.Session)
	RpcVTGate.Begin(nil, q.Session)
	if !q.Session.InTransaction {
		t.Errorf("want true, got false")
	}
	RpcVTGate.ExecuteShard(nil, &q, qr)
	wantSession := &proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestVTGateExecuteShard",
			Shard:         "0",
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(wantSession, q.Session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, q.Session)
	}

	RpcVTGate.Commit(nil, q.Session)
	if sbc.CommitCount != 1 {
		t.Errorf("want 1, got %d", sbc.CommitCount)
	}

	q.Session = new(proto.Session)
	RpcVTGate.Begin(nil, q.Session)
	RpcVTGate.ExecuteShard(nil, &q, qr)
	RpcVTGate.Rollback(nil, q.Session)
	/*
		// Flaky: This test should be run manually.
		runtime.Gosched()
		if sbc.RollbackCount != 1 {
			t.Errorf("want 1, got %d", sbc.RollbackCount)
		}
	*/
}

func TestVTGateExecuteKeyspaceIds(t *testing.T) {
	s := createSandbox("TestVTGateExecuteKeyspaceIds")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("20-40", sbc2)
	q := proto.KeyspaceIdQuery{
		Sql:         "query",
		Keyspace:    "TestVTGateExecuteKeyspaceIds",
		KeyspaceIds: []string{"10"},
		TabletType:  topo.TYPE_MASTER,
	}
	// Test for successful execution
	qr := new(proto.QueryResult)
	err := RpcVTGate.ExecuteKeyspaceIds(nil, &q, qr)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantqr := new(proto.QueryResult)
	proto.PopulateQueryResult(singleRowResult, wantqr)
	if !reflect.DeepEqual(wantqr, qr) {
		t.Errorf("want \n%+v, got \n%+v", singleRowResult, qr)
	}
	if qr.Session != nil {
		t.Errorf("want nil, got %+v\n", qr.Session)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("want 1, got %v\n", sbc1.ExecCount)
	}
	// Test for successful execution in transaction
	q.Session = new(proto.Session)
	RpcVTGate.Begin(nil, q.Session)
	if !q.Session.InTransaction {
		t.Errorf("want true, got false")
	}
	RpcVTGate.ExecuteKeyspaceIds(nil, &q, qr)
	wantSession := &proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestVTGateExecuteKeyspaceIds",
			Shard:         "-20",
			TransactionId: 1,
			TabletType:    topo.TYPE_MASTER,
		}},
	}
	if !reflect.DeepEqual(wantSession, q.Session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, q.Session)
	}
	RpcVTGate.Commit(nil, q.Session)
	if sbc1.CommitCount.Get() != 1 {
		t.Errorf("want 1, got %d", sbc1.CommitCount.Get())
	}
	// Test for multiple shards
	q.KeyspaceIds = []string{"10", "30"}
	RpcVTGate.ExecuteKeyspaceIds(nil, &q, qr)
	if qr.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.RowsAffected)
	}
}

func TestVTGateExecuteKeyRange(t *testing.T) {
	s := createSandbox("TestVTGateExecuteKeyRange")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("20-40", sbc2)
	q := proto.KeyRangeQuery{
		Sql:        "query",
		Keyspace:   "TestVTGateExecuteKeyRange",
		KeyRange:   "-20",
		TabletType: topo.TYPE_MASTER,
	}
	// Test for successful execution
	qr := new(proto.QueryResult)
	err := RpcVTGate.ExecuteKeyRange(nil, &q, qr)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantqr := new(proto.QueryResult)
	proto.PopulateQueryResult(singleRowResult, wantqr)
	if !reflect.DeepEqual(wantqr, qr) {
		t.Errorf("want \n%+v, got \n%+v", singleRowResult, qr)
	}
	if qr.Session != nil {
		t.Errorf("want nil, got %+v\n", qr.Session)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("want 1, got %v\n", sbc1.ExecCount)
	}
	// Test for successful execution in transaction
	q.Session = new(proto.Session)
	RpcVTGate.Begin(nil, q.Session)
	if !q.Session.InTransaction {
		t.Errorf("want true, got false")
	}
	err = RpcVTGate.ExecuteKeyRange(nil, &q, qr)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantSession := &proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestVTGateExecuteKeyRange",
			Shard:         "-20",
			TransactionId: 1,
			TabletType:    topo.TYPE_MASTER,
		}},
	}
	if !reflect.DeepEqual(wantSession, q.Session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, q.Session)
	}
	RpcVTGate.Commit(nil, q.Session)
	if sbc1.CommitCount.Get() != 1 {
		t.Errorf("want 1, got %v", sbc1.CommitCount.Get())
	}
	// Test for multiple shards
	q.KeyRange = "10-30"
	RpcVTGate.ExecuteKeyRange(nil, &q, qr)
	if qr.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.RowsAffected)
	}
}

func TestVTGateExecuteBatchShard(t *testing.T) {
	s := createSandbox("TestVTGateExecuteBatchShard")
	s.MapTestConn("-20", &sandboxConn{})
	s.MapTestConn("20-40", &sandboxConn{})
	q := proto.BatchQueryShard{
		Queries: []tproto.BoundQuery{{
			"query",
			nil,
		}, {
			"query",
			nil,
		}},
		Keyspace: "TestVTGateExecuteBatchShard",
		Shards:   []string{"-20", "20-40"},
	}
	qrl := new(proto.QueryResultList)
	err := RpcVTGate.ExecuteBatchShard(nil, &q, qrl)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if len(qrl.List) != 2 {
		t.Errorf("want 2, got %v", len(qrl.List))
	}
	if qrl.List[0].RowsAffected != 2 {
		t.Errorf("want 2, got %v", qrl.List[0].RowsAffected)
	}
	if qrl.Session != nil {
		t.Errorf("want nil, got %+v\n", qrl.Session)
	}

	q.Session = new(proto.Session)
	RpcVTGate.Begin(nil, q.Session)
	err = RpcVTGate.ExecuteBatchShard(nil, &q, qrl)
	if len(q.Session.ShardSessions) != 2 {
		t.Errorf("want 2, got %d", len(q.Session.ShardSessions))
	}
}

func TestVTGateExecuteBatchKeyspaceIds(t *testing.T) {
	s := createSandbox("TestVTGateExecuteBatchKeyspaceIds")
	s.MapTestConn("-20", &sandboxConn{})
	s.MapTestConn("20-40", &sandboxConn{})
	q := proto.KeyspaceIdBatchQuery{
		Queries: []tproto.BoundQuery{{
			"query",
			nil,
		}, {
			"query",
			nil,
		}},
		Keyspace:    "TestVTGateExecuteBatchKeyspaceIds",
		KeyspaceIds: []string{"10", "30"},
		TabletType:  topo.TYPE_MASTER,
	}
	qrl := new(proto.QueryResultList)
	err := RpcVTGate.ExecuteBatchKeyspaceIds(nil, &q, qrl)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if len(qrl.List) != 2 {
		t.Errorf("want 2, got %v", len(qrl.List))
	}
	if qrl.List[0].RowsAffected != 2 {
		t.Errorf("want 2, got %v", qrl.List[0].RowsAffected)
	}
	if qrl.Session != nil {
		t.Errorf("want nil, got %+v\n", qrl.Session)
	}

	q.Session = new(proto.Session)
	RpcVTGate.Begin(nil, q.Session)
	err = RpcVTGate.ExecuteBatchKeyspaceIds(nil, &q, qrl)
	if len(q.Session.ShardSessions) != 2 {
		t.Errorf("want 2, got %d", len(q.Session.ShardSessions))
	}
}

func TestVTGateStreamExecuteKeyRange(t *testing.T) {
	s := createSandbox("TestVTGateStreamExecuteKeyRange")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)
	sq := proto.KeyRangeQuery{
		Sql:        "query",
		Keyspace:   "TestVTGateStreamExecuteKeyRange",
		KeyRange:   "-20",
		TabletType: topo.TYPE_MASTER,
	}
	// Test for successful execution
	var qrs []*proto.QueryResult
	err := RpcVTGate.StreamExecuteKeyRange(nil, &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	row := new(proto.QueryResult)
	proto.PopulateQueryResult(singleRowResult, row)
	want := []*proto.QueryResult{row}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}

	sq.Session = new(proto.Session)
	qrs = nil
	RpcVTGate.Begin(nil, sq.Session)
	err = RpcVTGate.StreamExecuteKeyRange(nil, &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	want = []*proto.QueryResult{
		row,
		&proto.QueryResult{
			Session: &proto.Session{
				InTransaction: true,
				ShardSessions: []*proto.ShardSession{{
					Keyspace:      "TestVTGateStreamExecuteKeyRange",
					Shard:         "-20",
					TransactionId: 1,
					TabletType:    topo.TYPE_MASTER,
				}},
			},
		},
	}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}

	// Test for error condition - multiple shards
	sq.KeyRange = "10-40"
	err = RpcVTGate.StreamExecuteKeyRange(nil, &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err == nil {
		t.Errorf("want not nil, got %v", err)
	}
	// Test for error condition - multiple shards, non-partial keyspace
	sq.KeyRange = ""
	err = RpcVTGate.StreamExecuteKeyRange(nil, &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err == nil {
		t.Errorf("want not nil, got %v", err)
	}
}

func TestVTGateStreamExecuteShard(t *testing.T) {
	s := createSandbox("TestVTGateStreamExecuteShard")
	sbc := &sandboxConn{}
	s.MapTestConn("0", sbc)
	q := proto.QueryShard{
		Sql:        "query",
		Keyspace:   "TestVTGateStreamExecuteShard",
		Shards:     []string{"0"},
		TabletType: topo.TYPE_MASTER,
	}
	// Test for successful execution
	var qrs []*proto.QueryResult
	err := RpcVTGate.StreamExecuteShard(nil, &q, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	row := new(proto.QueryResult)
	proto.PopulateQueryResult(singleRowResult, row)
	want := []*proto.QueryResult{row}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}

	q.Session = new(proto.Session)
	qrs = nil
	RpcVTGate.Begin(nil, q.Session)
	err = RpcVTGate.StreamExecuteShard(nil, &q, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	want = []*proto.QueryResult{
		row,
		&proto.QueryResult{
			Session: &proto.Session{
				InTransaction: true,
				ShardSessions: []*proto.ShardSession{{
					Keyspace:      "TestVTGateStreamExecuteShard",
					Shard:         "0",
					TransactionId: 1,
					TabletType:    topo.TYPE_MASTER,
				}},
			},
		},
	}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}

}
