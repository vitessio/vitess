// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/context"
	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

// This file uses the sandbox_test framework.

func init() {
	Init(new(sandboxTopo), "aa", 1*time.Second, 10, 1*time.Millisecond, 0)
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
	err := RpcVTGate.ExecuteShard(&context.DummyContext{}, &q, qr)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantqr := new(proto.QueryResult)
	wantqr.Result = singleRowResult
	if !reflect.DeepEqual(wantqr, qr) {
		t.Errorf("want \n%+v, got \n%+v", singleRowResult, qr)
	}
	if qr.Session != nil {
		t.Errorf("want nil, got %+v\n", qr.Session)
	}

	q.Session = new(proto.Session)
	RpcVTGate.Begin(&context.DummyContext{}, q.Session)
	if !q.Session.InTransaction {
		t.Errorf("want true, got false")
	}
	RpcVTGate.ExecuteShard(&context.DummyContext{}, &q, qr)
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

	RpcVTGate.Commit(&context.DummyContext{}, q.Session)
	if sbc.CommitCount != 1 {
		t.Errorf("want 1, got %d", sbc.CommitCount)
	}

	q.Session = new(proto.Session)
	RpcVTGate.Begin(&context.DummyContext{}, q.Session)
	RpcVTGate.ExecuteShard(&context.DummyContext{}, &q, qr)
	RpcVTGate.Rollback(&context.DummyContext{}, q.Session)
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
	kid10, err := key.HexKeyspaceId("10").Unhex()
	if err != nil {
		t.Errorf("want nil, got %+v", err)
	}
	q := proto.KeyspaceIdQuery{
		Sql:         "query",
		Keyspace:    "TestVTGateExecuteKeyspaceIds",
		KeyspaceIds: []key.KeyspaceId{kid10},
		TabletType:  topo.TYPE_MASTER,
	}
	// Test for successful execution
	qr := new(proto.QueryResult)
	err = RpcVTGate.ExecuteKeyspaceIds(&context.DummyContext{}, &q, qr)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantqr := new(proto.QueryResult)
	wantqr.Result = singleRowResult
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
	RpcVTGate.Begin(&context.DummyContext{}, q.Session)
	if !q.Session.InTransaction {
		t.Errorf("want true, got false")
	}
	RpcVTGate.ExecuteKeyspaceIds(&context.DummyContext{}, &q, qr)
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
	RpcVTGate.Commit(&context.DummyContext{}, q.Session)
	if sbc1.CommitCount.Get() != 1 {
		t.Errorf("want 1, got %d", sbc1.CommitCount.Get())
	}
	// Test for multiple shards
	kid30, err := key.HexKeyspaceId("30").Unhex()
	if err != nil {
		t.Errorf("want nil, got %+v", err)
	}
	q.KeyspaceIds = []key.KeyspaceId{kid10, kid30}
	RpcVTGate.ExecuteKeyspaceIds(&context.DummyContext{}, &q, qr)
	if qr.Result.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.Result.RowsAffected)
	}
}

func TestVTGateExecuteKeyRanges(t *testing.T) {
	s := createSandbox("TestVTGateExecuteKeyRanges")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("20-40", sbc2)
	kr, err := key.ParseKeyRangeParts("", "20")
	q := proto.KeyRangeQuery{
		Sql:        "query",
		Keyspace:   "TestVTGateExecuteKeyRanges",
		KeyRanges:  []key.KeyRange{kr},
		TabletType: topo.TYPE_MASTER,
	}
	// Test for successful execution
	qr := new(proto.QueryResult)
	err = RpcVTGate.ExecuteKeyRanges(&context.DummyContext{}, &q, qr)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantqr := new(proto.QueryResult)
	wantqr.Result = singleRowResult
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
	RpcVTGate.Begin(&context.DummyContext{}, q.Session)
	if !q.Session.InTransaction {
		t.Errorf("want true, got false")
	}
	err = RpcVTGate.ExecuteKeyRanges(&context.DummyContext{}, &q, qr)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantSession := &proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestVTGateExecuteKeyRanges",
			Shard:         "-20",
			TransactionId: 1,
			TabletType:    topo.TYPE_MASTER,
		}},
	}
	if !reflect.DeepEqual(wantSession, q.Session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, q.Session)
	}
	RpcVTGate.Commit(&context.DummyContext{}, q.Session)
	if sbc1.CommitCount.Get() != 1 {
		t.Errorf("want 1, got %v", sbc1.CommitCount.Get())
	}
	// Test for multiple shards
	kr, err = key.ParseKeyRangeParts("10", "30")
	q.KeyRanges = []key.KeyRange{kr}
	RpcVTGate.ExecuteKeyRanges(&context.DummyContext{}, &q, qr)
	if qr.Result.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.Result.RowsAffected)
	}
}

func TestVTGateExecuteEntityIds(t *testing.T) {
	s := createSandbox("TestVTGateExecuteEntityIds")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("20-40", sbc2)
	kid10, err := key.HexKeyspaceId("10").Unhex()
	if err != nil {
		t.Errorf("want nil, got %+v", err)
	}
	q := proto.EntityIdsQuery{
		Sql:              "query",
		Keyspace:         "TestVTGateExecuteEntityIds",
		EntityColumnName: "kid",
		EntityKeyspaceIDs: []proto.EntityId{
			proto.EntityId{
				ExternalID: "id1",
				KeyspaceID: kid10,
			},
		},
		TabletType: topo.TYPE_MASTER,
	}
	// Test for successful execution
	qr := new(proto.QueryResult)
	err = RpcVTGate.ExecuteEntityIds(&context.DummyContext{}, &q, qr)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantqr := new(proto.QueryResult)
	wantqr.Result = singleRowResult
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
	RpcVTGate.Begin(&context.DummyContext{}, q.Session)
	if !q.Session.InTransaction {
		t.Errorf("want true, got false")
	}
	RpcVTGate.ExecuteEntityIds(&context.DummyContext{}, &q, qr)
	wantSession := &proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestVTGateExecuteEntityIds",
			Shard:         "-20",
			TransactionId: 1,
			TabletType:    topo.TYPE_MASTER,
		}},
	}
	if !reflect.DeepEqual(wantSession, q.Session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, q.Session)
	}
	RpcVTGate.Commit(&context.DummyContext{}, q.Session)
	if sbc1.CommitCount.Get() != 1 {
		t.Errorf("want 1, got %d", sbc1.CommitCount.Get())
	}
	// Test for multiple shards
	kid30, err := key.HexKeyspaceId("30").Unhex()
	if err != nil {
		t.Errorf("want nil, got %+v", err)
	}
	q.EntityKeyspaceIDs = append(q.EntityKeyspaceIDs, proto.EntityId{ExternalID: "id2", KeyspaceID: kid30})
	RpcVTGate.ExecuteEntityIds(&context.DummyContext{}, &q, qr)
	if qr.Result.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.Result.RowsAffected)
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
	err := RpcVTGate.ExecuteBatchShard(&context.DummyContext{}, &q, qrl)
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
	RpcVTGate.Begin(&context.DummyContext{}, q.Session)
	RpcVTGate.ExecuteBatchShard(&context.DummyContext{}, &q, qrl)
	if len(q.Session.ShardSessions) != 2 {
		t.Errorf("want 2, got %d", len(q.Session.ShardSessions))
	}
}

func TestVTGateExecuteBatchKeyspaceIds(t *testing.T) {
	s := createSandbox("TestVTGateExecuteBatchKeyspaceIds")
	s.MapTestConn("-20", &sandboxConn{})
	s.MapTestConn("20-40", &sandboxConn{})
	kid10, err := key.HexKeyspaceId("10").Unhex()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	kid30, err := key.HexKeyspaceId("30").Unhex()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	q := proto.KeyspaceIdBatchQuery{
		Queries: []tproto.BoundQuery{{
			"query",
			nil,
		}, {
			"query",
			nil,
		}},
		Keyspace:    "TestVTGateExecuteBatchKeyspaceIds",
		KeyspaceIds: []key.KeyspaceId{kid10, kid30},
		TabletType:  topo.TYPE_MASTER,
	}
	qrl := new(proto.QueryResultList)
	err = RpcVTGate.ExecuteBatchKeyspaceIds(&context.DummyContext{}, &q, qrl)
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
	RpcVTGate.Begin(&context.DummyContext{}, q.Session)
	RpcVTGate.ExecuteBatchKeyspaceIds(&context.DummyContext{}, &q, qrl)
	if len(q.Session.ShardSessions) != 2 {
		t.Errorf("want 2, got %d", len(q.Session.ShardSessions))
	}
}

func TestVTGateStreamExecuteKeyspaceIds(t *testing.T) {
	s := createSandbox("TestVTGateStreamExecuteKeyspaceIds")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)
	sbc1 := &sandboxConn{}
	s.MapTestConn("20-40", sbc1)
	kid10, err := key.HexKeyspaceId("10").Unhex()
	if err != nil {
		t.Errorf("want nil, got %+v", err)
	}
	sq := proto.KeyspaceIdQuery{
		Sql:         "query",
		Keyspace:    "TestVTGateStreamExecuteKeyspaceIds",
		KeyspaceIds: []key.KeyspaceId{kid10},
		TabletType:  topo.TYPE_MASTER,
	}
	// Test for successful execution
	var qrs []*proto.QueryResult
	err = RpcVTGate.StreamExecuteKeyspaceIds(&context.DummyContext{}, &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	row := new(proto.QueryResult)
	row.Result = singleRowResult
	want := []*proto.QueryResult{row}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}

	// Test for successful execution in transaction
	sq.Session = new(proto.Session)
	qrs = nil
	RpcVTGate.Begin(&context.DummyContext{}, sq.Session)
	err = RpcVTGate.StreamExecuteKeyspaceIds(&context.DummyContext{}, &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	want = []*proto.QueryResult{
		row,
		&proto.QueryResult{
			Session: &proto.Session{
				InTransaction: true,
				ShardSessions: []*proto.ShardSession{{
					Keyspace:      "TestVTGateStreamExecuteKeyspaceIds",
					Shard:         "-20",
					TransactionId: 1,
					TabletType:    topo.TYPE_MASTER,
				}},
			},
		},
	}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want\n%#v\ngot\n%#v", want, qrs)
	}
	RpcVTGate.Commit(&context.DummyContext{}, sq.Session)
	if sbc.CommitCount.Get() != 1 {
		t.Errorf("want 1, got %d", sbc.CommitCount.Get())
	}
	// Test for successful execution - multiple keyspaceids in single shard
	sq.Session = nil
	qrs = nil
	kid15, err := key.HexKeyspaceId("15").Unhex()
	if err != nil {
		t.Errorf("want nil, got %+v", err)
	}
	sq.KeyspaceIds = []key.KeyspaceId{kid10, kid15}
	err = RpcVTGate.StreamExecuteKeyspaceIds(&context.DummyContext{}, &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	row = new(proto.QueryResult)
	row.Result = singleRowResult
	want = []*proto.QueryResult{row}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}
	// Test for successful execution - multiple keyspaceids in multiple shards
	kid30, err := key.HexKeyspaceId("30").Unhex()
	if err != nil {
		t.Errorf("want nil, got %+v", err)
	}
	sq.KeyspaceIds = []key.KeyspaceId{kid10, kid30}
	err = RpcVTGate.StreamExecuteKeyspaceIds(&context.DummyContext{}, &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
}

func TestVTGateStreamExecuteKeyRanges(t *testing.T) {
	s := createSandbox("TestVTGateStreamExecuteKeyRanges")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)
	sbc1 := &sandboxConn{}
	s.MapTestConn("20-40", sbc1)
	kr, err := key.ParseKeyRangeParts("", "20")
	sq := proto.KeyRangeQuery{
		Sql:        "query",
		Keyspace:   "TestVTGateStreamExecuteKeyRanges",
		KeyRanges:  []key.KeyRange{kr},
		TabletType: topo.TYPE_MASTER,
	}
	// Test for successful execution
	var qrs []*proto.QueryResult
	err = RpcVTGate.StreamExecuteKeyRanges(&context.DummyContext{}, &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	row := new(proto.QueryResult)
	row.Result = singleRowResult
	want := []*proto.QueryResult{row}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}

	sq.Session = new(proto.Session)
	qrs = nil
	RpcVTGate.Begin(&context.DummyContext{}, sq.Session)
	err = RpcVTGate.StreamExecuteKeyRanges(&context.DummyContext{}, &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	want = []*proto.QueryResult{
		row,
		&proto.QueryResult{
			Session: &proto.Session{
				InTransaction: true,
				ShardSessions: []*proto.ShardSession{{
					Keyspace:      "TestVTGateStreamExecuteKeyRanges",
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

	// Test for successful execution - multiple shards
	kr, err = key.ParseKeyRangeParts("10", "40")
	sq.KeyRanges = []key.KeyRange{kr}
	err = RpcVTGate.StreamExecuteKeyRanges(&context.DummyContext{}, &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
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
	err := RpcVTGate.StreamExecuteShard(&context.DummyContext{}, &q, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	row := new(proto.QueryResult)
	row.Result = singleRowResult
	want := []*proto.QueryResult{row}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}

	q.Session = new(proto.Session)
	qrs = nil
	RpcVTGate.Begin(&context.DummyContext{}, q.Session)
	err = RpcVTGate.StreamExecuteShard(&context.DummyContext{}, &q, func(r *proto.QueryResult) error {
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
