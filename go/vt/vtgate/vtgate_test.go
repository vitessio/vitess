// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
	pbg "github.com/youtube/vitess/go/vt/proto/vtgate"
)

// This file uses the sandbox_test framework.

func init() {
	schema := createTestSchema(`
{
  "Keyspaces": {
    "TestUnsharded": {
      "Sharded": false,
      "Tables": {
        "t1": ""
      }
    }
  }
}
`)
	Init(nil, topo.Server{}, new(sandboxTopo), schema, "aa", 1*time.Second, 10, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour, 0, "")
}

func TestVTGateExecute(t *testing.T) {
	sandbox := createSandbox(KsTestUnsharded)
	sbc := &sandboxConn{}
	sandbox.MapTestConn("0", sbc)
	qr := new(proto.QueryResult)
	err := rpcVTGate.Execute(context.Background(),
		"select * from t1",
		nil,
		pb.TabletType_MASTER,
		nil,
		false,
		qr)
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

	session := new(proto.Session)
	rpcVTGate.Begin(context.Background(), session)
	if !session.InTransaction {
		t.Errorf("want true, got false")
	}
	rpcVTGate.Execute(context.Background(),
		"select * from t1",
		nil,
		pb.TabletType_MASTER,
		session,
		false,
		qr)
	wantSession := &proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      KsTestUnsharded,
			Shard:         "0",
			TabletType:    topo.TYPE_MASTER,
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(wantSession, session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, session)
	}

	rpcVTGate.Commit(context.Background(), session)
	if commitCount := sbc.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	session = new(proto.Session)
	rpcVTGate.Begin(context.Background(), session)
	rpcVTGate.Execute(context.Background(),
		"select * from t1",
		nil,
		pb.TabletType_MASTER,
		session,
		false,
		qr)
	rpcVTGate.Rollback(context.Background(), session)
}

func TestVTGateExecuteShards(t *testing.T) {
	sandbox := createSandbox("TestVTGateExecuteShards")
	sbc := &sandboxConn{}
	sandbox.MapTestConn("0", sbc)
	qr := new(proto.QueryResult)
	err := rpcVTGate.ExecuteShards(context.Background(),
		"query",
		nil,
		"TestVTGateExecuteShards",
		[]string{"0"},
		pb.TabletType_REPLICA,
		nil,
		false,
		qr)
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

	session := new(proto.Session)
	rpcVTGate.Begin(context.Background(), session)
	if !session.InTransaction {
		t.Errorf("want true, got false")
	}
	rpcVTGate.ExecuteShards(context.Background(),
		"query",
		nil,
		"TestVTGateExecuteShards",
		[]string{"0"},
		pb.TabletType_REPLICA,
		session,
		false,
		qr)
	wantSession := &proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestVTGateExecuteShards",
			Shard:         "0",
			TabletType:    topo.TYPE_REPLICA,
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(wantSession, session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, session)
	}

	rpcVTGate.Commit(context.Background(), session)
	if commitCount := sbc.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	session = new(proto.Session)
	rpcVTGate.Begin(context.Background(), session)
	rpcVTGate.ExecuteShards(context.Background(),
		"query",
		nil,
		"TestVTGateExecuteShards",
		[]string{"0"},
		pb.TabletType_REPLICA,
		session,
		false,
		qr)
	rpcVTGate.Rollback(context.Background(), session)
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
	// Test for successful execution
	qr := new(proto.QueryResult)
	err := rpcVTGate.ExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		"TestVTGateExecuteKeyspaceIds",
		[][]byte{[]byte{0x10}},
		pb.TabletType_MASTER,
		nil,
		false,
		qr)
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
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v\n", execCount)
	}
	// Test for successful execution in transaction
	session := new(proto.Session)
	rpcVTGate.Begin(context.Background(), session)
	if !session.InTransaction {
		t.Errorf("want true, got false")
	}
	rpcVTGate.ExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		"TestVTGateExecuteKeyspaceIds",
		[][]byte{[]byte{0x10}},
		pb.TabletType_MASTER,
		session,
		false,
		qr)
	wantSession := &proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestVTGateExecuteKeyspaceIds",
			Shard:         "-20",
			TransactionId: 1,
			TabletType:    topo.TYPE_MASTER,
		}},
	}
	if !reflect.DeepEqual(wantSession, session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, session)
	}
	rpcVTGate.Commit(context.Background(), session)
	if commitCount := sbc1.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
	// Test for multiple shards
	rpcVTGate.ExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		"TestVTGateExecuteKeyspaceIds",
		[][]byte{[]byte{0x10}, []byte{0x30}},
		pb.TabletType_MASTER,
		session,
		false,
		qr)
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
	// Test for successful execution
	qr := new(proto.QueryResult)
	err := rpcVTGate.ExecuteKeyRanges(context.Background(),
		"query",
		nil,
		"TestVTGateExecuteKeyRanges",
		[]*pb.KeyRange{&pb.KeyRange{End: []byte{0x20}}},
		pb.TabletType_MASTER,
		nil,
		false,
		qr)
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
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v\n", execCount)
	}
	// Test for successful execution in transaction
	session := new(proto.Session)
	rpcVTGate.Begin(context.Background(), session)
	if !session.InTransaction {
		t.Errorf("want true, got false")
	}
	err = rpcVTGate.ExecuteKeyRanges(context.Background(),
		"query",
		nil,
		"TestVTGateExecuteKeyRanges",
		[]*pb.KeyRange{&pb.KeyRange{End: []byte{0x20}}},
		pb.TabletType_MASTER,
		session,
		false,
		qr)
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
	if !reflect.DeepEqual(wantSession, session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, session)
	}
	rpcVTGate.Commit(context.Background(), session)
	if commitCount := sbc1.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %v", commitCount)
	}
	// Test for multiple shards
	rpcVTGate.ExecuteKeyRanges(context.Background(), "query",
		nil,
		"TestVTGateExecuteKeyRanges",
		[]*pb.KeyRange{&pb.KeyRange{Start: []byte{0x10}, End: []byte{0x30}}},
		pb.TabletType_MASTER,
		nil,
		false, qr)
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
	// Test for successful execution
	qr := new(proto.QueryResult)
	err := rpcVTGate.ExecuteEntityIds(context.Background(),
		"query",
		nil,
		"TestVTGateExecuteEntityIds",
		"kid",
		[]*pbg.ExecuteEntityIdsRequest_EntityId{
			&pbg.ExecuteEntityIdsRequest_EntityId{
				XidType:    pbg.ExecuteEntityIdsRequest_EntityId_TYPE_BYTES,
				XidBytes:   []byte("id1"),
				KeyspaceId: []byte{0x10},
			},
		},
		pb.TabletType_MASTER,
		nil,
		false,
		qr)
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
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v\n", execCount)
	}
	// Test for successful execution in transaction
	session := new(proto.Session)
	rpcVTGate.Begin(context.Background(), session)
	if !session.InTransaction {
		t.Errorf("want true, got false")
	}
	rpcVTGate.ExecuteEntityIds(context.Background(),
		"query",
		nil,
		"TestVTGateExecuteEntityIds",
		"kid",
		[]*pbg.ExecuteEntityIdsRequest_EntityId{
			&pbg.ExecuteEntityIdsRequest_EntityId{
				XidType:    pbg.ExecuteEntityIdsRequest_EntityId_TYPE_BYTES,
				XidBytes:   []byte("id1"),
				KeyspaceId: []byte{0x10},
			},
		},
		pb.TabletType_MASTER,
		session,
		false,
		qr)
	wantSession := &proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestVTGateExecuteEntityIds",
			Shard:         "-20",
			TransactionId: 1,
			TabletType:    topo.TYPE_MASTER,
		}},
	}
	if !reflect.DeepEqual(wantSession, session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, session)
	}
	rpcVTGate.Commit(context.Background(), session)
	if commitCount := sbc1.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	// Test for multiple shards
	rpcVTGate.ExecuteEntityIds(context.Background(), "query",
		nil,
		"TestVTGateExecuteEntityIds",
		"kid",
		[]*pbg.ExecuteEntityIdsRequest_EntityId{
			&pbg.ExecuteEntityIdsRequest_EntityId{
				XidType:    pbg.ExecuteEntityIdsRequest_EntityId_TYPE_BYTES,
				XidBytes:   []byte("id1"),
				KeyspaceId: []byte{0x10},
			},
			&pbg.ExecuteEntityIdsRequest_EntityId{
				XidType:    pbg.ExecuteEntityIdsRequest_EntityId_TYPE_BYTES,
				XidBytes:   []byte("id2"),
				KeyspaceId: []byte{0x30},
			},
		},
		pb.TabletType_MASTER,
		nil,
		false,
		qr)
	if qr.Result.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.Result.RowsAffected)
	}
}

func TestVTGateExecuteBatchShards(t *testing.T) {
	s := createSandbox("TestVTGateExecuteBatchShards")
	s.MapTestConn("-20", &sandboxConn{})
	s.MapTestConn("20-40", &sandboxConn{})
	qrl := new(proto.QueryResultList)
	err := rpcVTGate.ExecuteBatchShards(context.Background(),
		[]proto.BoundShardQuery{{
			Sql:           "query",
			BindVariables: nil,
			Keyspace:      "TestVTGateExecuteBatchShards",
			Shards:        []string{"-20", "20-40"},
		}, {
			Sql:           "query",
			BindVariables: nil,
			Keyspace:      "TestVTGateExecuteBatchShards",
			Shards:        []string{"-20", "20-40"},
		}},
		pb.TabletType_MASTER,
		false,
		nil,
		qrl)
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

	session := new(proto.Session)
	rpcVTGate.Begin(context.Background(), session)
	rpcVTGate.ExecuteBatchShards(context.Background(),
		[]proto.BoundShardQuery{{
			Sql:           "query",
			BindVariables: nil,
			Keyspace:      "TestVTGateExecuteBatchShards",
			Shards:        []string{"-20", "20-40"},
		}, {
			Sql:           "query",
			BindVariables: nil,
			Keyspace:      "TestVTGateExecuteBatchShards",
			Shards:        []string{"-20", "20-40"},
		}},
		pb.TabletType_MASTER,
		false,
		session,
		qrl)
	if len(session.ShardSessions) != 2 {
		t.Errorf("want 2, got %d", len(session.ShardSessions))
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
	qrl := new(proto.QueryResultList)
	err = rpcVTGate.ExecuteBatchKeyspaceIds(context.Background(),
		[]proto.BoundKeyspaceIdQuery{{
			Sql:           "query",
			BindVariables: nil,
			Keyspace:      "TestVTGateExecuteBatchKeyspaceIds",
			KeyspaceIds:   []key.KeyspaceId{kid10, kid30},
		}, {
			Sql:           "query",
			BindVariables: nil,
			Keyspace:      "TestVTGateExecuteBatchKeyspaceIds",
			KeyspaceIds:   []key.KeyspaceId{kid10, kid30},
		}},
		pb.TabletType_MASTER,
		false,
		nil,
		qrl)
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

	session := new(proto.Session)
	rpcVTGate.Begin(context.Background(), session)
	rpcVTGate.ExecuteBatchKeyspaceIds(context.Background(),
		[]proto.BoundKeyspaceIdQuery{{
			Sql:           "query",
			BindVariables: nil,
			Keyspace:      "TestVTGateExecuteBatchKeyspaceIds",
			KeyspaceIds:   []key.KeyspaceId{kid10, kid30},
		}, {
			Sql:           "query",
			BindVariables: nil,
			Keyspace:      "TestVTGateExecuteBatchKeyspaceIds",
			KeyspaceIds:   []key.KeyspaceId{kid10, kid30},
		}},
		pb.TabletType_MASTER,
		false,
		session,
		qrl)
	if len(session.ShardSessions) != 2 {
		t.Errorf("want 2, got %d", len(session.ShardSessions))
	}
}

func TestVTGateStreamExecute(t *testing.T) {
	sandbox := createSandbox(KsTestUnsharded)
	sbc := &sandboxConn{}
	sandbox.MapTestConn("0", sbc)
	var qrs []*proto.QueryResult
	err := rpcVTGate.StreamExecute(context.Background(),
		"select * from t1",
		nil,
		pb.TabletType_MASTER,
		func(r *proto.QueryResult) error {
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
}

func TestVTGateStreamExecuteKeyspaceIds(t *testing.T) {
	s := createSandbox("TestVTGateStreamExecuteKeyspaceIds")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)
	sbc1 := &sandboxConn{}
	s.MapTestConn("20-40", sbc1)
	// Test for successful execution
	var qrs []*proto.QueryResult
	err := rpcVTGate.StreamExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		"TestVTGateStreamExecuteKeyspaceIds",
		[][]byte{[]byte{0x10}},
		pb.TabletType_MASTER,
		func(r *proto.QueryResult) error {
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

	// Test for successful execution - multiple keyspaceids in single shard
	qrs = nil
	err = rpcVTGate.StreamExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		"TestVTGateStreamExecuteKeyspaceIds",
		[][]byte{[]byte{0x10}, []byte{0x15}},
		pb.TabletType_MASTER,
		func(r *proto.QueryResult) error {
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
	err = rpcVTGate.StreamExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		"TestVTGateStreamExecuteKeyspaceIds",
		[][]byte{[]byte{0x10}, []byte{0x30}},
		pb.TabletType_MASTER,
		func(r *proto.QueryResult) error {
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
	// Test for successful execution
	var qrs []*proto.QueryResult
	err := rpcVTGate.StreamExecuteKeyRanges(context.Background(),
		"query",
		nil,
		"TestVTGateStreamExecuteKeyRanges",
		[]*pb.KeyRange{&pb.KeyRange{End: []byte{0x20}}},
		pb.TabletType_MASTER,
		func(r *proto.QueryResult) error {
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

	// Test for successful execution - multiple shards
	err = rpcVTGate.StreamExecuteKeyRanges(context.Background(),
		"query",
		nil,
		"TestVTGateStreamExecuteKeyRanges",
		[]*pb.KeyRange{&pb.KeyRange{Start: []byte{0x10}, End: []byte{0x40}}},
		pb.TabletType_MASTER,
		func(r *proto.QueryResult) error {
			qrs = append(qrs, r)
			return nil
		})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
}

func TestVTGateStreamExecuteShard(t *testing.T) {
	s := createSandbox("TestVTGateStreamExecuteShards")
	sbc := &sandboxConn{}
	s.MapTestConn("0", sbc)
	// Test for successful execution
	var qrs []*proto.QueryResult
	err := rpcVTGate.StreamExecuteShards(context.Background(),
		"query",
		nil,
		"TestVTGateStreamExecuteShards",
		[]string{"0"},
		pb.TabletType_MASTER,
		func(r *proto.QueryResult) error {
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
}

func TestVTGateSplitQuery(t *testing.T) {
	keyspace := "TestVTGateSplitQuery"
	keyranges, _ := key.ParseShardingSpec(DefaultShardSpec)
	s := createSandbox(keyspace)
	for _, kr := range keyranges {
		s.MapTestConn(key.KeyRangeString(kr), &sandboxConn{})
	}
	sql := "select col1, col2 from table"
	splitCount := 24
	splits, err := rpcVTGate.SplitQuery(context.Background(),
		keyspace,
		sql,
		nil,
		"",
		splitCount)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	_, err = getAllShards(DefaultShardSpec)
	// Total number of splits should be number of shards * splitsPerShard
	if splitCount != len(splits) {
		t.Errorf("wrong number of splits, want \n%+v, got \n%+v", splitCount, len(splits))
	}
	actualSqlsByKeyRange := map[string][]string{}
	for _, split := range splits {
		if split.Size != sandboxSQRowCount {
			t.Errorf("wrong split size, want \n%+v, got \n%+v", sandboxSQRowCount, split.Size)
		}
		if split.KeyRangePart.Keyspace != keyspace {
			t.Errorf("wrong split size, want \n%+v, got \n%+v", keyspace, split.KeyRangePart.Keyspace)
		}
		if len(split.KeyRangePart.KeyRanges) != 1 {
			t.Errorf("wrong number of keyranges, want \n%+v, got \n%+v", 1, len(split.KeyRangePart.KeyRanges))
		}
		kr := key.KeyRangeString(split.KeyRangePart.KeyRanges[0])
		actualSqlsByKeyRange[kr] = append(actualSqlsByKeyRange[kr], split.Query.Sql)
	}
	expectedSqlsByKeyRange := map[string][]string{}
	for _, kr := range keyranges {
		expectedSqlsByKeyRange[key.KeyRangeString(kr)] = []string{
			"select col1, col2 from table /*split 0 */",
			"select col1, col2 from table /*split 1 */",
			"select col1, col2 from table /*split 2 */",
		}
	}
	if !reflect.DeepEqual(actualSqlsByKeyRange, expectedSqlsByKeyRange) {
		t.Errorf("splits contain the wrong sqls and/or keyranges, got: %v, want: %v", actualSqlsByKeyRange, expectedSqlsByKeyRange)
	}
}

func TestIsErrorCausedByVTGate(t *testing.T) {
	unknownError := fmt.Errorf("unknown error")
	serverError := &tabletconn.ServerError{
		Code: tabletconn.ERR_RETRY,
		Err:  "vttablet: retry: error message",
	}
	shardConnUnknownErr := &ShardConnError{Err: unknownError}
	shardConnServerErr := &ShardConnError{Err: serverError}
	shardConnCancelledErr := &ShardConnError{Err: tabletconn.Cancelled}

	scatterConnErrAllUnknownErrs := &ScatterConnError{
		Errs: []error{unknownError, unknownError, unknownError},
	}
	scatterConnErrMixed := &ScatterConnError{
		Errs: []error{unknownError, shardConnServerErr, shardConnCancelledErr},
	}
	scatterConnErrAllNonVTGateErrs := &ScatterConnError{
		Errs: []error{shardConnServerErr, shardConnServerErr, shardConnCancelledErr},
	}

	inputToWant := map[error]bool{
		unknownError:         true,
		serverError:          false,
		tabletconn.Cancelled: false,
		// OperationalErrors that are not tabletconn.Cancelled might be from VTGate
		tabletconn.ConnClosed: true,
		// Errors wrapped in ShardConnError should get unwrapped
		shardConnUnknownErr:   true,
		shardConnServerErr:    false,
		shardConnCancelledErr: false,
		// We consider a ScatterConnErr with all unknown errors to be from VTGate
		scatterConnErrAllUnknownErrs: true,
		// We consider a ScatterConnErr with a mix of errors to be from VTGate
		scatterConnErrMixed: true,
		// If every error in ScatterConnErr list is caused by external components, we shouldn't
		// consider the error to be from VTGate
		scatterConnErrAllNonVTGateErrs: false,
	}

	for input, want := range inputToWant {
		got := isErrorCausedByVTGate(input)
		if got != want {
			t.Errorf("isErrorCausedByVTGate(%v) => %v, want %v",
				input, got, want)
		}
	}
}
