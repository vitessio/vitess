// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// This file uses the sandbox_test framework.

var hcVTGateTest *fakeHealthCheck

func init() {
	getSandbox(KsTestUnsharded).VSchema = `
{
	"Sharded": false,
	"Tables": {
		"t1": {}
	}
}
`
	hcVTGateTest = newFakeHealthCheck()
	Init(context.Background(), hcVTGateTest, topo.Server{}, new(sandboxTopo), "aa", 10, nil)
}

func TestVTGateExecute(t *testing.T) {
	createSandbox(KsTestUnsharded)
	sbc := &sandboxConn{}
	hcVTGateTest.Reset()
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil, sbc)
	qr, err := rpcVTGate.Execute(context.Background(),
		"select id from t1",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		nil,
		false)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if !reflect.DeepEqual(singleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", singleRowResult, qr)
	}

	session, err := rpcVTGate.Begin(context.Background())
	if !session.InTransaction {
		t.Errorf("want true, got false")
	}
	rpcVTGate.Execute(context.Background(),
		"select id from t1",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		session,
		false)
	wantSession := &vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   KsTestUnsharded,
				Shard:      "0",
				TabletType: topodatapb.TabletType_MASTER,
			},
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

	session, err = rpcVTGate.Begin(context.Background())
	rpcVTGate.Execute(context.Background(),
		"select id from t1",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		session,
		false)
	rpcVTGate.Rollback(context.Background(), session)
}

func TestVTGateExecuteWithKeyspace(t *testing.T) {
	createSandbox(KsTestUnsharded)
	sbc := &sandboxConn{}
	hcVTGateTest.Reset()
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil, sbc)
	qr, err := rpcVTGate.Execute(context.Background(),
		"select id from none",
		nil,
		KsTestUnsharded,
		topodatapb.TabletType_MASTER,
		nil,
		false)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if !reflect.DeepEqual(singleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", singleRowResult, qr)
	}
	_, err = rpcVTGate.Execute(context.Background(),
		"select id from none",
		nil,
		"aa",
		topodatapb.TabletType_MASTER,
		nil,
		false)
	want := "keyspace aa not found in vschema"
	if err == nil || err.Error() != want {
		t.Errorf("Execute: %v, want %s", err, want)
	}
}

func TestVTGateExecuteShards(t *testing.T) {
	ks := "TestVTGateExecuteShards"
	shard := "0"
	createSandbox(ks)
	sbc := &sandboxConn{}
	hcVTGateTest.Reset()
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1001, ks, shard, topodatapb.TabletType_REPLICA, true, 1, nil, sbc)
	qr, err := rpcVTGate.ExecuteShards(context.Background(),
		"query",
		nil,
		ks,
		[]string{shard},
		topodatapb.TabletType_REPLICA,
		nil,
		false)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if !reflect.DeepEqual(singleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", singleRowResult, qr)
	}

	session, err := rpcVTGate.Begin(context.Background())
	if !session.InTransaction {
		t.Errorf("want true, got false")
	}
	rpcVTGate.ExecuteShards(context.Background(),
		"query",
		nil,
		ks,
		[]string{shard},
		topodatapb.TabletType_REPLICA,
		session,
		false)
	wantSession := &vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   ks,
				Shard:      shard,
				TabletType: topodatapb.TabletType_REPLICA,
			},
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

	session, err = rpcVTGate.Begin(context.Background())
	rpcVTGate.ExecuteShards(context.Background(),
		"query",
		nil,
		ks,
		[]string{shard},
		topodatapb.TabletType_REPLICA,
		session,
		false)
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
	ks := "TestVTGateExecuteKeyspaceIds"
	shard1 := "-20"
	shard2 := "20-40"
	createSandbox(ks)
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	hcVTGateTest.Reset()
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1001, ks, shard1, topodatapb.TabletType_MASTER, true, 1, nil, sbc1)
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1002, ks, shard2, topodatapb.TabletType_MASTER, true, 1, nil, sbc2)
	// Test for successful execution
	qr, err := rpcVTGate.ExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		ks,
		[][]byte{{0x10}},
		topodatapb.TabletType_MASTER,
		nil,
		false)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if !reflect.DeepEqual(singleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", singleRowResult, qr)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v\n", execCount)
	}
	// Test for successful execution in transaction
	session, err := rpcVTGate.Begin(context.Background())
	if !session.InTransaction {
		t.Errorf("want true, got false")
	}
	rpcVTGate.ExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		ks,
		[][]byte{{0x10}},
		topodatapb.TabletType_MASTER,
		session,
		false)
	wantSession := &vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   ks,
				Shard:      shard1,
				TabletType: topodatapb.TabletType_MASTER,
			},
			TransactionId: 1,
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
	qr, err = rpcVTGate.ExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		ks,
		[][]byte{{0x10}, {0x30}},
		topodatapb.TabletType_MASTER,
		session,
		false)
	if qr.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.RowsAffected)
	}
}

func TestVTGateExecuteKeyRanges(t *testing.T) {
	ks := "TestVTGateExecuteKeyRanges"
	shard1 := "-20"
	shard2 := "20-40"
	createSandbox(ks)
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	hcVTGateTest.Reset()
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1001, ks, shard1, topodatapb.TabletType_MASTER, true, 1, nil, sbc1)
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1002, ks, shard2, topodatapb.TabletType_MASTER, true, 1, nil, sbc2)
	// Test for successful execution
	qr, err := rpcVTGate.ExecuteKeyRanges(context.Background(),
		"query",
		nil,
		ks,
		[]*topodatapb.KeyRange{{End: []byte{0x20}}},
		topodatapb.TabletType_MASTER,
		nil,
		false)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if !reflect.DeepEqual(singleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", singleRowResult, qr)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v\n", execCount)
	}
	// Test for successful execution in transaction
	session, err := rpcVTGate.Begin(context.Background())
	if !session.InTransaction {
		t.Errorf("want true, got false")
	}
	qr, err = rpcVTGate.ExecuteKeyRanges(context.Background(),
		"query",
		nil,
		ks,
		[]*topodatapb.KeyRange{{End: []byte{0x20}}},
		topodatapb.TabletType_MASTER,
		session,
		false)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantSession := &vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   ks,
				Shard:      shard1,
				TabletType: topodatapb.TabletType_MASTER,
			},
			TransactionId: 1,
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
	qr, err = rpcVTGate.ExecuteKeyRanges(context.Background(), "query",
		nil,
		ks,
		[]*topodatapb.KeyRange{{Start: []byte{0x10}, End: []byte{0x30}}},
		topodatapb.TabletType_MASTER,
		nil,
		false)
	if qr.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.RowsAffected)
	}
}

func TestVTGateExecuteEntityIds(t *testing.T) {
	ks := "TestVTGateExecuteEntityIds"
	shard1 := "-20"
	shard2 := "20-40"
	createSandbox(ks)
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	hcVTGateTest.Reset()
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1001, ks, shard1, topodatapb.TabletType_MASTER, true, 1, nil, sbc1)
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1002, ks, shard2, topodatapb.TabletType_MASTER, true, 1, nil, sbc2)
	// Test for successful execution
	qr, err := rpcVTGate.ExecuteEntityIds(context.Background(),
		"query",
		nil,
		ks,
		"kid",
		[]*vtgatepb.ExecuteEntityIdsRequest_EntityId{
			{
				Type:       sqltypes.VarBinary,
				Value:      []byte("id1"),
				KeyspaceId: []byte{0x10},
			},
		},
		topodatapb.TabletType_MASTER,
		nil,
		false)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if !reflect.DeepEqual(singleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", singleRowResult, qr)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v\n", execCount)
	}
	// Test for successful execution in transaction
	session, err := rpcVTGate.Begin(context.Background())
	if !session.InTransaction {
		t.Errorf("want true, got false")
	}
	rpcVTGate.ExecuteEntityIds(context.Background(),
		"query",
		nil,
		ks,
		"kid",
		[]*vtgatepb.ExecuteEntityIdsRequest_EntityId{
			{
				Type:       sqltypes.VarBinary,
				Value:      []byte("id1"),
				KeyspaceId: []byte{0x10},
			},
		},
		topodatapb.TabletType_MASTER,
		session,
		false)
	wantSession := &vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   ks,
				Shard:      shard1,
				TabletType: topodatapb.TabletType_MASTER,
			},
			TransactionId: 1,
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
	qr, err = rpcVTGate.ExecuteEntityIds(context.Background(), "query",
		nil,
		ks,
		"kid",
		[]*vtgatepb.ExecuteEntityIdsRequest_EntityId{
			{
				Type:       sqltypes.VarBinary,
				Value:      []byte("id1"),
				KeyspaceId: []byte{0x10},
			},
			{
				Type:       sqltypes.VarBinary,
				Value:      []byte("id2"),
				KeyspaceId: []byte{0x30},
			},
		},
		topodatapb.TabletType_MASTER,
		nil,
		false)
	if qr.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.RowsAffected)
	}
}

func TestVTGateExecuteBatchShards(t *testing.T) {
	ks := "TestVTGateExecuteBatchShards"
	createSandbox(ks)
	shard1 := "-20"
	shard2 := "20-40"
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	hcVTGateTest.Reset()
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1001, ks, shard1, topodatapb.TabletType_MASTER, true, 1, nil, sbc1)
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1002, ks, shard2, topodatapb.TabletType_MASTER, true, 1, nil, sbc2)
	qrl, err := rpcVTGate.ExecuteBatchShards(context.Background(),
		[]*vtgatepb.BoundShardQuery{{
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace: ks,
			Shards:   []string{shard1, shard2},
		}, {
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace: ks,
			Shards:   []string{shard1, shard2},
		}},
		topodatapb.TabletType_MASTER,
		false,
		nil)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if len(qrl) != 2 {
		t.Errorf("want 2, got %v", len(qrl))
	}
	if qrl[0].RowsAffected != 2 {
		t.Errorf("want 2, got %v", qrl[0].RowsAffected)
	}

	session, err := rpcVTGate.Begin(context.Background())
	rpcVTGate.ExecuteBatchShards(context.Background(),
		[]*vtgatepb.BoundShardQuery{{
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace: ks,
			Shards:   []string{shard1, shard2},
		}, {
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace: ks,
			Shards:   []string{shard1, shard2},
		}},
		topodatapb.TabletType_MASTER,
		false,
		session)
	if len(session.ShardSessions) != 2 {
		t.Errorf("want 2, got %d", len(session.ShardSessions))
	}
}

func TestVTGateExecuteBatchKeyspaceIds(t *testing.T) {
	ks := "TestVTGateExecuteBatchKeyspaceIds"
	shard1 := "-20"
	shard2 := "20-40"
	createSandbox(ks)
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	hcVTGateTest.Reset()
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1001, ks, shard1, topodatapb.TabletType_MASTER, true, 1, nil, sbc1)
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1002, ks, shard2, topodatapb.TabletType_MASTER, true, 1, nil, sbc2)
	kid10 := []byte{0x10}
	kid30 := []byte{0x30}
	qrl, err := rpcVTGate.ExecuteBatchKeyspaceIds(context.Background(),
		[]*vtgatepb.BoundKeyspaceIdQuery{{
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace:    ks,
			KeyspaceIds: [][]byte{kid10, kid30},
		}, {
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace:    ks,
			KeyspaceIds: [][]byte{kid10, kid30},
		}},
		topodatapb.TabletType_MASTER,
		false,
		nil)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if len(qrl) != 2 {
		t.Errorf("want 2, got %v", len(qrl))
	}
	if qrl[0].RowsAffected != 2 {
		t.Errorf("want 2, got %v", qrl[0].RowsAffected)
	}

	session, err := rpcVTGate.Begin(context.Background())
	rpcVTGate.ExecuteBatchKeyspaceIds(context.Background(),
		[]*vtgatepb.BoundKeyspaceIdQuery{{
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace:    ks,
			KeyspaceIds: [][]byte{kid10, kid30},
		}, {
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace:    ks,
			KeyspaceIds: [][]byte{kid10, kid30},
		}},
		topodatapb.TabletType_MASTER,
		false,
		session)
	if len(session.ShardSessions) != 2 {
		t.Errorf("want 2, got %d", len(session.ShardSessions))
	}
}

func TestVTGateStreamExecute(t *testing.T) {
	ks := KsTestUnsharded
	shard := "0"
	createSandbox(ks)
	sbc := &sandboxConn{}
	hcVTGateTest.Reset()
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1001, ks, shard, topodatapb.TabletType_MASTER, true, 1, nil, sbc)
	var qrs []*sqltypes.Result
	err := rpcVTGate.StreamExecute(context.Background(),
		"select id from t1",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		func(r *sqltypes.Result) error {
			qrs = append(qrs, r)
			return nil
		})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	want := []*sqltypes.Result{singleRowResult}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}
}

func TestVTGateStreamExecuteKeyspaceIds(t *testing.T) {
	ks := "TestVTGateStreamExecuteKeyspaceIds"
	shard1 := "-20"
	shard2 := "20-40"
	createSandbox(ks)
	sbc := &sandboxConn{}
	sbc1 := &sandboxConn{}
	hcVTGateTest.Reset()
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1001, ks, shard1, topodatapb.TabletType_MASTER, true, 1, nil, sbc)
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1002, ks, shard2, topodatapb.TabletType_MASTER, true, 1, nil, sbc1)
	// Test for successful execution
	var qrs []*sqltypes.Result
	err := rpcVTGate.StreamExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		ks,
		[][]byte{{0x10}},
		topodatapb.TabletType_MASTER,
		func(r *sqltypes.Result) error {
			qrs = append(qrs, r)
			return nil
		})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	want := []*sqltypes.Result{singleRowResult}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}

	// Test for successful execution - multiple keyspaceids in single shard
	qrs = nil
	err = rpcVTGate.StreamExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		ks,
		[][]byte{{0x10}, {0x15}},
		topodatapb.TabletType_MASTER,
		func(r *sqltypes.Result) error {
			qrs = append(qrs, r)
			return nil
		})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	want = []*sqltypes.Result{singleRowResult}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}
	// Test for successful execution - multiple keyspaceids in multiple shards
	err = rpcVTGate.StreamExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		ks,
		[][]byte{{0x10}, {0x30}},
		topodatapb.TabletType_MASTER,
		func(r *sqltypes.Result) error {
			qrs = append(qrs, r)
			return nil
		})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
}

func TestVTGateStreamExecuteKeyRanges(t *testing.T) {
	ks := "TestVTGateStreamExecuteKeyRanges"
	shard1 := "-20"
	shard2 := "20-40"
	createSandbox(ks)
	sbc := &sandboxConn{}
	sbc1 := &sandboxConn{}
	hcVTGateTest.Reset()
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1001, ks, shard1, topodatapb.TabletType_MASTER, true, 1, nil, sbc)
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1002, ks, shard2, topodatapb.TabletType_MASTER, true, 1, nil, sbc1)
	// Test for successful execution
	var qrs []*sqltypes.Result
	err := rpcVTGate.StreamExecuteKeyRanges(context.Background(),
		"query",
		nil,
		ks,
		[]*topodatapb.KeyRange{{End: []byte{0x20}}},
		topodatapb.TabletType_MASTER,
		func(r *sqltypes.Result) error {
			qrs = append(qrs, r)
			return nil
		})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	want := []*sqltypes.Result{singleRowResult}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}

	// Test for successful execution - multiple shards
	err = rpcVTGate.StreamExecuteKeyRanges(context.Background(),
		"query",
		nil,
		ks,
		[]*topodatapb.KeyRange{{Start: []byte{0x10}, End: []byte{0x40}}},
		topodatapb.TabletType_MASTER,
		func(r *sqltypes.Result) error {
			qrs = append(qrs, r)
			return nil
		})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
}

func TestVTGateStreamExecuteShards(t *testing.T) {
	ks := "TestVTGateStreamExecuteShards"
	shard := "0"
	createSandbox(ks)
	sbc := &sandboxConn{}
	hcVTGateTest.Reset()
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1001, ks, shard, topodatapb.TabletType_MASTER, true, 1, nil, sbc)
	// Test for successful execution
	var qrs []*sqltypes.Result
	err := rpcVTGate.StreamExecuteShards(context.Background(),
		"query",
		nil,
		ks,
		[]string{shard},
		topodatapb.TabletType_MASTER,
		func(r *sqltypes.Result) error {
			qrs = append(qrs, r)
			return nil
		})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	want := []*sqltypes.Result{singleRowResult}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}
}

func TestVTGateSplitQuery(t *testing.T) {
	keyspace := "TestVTGateSplitQuery"
	keyranges, _ := key.ParseShardingSpec(DefaultShardSpec)
	createSandbox(keyspace)
	hcVTGateTest.Reset()
	port := int32(1001)
	for _, kr := range keyranges {
		sbc := &sandboxConn{}
		hcVTGateTest.addTestTablet("aa", "1.1.1.1", port, keyspace, key.KeyRangeString(kr), topodatapb.TabletType_RDONLY, true, 1, nil, sbc)
		port++
	}
	sql := "select col1, col2 from table"
	splitCount := 24
	splits, err := rpcVTGate.SplitQuery(context.Background(),
		keyspace,
		sql,
		nil,
		"",
		int64(splitCount))
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
			t.Errorf("wrong keyspace, want \n%+v, got \n%+v", keyspace, split.KeyRangePart.Keyspace)
		}
		if len(split.KeyRangePart.KeyRanges) != 1 {
			t.Errorf("wrong number of keyranges, want \n%+v, got \n%+v", 1, len(split.KeyRangePart.KeyRanges))
		}
		kr := key.KeyRangeString(split.KeyRangePart.KeyRanges[0])
		actualSqlsByKeyRange[kr] = append(actualSqlsByKeyRange[kr], split.Query.Sql)
	}
	// Sort the sqls for each KeyRange so that we can compare them without
	// regard to the order in which they were returned by the vtgate.
	for _, sqlsForKeyRange := range actualSqlsByKeyRange {
		sort.Strings(sqlsForKeyRange)
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

// TODO(erez): Rename after migration to SplitQuery V2 is done.
func TestVTGateSplitQueryV2Sharded(t *testing.T) {
	keyspace := "TestVTGateSplitQuery"
	keyranges, err := key.ParseShardingSpec(DefaultShardSpec)
	if err != nil {
		t.Fatalf("got: %v, want: nil", err)
	}
	createSandbox(keyspace)
	hcVTGateTest.Reset()
	port := int32(1001)
	for _, kr := range keyranges {
		sbc := &sandboxConn{}
		hcVTGateTest.addTestTablet("aa", "1.1.1.1", port, keyspace, key.KeyRangeString(kr), topodatapb.TabletType_RDONLY, true, 1, nil, sbc)
		port++
	}
	sql := "select col1, col2 from table"
	bindVars := map[string]interface{}{"bv1": nil}
	splitColumns := []string{"sc1", "sc2"}
	algorithm := querypb.SplitQueryRequest_FULL_SCAN
	type testCaseType struct {
		splitCount          int64
		numRowsPerQueryPart int64
		algorithm           querypb.SplitQueryRequest_Algorithm
	}
	testCases := []testCaseType{
		{splitCount: 100, numRowsPerQueryPart: 0},
		{splitCount: 0, numRowsPerQueryPart: 123},
	}
	for _, testCase := range testCases {
		splits, err := rpcVTGate.SplitQueryV2(
			context.Background(),
			keyspace,
			sql,
			bindVars,
			splitColumns,
			testCase.splitCount,
			testCase.numRowsPerQueryPart,
			algorithm)
		if err != nil {
			t.Errorf("got %v, want: nil. testCase: %+v", err, testCase)
		}
		// Total number of splits should be number of shards as our sandbox returns a single split
		// for its fake implementation of SplitQuery.
		if len(keyranges) != len(splits) {
			t.Errorf("wrong number of splits, got %+v, want %+v. testCase:\n%+v",
				len(splits), len(keyranges), testCase)
		}
		actualSqlsByKeyRange := map[string][]string{}
		for _, split := range splits {
			if split.KeyRangePart.Keyspace != keyspace {
				t.Errorf("wrong keyspace, got \n%+v, want \n%+v. testCase:\n%+v",
					keyspace, split.KeyRangePart.Keyspace, testCase)
			}
			if len(split.KeyRangePart.KeyRanges) != 1 {
				t.Errorf("wrong number of keyranges, got \n%+v, want \n%+v. testCase:\n%+v",
					1, len(split.KeyRangePart.KeyRanges), testCase)
			}
			kr := key.KeyRangeString(split.KeyRangePart.KeyRanges[0])
			actualSqlsByKeyRange[kr] = append(actualSqlsByKeyRange[kr], split.Query.Sql)
		}
		expectedSqlsByKeyRange := map[string][]string{}
		for _, kr := range keyranges {
			perShardSplitCount := int64(math.Ceil(float64(testCase.splitCount) / float64(len(keyranges))))
			shard := key.KeyRangeString(kr)
			expectedSqlsByKeyRange[shard] = []string{
				fmt.Sprintf(
					"query:%v, splitColumns:%v, splitCount:%v,"+
						" numRowsPerQueryPart:%v, algorithm:%v, shard:%v",
					querytypes.BoundQuery{Sql: sql, BindVariables: bindVars},
					splitColumns,
					perShardSplitCount,
					testCase.numRowsPerQueryPart,
					algorithm,
					shard,
				),
			}
		}
		if !reflect.DeepEqual(actualSqlsByKeyRange, expectedSqlsByKeyRange) {
			t.Errorf(
				"splits contain the wrong sqls and/or keyranges, "+
					"got:\n%+v\n, want:\n%+v\n. testCase:\n%+v",
				actualSqlsByKeyRange, expectedSqlsByKeyRange, testCase)
		}
	}
}

func TestVTGateSplitQueryV2Unsharded(t *testing.T) {
	keyspace := KsTestUnsharded
	createSandbox(keyspace)
	sbc := &sandboxConn{}
	hcVTGateTest.Reset()
	hcVTGateTest.addTestTablet("aa", "1.1.1.1", 1001, keyspace, "0", topodatapb.TabletType_RDONLY, true, 1, nil, sbc)
	sql := "select col1, col2 from table"
	bindVars := map[string]interface{}{"bv1": nil}
	splitColumns := []string{"sc1", "sc2"}
	algorithm := querypb.SplitQueryRequest_FULL_SCAN
	type testCaseType struct {
		splitCount          int64
		numRowsPerQueryPart int64
		algorithm           querypb.SplitQueryRequest_Algorithm
	}
	testCases := []testCaseType{
		{splitCount: 100, numRowsPerQueryPart: 0},
		{splitCount: 0, numRowsPerQueryPart: 123},
	}
	for _, testCase := range testCases {
		splits, err := rpcVTGate.SplitQueryV2(
			context.Background(),
			keyspace,
			sql,
			bindVars,
			splitColumns,
			testCase.splitCount,
			testCase.numRowsPerQueryPart,
			algorithm)
		if err != nil {
			t.Errorf("got %v, want: nil. testCase: %+v", err, testCase)
		}
		// Total number of splits should be number of shards (1) as our sandbox returns a single split
		// for its fake implementation of SplitQuery.
		if 1 != len(splits) {
			t.Errorf("wrong number of splits, got %+v, want %+v. testCase:\n%+v",
				len(splits), 1, testCase)
			continue
		}
		split := splits[0]
		if split.KeyRangePart != nil {
			t.Errorf("KeyRangePart should not be populated. Got:\n%+v\n, testCase:\n%+v\n",
				keyspace, split.KeyRangePart)
		}
		if split.ShardPart.Keyspace != keyspace {
			t.Errorf("wrong keyspace, got \n%+v, want \n%+v. testCase:\n%+v",
				keyspace, split.ShardPart.Keyspace, testCase)
		}
		if len(split.ShardPart.Shards) != 1 {
			t.Errorf("wrong number of shards, got \n%+v, want \n%+v. testCase:\n%+v",
				1, len(split.ShardPart.Shards), testCase)
		}
		expectedShard := "0"
		expectedSQL := fmt.Sprintf(
			"query:%v, splitColumns:%v, splitCount:%v,"+
				" numRowsPerQueryPart:%v, algorithm:%v, shard:%v",
			querytypes.BoundQuery{Sql: sql, BindVariables: bindVars},
			splitColumns,
			testCase.splitCount,
			testCase.numRowsPerQueryPart,
			algorithm,
			expectedShard,
		)
		if split.Query.Sql != expectedSQL {
			t.Errorf("got:\n%v\n, want:\n%v\n, testCase:\n%+v",
				split.Query.Sql, expectedSQL, testCase)
		}
	}
}

func TestIsErrorCausedByVTGate(t *testing.T) {
	unknownError := fmt.Errorf("unknown error")
	serverError := &tabletconn.ServerError{
		ServerCode: vtrpcpb.ErrorCode_QUERY_NOT_SERVED,
		Err:        "vttablet: retry: error message",
	}
	shardConnUnknownErr := &ShardError{Err: unknownError}
	shardConnServerErr := &ShardError{Err: serverError}
	shardConnCancelledErr := &ShardError{Err: context.Canceled}
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
		unknownError:     true,
		serverError:      false,
		context.Canceled: false,
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

// Functions for testing
// keyspace_id and 'filtered_replication_unfriendly'
// annotations.
func TestAnnotatingExecuteKeyspaceIds(t *testing.T) {
	keyspace, shards := setUpSandboxWithTwoShards("TestAnnotatingExecuteKeyspaceIds")

	_, err := rpcVTGate.ExecuteKeyspaceIds(
		context.Background(),
		"INSERT INTO table () VALUES();",
		nil,
		keyspace,
		[][]byte{{0x10}},
		topodatapb.TabletType_MASTER,
		nil,
		false)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}

	verifyQueryAnnotatedWithKeyspaceID(t, []byte{0x10}, shards[0])
}

func TestAnnotatingExecuteKeyspaceIdsMultipleIds(t *testing.T) {
	keyspace, shards := setUpSandboxWithTwoShards("TestAnnotatingExecuteKeyspaceIdsMultipleIds")

	_, err := rpcVTGate.ExecuteKeyspaceIds(
		context.Background(),
		"INSERT INTO table () VALUES();",
		nil,
		keyspace,
		[][]byte{{0x10}, {0x15}},
		topodatapb.TabletType_MASTER,
		nil,
		false)
	if err == nil || !strings.Contains(err.Error(), "DML should not span multiple keyspace_ids") {
		t.Fatalf("want specific error, got %v", err)
	}

	// Currently, there's logic in resolver.go for rejecting
	// multiple-ids DML's so we expect 0 queries here.
	verifyNumQueries(t, 0, shards[0].Queries)
}

func TestAnnotatingExecuteKeyRanges(t *testing.T) {
	keyspace, shards := setUpSandboxWithTwoShards("TestAnnotatingExecuteKeyRanges")

	_, err := rpcVTGate.ExecuteKeyRanges(
		context.Background(),
		"UPDATE table SET col1=1 WHERE col2>3;",
		nil,
		keyspace,
		[]*topodatapb.KeyRange{{Start: []byte{0x10}, End: []byte{0x40}}},
		topodatapb.TabletType_MASTER,
		nil,
		false)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}

	// Keyrange spans both shards.
	verifyQueryAnnotatedAsUnfriendly(t, shards[0])
	verifyQueryAnnotatedAsUnfriendly(t, shards[1])
}

func TestAnnotatingExecuteEntityIds(t *testing.T) {
	keyspace, shards := setUpSandboxWithTwoShards("TestAnnotatingExecuteEntityIds")

	_, err := rpcVTGate.ExecuteEntityIds(
		context.Background(),
		"INSERT INTO table () VALUES();",
		nil,
		keyspace,
		"entity_column_name",
		[]*vtgatepb.ExecuteEntityIdsRequest_EntityId{
			{
				Type:       sqltypes.Int64,
				Value:      []byte("0"),
				KeyspaceId: []byte{0x10}, // First shard.
			},
			{
				Type:       sqltypes.Int64,
				Value:      []byte("1"),
				KeyspaceId: []byte{0x25}, // Second shard.
			},
		},
		topodatapb.TabletType_MASTER,
		nil,
		false)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}

	verifyQueryAnnotatedAsUnfriendly(t, shards[0])
	verifyQueryAnnotatedAsUnfriendly(t, shards[1])
}

func TestAnnotatingExecuteShards(t *testing.T) {
	keyspace, shards := setUpSandboxWithTwoShards("TestAnnotatingExecuteShards")
	_, err := rpcVTGate.ExecuteShards(
		context.Background(),
		"INSERT INTO table () VALUES();",
		nil,
		keyspace,
		[]string{"20-40"},
		topodatapb.TabletType_MASTER,
		nil,
		false)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}

	verifyQueryAnnotatedAsUnfriendly(t, shards[1])
}

func TestAnnotatingExecuteBatchKeyspaceIds(t *testing.T) {
	keyspace, shards := setUpSandboxWithTwoShards("TestAnnotatingExecuteBatchKeyspaceIds")
	_, err := rpcVTGate.ExecuteBatchKeyspaceIds(
		context.Background(),
		[]*vtgatepb.BoundKeyspaceIdQuery{
			{
				Query: &querypb.BoundQuery{
					Sql: "INSERT INTO table () VALUES();",
				},
				Keyspace:    keyspace,
				KeyspaceIds: [][]byte{{0x10}},
			},
			{
				Query: &querypb.BoundQuery{
					Sql: "UPDATE table SET col1=1 WHERE col2>3;",
				},
				Keyspace:    keyspace,
				KeyspaceIds: [][]byte{{0x15}},
			},
			{
				Query: &querypb.BoundQuery{
					Sql: "DELETE FROM table WHERE col1==4;",
				},
				Keyspace:    keyspace,
				KeyspaceIds: [][]byte{{0x25}},
			},
		},
		topodatapb.TabletType_MASTER,
		false,
		nil)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}

	verifyBatchQueryAnnotatedWithKeyspaceIds(
		t,
		[][]byte{{0x10}, {0x15}},
		shards[0])
	verifyBatchQueryAnnotatedWithKeyspaceIds(
		t,
		[][]byte{{0x25}},
		shards[1])
}

func TestAnnotatingExecuteBatchKeyspaceIdsMultipleIds(t *testing.T) {
	keyspace, shards := setUpSandboxWithTwoShards("TestAnnotatingExecuteBatchKeyspaceIdsMultipleIds")
	_, err := rpcVTGate.ExecuteBatchKeyspaceIds(
		context.Background(),
		[]*vtgatepb.BoundKeyspaceIdQuery{
			{
				Query: &querypb.BoundQuery{
					Sql: "INSERT INTO table () VALUES();",
				},
				Keyspace: keyspace,
				KeyspaceIds: [][]byte{
					{0x10},
					{0x15},
				},
			},
		},
		topodatapb.TabletType_MASTER,
		false,
		nil)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}

	verifyBatchQueryAnnotatedAsUnfriendly(
		t,
		1, // expectedNumQueries
		shards[0])
}

func TestAnnotatingExecuteBatchShards(t *testing.T) {
	keyspace, shards := setUpSandboxWithTwoShards("TestAnnotatingExecuteBatchShards")

	_, err := rpcVTGate.ExecuteBatchShards(
		context.Background(),
		[]*vtgatepb.BoundShardQuery{
			{
				Query: &querypb.BoundQuery{
					Sql: "INSERT INTO table () VALUES();",
				},
				Keyspace: keyspace,
				Shards:   []string{"-20", "20-40"},
			},
			{
				Query: &querypb.BoundQuery{
					Sql: "UPDATE table SET col1=1 WHERE col2>3;",
				},
				Keyspace: keyspace,
				Shards:   []string{"-20"},
			},
			{
				Query: &querypb.BoundQuery{
					Sql: "UPDATE table SET col1=1 WHERE col2>3;",
				},
				Keyspace: keyspace,
				Shards:   []string{"20-40"},
			},
			{
				Query: &querypb.BoundQuery{
					Sql: "DELETE FROM table WHERE col1==4;",
				},
				Keyspace: keyspace,
				Shards:   []string{"20-40"},
			},
		},
		topodatapb.TabletType_MASTER,
		false,
		nil)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}

	verifyBatchQueryAnnotatedAsUnfriendly(
		t,
		2, // expectedNumQueries
		shards[0])
	verifyBatchQueryAnnotatedAsUnfriendly(
		t,
		3, // expectedNumQueries
		shards[1])
}

// TODO(erez): Add testing annotations of vtgate.Execute (V3)

// Sets up a sandbox with two shards:
//   the first named "-20" for the -20 keyrange, and
//   the second named "20-40" for the 20-40 keyrange.
// It returns the created shards and as a convenience the given
// keyspace.
//
// NOTE: You should not call this method multiple times with
// the same 'keyspace' parameter: "shardGateway" caches connections
// for a keyspace, and may re-send queries to the shards created in
// a previous call to this method.
func setUpSandboxWithTwoShards(keyspace string) (string, []*sandboxConn) {
	shards := []*sandboxConn{{}, {}}
	createSandbox(keyspace)
	hcVTGateTest.Reset()
	hcVTGateTest.addTestTablet("aa", "-20", 1, keyspace, "-20", topodatapb.TabletType_MASTER, true, 1, nil, shards[0])
	hcVTGateTest.addTestTablet("aa", "20-40", 1, keyspace, "20-40", topodatapb.TabletType_MASTER, true, 1, nil, shards[1])
	return keyspace, shards
}

// Verifies that 'shard' was sent exactly one query and that it
// was annotated with 'expectedKeyspaceID'
func verifyQueryAnnotatedWithKeyspaceID(t *testing.T, expectedKeyspaceID []byte, shard *sandboxConn) {
	if !verifyNumQueries(t, 1, shard.Queries) {
		return
	}
	verifyBoundQueryAnnotatedWithKeyspaceID(t, expectedKeyspaceID, &shard.Queries[0])
}

// Verifies that 'shard' was sent exactly one query and that it
// was annotated as unfriendly.
func verifyQueryAnnotatedAsUnfriendly(t *testing.T, shard *sandboxConn) {
	if !verifyNumQueries(t, 1, shard.Queries) {
		return
	}
	verifyBoundQueryAnnotatedAsUnfriendly(t, &shard.Queries[0])
}

// Verifies 'queries' has exactly 'expectedNumQueries' elements.
// Returns true if verification succeeds.
func verifyNumQueries(t *testing.T, expectedNumQueries int, queries []querytypes.BoundQuery) bool {
	numElements := len(queries)
	if numElements != expectedNumQueries {
		t.Errorf("want %v queries, got: %v (queries: %v)", expectedNumQueries, numElements, queries)
		return false
	}
	return true
}

// Verifies 'batchQueries' has exactly 'expectedNumQueries' elements.
// Returns true if verification succeeds.
func verifyNumBatchQueries(t *testing.T, expectedNumQueries int, batchQueries [][]querytypes.BoundQuery) bool {
	numElements := len(batchQueries)
	if numElements != expectedNumQueries {
		t.Errorf("want %v batch queries, got: %v (batch queries: %v)", expectedNumQueries, numElements, batchQueries)
		return false
	}
	return true
}

func verifyBoundQueryAnnotatedWithKeyspaceID(t *testing.T, expectedKeyspaceID []byte, query *querytypes.BoundQuery) {
	verifyBoundQueryAnnotatedWithComment(
		t,
		"/* vtgate:: keyspace_id:"+hex.EncodeToString(expectedKeyspaceID)+" */",
		query)
}

func verifyBoundQueryAnnotatedAsUnfriendly(t *testing.T, query *querytypes.BoundQuery) {
	verifyBoundQueryAnnotatedWithComment(
		t,
		"/* vtgate:: filtered_replication_unfriendly */",
		query)
}

func verifyBoundQueryAnnotatedWithComment(t *testing.T, expectedComment string, query *querytypes.BoundQuery) {
	if !strings.Contains(query.Sql, expectedComment) {
		t.Errorf("want query '%v' to be annotated with '%v'", query.Sql, expectedComment)
	}
}

// Verifies that 'shard' was sent exactly one batch-query and that its
// (single) queries are annotated with the elements of expectedKeyspaceIDs
// in order.
func verifyBatchQueryAnnotatedWithKeyspaceIds(t *testing.T, expectedKeyspaceIDs [][]byte, shard *sandboxConn) {
	if !verifyNumBatchQueries(t, 1, shard.BatchQueries) {
		return
	}
	verifyBoundQueriesAnnotatedWithKeyspaceIds(t, expectedKeyspaceIDs, shard.BatchQueries[0])
}

// Verifies that 'shard' was sent exactly one batch-query and that its
// (single) queries are annotated as unfriendly.
func verifyBatchQueryAnnotatedAsUnfriendly(t *testing.T, expectedNumQueries int, shard *sandboxConn) {
	if !verifyNumBatchQueries(t, 1, shard.BatchQueries) {
		return
	}
	verifyBoundQueriesAnnotatedAsUnfriendly(t, expectedNumQueries, shard.BatchQueries[0])
}

func verifyBoundQueriesAnnotatedWithKeyspaceIds(t *testing.T, expectedKeyspaceIDs [][]byte, queries []querytypes.BoundQuery) {
	if !verifyNumQueries(t, len(expectedKeyspaceIDs), queries) {
		return
	}
	for i := range queries {
		verifyBoundQueryAnnotatedWithKeyspaceID(t, expectedKeyspaceIDs[i], &queries[i])
	}
}

func verifyBoundQueriesAnnotatedAsUnfriendly(t *testing.T, expectedNumQueries int, queries []querytypes.BoundQuery) {
	if !verifyNumQueries(t, expectedNumQueries, queries) {
		return
	}
	for i := range queries {
		verifyBoundQueryAnnotatedAsUnfriendly(t, &queries[i])
	}
}
