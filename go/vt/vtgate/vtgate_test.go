/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtgate

import (
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// This file uses the sandbox_test framework.

var hcVTGateTest *discovery.FakeHealthCheck

var executeOptions = &querypb.ExecuteOptions{
	IncludedFields: querypb.ExecuteOptions_TYPE_ONLY,
}

var masterSession = &vtgatepb.Session{
	TargetString: "@master",
}

func init() {
	getSandbox(KsTestUnsharded).VSchema = `
{
	"sharded": false,
	"tables": {
		"t1": {}
	}
}
`
	hcVTGateTest = discovery.NewFakeHealthCheck()
	*transactionMode = "MULTI"
	// The topo.Server is used to start watching the cells described
	// in '-cells_to_watch' command line parameter, which is
	// empty by default. So it's unused in this test, set to nil.
	Init(context.Background(), hcVTGateTest, new(sandboxTopo), "aa", 10, nil)

	*mysqlServerPort = 0
	*mysqlAuthServerImpl = "none"
	initMySQLProtocol()
}

func TestVTGateBegin(t *testing.T) {
	save := rpcVTGate.txConn.mode
	defer func() {
		rpcVTGate.txConn.mode = save
	}()

	rpcVTGate.txConn.mode = vtgatepb.TransactionMode_SINGLE
	got, err := rpcVTGate.Begin(context.Background(), true)
	if err != nil {
		t.Error(err)
	}
	wantSession := &vtgatepb.Session{
		InTransaction: true,
		SingleDb:      true,
	}
	if !proto.Equal(got, wantSession) {
		t.Errorf("Begin(single): %v, want %v", got, wantSession)
	}

	_, err = rpcVTGate.Begin(context.Background(), false)
	wantErr := "multi-db transaction disallowed"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Begin(multi): %v, want %s", err, wantErr)
	}

	rpcVTGate.txConn.mode = vtgatepb.TransactionMode_MULTI
	got, err = rpcVTGate.Begin(context.Background(), true)
	if err != nil {
		t.Error(err)
	}
	wantSession = &vtgatepb.Session{
		InTransaction: true,
		SingleDb:      true,
	}
	if !proto.Equal(got, wantSession) {
		t.Errorf("Begin(single): %v, want %v", got, wantSession)
	}

	got, err = rpcVTGate.Begin(context.Background(), false)
	if err != nil {
		t.Error(err)
	}
	wantSession = &vtgatepb.Session{
		InTransaction: true,
	}
	if !proto.Equal(got, wantSession) {
		t.Errorf("Begin(single): %v, want %v", got, wantSession)
	}

	rpcVTGate.txConn.mode = vtgatepb.TransactionMode_TWOPC
	got, err = rpcVTGate.Begin(context.Background(), true)
	if err != nil {
		t.Error(err)
	}
	wantSession = &vtgatepb.Session{
		InTransaction: true,
		SingleDb:      true,
	}
	if !proto.Equal(got, wantSession) {
		t.Errorf("Begin(single): %v, want %v", got, wantSession)
	}

	got, err = rpcVTGate.Begin(context.Background(), false)
	if err != nil {
		t.Error(err)
	}
	wantSession = &vtgatepb.Session{
		InTransaction: true,
	}
	if !proto.Equal(got, wantSession) {
		t.Errorf("Begin(single): %v, want %v", got, wantSession)
	}
}

func TestVTGateCommit(t *testing.T) {
	save := rpcVTGate.txConn.mode
	defer func() {
		rpcVTGate.txConn.mode = save
	}()

	session := &vtgatepb.Session{
		InTransaction: true,
	}

	rpcVTGate.txConn.mode = vtgatepb.TransactionMode_SINGLE
	err := rpcVTGate.Commit(context.Background(), true, session)
	wantErr := "vtgate: : 2pc transaction disallowed"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Begin(multi): %v, want %s", err, wantErr)
	}

	session = &vtgatepb.Session{
		InTransaction: true,
	}
	err = rpcVTGate.Commit(context.Background(), false, session)
	if err != nil {
		t.Error(err)
	}

	rpcVTGate.txConn.mode = vtgatepb.TransactionMode_MULTI
	session = &vtgatepb.Session{
		InTransaction: true,
	}
	err = rpcVTGate.Commit(context.Background(), true, session)
	if err == nil || err.Error() != wantErr {
		t.Errorf("Begin(multi): %v, want %s", err, wantErr)
	}

	session = &vtgatepb.Session{
		InTransaction: true,
	}
	err = rpcVTGate.Commit(context.Background(), false, session)
	if err != nil {
		t.Error(err)
	}

	rpcVTGate.txConn.mode = vtgatepb.TransactionMode_TWOPC
	session = &vtgatepb.Session{
		InTransaction: true,
	}
	err = rpcVTGate.Commit(context.Background(), true, session)
	if err != nil {
		t.Error(err)
	}

	session = &vtgatepb.Session{
		InTransaction: true,
	}
	err = rpcVTGate.Commit(context.Background(), false, session)
	if err != nil {
		t.Error(err)
	}
}

func TestVTGateRollbackNil(t *testing.T) {
	err := rpcVTGate.Rollback(context.Background(), nil)
	if err != nil {
		t.Error(err)
	}
}

func TestVTGateExecute(t *testing.T) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	_, qr, err := rpcVTGate.Execute(
		context.Background(),
		&vtgatepb.Session{
			Autocommit:   true,
			TargetString: "@master",
			Options:      executeOptions,
		},
		"select id from t1",
		nil,
	)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if !reflect.DeepEqual(sandboxconn.SingleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", sandboxconn.SingleRowResult, qr)
	}
	if !proto.Equal(sbc.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc.Options[0], executeOptions)
	}

	session, err := rpcVTGate.Begin(context.Background(), false)
	if !session.InTransaction {
		t.Errorf("want true, got false")
	}
	rpcVTGate.Execute(
		context.Background(),
		session,
		"select id from t1",
		nil,
	)
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
	if !proto.Equal(wantSession, session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, session)
	}

	rpcVTGate.Commit(context.Background(), false, session)
	if commitCount := sbc.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	session, err = rpcVTGate.Begin(context.Background(), false)
	rpcVTGate.Execute(
		context.Background(),
		session,
		"select id from t1",
		nil,
	)
	rpcVTGate.Rollback(context.Background(), session)
	if sbc.RollbackCount.Get() != 1 {
		t.Errorf("want 1, got %d", sbc.RollbackCount.Get())
	}
}

func TestVTGateExecuteWithKeyspaceShard(t *testing.T) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

	// Valid keyspace.
	_, qr, err := rpcVTGate.Execute(
		context.Background(),
		&vtgatepb.Session{
			TargetString: KsTestUnsharded,
		},
		"select id from none",
		nil,
	)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if !reflect.DeepEqual(sandboxconn.SingleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", sandboxconn.SingleRowResult, qr)
	}

	// Invalid keyspace.
	_, _, err = rpcVTGate.Execute(
		context.Background(),
		&vtgatepb.Session{
			TargetString: "invalid_keyspace",
		},
		"select id from none",
		nil,
	)
	want := "vtgate: : keyspace invalid_keyspace not found in vschema"
	if err == nil || err.Error() != want {
		t.Errorf("Execute: %v, want %s", err, want)
	}

	// Valid keyspace/shard.
	_, qr, err = rpcVTGate.Execute(
		context.Background(),
		&vtgatepb.Session{
			TargetString: KsTestUnsharded + ":0@master",
		},
		"select id from none",
		nil,
	)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if !reflect.DeepEqual(sandboxconn.SingleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", sandboxconn.SingleRowResult, qr)
	}

	// Invalid keyspace/shard.
	_, _, err = rpcVTGate.Execute(
		context.Background(),
		&vtgatepb.Session{
			TargetString: KsTestUnsharded + ":noshard@master",
		},
		"select id from none",
		nil,
	)
	want = "vtgate: : target: TestUnsharded.noshard.master, no valid tablet: node doesn't exist: TestUnsharded/noshard (MASTER)"
	if err == nil || err.Error() != want {
		t.Errorf("Execute: %v, want %s", err, want)
	}
}
func TestVTGateExecuteShards(t *testing.T) {
	ks := "TestVTGateExecuteShards"
	shard := "0"
	createSandbox(ks)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard, topodatapb.TabletType_REPLICA, true, 1, nil)
	qr, err := rpcVTGate.ExecuteShards(context.Background(),
		"query",
		nil,
		ks,
		[]string{shard},
		topodatapb.TabletType_REPLICA,
		nil,
		false,
		executeOptions)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if !reflect.DeepEqual(sandboxconn.SingleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", sandboxconn.SingleRowResult, qr)
	}
	if !proto.Equal(sbc.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc.Options[0], executeOptions)
	}

	session, err := rpcVTGate.Begin(context.Background(), false)
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
		false,
		nil)
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
	if !proto.Equal(wantSession, session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, session)
	}

	rpcVTGate.Commit(context.Background(), false, session)
	if commitCount := sbc.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	session, err = rpcVTGate.Begin(context.Background(), false)
	rpcVTGate.ExecuteShards(context.Background(),
		"query",
		nil,
		ks,
		[]string{shard},
		topodatapb.TabletType_REPLICA,
		session,
		false,
		nil)
	rpcVTGate.Rollback(context.Background(), session)
	if sbc.RollbackCount.Get() != 1 {
		t.Errorf("want 1, got %d", sbc.RollbackCount.Get())
	}
}

func TestVTGateExecuteKeyspaceIds(t *testing.T) {
	ks := "TestVTGateExecuteKeyspaceIds"
	shard1 := "-20"
	shard2 := "20-40"
	createSandbox(ks)
	hcVTGateTest.Reset()
	sbc1 := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard1, topodatapb.TabletType_MASTER, true, 1, nil)
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1002, ks, shard2, topodatapb.TabletType_MASTER, true, 1, nil)
	// Test for successful execution
	qr, err := rpcVTGate.ExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		ks,
		[][]byte{{0x10}},
		topodatapb.TabletType_MASTER,
		nil,
		false,
		executeOptions)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if !reflect.DeepEqual(sandboxconn.SingleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", sandboxconn.SingleRowResult, qr)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v\n", execCount)
	}
	if !proto.Equal(sbc1.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc1.Options[0], executeOptions)
	}
	// Test for successful execution in transaction
	session, err := rpcVTGate.Begin(context.Background(), false)
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
		false,
		nil)
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
	if !proto.Equal(wantSession, session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, session)
	}
	rpcVTGate.Commit(context.Background(), false, session)
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
		false,
		nil)
	if err != nil {
		t.Fatalf("ExecuteKeyspaceIds failed: %v", err)
	}
	if qr.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.RowsAffected)
	}
	// Test for multiple shards for DML
	qr, err = rpcVTGate.ExecuteKeyspaceIds(context.Background(),
		"update table set a = b",
		nil,
		ks,
		[][]byte{{0x10}, {0x30}},
		topodatapb.TabletType_MASTER,
		session,
		false,
		nil)
	errStr := "DML should not span multiple keyspace_ids"
	if err == nil || !strings.Contains(err.Error(), errStr) {
		t.Errorf("want '%v', got '%v'", errStr, err)
	}
}

func TestVTGateExecuteKeyRanges(t *testing.T) {
	ks := "TestVTGateExecuteKeyRanges"
	shard1 := "-20"
	shard2 := "20-40"
	createSandbox(ks)
	hcVTGateTest.Reset()
	sbc1 := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard1, topodatapb.TabletType_MASTER, true, 1, nil)
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1002, ks, shard2, topodatapb.TabletType_MASTER, true, 1, nil)
	// Test for successful execution
	qr, err := rpcVTGate.ExecuteKeyRanges(context.Background(),
		"query",
		nil,
		ks,
		[]*topodatapb.KeyRange{{End: []byte{0x20}}},
		topodatapb.TabletType_MASTER,
		nil,
		false,
		executeOptions)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if !reflect.DeepEqual(sandboxconn.SingleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", sandboxconn.SingleRowResult, qr)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v\n", execCount)
	}
	if !proto.Equal(sbc1.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc1.Options[0], executeOptions)
	}
	// Test for successful execution in transaction
	session, err := rpcVTGate.Begin(context.Background(), false)
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
		false,
		nil)
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
	if !proto.Equal(wantSession, session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, session)
	}
	rpcVTGate.Commit(context.Background(), false, session)
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
		false,
		nil)
	if err != nil {
		t.Fatalf("ExecuteKeyRanges failed: %v", err)
	}
	if qr.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.RowsAffected)
	}
}

func TestVTGateExecuteEntityIds(t *testing.T) {
	ks := "TestVTGateExecuteEntityIds"
	shard1 := "-20"
	shard2 := "20-40"
	createSandbox(ks)
	hcVTGateTest.Reset()
	sbc1 := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard1, topodatapb.TabletType_MASTER, true, 1, nil)
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1002, ks, shard2, topodatapb.TabletType_MASTER, true, 1, nil)
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
		false,
		executeOptions)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if !reflect.DeepEqual(sandboxconn.SingleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", sandboxconn.SingleRowResult, qr)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v\n", execCount)
	}
	if !proto.Equal(sbc1.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc1.Options[0], executeOptions)
	}
	// Test for successful execution in transaction
	session, err := rpcVTGate.Begin(context.Background(), false)
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
		false,
		nil)
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
	if !proto.Equal(wantSession, session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, session)
	}
	rpcVTGate.Commit(context.Background(), false, session)
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
		false,
		nil)
	if err != nil {
		t.Fatalf("ExecuteEntityIds failed: %v", err)
	}
	if qr.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.RowsAffected)
	}
}

func TestVTGateExecuteBatch(t *testing.T) {
	// TODO(sougou): using masterSession as global has some bugs
	// which requires us to reset it here. This needs to be fixed.
	masterSession = &vtgatepb.Session{
		TargetString: "@master",
	}

	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

	startCommit := sbc.CommitCount.Get()
	startRollback := sbc.RollbackCount.Get()

	sqlList := []string{
		"begin",
		"select id from t1",
		"begin",
		"select id from t1",
		"commit",
		"select id from t1",
		"commit",
		"begin",
		"select id from t1",
		"rollback",
		"select id from t1",
		"rollback",
		"begin",
		"select id from t1",
	}

	session, qrl, err := rpcVTGate.ExecuteBatch(context.Background(), masterSession, sqlList, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Spot-check one result.
	qr := qrl[3].QueryResult
	if !reflect.DeepEqual(sandboxconn.SingleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", sandboxconn.SingleRowResult, qr)
	}
	if len(session.ShardSessions) != 1 {
		t.Errorf("want 1, got %d", len(session.ShardSessions))
	}
	if got, want := sbc.CommitCount.Get(), startCommit+3; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
	if got, want := sbc.RollbackCount.Get(), startRollback+2; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}

func TestVTGateExecuteBatchShards(t *testing.T) {
	ks := "TestVTGateExecuteBatchShards"
	createSandbox(ks)
	shard1 := "-20"
	shard2 := "20-40"
	hcVTGateTest.Reset()
	sbc1 := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard1, topodatapb.TabletType_MASTER, true, 1, nil)
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1002, ks, shard2, topodatapb.TabletType_MASTER, true, 1, nil)
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
		nil,
		executeOptions)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if len(qrl) != 2 {
		t.Errorf("want 2, got %v", len(qrl))
	}
	if qrl[0].RowsAffected != 2 {
		t.Errorf("want 2, got %v", qrl[0].RowsAffected)
	}
	if !proto.Equal(sbc1.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc1.Options[0], executeOptions)
	}

	session, err := rpcVTGate.Begin(context.Background(), false)
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
		session,
		nil)
	if len(session.ShardSessions) != 2 {
		t.Errorf("want 2, got %d", len(session.ShardSessions))
	}

	timingsCount := rpcVTGate.timings.Counts()["ExecuteBatchShards.TestVTGateExecuteBatchShards.master"]
	if got, want := timingsCount, int64(2); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}
}

func TestVTGateExecuteBatchKeyspaceIds(t *testing.T) {
	ks := "TestVTGateExecuteBatchKeyspaceIds"
	shard1 := "-20"
	shard2 := "20-40"
	createSandbox(ks)
	hcVTGateTest.Reset()
	sbc1 := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard1, topodatapb.TabletType_MASTER, true, 1, nil)
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1002, ks, shard2, topodatapb.TabletType_MASTER, true, 1, nil)
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
		nil,
		executeOptions)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if len(qrl) != 2 {
		t.Errorf("want 2, got %v", len(qrl))
	}
	if qrl[0].RowsAffected != 2 {
		t.Errorf("want 2, got %v", qrl[0].RowsAffected)
	}
	if !proto.Equal(sbc1.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc1.Options[0], executeOptions)
	}

	session, err := rpcVTGate.Begin(context.Background(), false)
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
		session,
		nil)
	if len(session.ShardSessions) != 2 {
		t.Errorf("want 2, got %d", len(session.ShardSessions))
	}

	timingsCount := rpcVTGate.timings.Counts()["ExecuteBatchKeyspaceIds.TestVTGateExecuteBatchKeyspaceIds.master"]
	if got, want := timingsCount, int64(2); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}
}

func TestVTGateStreamExecute(t *testing.T) {
	ks := KsTestUnsharded
	shard := "0"
	createSandbox(ks)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard, topodatapb.TabletType_MASTER, true, 1, nil)
	var qrs []*sqltypes.Result
	err := rpcVTGate.StreamExecute(
		context.Background(),
		&vtgatepb.Session{
			TargetString: "@master",
			Options:      executeOptions,
		},
		"select id from t1",
		nil,
		func(r *sqltypes.Result) error {
			qrs = append(qrs, r)
			return nil
		},
	)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	want := []*sqltypes.Result{{
		Fields: sandboxconn.StreamRowResult.Fields,
	}, {
		Rows: sandboxconn.StreamRowResult.Rows,
	}}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}
	if !proto.Equal(sbc.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc.Options[0], executeOptions)
	}
}

func TestVTGateStreamExecuteKeyspaceShard(t *testing.T) {
	ks := KsTestUnsharded
	shard := "0"
	createSandbox(ks)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard, topodatapb.TabletType_MASTER, true, 1, nil)
	var qrs []*sqltypes.Result
	err := rpcVTGate.StreamExecute(
		context.Background(),
		&vtgatepb.Session{
			TargetString: ks + "/" + shard + "@master",
			Options:      executeOptions,
		},
		"random statement",
		nil,
		func(r *sqltypes.Result) error {
			qrs = append(qrs, r)
			return nil
		},
	)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	want := []*sqltypes.Result{sandboxconn.SingleRowResult}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}
	if !proto.Equal(sbc.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc.Options[0], executeOptions)
	}
}

func TestVTGateStreamExecuteKeyspaceIds(t *testing.T) {
	ks := "TestVTGateStreamExecuteKeyspaceIds"
	shard1 := "-20"
	shard2 := "20-40"
	createSandbox(ks)
	hcVTGateTest.Reset()
	sbc1 := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard1, topodatapb.TabletType_MASTER, true, 1, nil)
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1002, ks, shard2, topodatapb.TabletType_MASTER, true, 1, nil)
	// Test for successful execution
	var qrs []*sqltypes.Result
	err := rpcVTGate.StreamExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		ks,
		[][]byte{{0x10}},
		topodatapb.TabletType_MASTER,
		executeOptions,
		func(r *sqltypes.Result) error {
			qrs = append(qrs, r)
			return nil
		})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	want := []*sqltypes.Result{sandboxconn.SingleRowResult}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}
	if !proto.Equal(sbc1.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc1.Options[0], executeOptions)
	}

	// Test for successful execution - multiple keyspaceids in single shard
	qrs = nil
	err = rpcVTGate.StreamExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		ks,
		[][]byte{{0x10}, {0x15}},
		topodatapb.TabletType_MASTER,
		nil,
		func(r *sqltypes.Result) error {
			qrs = append(qrs, r)
			return nil
		})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	want = []*sqltypes.Result{sandboxconn.SingleRowResult}
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
		nil,
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
	hcVTGateTest.Reset()
	sbc1 := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard1, topodatapb.TabletType_MASTER, true, 1, nil)
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1002, ks, shard2, topodatapb.TabletType_MASTER, true, 1, nil)
	// Test for successful execution
	var qrs []*sqltypes.Result
	err := rpcVTGate.StreamExecuteKeyRanges(context.Background(),
		"query",
		nil,
		ks,
		[]*topodatapb.KeyRange{{End: []byte{0x20}}},
		topodatapb.TabletType_MASTER,
		executeOptions,
		func(r *sqltypes.Result) error {
			qrs = append(qrs, r)
			return nil
		})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	want := []*sqltypes.Result{sandboxconn.SingleRowResult}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}
	if !proto.Equal(sbc1.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc1.Options[0], executeOptions)
	}

	// Test for successful execution - multiple shards
	err = rpcVTGate.StreamExecuteKeyRanges(context.Background(),
		"query",
		nil,
		ks,
		[]*topodatapb.KeyRange{{Start: []byte{0x10}, End: []byte{0x40}}},
		topodatapb.TabletType_MASTER,
		nil,
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
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard, topodatapb.TabletType_MASTER, true, 1, nil)
	// Test for successful execution
	var qrs []*sqltypes.Result
	err := rpcVTGate.StreamExecuteShards(context.Background(),
		"query",
		nil,
		ks,
		[]string{shard},
		topodatapb.TabletType_MASTER,
		executeOptions,
		func(r *sqltypes.Result) error {
			qrs = append(qrs, r)
			return nil
		})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	want := []*sqltypes.Result{sandboxconn.SingleRowResult}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}
	if !proto.Equal(sbc.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc.Options[0], executeOptions)
	}
}

func TestVTGateSplitQuerySharded(t *testing.T) {
	keyspace := "TestVTGateSplitQuery"
	keyranges, err := key.ParseShardingSpec(DefaultShardSpec)
	if err != nil {
		t.Fatalf("got: %v, want: nil", err)
	}
	createSandbox(keyspace)
	hcVTGateTest.Reset()
	port := int32(1001)
	for _, kr := range keyranges {
		hcVTGateTest.AddTestTablet("aa", "1.1.1.1", port, keyspace, key.KeyRangeString(kr), topodatapb.TabletType_RDONLY, true, 1, nil)
		port++
	}
	sql := "select col1, col2 from table"
	bindVars := map[string]*querypb.BindVariable{}
	splitColumns := []string{"sc1", "sc2"}
	algorithm := querypb.SplitQueryRequest_FULL_SCAN
	type testCaseType struct {
		splitCount          int64
		numRowsPerQueryPart int64
	}
	testCases := []testCaseType{
		{splitCount: 100, numRowsPerQueryPart: 0},
		{splitCount: 0, numRowsPerQueryPart: 123},
	}
	for _, testCase := range testCases {
		splits, err := rpcVTGate.SplitQuery(
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
					&querypb.BoundQuery{Sql: sql, BindVariables: map[string]*querypb.BindVariable{}},
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

func TestVTGateMessageStreamSharded(t *testing.T) {
	ks := "TestVTGateMessageStreamSharded"
	createSandbox(ks)
	hcVTGateTest.Reset()
	_ = hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1002, ks, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
	ch := make(chan *sqltypes.Result)
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		kr := &topodatapb.KeyRange{End: []byte{0x40}}
		err := rpcVTGate.MessageStream(ctx, ks, "", kr, "msg", func(qr *sqltypes.Result) error {
			select {
			case <-ctx.Done():
				return io.EOF
			case ch <- qr:
			}
			return nil
		})
		if err != nil {
			t.Error(err)
		}
		close(done)
	}()
	// We should get two messages.
	<-ch
	got := <-ch
	if !reflect.DeepEqual(got, sandboxconn.SingleRowResult) {
		t.Errorf("MessageStream: %v, want %v", got, sandboxconn.SingleRowResult)
	}
	// Once we cancel, the function should return.
	cancel()
	<-done

	// Test error case.
	kr := &topodatapb.KeyRange{End: []byte{0x30}}
	err := rpcVTGate.MessageStream(context.Background(), ks, "", kr, "msg", func(qr *sqltypes.Result) error {
		ch <- qr
		return nil
	})
	want := "keyrange -30 does not exactly match shards"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MessageStream: '%v', must contain '%s'", err, want)
	}
}

func TestVTGateMessageStreamUnsharded(t *testing.T) {
	ks := KsTestUnsharded
	createSandbox(ks)
	hcVTGateTest.Reset()
	_ = hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	ch := make(chan *sqltypes.Result)
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := rpcVTGate.MessageStream(ctx, ks, "0", nil, "msg", func(qr *sqltypes.Result) error {
			select {
			case <-ctx.Done():
				return io.EOF
			case ch <- qr:
			}
			return nil
		})
		if err != nil {
			t.Error(err)
		}
		close(done)
	}()
	got := <-ch
	if !reflect.DeepEqual(got, sandboxconn.SingleRowResult) {
		t.Errorf("MessageStream: %v, want %v", got, sandboxconn.SingleRowResult)
	}
	// Function should return after cancel.
	cancel()
	<-done
}

func TestVTGateMessageStreamRetry(t *testing.T) {
	*messageStreamGracePeriod = 5 * time.Second
	defer func() {
		*messageStreamGracePeriod = 30 * time.Second
	}()
	ks := KsTestUnsharded
	createSandbox(ks)
	hcVTGateTest.Reset()
	_ = hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	ch := make(chan *sqltypes.Result)
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := rpcVTGate.MessageStream(ctx, ks, "0", nil, "msg", func(qr *sqltypes.Result) error {
			select {
			case <-ctx.Done():
				return io.EOF
			case ch <- qr:
			}
			return nil
		})
		if err != nil {
			t.Error(err)
		}
		close(done)
	}()
	<-ch

	// By default, will end the stream after the first message,
	// which should make vtgate wait for 1s (5s/5) and retry.
	start := time.Now()
	<-ch
	duration := time.Now().Sub(start)
	if duration < 1*time.Second || duration > 2*time.Second {
		t.Errorf("Retry duration should be around 1 second: %v", duration)
	}
	// Function should return after cancel.
	cancel()
	<-done
}

func TestVTGateMessageStreamUnavailable(t *testing.T) {
	*messageStreamGracePeriod = 5 * time.Second
	defer func() {
		*messageStreamGracePeriod = 30 * time.Second
	}()
	ks := KsTestUnsharded
	createSandbox(ks)
	hcVTGateTest.Reset()
	tablet := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	// Unavailable error should cause vtgate to wait 1s and retry.
	tablet.MustFailCodes[vtrpcpb.Code_UNAVAILABLE] = 1
	ch := make(chan *sqltypes.Result)
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := rpcVTGate.MessageStream(ctx, ks, "0", nil, "msg", func(qr *sqltypes.Result) error {
			select {
			case <-ctx.Done():
				return io.EOF
			case ch <- qr:
			}
			return nil
		})
		if err != nil {
			t.Error(err)
		}
		close(done)
	}()

	// Verify the 1s delay.
	start := time.Now()
	<-ch
	duration := time.Now().Sub(start)
	if duration < 1*time.Second || duration > 2*time.Second {
		t.Errorf("Retry duration should be around 1 second: %v", duration)
	}
	// Function should return after cancel.
	cancel()
	<-done
}

func TestVTGateMessageStreamGracePeriod(t *testing.T) {
	*messageStreamGracePeriod = 1 * time.Second
	defer func() {
		*messageStreamGracePeriod = 30 * time.Second
	}()
	ks := KsTestUnsharded
	createSandbox(ks)
	hcVTGateTest.Reset()
	tablet := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	// tablet should return no results for at least 5 calls for it to exceed the grace period.
	tablet.SetResults([]*sqltypes.Result{
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	})
	start := time.Now()
	err := rpcVTGate.MessageStream(context.Background(), ks, "0", nil, "msg", func(qr *sqltypes.Result) error {
		return nil
	})
	want := "has repeatedly failed for longer than 1s"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MessageStream err: %v, must contain %s", err, want)
	}
	duration := time.Now().Sub(start)
	if duration < 1*time.Second || duration > 2*time.Second {
		t.Errorf("Retry duration should be around 1 second: %v", duration)
	}
}

func TestVTGateMessageStreamFail(t *testing.T) {
	ks := KsTestUnsharded
	createSandbox(ks)
	hcVTGateTest.Reset()
	tablet := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	// tablet should should fail immediately if the error is not EOF or UNAVAILABLE.
	tablet.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1
	err := rpcVTGate.MessageStream(context.Background(), ks, "0", nil, "msg", func(qr *sqltypes.Result) error {
		return nil
	})
	want := "RESOURCE_EXHAUSTED error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MessageStream err: %v, must contain %s", err, want)
	}
}

func TestVTGateMessageAck(t *testing.T) {
	ks := KsTestUnsharded
	createSandbox(ks)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	ids := []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}, {
		Type:  sqltypes.VarChar,
		Value: []byte("2"),
	}}
	count, err := rpcVTGate.MessageAck(context.Background(), ks, "msg", ids)
	if err != nil {
		t.Error(err)
	}
	if count != 2 {
		t.Errorf("MessageAck: %d, want 2", count)
	}
	if !sqltypes.Proto3ValuesEqual(sbc.MessageIDs, ids) {
		t.Errorf("sbc1.MessageIDs: %v, want %v", sbc.MessageIDs, ids)
	}
}

func TestVTGateMessageAckKeyspaceIds(t *testing.T) {
	ks := KsTestUnsharded
	createSandbox(ks)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	idKeyspaceIDs := []*vtgatepb.IdKeyspaceId{
		{
			Id: &querypb.Value{
				Type:  sqltypes.VarChar,
				Value: []byte("1"),
			},
		},
		{
			Id: &querypb.Value{
				Type:  sqltypes.VarChar,
				Value: []byte("2"),
			},
		},
	}
	count, err := rpcVTGate.MessageAckKeyspaceIds(context.Background(), ks, "msg", idKeyspaceIDs)
	if err != nil {
		t.Error(err)
	}
	if count != 2 {
		t.Errorf("MessageAck: %d, want 2", count)
	}
	wantids := []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}, {
		Type:  sqltypes.VarChar,
		Value: []byte("2"),
	}}
	if !sqltypes.Proto3ValuesEqual(sbc.MessageIDs, wantids) {
		t.Errorf("sbc1.MessageIDs: %v, want %v", sbc.MessageIDs, wantids)
	}
}

func TestVTGateSplitQueryUnsharded(t *testing.T) {
	keyspace := KsTestUnsharded
	createSandbox(keyspace)
	hcVTGateTest.Reset()
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, keyspace, "0", topodatapb.TabletType_RDONLY, true, 1, nil)
	sql := "select col1, col2 from table"
	bindVars := map[string]*querypb.BindVariable{}
	splitColumns := []string{"sc1", "sc2"}
	algorithm := querypb.SplitQueryRequest_FULL_SCAN
	type testCaseType struct {
		splitCount          int64
		numRowsPerQueryPart int64
	}
	testCases := []testCaseType{
		{splitCount: 100, numRowsPerQueryPart: 0},
		{splitCount: 0, numRowsPerQueryPart: 123},
	}
	for _, testCase := range testCases {
		splits, err := rpcVTGate.SplitQuery(
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
			&querypb.BoundQuery{Sql: sql, BindVariables: map[string]*querypb.BindVariable{}},
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

func TestVTGateBindVarError(t *testing.T) {
	ks := KsTestUnsharded
	createSandbox(ks)
	hcVTGateTest.Reset()
	ctx := context.Background()
	session := &vtgatepb.Session{}
	bindVars := map[string]*querypb.BindVariable{
		"v": {
			Type:  querypb.Type_EXPRESSION,
			Value: []byte("1"),
		},
	}
	want := "v: invalid type specified for MakeValue: EXPRESSION"

	tcases := []struct {
		name string
		f    func() error
	}{{
		name: "Execute",
		f: func() error {
			_, _, err := rpcVTGate.Execute(ctx, session, "", bindVars)
			return err
		},
	}, {
		name: "ExecuteBatch",
		f: func() error {
			_, _, err := rpcVTGate.ExecuteBatch(ctx, session, []string{""}, []map[string]*querypb.BindVariable{bindVars})
			return err
		},
	}, {
		name: "StreamExecute",
		f: func() error {
			return rpcVTGate.StreamExecute(ctx, session, "", bindVars, func(_ *sqltypes.Result) error { return nil })
		},
	}, {
		name: "ExecuteShards",
		f: func() error {
			_, err := rpcVTGate.ExecuteShards(ctx, "", bindVars, "", []string{""}, topodatapb.TabletType_MASTER, session, false, nil)
			return err
		},
	}, {
		name: "ExecuteKeyspaceIds",
		f: func() error {
			_, err := rpcVTGate.ExecuteKeyspaceIds(ctx, "", bindVars, "", [][]byte{}, topodatapb.TabletType_MASTER, session, false, nil)
			return err
		},
	}, {
		name: "ExecuteKeyRanges",
		f: func() error {
			_, err := rpcVTGate.ExecuteKeyRanges(ctx, "", bindVars, "", []*topodatapb.KeyRange{}, topodatapb.TabletType_MASTER, session, false, nil)
			return err
		},
	}, {
		name: "ExecuteEntityIds",
		f: func() error {
			_, err := rpcVTGate.ExecuteEntityIds(ctx, "", bindVars, "", "", []*vtgatepb.ExecuteEntityIdsRequest_EntityId{}, topodatapb.TabletType_MASTER, session, false, nil)
			return err
		},
	}, {
		name: "ExecuteBatchShards",
		f: func() error {
			_, err := rpcVTGate.ExecuteBatchShards(ctx,
				[]*vtgatepb.BoundShardQuery{{
					Query: &querypb.BoundQuery{
						BindVariables: bindVars,
					},
				}},
				topodatapb.TabletType_MASTER, false, session, nil)
			return err
		},
	}, {
		name: "ExecuteBatchKeyspaceIds",
		f: func() error {
			_, err := rpcVTGate.ExecuteBatchKeyspaceIds(ctx,
				[]*vtgatepb.BoundKeyspaceIdQuery{{
					Query: &querypb.BoundQuery{
						BindVariables: bindVars,
					},
				}},
				topodatapb.TabletType_MASTER, false, session, nil)
			return err
		},
	}, {
		name: "StreamExecuteKeyspaceIds",
		f: func() error {
			return rpcVTGate.StreamExecuteKeyspaceIds(ctx, "", bindVars, "", [][]byte{}, topodatapb.TabletType_MASTER, nil, func(_ *sqltypes.Result) error { return nil })
		},
	}, {
		name: "StreamExecuteKeyRanges",
		f: func() error {
			return rpcVTGate.StreamExecuteKeyRanges(ctx, "", bindVars, "", []*topodatapb.KeyRange{}, topodatapb.TabletType_MASTER, nil, func(_ *sqltypes.Result) error { return nil })
		},
	}, {
		name: "StreamExecuteShards",
		f: func() error {
			return rpcVTGate.StreamExecuteShards(ctx, "", bindVars, "", []string{}, topodatapb.TabletType_MASTER, nil, func(_ *sqltypes.Result) error { return nil })
		},
	}, {
		name: "SplitQuery",
		f: func() error {
			_, err := rpcVTGate.SplitQuery(ctx, "", "", bindVars, []string{}, 0, 0, querypb.SplitQueryRequest_FULL_SCAN)
			return err
		},
	}}
	for _, tcase := range tcases {
		if err := tcase.f(); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%v error: %v, must contain %s", tcase.name, err, want)
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
		false,
		nil)
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
		false,
		nil)
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
		false,
		nil)
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
		false,
		nil)
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
		false,
		nil)
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
		nil,
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
		nil,
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
		nil,
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
func setUpSandboxWithTwoShards(keyspace string) (string, []*sandboxconn.SandboxConn) {
	shards := []*sandboxconn.SandboxConn{{}, {}}
	createSandbox(keyspace)
	hcVTGateTest.Reset()
	shards[0] = hcVTGateTest.AddTestTablet("aa", "-20", 1, keyspace, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	shards[1] = hcVTGateTest.AddTestTablet("aa", "20-40", 1, keyspace, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
	return keyspace, shards
}

// Verifies that 'shard' was sent exactly one query and that it
// was annotated with 'expectedKeyspaceID'
func verifyQueryAnnotatedWithKeyspaceID(t *testing.T, expectedKeyspaceID []byte, shard *sandboxconn.SandboxConn) {
	if !verifyNumQueries(t, 1, shard.Queries) {
		return
	}
	verifyBoundQueryAnnotatedWithKeyspaceID(t, expectedKeyspaceID, shard.Queries[0])
}

// Verifies that 'shard' was sent exactly one query and that it
// was annotated as unfriendly.
func verifyQueryAnnotatedAsUnfriendly(t *testing.T, shard *sandboxconn.SandboxConn) {
	if !verifyNumQueries(t, 1, shard.Queries) {
		return
	}
	verifyBoundQueryAnnotatedAsUnfriendly(t, shard.Queries[0])
}

// Verifies 'queries' has exactly 'expectedNumQueries' elements.
// Returns true if verification succeeds.
func verifyNumQueries(t *testing.T, expectedNumQueries int, queries []*querypb.BoundQuery) bool {
	numElements := len(queries)
	if numElements != expectedNumQueries {
		t.Errorf("want %v queries, got: %v (queries: %v)", expectedNumQueries, numElements, queries)
		return false
	}
	return true
}

// Verifies 'batchQueries' has exactly 'expectedNumQueries' elements.
// Returns true if verification succeeds.
func verifyNumBatchQueries(t *testing.T, expectedNumQueries int, batchQueries [][]*querypb.BoundQuery) bool {
	numElements := len(batchQueries)
	if numElements != expectedNumQueries {
		t.Errorf("want %v batch queries, got: %v (batch queries: %v)", expectedNumQueries, numElements, batchQueries)
		return false
	}
	return true
}

func verifyBoundQueryAnnotatedWithKeyspaceID(t *testing.T, expectedKeyspaceID []byte, query *querypb.BoundQuery) {
	verifyBoundQueryAnnotatedWithComment(
		t,
		"/* vtgate:: keyspace_id:"+hex.EncodeToString(expectedKeyspaceID)+" */",
		query)
}

func verifyBoundQueryAnnotatedAsUnfriendly(t *testing.T, query *querypb.BoundQuery) {
	verifyBoundQueryAnnotatedWithComment(
		t,
		"/* vtgate:: filtered_replication_unfriendly */",
		query)
}

func verifyBoundQueryAnnotatedWithComment(t *testing.T, expectedComment string, query *querypb.BoundQuery) {
	if !strings.Contains(query.Sql, expectedComment) {
		t.Errorf("want query '%v' to be annotated with '%v'", query.Sql, expectedComment)
	}
}

// Verifies that 'shard' was sent exactly one batch-query and that its
// (single) queries are annotated with the elements of expectedKeyspaceIDs
// in order.
func verifyBatchQueryAnnotatedWithKeyspaceIds(t *testing.T, expectedKeyspaceIDs [][]byte, shard *sandboxconn.SandboxConn) {
	if !verifyNumBatchQueries(t, 1, shard.BatchQueries) {
		return
	}
	verifyBoundQueriesAnnotatedWithKeyspaceIds(t, expectedKeyspaceIDs, shard.BatchQueries[0])
}

// Verifies that 'shard' was sent exactly one batch-query and that its
// (single) queries are annotated as unfriendly.
func verifyBatchQueryAnnotatedAsUnfriendly(t *testing.T, expectedNumQueries int, shard *sandboxconn.SandboxConn) {
	if !verifyNumBatchQueries(t, 1, shard.BatchQueries) {
		return
	}
	verifyBoundQueriesAnnotatedAsUnfriendly(t, expectedNumQueries, shard.BatchQueries[0])
}

func verifyBoundQueriesAnnotatedWithKeyspaceIds(t *testing.T, expectedKeyspaceIDs [][]byte, queries []*querypb.BoundQuery) {
	if !verifyNumQueries(t, len(expectedKeyspaceIDs), queries) {
		return
	}
	for i := range queries {
		verifyBoundQueryAnnotatedWithKeyspaceID(t, expectedKeyspaceIDs[i], queries[i])
	}
}

func verifyBoundQueriesAnnotatedAsUnfriendly(t *testing.T, expectedNumQueries int, queries []*querypb.BoundQuery) {
	if !verifyNumQueries(t, expectedNumQueries, queries) {
		return
	}
	for i := range queries {
		verifyBoundQueryAnnotatedAsUnfriendly(t, queries[i])
	}
}

func testErrorPropagation(t *testing.T, sbcs []*sandboxconn.SandboxConn, before func(sbc *sandboxconn.SandboxConn), after func(sbc *sandboxconn.SandboxConn), expected vtrpcpb.Code) {

	// Execute
	for _, sbc := range sbcs {
		before(sbc)
	}
	_, _, err := rpcVTGate.Execute(
		context.Background(),
		masterSession,
		"select id from t1",
		nil,
	)
	if err == nil {
		t.Errorf("error %v not propagated for Execute", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got code %v err %v, want %v", ec, err, expected)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}

	// ExecuteShards
	for _, sbc := range sbcs {
		before(sbc)
	}
	_, err = rpcVTGate.ExecuteShards(context.Background(),
		"query",
		nil,
		KsTestUnsharded,
		[]string{"0"},
		topodatapb.TabletType_MASTER,
		nil,
		false,
		executeOptions)
	if err == nil {
		t.Errorf("error %v not propagated for ExecuteShards", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}

	// ExecuteKeyspaceIds
	for _, sbc := range sbcs {
		before(sbc)
	}
	_, err = rpcVTGate.ExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		KsTestUnsharded,
		[][]byte{{0x10}},
		topodatapb.TabletType_MASTER,
		nil,
		false,
		executeOptions)
	if err == nil {
		t.Errorf("error %v not propagated for ExecuteKeyspaceIds", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}

	// ExecuteKeyRanges
	for _, sbc := range sbcs {
		before(sbc)
	}
	_, err = rpcVTGate.ExecuteKeyRanges(context.Background(),
		"query",
		nil,
		KsTestUnsharded,
		[]*topodatapb.KeyRange{{End: []byte{0x20}}},
		topodatapb.TabletType_MASTER,
		nil,
		false,
		executeOptions)
	if err == nil {
		t.Errorf("error %v not propagated for ExecuteKeyRanges", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}

	// ExecuteEntityIds
	for _, sbc := range sbcs {
		before(sbc)
	}
	_, err = rpcVTGate.ExecuteEntityIds(context.Background(),
		"query",
		nil,
		KsTestUnsharded,
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
		false,
		executeOptions)
	if err == nil {
		t.Errorf("error %v not propagated for ExecuteEntityIds", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}

	// ExecuteBatchShards
	for _, sbc := range sbcs {
		before(sbc)
	}
	_, err = rpcVTGate.ExecuteBatchShards(context.Background(),
		[]*vtgatepb.BoundShardQuery{{
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace: KsTestUnsharded,
			Shards:   []string{"0", "0"},
		}, {
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace: KsTestUnsharded,
			Shards:   []string{"0", "0"},
		}},
		topodatapb.TabletType_MASTER,
		false,
		nil,
		executeOptions)
	if err == nil {
		t.Errorf("error %v not propagated for ExecuteBatchShards", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
		}
	}
	statsKey := fmt.Sprintf("%s.%s.master.%v", "ExecuteBatchShards", KsTestUnsharded, vterrors.Code(err))
	if got, want := errorCounts.Counts()[statsKey], int64(1); got != want {
		t.Errorf("errorCounts not increased for '%s': got = %v, want = %v", statsKey, got, want)
	}
	for _, sbc := range sbcs {
		after(sbc)
	}

	// ExecuteBatchKeyspaceIds
	for _, sbc := range sbcs {
		before(sbc)
	}
	kid10 := []byte{0x10}
	kid30 := []byte{0x30}
	_, err = rpcVTGate.ExecuteBatchKeyspaceIds(context.Background(),
		[]*vtgatepb.BoundKeyspaceIdQuery{{
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace:    KsTestUnsharded,
			KeyspaceIds: [][]byte{kid10, kid30},
		}, {
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace:    KsTestUnsharded,
			KeyspaceIds: [][]byte{kid10, kid30},
		}},
		topodatapb.TabletType_MASTER,
		false,
		nil,
		executeOptions)
	if err == nil {
		t.Errorf("error %v not propagated for ExecuteBatchKeyspaceIds", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
		}
	}
	statsKey = fmt.Sprintf("%s.%s.master.%v", "ExecuteBatchKeyspaceIds", KsTestUnsharded, vterrors.Code(err))
	if got, want := errorCounts.Counts()[statsKey], int64(1); got != want {
		t.Errorf("errorCounts not increased for '%s': got = %v, want = %v", statsKey, got, want)
	}
	for _, sbc := range sbcs {
		after(sbc)
	}

	// StreamExecute
	for _, sbc := range sbcs {
		before(sbc)
	}
	err = rpcVTGate.StreamExecute(
		context.Background(),
		masterSession,
		"select id from t1",
		nil,
		func(r *sqltypes.Result) error {
			return nil
		},
	)
	if err == nil {
		t.Errorf("error %v not propagated for StreamExecute", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}

	// StreamExecuteShards
	for _, sbc := range sbcs {
		before(sbc)
	}
	err = rpcVTGate.StreamExecuteShards(context.Background(),
		"query",
		nil,
		KsTestUnsharded,
		[]string{"0"},
		topodatapb.TabletType_MASTER,
		executeOptions,
		func(r *sqltypes.Result) error {
			return nil
		})
	if err == nil {
		t.Errorf("error %v not propagated for StreamExecuteShards", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}

	// StreamExecuteKeyspaceIds
	for _, sbc := range sbcs {
		before(sbc)
	}
	err = rpcVTGate.StreamExecuteKeyspaceIds(context.Background(),
		"query",
		nil,
		KsTestUnsharded,
		[][]byte{{0x10}},
		topodatapb.TabletType_MASTER,
		executeOptions,
		func(r *sqltypes.Result) error {
			return nil
		})
	if err == nil {
		t.Errorf("error %v not propagated for StreamExecuteKeyspaceIds", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}

	// StreamExecuteKeyRanges
	for _, sbc := range sbcs {
		before(sbc)
	}
	err = rpcVTGate.StreamExecuteKeyRanges(context.Background(),
		"query",
		nil,
		KsTestUnsharded,
		[]*topodatapb.KeyRange{{End: []byte{0x20}}},
		topodatapb.TabletType_MASTER,
		executeOptions,
		func(r *sqltypes.Result) error {
			return nil
		})
	if err == nil {
		t.Errorf("error %v not propagated for StreamExecuteKeyRanges", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}

	// Begin is skipped, it doesn't end up going to the tablet.

	// Commit
	for _, sbc := range sbcs {
		before(sbc)
	}
	session := &vtgatepb.Session{
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
	err = rpcVTGate.Commit(context.Background(), false, session)
	if err == nil {
		t.Errorf("error %v not propagated for Commit", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}

	// Rollback is skipped, it doesn't forward errors.

	// SplitQuery
	for _, sbc := range sbcs {
		before(sbc)
	}
	_, err = rpcVTGate.SplitQuery(context.Background(),
		KsTestUnsharded,
		"select col1, col2 from table",
		nil,
		[]string{"sc1", "sc2"},
		100,
		0,
		querypb.SplitQueryRequest_FULL_SCAN)
	if err == nil {
		t.Errorf("error %v not propagated for SplitQuery", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}
}

// TestErrorPropagation tests an error returned by sandboxconn is
// properly propagated through vtgate layers.  We need both a master
// tablet and a rdonly tablet because we don't control the routing of
// Commit nor SplitQuery{,V2}.
func TestErrorPropagation(t *testing.T) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	sbcm := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	sbcrdonly := hcVTGateTest.AddTestTablet("aa", "1.1.1.2", 1001, KsTestUnsharded, "0", topodatapb.TabletType_RDONLY, true, 1, nil)
	sbcs := []*sandboxconn.SandboxConn{
		sbcm,
		sbcrdonly,
	}

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_CANCELED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_CANCELED] = 0
	}, vtrpcpb.Code_CANCELED)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNKNOWN] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNKNOWN] = 0
	}, vtrpcpb.Code_UNKNOWN)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 0
	}, vtrpcpb.Code_INVALID_ARGUMENT)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_DEADLINE_EXCEEDED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_DEADLINE_EXCEEDED] = 0
	}, vtrpcpb.Code_DEADLINE_EXCEEDED)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 0
	}, vtrpcpb.Code_ALREADY_EXISTS)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_PERMISSION_DENIED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_PERMISSION_DENIED] = 0
	}, vtrpcpb.Code_PERMISSION_DENIED)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 0
	}, vtrpcpb.Code_RESOURCE_EXHAUSTED)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 0
	}, vtrpcpb.Code_FAILED_PRECONDITION)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_ABORTED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_ABORTED] = 0
	}, vtrpcpb.Code_ABORTED)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_INTERNAL] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_INTERNAL] = 0
	}, vtrpcpb.Code_INTERNAL)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNAVAILABLE] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNAVAILABLE] = 0
	}, vtrpcpb.Code_UNAVAILABLE)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNAUTHENTICATED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNAUTHENTICATED] = 0
	}, vtrpcpb.Code_UNAUTHENTICATED)
}

// This test makes sure that if we start a transaction and hit a critical
// error, a rollback is issued.
func TestErrorIssuesRollback(t *testing.T) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

	// Start a transaction, send one statement.
	// Simulate an error that should trigger a rollback:
	// vtrpcpb.Code_ABORTED case.
	session, err := rpcVTGate.Begin(context.Background(), false)
	if err != nil {
		t.Fatalf("cannot start a transaction: %v", err)
	}
	session, _, err = rpcVTGate.Execute(
		context.Background(),
		session,
		"select id from t1",
		nil,
	)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if sbc.RollbackCount.Get() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Get())
	}
	sbc.MustFailCodes[vtrpcpb.Code_ABORTED] = 20
	session, _, err = rpcVTGate.Execute(
		context.Background(),
		session,
		"select id from t1",
		nil,
	)
	if err == nil {
		t.Fatalf("want error but got nil")
	}
	if sbc.RollbackCount.Get() != 1 {
		t.Errorf("want 1, got %d", sbc.RollbackCount.Get())
	}
	sbc.RollbackCount.Set(0)
	sbc.MustFailCodes[vtrpcpb.Code_ABORTED] = 0

	// Start a transaction, send one statement.
	// Simulate an error that should trigger a rollback:
	// vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED case.
	session, err = rpcVTGate.Begin(context.Background(), false)
	if err != nil {
		t.Fatalf("cannot start a transaction: %v", err)
	}
	session, _, err = rpcVTGate.Execute(
		context.Background(),
		session,
		"select id from t1",
		nil,
	)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if sbc.RollbackCount.Get() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Get())
	}
	sbc.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 20
	session, _, err = rpcVTGate.Execute(
		context.Background(),
		session,
		"select id from t1",
		nil,
	)
	if err == nil {
		t.Fatalf("want error but got nil")
	}
	if sbc.RollbackCount.Get() != 1 {
		t.Errorf("want 1, got %d", sbc.RollbackCount.Get())
	}
	sbc.RollbackCount.Set(0)
	sbc.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 0

	// Start a transaction, send one statement.
	// Simulate an error that should *not* trigger a rollback:
	// vtrpcpb.Code_ALREADY_EXISTS case.
	session, err = rpcVTGate.Begin(context.Background(), false)
	if err != nil {
		t.Fatalf("cannot start a transaction: %v", err)
	}
	session, _, err = rpcVTGate.Execute(
		context.Background(),
		session,
		"select id from t1",
		nil,
	)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if sbc.RollbackCount.Get() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Get())
	}
	sbc.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 20
	session, _, err = rpcVTGate.Execute(
		context.Background(),
		session,
		"select id from t1",
		nil,
	)
	if err == nil {
		t.Fatalf("want error but got nil")
	}
	if sbc.RollbackCount.Get() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Get())
	}
	sbc.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 0
}
