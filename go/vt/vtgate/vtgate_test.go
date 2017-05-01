// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/sandboxconn"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// This file uses the sandbox_test framework.

var hcVTGateTest *discovery.FakeHealthCheck

var executeOptions = &querypb.ExecuteOptions{
	IncludedFields: querypb.ExecuteOptions_TYPE_ONLY,
}

func init() {
	getSandbox(KsTestUnsharded).VSchema = `
{
	"Sharded": false,
	"Tables": {
		"t1": {}
	}
}
`
	hcVTGateTest = discovery.NewFakeHealthCheck()
	*transactionMode = "multi"
	Init(context.Background(), hcVTGateTest, topo.Server{}, new(sandboxTopo), "aa", 10, nil)
}

func TestVTGateBegin(t *testing.T) {
	save := rpcVTGate.transactionMode
	defer func() {
		rpcVTGate.transactionMode = save
	}()

	rpcVTGate.transactionMode = TxSingle
	got, err := rpcVTGate.Begin(context.Background(), true)
	if err != nil {
		t.Error(err)
	}
	wantSession := &vtgatepb.Session{
		InTransaction: true,
		SingleDb:      true,
	}
	if !reflect.DeepEqual(got, wantSession) {
		t.Errorf("Begin(single): %v, want %v", got, wantSession)
	}

	_, err = rpcVTGate.Begin(context.Background(), false)
	wantErr := "multi-db transaction disallowed"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Begin(multi): %v, want %s", err, wantErr)
	}

	rpcVTGate.transactionMode = TxMulti
	got, err = rpcVTGate.Begin(context.Background(), true)
	if err != nil {
		t.Error(err)
	}
	wantSession = &vtgatepb.Session{
		InTransaction: true,
		SingleDb:      true,
	}
	if !reflect.DeepEqual(got, wantSession) {
		t.Errorf("Begin(single): %v, want %v", got, wantSession)
	}

	got, err = rpcVTGate.Begin(context.Background(), false)
	if err != nil {
		t.Error(err)
	}
	wantSession = &vtgatepb.Session{
		InTransaction: true,
	}
	if !reflect.DeepEqual(got, wantSession) {
		t.Errorf("Begin(single): %v, want %v", got, wantSession)
	}

	rpcVTGate.transactionMode = TxTwoPC
	got, err = rpcVTGate.Begin(context.Background(), true)
	if err != nil {
		t.Error(err)
	}
	wantSession = &vtgatepb.Session{
		InTransaction: true,
		SingleDb:      true,
	}
	if !reflect.DeepEqual(got, wantSession) {
		t.Errorf("Begin(single): %v, want %v", got, wantSession)
	}

	got, err = rpcVTGate.Begin(context.Background(), false)
	if err != nil {
		t.Error(err)
	}
	wantSession = &vtgatepb.Session{
		InTransaction: true,
	}
	if !reflect.DeepEqual(got, wantSession) {
		t.Errorf("Begin(single): %v, want %v", got, wantSession)
	}
}

func TestVTGateCommit(t *testing.T) {
	save := rpcVTGate.transactionMode
	defer func() {
		rpcVTGate.transactionMode = save
	}()

	session := &vtgatepb.Session{
		InTransaction: true,
	}

	rpcVTGate.transactionMode = TxSingle
	err := rpcVTGate.Commit(context.Background(), true, session)
	wantErr := "2pc transaction disallowed"
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

	rpcVTGate.transactionMode = TxMulti
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

	rpcVTGate.transactionMode = TxTwoPC
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
	_, qr, err := rpcVTGate.Execute(context.Background(),
		"select id from t1",
		nil,
		"",
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
	if !proto.Equal(sbc.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc.Options[0], executeOptions)
	}

	session, err := rpcVTGate.Begin(context.Background(), false)
	if !session.InTransaction {
		t.Errorf("want true, got false")
	}
	rpcVTGate.Execute(context.Background(),
		"select id from t1",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		session,
		false,
		nil)
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

	rpcVTGate.Commit(context.Background(), false, session)
	if commitCount := sbc.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	session, err = rpcVTGate.Begin(context.Background(), false)
	rpcVTGate.Execute(context.Background(),
		"select id from t1",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		session,
		false,
		nil)
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
	_, qr, err := rpcVTGate.Execute(context.Background(),
		"select id from none",
		nil,
		KsTestUnsharded,
		topodatapb.TabletType_MASTER,
		nil,
		false,
		nil)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if !reflect.DeepEqual(sandboxconn.SingleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", sandboxconn.SingleRowResult, qr)
	}

	// Invalid keyspace.
	_, _, err = rpcVTGate.Execute(context.Background(),
		"select id from none",
		nil,
		"invalid_keyspace",
		topodatapb.TabletType_MASTER,
		nil,
		false,
		nil)
	want := "vtgate: : keyspace invalid_keyspace not found in vschema"
	if err == nil || err.Error() != want {
		t.Errorf("Execute: %v, want %s", err, want)
	}

	// Valid keyspace/shard.
	_, qr, err = rpcVTGate.Execute(context.Background(),
		"random statement",
		nil,
		KsTestUnsharded+"/0",
		topodatapb.TabletType_MASTER,
		nil,
		false,
		nil)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if !reflect.DeepEqual(sandboxconn.SingleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", sandboxconn.SingleRowResult, qr)
	}

	// Invalid keyspace/shard.
	_, _, err = rpcVTGate.Execute(context.Background(),
		"select id from none",
		nil,
		KsTestUnsharded+"/noshard",
		topodatapb.TabletType_MASTER,
		nil,
		false,
		nil)
	want = "vtgate: : target: TestUnsharded.noshard.master, no valid tablet"
	if err == nil || err.Error() != want {
		t.Errorf("Execute: %v, want %s", err, want)
	}
}

func TestVTGateIntercept(t *testing.T) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

	// begin.
	session, _, err := rpcVTGate.Execute(context.Background(), "begin", nil, "", topodatapb.TabletType_MASTER, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession := &vtgatepb.Session{InTransaction: true}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	if commitCount := sbc.CommitCount.Get(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}

	// Begin again should cause a commit and a new begin.
	session, _, err = rpcVTGate.Execute(context.Background(), "select id from t1", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	session, _, err = rpcVTGate.Execute(context.Background(), "begin", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{InTransaction: true}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	if commitCount := sbc.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	// commit.
	session, _, err = rpcVTGate.Execute(context.Background(), "select id from t1", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	session, _, err = rpcVTGate.Execute(context.Background(), "commit", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	if commitCount := sbc.CommitCount.Get(); commitCount != 2 {
		t.Errorf("want 2, got %d", commitCount)
	}

	// Commit again should be a no-op.
	session, _, err = rpcVTGate.Execute(context.Background(), "select id from t1", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	session, _, err = rpcVTGate.Execute(context.Background(), "Commit", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	if commitCount := sbc.CommitCount.Get(); commitCount != 2 {
		t.Errorf("want 2, got %d", commitCount)
	}

	// rollback
	session, _, err = rpcVTGate.Execute(context.Background(), "begin", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	session, _, err = rpcVTGate.Execute(context.Background(), "select id from t1", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	session, _, err = rpcVTGate.Execute(context.Background(), "rollback", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	if rollbackCount := sbc.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("want 1, got %d", rollbackCount)
	}

	// rollback again should be a no-op
	session, _, err = rpcVTGate.Execute(context.Background(), "select id from t1", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	session, _, err = rpcVTGate.Execute(context.Background(), "RollBack", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	if rollbackCount := sbc.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("want 1, got %d", rollbackCount)
	}

	// set.
	session, _, err = rpcVTGate.Execute(context.Background(), "set autocommit=1", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{Autocommit: true}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	session, _, err = rpcVTGate.Execute(context.Background(), "set AUTOCOMMIT = 0", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}

	// complex set
	_, _, err = rpcVTGate.Execute(context.Background(), "set autocommit=1+1", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	wantErr := "invalid syntax: 1 + 1"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Execute: %v, want %s", err, wantErr)
	}

	// multi-set
	_, _, err = rpcVTGate.Execute(context.Background(), "set autocommit=1, a = 2", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	wantErr = "too many set values: set autocommit=1, a = 2"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Execute: %v, want %s", err, wantErr)
	}

	// unsupported set
	_, _, err = rpcVTGate.Execute(context.Background(), "set a = 2", nil, "", topodatapb.TabletType_MASTER, session, false, nil)
	wantErr = "unsupported construct: set a = 2"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Execute: %v, want %s", err, wantErr)
	}
}

func TestVTGateAutocommit(t *testing.T) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

	// autocommit = 0
	session, _, err := rpcVTGate.Execute(context.Background(), "select id from t1", nil, "", topodatapb.TabletType_MASTER, nil, false, executeOptions)
	if err != nil {
		t.Fatal(err)
	}
	wantSession := &vtgatepb.Session{}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("autocommit=0: %v, want %v", session, wantSession)
	}

	// autocommit = 1
	session, _, err = rpcVTGate.Execute(context.Background(), "set autocommit=1", nil, "", topodatapb.TabletType_MASTER, session, false, executeOptions)
	if err != nil {
		t.Fatal(err)
	}
	session, _, err = rpcVTGate.Execute(context.Background(), "update t1 set id=1", nil, "", topodatapb.TabletType_MASTER, session, false, executeOptions)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{Autocommit: true}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", session, wantSession)
	}
	if commitCount := sbc.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	// autocommit = 1, "begin"
	session, _, err = rpcVTGate.Execute(context.Background(), "begin", nil, "", topodatapb.TabletType_MASTER, session, false, executeOptions)
	if err != nil {
		t.Fatal(err)
	}
	session, _, err = rpcVTGate.Execute(context.Background(), "update t1 set id=1", nil, "", topodatapb.TabletType_MASTER, session, false, executeOptions)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{InTransaction: true, Autocommit: true}
	testSession := *session
	testSession.ShardSessions = nil
	if !reflect.DeepEqual(&testSession, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", &testSession, wantSession)
	}
	if commitCount := sbc.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
	session, _, err = rpcVTGate.Execute(context.Background(), "commit", nil, "", topodatapb.TabletType_MASTER, session, false, executeOptions)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{Autocommit: true}
	if !reflect.DeepEqual(session, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", session, wantSession)
	}
	if commitCount := sbc.CommitCount.Get(); commitCount != 2 {
		t.Errorf("want 2, got %d", commitCount)
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
	if !reflect.DeepEqual(wantSession, session) {
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
	if !reflect.DeepEqual(wantSession, session) {
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
	if !reflect.DeepEqual(wantSession, session) {
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
	if !reflect.DeepEqual(wantSession, session) {
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
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

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

	session, qrl, err := rpcVTGate.ExecuteBatch(context.Background(), sqlList, nil, "", topodatapb.TabletType_MASTER, nil, executeOptions)
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
	if commitCount := sbc.CommitCount.Get(); commitCount != 2 {
		t.Errorf("want 2, got %d", commitCount)
	}
	if rollbackCount := sbc.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("want 1, got %d", rollbackCount)
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
}

func TestVTGateStreamExecute(t *testing.T) {
	ks := KsTestUnsharded
	shard := "0"
	createSandbox(ks)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard, topodatapb.TabletType_MASTER, true, 1, nil)
	var qrs []*sqltypes.Result
	err := rpcVTGate.StreamExecute(context.Background(),
		"select id from t1",
		nil,
		"",
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

func TestVTGateStreamExecuteKeyspaceShard(t *testing.T) {
	ks := KsTestUnsharded
	shard := "0"
	createSandbox(ks)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard, topodatapb.TabletType_MASTER, true, 1, nil)
	var qrs []*sqltypes.Result
	err := rpcVTGate.StreamExecute(context.Background(),
		"random statement",
		nil,
		ks+"/"+shard,
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
	bindVars := map[string]interface{}{"bv1": nil}
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

func TestVTGateMessageStreamSharded(t *testing.T) {
	ks := "TestVTGateMessageStreamSharded"
	shard1 := "-20"
	shard2 := "20-40"
	createSandbox(ks)
	hcVTGateTest.Reset()
	_ = hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard1, topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1002, ks, shard2, topodatapb.TabletType_MASTER, true, 1, nil)
	ch := make(chan *sqltypes.Result)
	done := make(chan struct{})
	go func() {
		kr := &topodatapb.KeyRange{End: []byte{0x40}}
		err := rpcVTGate.MessageStream(context.Background(), ks, "", kr, "msg", func(qr *sqltypes.Result) error {
			ch <- qr
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
	<-done

	// Test error case.
	kr := &topodatapb.KeyRange{End: []byte{0x30}}
	err := rpcVTGate.MessageStream(context.Background(), ks, "", kr, "msg", func(qr *sqltypes.Result) error {
		ch <- qr
		return nil
	})
	want := "keyrange -30 does not exactly match shards"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MessageStream: %v, must contain %s", err, want)
	}
}

func TestVTGateMessageStreamUnsharded(t *testing.T) {
	ks := KsTestUnsharded
	createSandbox(ks)
	hcVTGateTest.Reset()
	_ = hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	ch := make(chan *sqltypes.Result)
	done := make(chan struct{})
	go func() {
		err := rpcVTGate.MessageStream(context.Background(), ks, "0", nil, "msg", func(qr *sqltypes.Result) error {
			ch <- qr
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
	<-done
}

func TestVTGateMessageAckUnsharded(t *testing.T) {
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
	if !reflect.DeepEqual(sbc.MessageIDs, ids) {
		t.Errorf("sbc1.MessageIDs: %v, want %v", sbc.MessageIDs, ids)
	}
}

func TestVTGateSplitQueryUnsharded(t *testing.T) {
	keyspace := KsTestUnsharded
	createSandbox(keyspace)
	hcVTGateTest.Reset()
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, keyspace, "0", topodatapb.TabletType_RDONLY, true, 1, nil)
	sql := "select col1, col2 from table"
	bindVars := map[string]interface{}{"bv1": nil}
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
	verifyBoundQueryAnnotatedWithKeyspaceID(t, expectedKeyspaceID, &shard.Queries[0])
}

// Verifies that 'shard' was sent exactly one query and that it
// was annotated as unfriendly.
func verifyQueryAnnotatedAsUnfriendly(t *testing.T, shard *sandboxconn.SandboxConn) {
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

func testErrorPropagation(t *testing.T, sbcs []*sandboxconn.SandboxConn, before func(sbc *sandboxconn.SandboxConn), after func(sbc *sandboxconn.SandboxConn), expected vtrpcpb.Code) {

	// Execute
	for _, sbc := range sbcs {
		before(sbc)
	}
	_, _, err := rpcVTGate.Execute(context.Background(),
		"select id from t1",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		nil,
		false,
		nil)
	if err == nil {
		t.Errorf("error %v not propagated for Execute", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
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
		t.Errorf("error %v not propagated for ExecuteBatchShards", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}

	// StreamExecute
	for _, sbc := range sbcs {
		before(sbc)
	}
	err = rpcVTGate.StreamExecute(context.Background(),
		"select id from t1",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		executeOptions,
		func(r *sqltypes.Result) error {
			return nil
		})
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
	session, _, err = rpcVTGate.Execute(context.Background(),
		"select id from t1",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		session,
		false,
		nil)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if sbc.RollbackCount.Get() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Get())
	}
	sbc.MustFailCodes[vtrpcpb.Code_ABORTED] = 20
	session, _, err = rpcVTGate.Execute(context.Background(),
		"select id from t1",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		session,
		false,
		nil)
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
	session, _, err = rpcVTGate.Execute(context.Background(),
		"select id from t1",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		session,
		false,
		nil)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if sbc.RollbackCount.Get() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Get())
	}
	sbc.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 20
	session, _, err = rpcVTGate.Execute(context.Background(),
		"select id from t1",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		session,
		false,
		nil)
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
	session, _, err = rpcVTGate.Execute(context.Background(),
		"select id from t1",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		session,
		false,
		nil)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if sbc.RollbackCount.Get() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Get())
	}
	sbc.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 20
	session, _, err = rpcVTGate.Execute(context.Background(),
		"select id from t1",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		session,
		false,
		nil)
	if err == nil {
		t.Fatalf("want error but got nil")
	}
	if sbc.RollbackCount.Get() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Get())
	}
	sbc.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 0
}

func valuesContain(rows [][]sqltypes.Value, ks string) bool {
	for _, v := range rows {
		if len(v) != 1 {
			return false
		}
		if v[0].String() == ks {
			return true
		}
	}
	return false
}

func TestVTGateShowMetadataUnsharded(t *testing.T) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

	_, qr, err := rpcVTGate.Execute(context.Background(),
		"show databases",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		nil,
		false,
		executeOptions)

	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	wantFields := []*querypb.Field{{
		Name: "Databases",
		Type: sqltypes.VarChar,
	}}

	if !reflect.DeepEqual(wantFields, qr.Fields) {
		t.Errorf("want \n%+v, got \n%+v", wantFields, qr.Fields)
	}

	if !valuesContain(qr.Rows, KsTestUnsharded) {
		t.Errorf("ks %s not found in Values \n%+v", KsTestUnsharded, qr.Rows)
	}

	_, qr, err = rpcVTGate.Execute(context.Background(),
		"show vitess_keyspaces",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		nil,
		false,
		executeOptions)

	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	wantFields = []*querypb.Field{{
		Name: "Databases",
		Type: sqltypes.VarChar,
	}}

	if !reflect.DeepEqual(wantFields, qr.Fields) {
		t.Errorf("want \n%+v, got \n%+v", wantFields, qr.Fields)
	}

	if !valuesContain(qr.Rows, KsTestUnsharded) {
		t.Errorf("ks %s not found in Values \n%+v", KsTestUnsharded, qr.Rows)
	}

	_, qr, err = rpcVTGate.Execute(context.Background(),
		"show vitess_shards",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		nil,
		false,
		executeOptions)

	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	wantFields = []*querypb.Field{{
		Name: "Shards",
		Type: sqltypes.VarChar,
	}}

	if !reflect.DeepEqual(wantFields, qr.Fields) {
		t.Errorf("want \n%+v, got \n%+v", wantFields, qr.Fields)
	}

	shard := KsTestUnsharded + "/0"
	if !valuesContain(qr.Rows, shard) {
		t.Errorf("shard %s not found in Values \n%+v", shard, qr.Rows)
	}

	_, qr, err = rpcVTGate.Execute(context.Background(),
		"show vschema_tables",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		nil,
		false,
		executeOptions)

	expected := "vtgate: : No keyspace selected"
	if err == nil || err.Error() != expected {
		t.Errorf("wanted %s, got %v", expected, err)
	}

	_, qr, err = rpcVTGate.Execute(context.Background(),
		"show vschema_tables",
		nil,
		"no_such_keyspace",
		topodatapb.TabletType_MASTER,
		nil,
		false,
		executeOptions)

	expected = "vtgate: : keyspace no_such_keyspace not found in vschema"
	if err == nil || err.Error() != expected {
		t.Errorf("wanted %s, got %v", expected, err)
	}

	_, qr, err = rpcVTGate.Execute(context.Background(),
		"show vschema_tables",
		nil,
		KsTestUnsharded,
		topodatapb.TabletType_MASTER,
		nil,
		false,
		executeOptions)

	if err != nil {
		t.Errorf("wanted nil, got %v", err)
	}

	wantFields = []*querypb.Field{{
		Name: "Tables",
		Type: sqltypes.VarChar,
	}}

	if !reflect.DeepEqual(wantFields, qr.Fields) {
		t.Errorf("want \n%+v, got \n%+v", wantFields, qr.Fields)
	}

	if !valuesContain(qr.Rows, "t1") {
		t.Errorf("table %s not found in Values \n%+v", "t1", qr.Rows)
	}

	_, qr, err = rpcVTGate.Execute(context.Background(),
		"show create databases",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		nil,
		false,
		executeOptions)

	expected = "vtgate: : unsupported show statement"
	if err == nil || err.Error() != expected {
		t.Errorf("wanted %s, got %v", expected, err)
	}
	_, qr, err = rpcVTGate.Execute(context.Background(),
		"show tables",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		nil,
		false,
		executeOptions)

	expected = "vtgate: : unimplemented metadata query: show tables"
	if err == nil || err.Error() != expected {
		t.Errorf("wanted %s, got %v", expected, err)
	}
}

func TestVTGateShowMetadataTwoShards(t *testing.T) {
	keyspace := "TestShowMetadataTwoShards"
	setUpSandboxWithTwoShards(keyspace)

	_, qr, err := rpcVTGate.Execute(context.Background(),
		"show databases",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		nil,
		false,
		executeOptions)

	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	wantFields := []*querypb.Field{{
		Name: "Databases",
		Type: sqltypes.VarChar,
	}}

	if !reflect.DeepEqual(wantFields, qr.Fields) {
		t.Errorf("want \n%+v, got \n%+v", wantFields, qr.Fields)
	}

	if !valuesContain(qr.Rows, keyspace) {
		t.Errorf("ks %s not found in Values \n%+v", keyspace, qr.Rows)
	}

	_, qr, err = rpcVTGate.Execute(context.Background(),
		"show vitess_shards",
		nil,
		"",
		topodatapb.TabletType_MASTER,
		nil,
		false,
		executeOptions)

	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	wantFields = []*querypb.Field{{
		Name: "Shards",
		Type: sqltypes.VarChar,
	}}

	if !reflect.DeepEqual(wantFields, qr.Fields) {
		t.Errorf("want \n%+v, got \n%+v", wantFields, qr.Fields)
	}

	shard0 := keyspace + "/-20"
	if !valuesContain(qr.Rows, shard0) {
		t.Errorf("shard %s not found in Values \n%+v", shard0, qr.Rows)
	}

	shard1 := keyspace + "/20-40"
	if !valuesContain(qr.Rows, shard1) {
		t.Errorf("shard %s not found in Values \n%+v", shard1, qr.Rows)
	}
}

func TestParseKeyspaceOptionalShard(t *testing.T) {
	testcases := []struct {
		keyspaceShard string
		keyspace      string
		shard         string
	}{{
		keyspaceShard: "ks",
		keyspace:      "ks",
		shard:         "",
	}, {
		keyspaceShard: "ks/-80",
		keyspace:      "ks",
		shard:         "-80",
	}, {
		keyspaceShard: "ks:-80",
		keyspace:      "ks",
		shard:         "-80",
	}}

	for _, tcase := range testcases {
		if keyspace, shard := parseKeyspaceOptionalShard(tcase.keyspaceShard); keyspace != tcase.keyspace || shard != tcase.shard {
			t.Errorf("parseKeyspaceShard(%s): %s:%s, want %s:%s", tcase.keyspaceShard, keyspace, shard, tcase.keyspace, tcase.shard)
		}
	}
}
