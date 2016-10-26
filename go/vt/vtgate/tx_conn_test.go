// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestTxConnCommitRollbackIncorrectSession(t *testing.T) {
	createSandbox("TestScatterCommitRollbackIncorrectSession")
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	hc.AddTestTablet("aa", "0", 1, "TestScatterCommitRollbackIncorrectSession", "0", topodatapb.TabletType_REPLICA, true, 1, nil)

	// nil session
	err := sc.txConn.Commit(context.Background(), false, nil)
	if got := vterrors.RecoverVtErrorCode(err); got != vtrpcpb.ErrorCode_BAD_INPUT {
		t.Errorf("Commit: %v, want %v", got, vtrpcpb.ErrorCode_BAD_INPUT)
	}

	err = sc.txConn.Rollback(context.Background(), nil)
	if err != nil {
		t.Error(err)
	}

	// not in transaction
	session := NewSafeSession(&vtgatepb.Session{})
	err = sc.txConn.Commit(context.Background(), false, session)
	if got := vterrors.RecoverVtErrorCode(err); got != vtrpcpb.ErrorCode_NOT_IN_TX {
		t.Errorf("Commit: %v, want %v", got, vtrpcpb.ErrorCode_NOT_IN_TX)
	}
}

func TestTxConnCommitSuccess(t *testing.T) {
	createSandbox("TestScatterConnCommitSuccess")
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestScatterConnCommitSuccess", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestScatterConnCommitSuccess", "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnCommitSuccess", []string{"0"}, topodatapb.TabletType_REPLICA, session, false, nil)
	wantSession := vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestScatterConnCommitSuccess",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
			},
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(*session.Session, wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnCommitSuccess", []string{"0", "1"}, topodatapb.TabletType_REPLICA, session, false, nil)
	wantSession = vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestScatterConnCommitSuccess",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
			},
			TransactionId: 1,
		}, {
			Target: &querypb.Target{
				Keyspace:   "TestScatterConnCommitSuccess",
				Shard:      "1",
				TabletType: topodatapb.TabletType_REPLICA,
			},
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(*session.Session, wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}

	sbc0.MustFailServer = 1
	err := sc.txConn.Commit(context.Background(), false, session)
	want := "error: err"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Commit: %v, want %s", err, want)
	}
	wantSession = vtgatepb.Session{}
	if !reflect.DeepEqual(*session.Session, wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}
	if commitCount := sbc0.CommitCount.Get(); commitCount != 1 {
		t.Errorf("sbc0.CommitCount: %d, want 1", commitCount)
	}
	if rollbackCount := sbc1.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("sbc1.RollbackCount: %d, want 1", rollbackCount)
	}
}

func TestTxConnCommit2PC(t *testing.T) {
	createSandbox("TestTxConnCommit2PC")
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestScatterConnRollback", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestScatterConnRollback", "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0"}, topodatapb.TabletType_REPLICA, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0", "1"}, topodatapb.TabletType_REPLICA, session, false, nil)
	err := sc.txConn.Commit(context.Background(), true, session)
	if err != nil {
		t.Error(err)
	}
	if c := sbc0.CreateTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.CreateTransactionCount: %d, want 1", c)
	}
	if c := sbc1.PrepareCount.Get(); c != 1 {
		t.Errorf("sbc1.PrepareCount: %d, want 1", c)
	}
	if c := sbc0.StartCommitCount.Get(); c != 1 {
		t.Errorf("sbc0.StartCommitCount: %d, want 1", c)
	}
	if c := sbc1.CommitPreparedCount.Get(); c != 1 {
		t.Errorf("sbc1.CommitPreparedCount: %d, want 1", c)
	}
	if c := sbc0.ResolveTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 1", c)
	}
}

func TestTxConnCommit2PCOneParticipant(t *testing.T) {
	createSandbox("TestTxConnCommit2PCOneParticipant")
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestScatterConnRollback", "0", topodatapb.TabletType_REPLICA, true, 1, nil)

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0"}, topodatapb.TabletType_REPLICA, session, false, nil)
	err := sc.txConn.Commit(context.Background(), true, session)
	if err != nil {
		t.Error(err)
	}
	if c := sbc0.CommitCount.Get(); c != 1 {
		t.Errorf("sbc0.CommitCount: %d, want 1", c)
	}
}

func TestTxConnCommit2PCCreateTransactionFail(t *testing.T) {
	createSandbox("TestTxConnCommit2PCCreateTransactionFail")
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestScatterConnRollback", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestScatterConnRollback", "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0"}, topodatapb.TabletType_REPLICA, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0", "1"}, topodatapb.TabletType_REPLICA, session, false, nil)

	sbc0.MustFailCreateTransaction = 1
	err := sc.txConn.Commit(context.Background(), true, session)
	want := "error: err"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Commit: %v, must contain %s", err, want)
	}
	if c := sbc0.CreateTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.CreateTransactionCount: %d, want 1", c)
	}
	if c := sbc0.RollbackCount.Get(); c != 1 {
		t.Errorf("sbc0.RollbackCount: %d, want 1", c)
	}
	if c := sbc1.RollbackCount.Get(); c != 1 {
		t.Errorf("sbc1.RollbackCount: %d, want 1", c)
	}
	if c := sbc1.PrepareCount.Get(); c != 0 {
		t.Errorf("sbc1.PrepareCount: %d, want 0", c)
	}
	if c := sbc0.StartCommitCount.Get(); c != 0 {
		t.Errorf("sbc0.StartCommitCount: %d, want 0", c)
	}
	if c := sbc1.CommitPreparedCount.Get(); c != 0 {
		t.Errorf("sbc1.CommitPreparedCount: %d, want 0", c)
	}
	if c := sbc0.ResolveTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 0", c)
	}
}

func TestTxConnCommit2PCPrepareFail(t *testing.T) {
	createSandbox("TestTxConnCommit2PCPrepareFail")
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestScatterConnRollback", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestScatterConnRollback", "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0"}, topodatapb.TabletType_REPLICA, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0", "1"}, topodatapb.TabletType_REPLICA, session, false, nil)

	sbc1.MustFailPrepare = 1
	err := sc.txConn.Commit(context.Background(), true, session)
	want := "error: err"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Commit: %v, must contain %s", err, want)
	}
	if c := sbc0.CreateTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.CreateTransactionCount: %d, want 1", c)
	}
	if c := sbc1.PrepareCount.Get(); c != 1 {
		t.Errorf("sbc1.PrepareCount: %d, want 1", c)
	}
	if c := sbc0.StartCommitCount.Get(); c != 0 {
		t.Errorf("sbc0.StartCommitCount: %d, want 0", c)
	}
	if c := sbc1.CommitPreparedCount.Get(); c != 0 {
		t.Errorf("sbc1.CommitPreparedCount: %d, want 0", c)
	}
	if c := sbc0.ResolveTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 0", c)
	}
}

func TestTxConnCommit2PCStartCommitFail(t *testing.T) {
	createSandbox("TestTxConnCommit2PCStartCommitFail")
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestScatterConnRollback", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestScatterConnRollback", "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0"}, topodatapb.TabletType_REPLICA, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0", "1"}, topodatapb.TabletType_REPLICA, session, false, nil)

	sbc0.MustFailStartCommit = 1
	err := sc.txConn.Commit(context.Background(), true, session)
	want := "error: err"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Commit: %v, must contain %s", err, want)
	}
	if c := sbc0.CreateTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.CreateTransactionCount: %d, want 1", c)
	}
	if c := sbc1.PrepareCount.Get(); c != 1 {
		t.Errorf("sbc1.PrepareCount: %d, want 1", c)
	}
	if c := sbc0.StartCommitCount.Get(); c != 1 {
		t.Errorf("sbc0.StartCommitCount: %d, want 1", c)
	}
	if c := sbc1.CommitPreparedCount.Get(); c != 0 {
		t.Errorf("sbc1.CommitPreparedCount: %d, want 0", c)
	}
	if c := sbc0.ResolveTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 0", c)
	}
}

func TestTxConnCommit2PCCommitPreparedFail(t *testing.T) {
	createSandbox("TestTxConnCommit2PCCommitPreparedFail")
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestScatterConnRollback", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestScatterConnRollback", "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0"}, topodatapb.TabletType_REPLICA, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0", "1"}, topodatapb.TabletType_REPLICA, session, false, nil)

	sbc1.MustFailCommitPrepared = 1
	err := sc.txConn.Commit(context.Background(), true, session)
	want := "error: err"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Commit: %v, must contain %s", err, want)
	}
	if c := sbc0.CreateTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.CreateTransactionCount: %d, want 1", c)
	}
	if c := sbc1.PrepareCount.Get(); c != 1 {
		t.Errorf("sbc1.PrepareCount: %d, want 1", c)
	}
	if c := sbc0.StartCommitCount.Get(); c != 1 {
		t.Errorf("sbc0.StartCommitCount: %d, want 1", c)
	}
	if c := sbc1.CommitPreparedCount.Get(); c != 1 {
		t.Errorf("sbc1.CommitPreparedCount: %d, want 1", c)
	}
	if c := sbc0.ResolveTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 0", c)
	}
}

func TestTxConnCommit2PCResolveTransactionFail(t *testing.T) {
	createSandbox("TestTxConnCommit2PCResolveTransactionFail")
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestScatterConnRollback", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestScatterConnRollback", "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0"}, topodatapb.TabletType_REPLICA, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0", "1"}, topodatapb.TabletType_REPLICA, session, false, nil)

	sbc0.MustFailResolveTransaction = 1
	err := sc.txConn.Commit(context.Background(), true, session)
	want := "error: err"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Commit: %v, must contain %s", err, want)
	}
	if c := sbc0.CreateTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.CreateTransactionCount: %d, want 1", c)
	}
	if c := sbc1.PrepareCount.Get(); c != 1 {
		t.Errorf("sbc1.PrepareCount: %d, want 1", c)
	}
	if c := sbc0.StartCommitCount.Get(); c != 1 {
		t.Errorf("sbc0.StartCommitCount: %d, want 1", c)
	}
	if c := sbc1.CommitPreparedCount.Get(); c != 1 {
		t.Errorf("sbc1.CommitPreparedCount: %d, want 1", c)
	}
	if c := sbc0.ResolveTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 1", c)
	}
}

func TestTxConnRollback(t *testing.T) {
	createSandbox("TestScatterConnRollback")
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestScatterConnRollback", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestScatterConnRollback", "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0"}, topodatapb.TabletType_REPLICA, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0", "1"}, topodatapb.TabletType_REPLICA, session, false, nil)
	err := sc.txConn.Rollback(context.Background(), session)
	if err != nil {
		t.Error(err)
	}
	wantSession := vtgatepb.Session{}
	if !reflect.DeepEqual(*session.Session, wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}
	if c := sbc0.RollbackCount.Get(); c != 1 {
		t.Errorf("sbc0.RollbackCount: %d, want 1", c)
	}
	if c := sbc1.RollbackCount.Get(); c != 1 {
		t.Errorf("sbc1.RollbackCount: %d, want 1", c)
	}
}

func TestTxConnErrorConsolidation(t *testing.T) {
	createSandbox("TestTxConnErrorConsolidation")
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, topo.Server{}, new(sandboxTopo), "", "aa", retryCount, nil)
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestScatterConnRollback", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestScatterConnRollback", "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0"}, topodatapb.TabletType_REPLICA, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestScatterConnRollback", []string{"0", "1"}, topodatapb.TabletType_REPLICA, session, false, nil)
	sbc0.MustFailServer = 1
	sbc1.MustFailServer = 1
	err := sc.txConn.Rollback(context.Background(), session)
	want0 := "TestScatterConnRollback.0.replica"
	want1 := "TestScatterConnRollback.1.replica"
	if err == nil || !strings.Contains(err.Error(), want0) || !strings.Contains(err.Error(), want1) {
		t.Errorf("Rollback: %v, must contain %s and %s", err, want0, want1)
	}
}
