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
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/sandboxconn"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestTxConnCommitRollbackIncorrectSession(t *testing.T) {
	sc, _, _ := newTestTxConnEnv("TestTxConn")
	// nil session
	err := sc.txConn.Commit(context.Background(), false, nil)
	if got := vterrors.Code(err); got != vtrpcpb.Code_INVALID_ARGUMENT {
		t.Errorf("Commit: %v, want %v", got, vtrpcpb.Code_INVALID_ARGUMENT)
	}

	err = sc.txConn.Rollback(context.Background(), nil)
	if err != nil {
		t.Error(err)
	}

	// not in transaction
	session := NewSafeSession(&vtgatepb.Session{})
	err = sc.txConn.Commit(context.Background(), false, session)
	if got := vterrors.Code(err); got != vtrpcpb.Code_ABORTED {
		t.Errorf("Commit: %v, want %v", got, vtrpcpb.Code_ABORTED)
	}
}

func TestTxConnCommitSuccess(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestTxConn", []string{"0"}, topodatapb.TabletType_MASTER, session, false, nil)
	wantSession := vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_MASTER,
			},
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(*session.Session, wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}
	sc.Execute(context.Background(), "query1", nil, "TestTxConn", []string{"0", "1"}, topodatapb.TabletType_MASTER, session, false, nil)
	wantSession = vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_MASTER,
			},
			TransactionId: 1,
		}, {
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "1",
				TabletType: topodatapb.TabletType_MASTER,
			},
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(*session.Session, wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}

	sbc0.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err := sc.txConn.Commit(context.Background(), false, session)
	want := "INVALID_ARGUMENT error"
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
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConnCommit2PC")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PC", []string{"0"}, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PC", []string{"0", "1"}, topodatapb.TabletType_MASTER, session, false, nil)
	if err := sc.txConn.Commit(context.Background(), true, session); err != nil {
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
	if c := sbc0.ConcludeTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.ConcludeTransactionCount: %d, want 1", c)
	}
}

func TestTxConnCommit2PCOneParticipant(t *testing.T) {
	sc, sbc0, _ := newTestTxConnEnv("TestTxConnCommit2PCOneParticipant")
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PCOneParticipant", []string{"0"}, topodatapb.TabletType_MASTER, session, false, nil)
	if err := sc.txConn.Commit(context.Background(), true, session); err != nil {
		t.Error(err)
	}
	if c := sbc0.CommitCount.Get(); c != 1 {
		t.Errorf("sbc0.CommitCount: %d, want 1", c)
	}
}

func TestTxConnCommit2PCCreateTransactionFail(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConnCommit2PCCreateTransactionFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PCCreateTransactionFail", []string{"0"}, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PCCreateTransactionFail", []string{"1"}, topodatapb.TabletType_MASTER, session, false, nil)

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
	if c := sbc0.ConcludeTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ConcludeTransactionCount: %d, want 0", c)
	}
}

func TestTxConnCommit2PCPrepareFail(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConnCommit2PCPrepareFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PCPrepareFail", []string{"0"}, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PCPrepareFail", []string{"0", "1"}, topodatapb.TabletType_MASTER, session, false, nil)

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
	if c := sbc0.ConcludeTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ConcludeTransactionCount: %d, want 0", c)
	}
}

func TestTxConnCommit2PCStartCommitFail(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConnCommit2PCStartCommitFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PCStartCommitFail", []string{"0"}, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PCStartCommitFail", []string{"0", "1"}, topodatapb.TabletType_MASTER, session, false, nil)

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
	if c := sbc0.ConcludeTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ConcludeTransactionCount: %d, want 0", c)
	}
}

func TestTxConnCommit2PCCommitPreparedFail(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConnCommit2PCCommitPreparedFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PCCommitPreparedFail", []string{"0"}, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PCCommitPreparedFail", []string{"0", "1"}, topodatapb.TabletType_MASTER, session, false, nil)

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
	if c := sbc0.ConcludeTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ConcludeTransactionCount: %d, want 0", c)
	}
}

func TestTxConnCommit2PCConcludeTransactionFail(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConnCommit2PCConcludeTransactionFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PCConcludeTransactionFail", []string{"0"}, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PCConcludeTransactionFail", []string{"0", "1"}, topodatapb.TabletType_MASTER, session, false, nil)

	sbc0.MustFailConcludeTransaction = 1
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
	if c := sbc0.ConcludeTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.ConcludeTransactionCount: %d, want 1", c)
	}
}

func TestTxConnRollback(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestTxConn", []string{"0"}, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestTxConn", []string{"0", "1"}, topodatapb.TabletType_MASTER, session, false, nil)
	if err := sc.txConn.Rollback(context.Background(), session); err != nil {
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

func TestTxConnResolveOnPrepare(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:1234"
	sbc0.ReadTransactionResults = []*querypb.TransactionMetadata{{
		Dtid:  dtid,
		State: querypb.TransactionState_PREPARE,
		Participants: []*querypb.Target{{
			Keyspace:   "TestTxConn",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}}
	err := sc.txConn.Resolve(context.Background(), dtid)
	if err != nil {
		t.Error(err)
	}
	if c := sbc0.SetRollbackCount.Get(); c != 1 {
		t.Errorf("sbc0.SetRollbackCount: %d, want 1", c)
	}
	if c := sbc1.RollbackPreparedCount.Get(); c != 1 {
		t.Errorf("sbc1.RollbackPreparedCount: %d, want 1", c)
	}
	if c := sbc1.CommitPreparedCount.Get(); c != 0 {
		t.Errorf("sbc1.CommitPreparedCount: %d, want 0", c)
	}
	if c := sbc0.ConcludeTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.ConcludeTransactionCount: %d, want 1", c)
	}
}

func TestTxConnResolveOnRollback(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:1234"
	sbc0.ReadTransactionResults = []*querypb.TransactionMetadata{{
		Dtid:  dtid,
		State: querypb.TransactionState_ROLLBACK,
		Participants: []*querypb.Target{{
			Keyspace:   "TestTxConn",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}}
	if err := sc.txConn.Resolve(context.Background(), dtid); err != nil {
		t.Error(err)
	}
	if c := sbc0.SetRollbackCount.Get(); c != 0 {
		t.Errorf("sbc0.SetRollbackCount: %d, want 0", c)
	}
	if c := sbc1.RollbackPreparedCount.Get(); c != 1 {
		t.Errorf("sbc1.RollbackPreparedCount: %d, want 1", c)
	}
	if c := sbc1.CommitPreparedCount.Get(); c != 0 {
		t.Errorf("sbc1.CommitPreparedCount: %d, want 0", c)
	}
	if c := sbc0.ConcludeTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.ConcludeTransactionCount: %d, want 1", c)
	}
}

func TestTxConnResolveOnCommit(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:1234"
	sbc0.ReadTransactionResults = []*querypb.TransactionMetadata{{
		Dtid:  dtid,
		State: querypb.TransactionState_COMMIT,
		Participants: []*querypb.Target{{
			Keyspace:   "TestTxConn",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}}
	if err := sc.txConn.Resolve(context.Background(), dtid); err != nil {
		t.Error(err)
	}
	if c := sbc0.SetRollbackCount.Get(); c != 0 {
		t.Errorf("sbc0.SetRollbackCount: %d, want 0", c)
	}
	if c := sbc1.RollbackPreparedCount.Get(); c != 0 {
		t.Errorf("sbc1.RollbackPreparedCount: %d, want 0", c)
	}
	if c := sbc1.CommitPreparedCount.Get(); c != 1 {
		t.Errorf("sbc1.CommitPreparedCount: %d, want 1", c)
	}
	if c := sbc0.ConcludeTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.ConcludeTransactionCount: %d, want 1", c)
	}
}

func TestTxConnResolveInvalidDTID(t *testing.T) {
	sc, _, _ := newTestTxConnEnv("TestTxConn")

	err := sc.txConn.Resolve(context.Background(), "abcd")
	want := "invalid parts in dtid: abcd"
	if err == nil || err.Error() != want {
		t.Errorf("Resolve: %v, want %s", err, want)
	}
}

func TestTxConnResolveReadTransactionFail(t *testing.T) {
	sc, sbc0, _ := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:1234"
	sbc0.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err := sc.txConn.Resolve(context.Background(), dtid)
	want := "INVALID_ARGUMENT error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Resolve: %v, want %s", err, want)
	}
}

func TestTxConnResolveInternalError(t *testing.T) {
	sc, sbc0, _ := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:1234"
	sbc0.ReadTransactionResults = []*querypb.TransactionMetadata{{
		Dtid:  dtid,
		State: querypb.TransactionState_UNKNOWN,
		Participants: []*querypb.Target{{
			Keyspace:   "TestTxConn",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}}
	err := sc.txConn.Resolve(context.Background(), dtid)
	want := "invalid state: UNKNOWN"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Resolve: %v, want %s", err, want)
	}
}

func TestTxConnResolveSetRollbackFail(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:1234"
	sbc0.ReadTransactionResults = []*querypb.TransactionMetadata{{
		Dtid:  dtid,
		State: querypb.TransactionState_PREPARE,
		Participants: []*querypb.Target{{
			Keyspace:   "TestTxConn",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}}
	sbc0.MustFailSetRollback = 1
	err := sc.txConn.Resolve(context.Background(), dtid)
	want := "error: err"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Resolve: %v, want %s", err, want)
	}
	if c := sbc0.SetRollbackCount.Get(); c != 1 {
		t.Errorf("sbc0.SetRollbackCount: %d, want 1", c)
	}
	if c := sbc1.RollbackPreparedCount.Get(); c != 0 {
		t.Errorf("sbc1.RollbackPreparedCount: %d, want 0", c)
	}
	if c := sbc1.CommitPreparedCount.Get(); c != 0 {
		t.Errorf("sbc1.CommitPreparedCount: %d, want 0", c)
	}
	if c := sbc0.ConcludeTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ConcludeTransactionCount: %d, want 0", c)
	}
}

func TestTxConnResolveRollbackPreparedFail(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:1234"
	sbc0.ReadTransactionResults = []*querypb.TransactionMetadata{{
		Dtid:  dtid,
		State: querypb.TransactionState_ROLLBACK,
		Participants: []*querypb.Target{{
			Keyspace:   "TestTxConn",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}}
	sbc1.MustFailRollbackPrepared = 1
	err := sc.txConn.Resolve(context.Background(), dtid)
	want := "error: err"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Resolve: %v, want %s", err, want)
	}
	if c := sbc0.SetRollbackCount.Get(); c != 0 {
		t.Errorf("sbc0.SetRollbackCount: %d, want 0", c)
	}
	if c := sbc1.RollbackPreparedCount.Get(); c != 1 {
		t.Errorf("sbc1.RollbackPreparedCount: %d, want 1", c)
	}
	if c := sbc1.CommitPreparedCount.Get(); c != 0 {
		t.Errorf("sbc1.CommitPreparedCount: %d, want 0", c)
	}
	if c := sbc0.ConcludeTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ConcludeTransactionCount: %d, want 0", c)
	}
}

func TestTxConnResolveCommitPreparedFail(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:1234"
	sbc0.ReadTransactionResults = []*querypb.TransactionMetadata{{
		Dtid:  dtid,
		State: querypb.TransactionState_COMMIT,
		Participants: []*querypb.Target{{
			Keyspace:   "TestTxConn",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}}
	sbc1.MustFailCommitPrepared = 1
	err := sc.txConn.Resolve(context.Background(), dtid)
	want := "error: err"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Resolve: %v, want %s", err, want)
	}
	if c := sbc0.SetRollbackCount.Get(); c != 0 {
		t.Errorf("sbc0.SetRollbackCount: %d, want 0", c)
	}
	if c := sbc1.RollbackPreparedCount.Get(); c != 0 {
		t.Errorf("sbc1.RollbackPreparedCount: %d, want 0", c)
	}
	if c := sbc1.CommitPreparedCount.Get(); c != 1 {
		t.Errorf("sbc1.CommitPreparedCount: %d, want 1", c)
	}
	if c := sbc0.ConcludeTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ConcludeTransactionCount: %d, want 0", c)
	}
}

func TestTxConnResolveConcludeTransactionFail(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:1234"
	sbc0.ReadTransactionResults = []*querypb.TransactionMetadata{{
		Dtid:  dtid,
		State: querypb.TransactionState_COMMIT,
		Participants: []*querypb.Target{{
			Keyspace:   "TestTxConn",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}}
	sbc0.MustFailConcludeTransaction = 1
	err := sc.txConn.Resolve(context.Background(), dtid)
	want := "error: err"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Resolve: %v, want %s", err, want)
	}
	if c := sbc0.SetRollbackCount.Get(); c != 0 {
		t.Errorf("sbc0.SetRollbackCount: %d, want 0", c)
	}
	if c := sbc1.RollbackPreparedCount.Get(); c != 0 {
		t.Errorf("sbc1.RollbackPreparedCount: %d, want 0", c)
	}
	if c := sbc1.CommitPreparedCount.Get(); c != 1 {
		t.Errorf("sbc1.CommitPreparedCount: %d, want 1", c)
	}
	if c := sbc0.ConcludeTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.ConcludeTransactionCount: %d, want 1", c)
	}
}

func TestTxConnMultiGoSessions(t *testing.T) {
	txc := &TxConn{}

	input := []*vtgatepb.Session_ShardSession{{
		Target: &querypb.Target{
			Keyspace: "0",
		},
	}}
	err := txc.runSessions(input, func(s *vtgatepb.Session_ShardSession) error {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "err %s", s.Target.Keyspace)
	})
	want := "err 0"
	if err == nil || err.Error() != want {
		t.Errorf("runSessions(1): %v, want %s", err, want)
	}

	input = []*vtgatepb.Session_ShardSession{{
		Target: &querypb.Target{
			Keyspace: "0",
		},
	}, {
		Target: &querypb.Target{
			Keyspace: "1",
		},
	}}
	err = txc.runSessions(input, func(s *vtgatepb.Session_ShardSession) error {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "err %s", s.Target.Keyspace)
	})
	want = "err 0\nerr 1"
	if err == nil || err.Error() != want {
		t.Errorf("runSessions(2): %v, want %s", err, want)
	}
	wantCode := vtrpcpb.Code_INTERNAL
	if code := vterrors.Code(err); code != wantCode {
		t.Errorf("Error code: %v, want %v", code, wantCode)
	}

	err = txc.runSessions(input, func(s *vtgatepb.Session_ShardSession) error {
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func TestTxConnMultiGoTargets(t *testing.T) {
	txc := &TxConn{}
	input := []*querypb.Target{{
		Keyspace: "0",
	}}
	err := txc.runTargets(input, func(t *querypb.Target) error {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "err %s", t.Keyspace)
	})
	want := "err 0"
	if err == nil || err.Error() != want {
		t.Errorf("runTargets(1): %v, want %s", err, want)
	}

	input = []*querypb.Target{{
		Keyspace: "0",
	}, {
		Keyspace: "1",
	}}
	err = txc.runTargets(input, func(t *querypb.Target) error {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "err %s", t.Keyspace)
	})
	want = "err 0\nerr 1"
	if err == nil || err.Error() != want {
		t.Errorf("runTargets(2): %v, want %s", err, want)
	}
	wantCode := vtrpcpb.Code_INTERNAL
	if code := vterrors.Code(err); code != wantCode {
		t.Errorf("Error code: %v, want %v", code, wantCode)
	}

	err = txc.runTargets(input, func(t *querypb.Target) error {
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func newTestTxConnEnv(name string) (sc *ScatterConn, sbc0, sbc1 *sandboxconn.SandboxConn) {
	createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	sc = newTestScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 = hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "1", 1, name, "1", topodatapb.TabletType_MASTER, true, 1, nil)
	return sc, sbc0, sbc1
}
