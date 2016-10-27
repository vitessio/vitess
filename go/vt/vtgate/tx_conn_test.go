// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/tabletserver/sandboxconn"
	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestTxConnCommitRollbackIncorrectSession(t *testing.T) {
	sc, _, _ := newTestTxConnEnv("TestTxConn")
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
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConnCommit2PC")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PC", []string{"0"}, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PC", []string{"0", "1"}, topodatapb.TabletType_MASTER, session, false, nil)
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
	sc, sbc0, _ := newTestTxConnEnv("TestTxConnCommit2PCOneParticipant")
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PCOneParticipant", []string{"0"}, topodatapb.TabletType_MASTER, session, false, nil)
	err := sc.txConn.Commit(context.Background(), true, session)
	if err != nil {
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
	if c := sbc0.ResolveTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 0", c)
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
	if c := sbc0.ResolveTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 0", c)
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
	if c := sbc0.ResolveTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 0", c)
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
	if c := sbc0.ResolveTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 0", c)
	}
}

func TestTxConnCommit2PCResolveTransactionFail(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConnCommit2PCResolveTransactionFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PCResolveTransactionFail", []string{"0"}, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestTxConnCommit2PCResolveTransactionFail", []string{"0", "1"}, topodatapb.TabletType_MASTER, session, false, nil)

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
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, "TestTxConn", []string{"0"}, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, "TestTxConn", []string{"0", "1"}, topodatapb.TabletType_MASTER, session, false, nil)
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

func TestTxConnResumeOnPrepare(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:0:1234"
	sbc0.ReadTransactionResults = []*querypb.TransactionMetadata{{
		Dtid:  dtid,
		State: querypb.TransactionState_PREPARE,
		Participants: []*querypb.Target{{
			Keyspace:   "TestTxConn",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}}
	err := sc.txConn.Resume(context.Background(), dtid)
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
	if c := sbc0.ResolveTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 1", c)
	}
}

func TestTxConnResumeOnRollback(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:0:1234"
	sbc0.ReadTransactionResults = []*querypb.TransactionMetadata{{
		Dtid:  dtid,
		State: querypb.TransactionState_ROLLBACK,
		Participants: []*querypb.Target{{
			Keyspace:   "TestTxConn",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}}
	err := sc.txConn.Resume(context.Background(), dtid)
	if err != nil {
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
	if c := sbc0.ResolveTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 1", c)
	}
}

func TestTxConnResumeOnCommit(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:0:1234"
	sbc0.ReadTransactionResults = []*querypb.TransactionMetadata{{
		Dtid:  dtid,
		State: querypb.TransactionState_COMMIT,
		Participants: []*querypb.Target{{
			Keyspace:   "TestTxConn",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}}
	err := sc.txConn.Resume(context.Background(), dtid)
	if err != nil {
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
	if c := sbc0.ResolveTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 1", c)
	}
}

func TestTxConnResumeInvalidDTID(t *testing.T) {
	sc, _, _ := newTestTxConnEnv("TestTxConn")

	err := sc.txConn.Resume(context.Background(), "abcd")
	want := "invalid parts in dtid: abcd"
	if err == nil || err.Error() != want {
		t.Errorf("Resume: %v, want %s", err, want)
	}
}

func TestTxConnResumeReadTransactionFail(t *testing.T) {
	sc, sbc0, _ := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:0:1234"
	sbc0.MustFailServer = 1
	err := sc.txConn.Resume(context.Background(), dtid)
	want := "error: err"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Resume: %v, want %s", err, want)
	}
}

func TestTxConnResumeInternalError(t *testing.T) {
	sc, sbc0, _ := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:0:1234"
	sbc0.ReadTransactionResults = []*querypb.TransactionMetadata{{
		Dtid:  dtid,
		State: querypb.TransactionState_UNKNOWN,
		Participants: []*querypb.Target{{
			Keyspace:   "TestTxConn",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}}
	err := sc.txConn.Resume(context.Background(), dtid)
	want := "invalid state: UNKNOWN"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Resume: %v, want %s", err, want)
	}
}

func TestTxConnResumeSetRollbackFail(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:0:1234"
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
	err := sc.txConn.Resume(context.Background(), dtid)
	want := "error: err"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Resume: %v, want %s", err, want)
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
	if c := sbc0.ResolveTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 0", c)
	}
}

func TestTxConnResumeRollbackPreparedFail(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:0:1234"
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
	err := sc.txConn.Resume(context.Background(), dtid)
	want := "error: err"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Resume: %v, want %s", err, want)
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
	if c := sbc0.ResolveTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 0", c)
	}
}

func TestTxConnResumeCommitPreparedFail(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:0:1234"
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
	err := sc.txConn.Resume(context.Background(), dtid)
	want := "error: err"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Resume: %v, want %s", err, want)
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
	if c := sbc0.ResolveTransactionCount.Get(); c != 0 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 0", c)
	}
}

func TestTxConnResumeResolveTransactionFail(t *testing.T) {
	sc, sbc0, sbc1 := newTestTxConnEnv("TestTxConn")

	dtid := "TestTxConn:0:0:1234"
	sbc0.ReadTransactionResults = []*querypb.TransactionMetadata{{
		Dtid:  dtid,
		State: querypb.TransactionState_COMMIT,
		Participants: []*querypb.Target{{
			Keyspace:   "TestTxConn",
			Shard:      "1",
			TabletType: topodatapb.TabletType_MASTER,
		}},
	}}
	sbc0.MustFailResolveTransaction = 1
	err := sc.txConn.Resume(context.Background(), dtid)
	want := "error: err"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Resume: %v, want %s", err, want)
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
	if c := sbc0.ResolveTransactionCount.Get(); c != 1 {
		t.Errorf("sbc0.ResolveTransactionCount: %d, want 1", c)
	}
}

func TestTxConnMultiGoSessions(t *testing.T) {
	txc := &TxConn{}

	input := []*vtgatepb.Session_ShardSession{{
		Target: &querypb.Target{
			Keyspace: "0",
		},
	}}
	err := txc.multiGoSessions(input, func(s *vtgatepb.Session_ShardSession) error {
		return vterrors.FromError(vtrpcpb.ErrorCode_INTERNAL_ERROR, fmt.Errorf("err %s", s.Target.Keyspace))
	})
	want := "err 0"
	if err == nil || err.Error() != want {
		t.Errorf("multiGoSessions(1): %v, want %s", err, want)
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
	err = txc.multiGoSessions(input, func(s *vtgatepb.Session_ShardSession) error {
		return vterrors.FromError(vtrpcpb.ErrorCode_INTERNAL_ERROR, fmt.Errorf("err %s", s.Target.Keyspace))
	})
	want = "err 0\nerr 1"
	if err == nil || err.Error() != want {
		t.Errorf("multiGoSessions(2): %v, want %s", err, want)
	}
	errCode := err.(*ScatterConnError).VtErrorCode()
	wantCode := vtrpcpb.ErrorCode_INTERNAL_ERROR
	if errCode != wantCode {
		t.Errorf("Error code: %v, want %v", errCode, wantCode)
	}

	err = txc.multiGoSessions(input, func(s *vtgatepb.Session_ShardSession) error {
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
	err := txc.multiGoTargets(input, func(t *querypb.Target) error {
		return vterrors.FromError(vtrpcpb.ErrorCode_INTERNAL_ERROR, fmt.Errorf("err %s", t.Keyspace))
	})
	want := "err 0"
	if err == nil || err.Error() != want {
		t.Errorf("multiGoTargets(1): %v, want %s", err, want)
	}

	input = []*querypb.Target{{
		Keyspace: "0",
	}, {
		Keyspace: "1",
	}}
	err = txc.multiGoTargets(input, func(t *querypb.Target) error {
		return vterrors.FromError(vtrpcpb.ErrorCode_INTERNAL_ERROR, fmt.Errorf("err %s", t.Keyspace))
	})
	want = "err 0\nerr 1"
	if err == nil || err.Error() != want {
		t.Errorf("multiGoTargets(2): %v, want %s", err, want)
	}
	errCode := err.(*ScatterConnError).VtErrorCode()
	wantCode := vtrpcpb.ErrorCode_INTERNAL_ERROR
	if errCode != wantCode {
		t.Errorf("Error code: %v, want %v", errCode, wantCode)
	}

	err = txc.multiGoTargets(input, func(t *querypb.Target) error {
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func TestDTID(t *testing.T) {
	in := &vtgatepb.Session_ShardSession{
		Target: &querypb.Target{
			Keyspace:   "aa",
			Shard:      "0",
			TabletType: topodatapb.TabletType_MASTER,
		},
		TransactionId: 1,
	}
	txc := &TxConn{}
	dtid := txc.generateDTID(in)
	want := "aa:0:0:1"
	if dtid != want {
		t.Errorf("generateDTID: %s, want %s", dtid, want)
	}
	out, err := txc.dtidToShardSession(dtid)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("dtidToShardSession: %+v, want %+v", out, in)
	}
	_, err = txc.dtidToShardSession("badParts")
	want = "invalid parts in dtid: badParts"
	if err == nil || err.Error() != want {
		t.Errorf("dtidToShardSession(\"badParts\"): %v, want %s", err, want)
	}
	_, err = txc.dtidToShardSession("a:b:0:badid")
	want = "invalid transaction id in dtid: a:b:0:badid"
	if err == nil || err.Error() != want {
		t.Errorf("dtidToShardSession(\"a:b:0:badid\"): %v, want %s", err, want)
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
