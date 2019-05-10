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
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestTxConnBegin(t *testing.T) {
	sc, sbc0, _, rss0, _, _ := newTestTxConnEnv(t, "TestTxConn")
	session := &vtgatepb.Session{}

	// begin
	if err := sc.txConn.Begin(context.Background(), NewSafeSession(session)); err != nil {
		t.Error(err)
	}
	wantSession := &vtgatepb.Session{InTransaction: true}
	if !proto.Equal(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	if _, err := sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_MASTER, NewSafeSession(session), false, nil); err != nil {
		t.Error(err)
	}

	// Begin again should cause a commit and a new begin.
	if err := sc.txConn.Begin(context.Background(), NewSafeSession(session)); err != nil {
		t.Error(err)
	}
	if !proto.Equal(session, wantSession) {
		t.Errorf("begin: %v, want %v", session, wantSession)
	}
	if commitCount := sbc0.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
}

func TestTxConnBeginDisallowed(t *testing.T) {
	sc, _, _, _, _, _ := newTestTxConnEnv(t, "TestTxConn")

	sc.txConn.mode = vtgatepb.TransactionMode_SINGLE
	session := &vtgatepb.Session{TransactionMode: vtgatepb.TransactionMode_MULTI}
	err := sc.txConn.Begin(context.Background(), NewSafeSession(session))
	wantErr := "requested transaction mode MULTI disallowed: vtgate must be started with --transaction_mode=MULTI (or TWOPC). Current transaction mode: SINGLE"
	if err == nil || err.Error() != wantErr {
		t.Errorf("txConn.Begin: %v, want %s", err, wantErr)
	}

	sc.txConn.mode = vtgatepb.TransactionMode_MULTI
	session = &vtgatepb.Session{TransactionMode: vtgatepb.TransactionMode_TWOPC}
	err = sc.txConn.Begin(context.Background(), NewSafeSession(session))
	wantErr = "requested transaction mode TWOPC disallowed: vtgate must be started with --transaction_mode=TWOPC. Current transaction mode: MULTI"
	if err == nil || err.Error() != wantErr {
		t.Errorf("txConn.Begin: %v, want %s", err, wantErr)
	}
}

func TestTxConnCommitSuccess(t *testing.T) {
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_MASTER, session, false, nil)
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
	if !proto.Equal(session.Session, &wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}
	sc.Execute(context.Background(), "query1", nil, rss01, topodatapb.TabletType_MASTER, session, false, nil)
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
	if !proto.Equal(session.Session, &wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}

	if err := sc.txConn.Commit(context.Background(), session); err != nil {
		t.Error(err)
	}
	wantSession = vtgatepb.Session{}
	if !proto.Equal(session.Session, &wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}
	if commitCount := sbc0.CommitCount.Get(); commitCount != 1 {
		t.Errorf("sbc0.CommitCount: %d, want 1", commitCount)
	}
	if commitCount := sbc1.CommitCount.Get(); commitCount != 1 {
		t.Errorf("sbc1.commitCount: %d, want 1", commitCount)
	}
}

func TestTxConnCommitOrderFailure1(t *testing.T) {
	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	queries := []*querypb.BoundQuery{{
		Sql: "query1",
	}}

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(context.Background(), rss0, queries, topodatapb.TabletType_MASTER, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_PRE)
	sc.ExecuteMultiShard(context.Background(), rss0, queries, topodatapb.TabletType_MASTER, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_POST)
	sc.ExecuteMultiShard(context.Background(), rss1, queries, topodatapb.TabletType_MASTER, session, false, false)

	sbc0.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err := sc.txConn.Commit(context.Background(), session)
	want := "INVALID_ARGUMENT error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Commit: %v, want %s", err, want)
	}

	wantSession := vtgatepb.Session{}
	if !proto.Equal(session.Session, &wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}
	if commitCount := sbc0.CommitCount.Get(); commitCount != 1 {
		t.Errorf("sbc0.CommitCount: %d, want 1", commitCount)
	}
	if rollbackCount := sbc0.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("sbc0.rollbackCount: %d, want 1", rollbackCount)
	}
	if rollbackCount := sbc1.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("sbc1.rollbackCount: %d, want 1", rollbackCount)
	}
}

func TestTxConnCommitOrderFailure2(t *testing.T) {
	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	queries := []*querypb.BoundQuery{{
		Sql: "query1",
	}}

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(context.Background(), rss1, queries, topodatapb.TabletType_MASTER, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_PRE)
	sc.ExecuteMultiShard(context.Background(), rss0, queries, topodatapb.TabletType_MASTER, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_POST)
	sc.ExecuteMultiShard(context.Background(), rss1, queries, topodatapb.TabletType_MASTER, session, false, false)

	sbc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err := sc.txConn.Commit(context.Background(), session)
	want := "INVALID_ARGUMENT error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Commit: %v, want %s", err, want)
	}

	wantSession := vtgatepb.Session{}
	if !proto.Equal(session.Session, &wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}
	if commitCount := sbc0.CommitCount.Get(); commitCount != 1 {
		t.Errorf("sbc0.CommitCount: %d, want 1", commitCount)
	}
	if commitCount := sbc1.CommitCount.Get(); commitCount != 1 {
		t.Errorf("sbc1.commitCount: %d, want 1", commitCount)
	}
	if rollbackCount := sbc1.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("sbc1.rollbackCount: %d, want 1", rollbackCount)
	}
}

func TestTxConnCommitOrderFailure3(t *testing.T) {
	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	queries := []*querypb.BoundQuery{{
		Sql: "query1",
	}}

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(context.Background(), rss0, queries, topodatapb.TabletType_MASTER, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_PRE)
	sc.ExecuteMultiShard(context.Background(), rss0, queries, topodatapb.TabletType_MASTER, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_POST)
	sc.ExecuteMultiShard(context.Background(), rss1, queries, topodatapb.TabletType_MASTER, session, false, false)

	sbc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	if err := sc.txConn.Commit(context.Background(), session); err != nil {
		t.Error(err)
	}

	// The last failed commit must generate a warning.
	wantSession := vtgatepb.Session{
		Warnings: []*querypb.QueryWarning{{
			Message: "post-operation transaction had an error: Code: INVALID_ARGUMENT\nINVALID_ARGUMENT error\n\ntarget: TestTxConn.1.master, used tablet: aa-0 (1)",
		}},
	}
	if !proto.Equal(session.Session, &wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}
	if commitCount := sbc0.CommitCount.Get(); commitCount != 2 {
		t.Errorf("sbc0.CommitCount: %d, want 2", commitCount)
	}
	if commitCount := sbc1.CommitCount.Get(); commitCount != 1 {
		t.Errorf("sbc1.commitCount: %d, want 1", commitCount)
	}
}

func TestTxConnCommitOrderSuccess(t *testing.T) {
	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	queries := []*querypb.BoundQuery{{
		Sql: "query1",
	}}

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(context.Background(), rss0, queries, topodatapb.TabletType_MASTER, session, false, false)
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
	if !proto.Equal(session.Session, &wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}

	session.SetCommitOrder(vtgatepb.CommitOrder_PRE)
	sc.ExecuteMultiShard(context.Background(), rss0, queries, topodatapb.TabletType_MASTER, session, false, false)
	wantSession = vtgatepb.Session{
		InTransaction: true,
		PreSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_MASTER,
			},
			TransactionId: 2,
		}},
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_MASTER,
			},
			TransactionId: 1,
		}},
	}
	if !proto.Equal(session.Session, &wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}

	session.SetCommitOrder(vtgatepb.CommitOrder_POST)
	sc.ExecuteMultiShard(context.Background(), rss1, queries, topodatapb.TabletType_MASTER, session, false, false)
	wantSession = vtgatepb.Session{
		InTransaction: true,
		PreSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_MASTER,
			},
			TransactionId: 2,
		}},
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_MASTER,
			},
			TransactionId: 1,
		}},
		PostSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "1",
				TabletType: topodatapb.TabletType_MASTER,
			},
			TransactionId: 1,
		}},
	}
	if !proto.Equal(session.Session, &wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}

	// Ensure nothing changes if we reuse a transaction.
	sc.ExecuteMultiShard(context.Background(), rss1, queries, topodatapb.TabletType_MASTER, session, false, false)
	if !proto.Equal(session.Session, &wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}

	if err := sc.txConn.Commit(context.Background(), session); err != nil {
		t.Error(err)
	}
	wantSession = vtgatepb.Session{}
	if !proto.Equal(session.Session, &wantSession) {
		t.Errorf("Session:\n%+v, want\n%+v", *session.Session, wantSession)
	}
	if commitCount := sbc0.CommitCount.Get(); commitCount != 2 {
		t.Errorf("sbc0.CommitCount: %d, want 2", commitCount)
	}
	if commitCount := sbc1.CommitCount.Get(); commitCount != 1 {
		t.Errorf("sbc1.commitCount: %d, want 1", commitCount)
	}
}

func TestTxConnCommit2PC(t *testing.T) {
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TestTxConnCommit2PC")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, rss01, topodatapb.TabletType_MASTER, session, false, nil)
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	if err := sc.txConn.Commit(context.Background(), session); err != nil {
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
	sc, sbc0, _, rss0, _, _ := newTestTxConnEnv(t, "TestTxConnCommit2PCOneParticipant")
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_MASTER, session, false, nil)
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	if err := sc.txConn.Commit(context.Background(), session); err != nil {
		t.Error(err)
	}
	if c := sbc0.CommitCount.Get(); c != 1 {
		t.Errorf("sbc0.CommitCount: %d, want 1", c)
	}
}

func TestTxConnCommit2PCCreateTransactionFail(t *testing.T) {
	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, "TestTxConnCommit2PCCreateTransactionFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, rss1, topodatapb.TabletType_MASTER, session, false, nil)

	sbc0.MustFailCreateTransaction = 1
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	err := sc.txConn.Commit(context.Background(), session)
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
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TestTxConnCommit2PCPrepareFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, rss01, topodatapb.TabletType_MASTER, session, false, nil)

	sbc1.MustFailPrepare = 1
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	err := sc.txConn.Commit(context.Background(), session)
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
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TestTxConnCommit2PCStartCommitFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, rss01, topodatapb.TabletType_MASTER, session, false, nil)

	sbc0.MustFailStartCommit = 1
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	err := sc.txConn.Commit(context.Background(), session)
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
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TestTxConnCommit2PCCommitPreparedFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, rss01, topodatapb.TabletType_MASTER, session, false, nil)

	sbc1.MustFailCommitPrepared = 1
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	err := sc.txConn.Commit(context.Background(), session)
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
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TestTxConnCommit2PCConcludeTransactionFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, rss01, topodatapb.TabletType_MASTER, session, false, nil)

	sbc0.MustFailConcludeTransaction = 1
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	err := sc.txConn.Commit(context.Background(), session)
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
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TxConnRollback")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_MASTER, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, rss01, topodatapb.TabletType_MASTER, session, false, nil)
	if err := sc.txConn.Rollback(context.Background(), session); err != nil {
		t.Error(err)
	}
	wantSession := vtgatepb.Session{}
	if !proto.Equal(session.Session, &wantSession) {
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
	sc, sbc0, sbc1, _, _, _ := newTestTxConnEnv(t, "TestTxConn")

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
	sc, sbc0, sbc1, _, _, _ := newTestTxConnEnv(t, "TestTxConn")

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
	sc, sbc0, sbc1, _, _, _ := newTestTxConnEnv(t, "TestTxConn")

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
	sc, _, _, _, _, _ := newTestTxConnEnv(t, "TestTxConn")

	err := sc.txConn.Resolve(context.Background(), "abcd")
	want := "invalid parts in dtid: abcd"
	if err == nil || err.Error() != want {
		t.Errorf("Resolve: %v, want %s", err, want)
	}
}

func TestTxConnResolveReadTransactionFail(t *testing.T) {
	sc, sbc0, _, _, _, _ := newTestTxConnEnv(t, "TestTxConn")

	dtid := "TestTxConn:0:1234"
	sbc0.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err := sc.txConn.Resolve(context.Background(), dtid)
	want := "INVALID_ARGUMENT error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Resolve: %v, want %s", err, want)
	}
}

func TestTxConnResolveInternalError(t *testing.T) {
	sc, sbc0, _, _, _, _ := newTestTxConnEnv(t, "TestTxConn")

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
	sc, sbc0, sbc1, _, _, _ := newTestTxConnEnv(t, "TestTxConn")

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
	sc, sbc0, sbc1, _, _, _ := newTestTxConnEnv(t, "TestTxConn")

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
	sc, sbc0, sbc1, _, _, _ := newTestTxConnEnv(t, "TestTxConn")

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
	sc, sbc0, sbc1, _, _, _ := newTestTxConnEnv(t, "TestTxConn")

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

func newTestTxConnEnv(t *testing.T, name string) (sc *ScatterConn, sbc0, sbc1 *sandboxconn.SandboxConn, rss0, rss1, rss01 []*srvtopo.ResolvedShard) {
	createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	sc = newTestScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 = hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "1", 1, name, "1", topodatapb.TabletType_MASTER, true, 1, nil)
	res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
	var err error
	rss0, err = res.ResolveDestination(context.Background(), name, topodatapb.TabletType_MASTER, key.DestinationShard("0"))
	if err != nil {
		t.Fatalf("ResolveDestination(0) failed: %v", err)
	}
	rss1, err = res.ResolveDestination(context.Background(), name, topodatapb.TabletType_MASTER, key.DestinationShard("1"))
	if err != nil {
		t.Fatalf("ResolveDestination(1) failed: %v", err)
	}
	rss01, err = res.ResolveDestination(context.Background(), name, topodatapb.TabletType_MASTER, key.DestinationShards([]string{"0", "1"}))
	if err != nil {
		t.Fatalf("ResolveDestination(0, 1) failed: %v", err)
	}
	return sc, sbc0, sbc1, rss0, rss1, rss01
}
