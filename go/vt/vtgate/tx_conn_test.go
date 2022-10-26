/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"

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

var queries = []*querypb.BoundQuery{{Sql: "query1"}}
var twoQueries = []*querypb.BoundQuery{{Sql: "query1"}, {Sql: "query1"}}

func TestTxConnBegin(t *testing.T) {
	sc, sbc0, _, rss0, _, _ := newTestTxConnEnv(t, "TestTxConn")
	session := &vtgatepb.Session{}

	// begin
	safeSession := NewSafeSession(session)
	err := sc.txConn.Begin(ctx, safeSession)
	require.NoError(t, err)
	wantSession := vtgatepb.Session{InTransaction: true}
	utils.MustMatch(t, &wantSession, session, "Session")
	_, errors := sc.ExecuteMultiShard(ctx, rss0, queries, safeSession, false, false)
	require.Empty(t, errors)

	// Begin again should cause a commit and a new begin.
	require.NoError(t,
		sc.txConn.Begin(ctx, safeSession))
	utils.MustMatch(t, &wantSession, session, "Session")
	assert.EqualValues(t, 1, sbc0.CommitCount.Get(), "sbc0.CommitCount")
}

func TestTxConnCommitFailure(t *testing.T) {
	sc, sbc0, sbc1, rss0, rss1, rss01 := newTestTxConnEnv(t, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	wantSession := vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbc0.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	sc.ExecuteMultiShard(ctx, rss01, twoQueries, session, false, false)
	wantSession = vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbc0.Tablet().Alias,
		}, {
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbc1.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	sbc1.MustFailCodes[vtrpcpb.Code_DEADLINE_EXCEEDED] = 1

	expectErr := NewShardError(vterrors.New(
		vtrpcpb.Code_DEADLINE_EXCEEDED,
		fmt.Sprintf("%v error", vtrpcpb.Code_DEADLINE_EXCEEDED)),
		rss1[0].Target)

	require.ErrorContains(t, sc.txConn.Commit(ctx, session), expectErr.Error())
	wantSession = vtgatepb.Session{}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbc0.CommitCount.Get(), "sbc0.CommitCount")
	assert.EqualValues(t, 1, sbc1.CommitCount.Get(), "sbc1.CommitCount")
}

func TestTxConnCommitSuccess(t *testing.T) {
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	wantSession := vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbc0.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	sc.ExecuteMultiShard(ctx, rss01, twoQueries, session, false, false)
	wantSession = vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbc0.Tablet().Alias,
		}, {
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbc1.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	require.NoError(t,
		sc.txConn.Commit(ctx, session))
	wantSession = vtgatepb.Session{}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbc0.CommitCount.Get(), "sbc0.CommitCount")
	assert.EqualValues(t, 1, sbc1.CommitCount.Get(), "sbc1.CommitCount")
}

func TestTxConnReservedCommitSuccess(t *testing.T) {
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true, InReservedConn: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	wantSession := vtgatepb.Session{
		InTransaction:  true,
		InReservedConn: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			ReservedId:    1,
			TabletAlias:   sbc0.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	sc.ExecuteMultiShard(ctx, rss01, twoQueries, session, false, false)
	wantSession = vtgatepb.Session{
		InTransaction:  true,
		InReservedConn: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			ReservedId:    1,
			TabletAlias:   sbc0.Tablet().Alias,
		}, {
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			ReservedId:    1,
			TabletAlias:   sbc1.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	require.NoError(t,
		sc.txConn.Commit(ctx, session))
	wantSession = vtgatepb.Session{
		InReservedConn: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  2,
			TabletAlias: sbc0.Tablet().Alias,
		}, {
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  2,
			TabletAlias: sbc1.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbc0.CommitCount.Get(), "sbc0.CommitCount")
	assert.EqualValues(t, 1, sbc1.CommitCount.Get(), "sbc1.CommitCount")

	require.NoError(t,
		sc.txConn.Release(ctx, session))
	wantSession = vtgatepb.Session{InReservedConn: true}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbc0.ReleaseCount.Get(), "sbc0.ReleaseCount")
	assert.EqualValues(t, 1, sbc1.ReleaseCount.Get(), "sbc1.ReleaseCount")
}

func TestTxConnReservedOn2ShardTxOn1ShardAndCommit(t *testing.T) {
	keyspace := "TestTxConn"
	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, keyspace)
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	// Sequence the executes to ensure shard session order
	session := NewSafeSession(&vtgatepb.Session{InReservedConn: true})

	// this will create reserved connections against all tablets
	_, errs := sc.ExecuteMultiShard(ctx, rss1, queries, session, false, false)
	require.Empty(t, errs)
	_, errs = sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	require.Empty(t, errs)

	wantSession := vtgatepb.Session{
		InReservedConn: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   keyspace,
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  1,
			TabletAlias: sbc1.Tablet().Alias,
		}, {
			Target: &querypb.Target{
				Keyspace:   keyspace,
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  1,
			TabletAlias: sbc0.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	session.Session.InTransaction = true

	// start a transaction against rss0
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	wantSession = vtgatepb.Session{
		InTransaction:  true,
		InReservedConn: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   keyspace,
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  1,
			TabletAlias: sbc1.Tablet().Alias,
		}, {
			Target: &querypb.Target{
				Keyspace:   keyspace,
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			ReservedId:    1,
			TabletAlias:   sbc0.Tablet().Alias,
		}},
	}

	utils.MustMatch(t, &wantSession, session.Session, "Session")

	require.NoError(t,
		sc.txConn.Commit(ctx, session))

	wantSession = vtgatepb.Session{
		InReservedConn: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   keyspace,
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  1,
			TabletAlias: sbc1.Tablet().Alias,
		}, {
			Target: &querypb.Target{
				Keyspace:   keyspace,
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  2,
			TabletAlias: sbc0.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbc0.CommitCount.Get(), "sbc0.CommitCount")
	assert.EqualValues(t, 0, sbc1.CommitCount.Get(), "sbc1.CommitCount")
}

func TestTxConnReservedOn2ShardTxOn1ShardAndRollback(t *testing.T) {
	keyspace := "TestTxConn"
	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, keyspace)
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	// Sequence the executes to ensure shard session order
	session := NewSafeSession(&vtgatepb.Session{InReservedConn: true})

	// this will create reserved connections against all tablets
	_, errs := sc.ExecuteMultiShard(ctx, rss1, queries, session, false, false)
	require.Empty(t, errs)
	_, errs = sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	require.Empty(t, errs)

	wantSession := vtgatepb.Session{
		InReservedConn: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   keyspace,
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  1,
			TabletAlias: sbc1.Tablet().Alias,
		}, {
			Target: &querypb.Target{
				Keyspace:   keyspace,
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  1,
			TabletAlias: sbc0.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	session.Session.InTransaction = true

	// start a transaction against rss0
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	wantSession = vtgatepb.Session{
		InTransaction:  true,
		InReservedConn: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   keyspace,
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  1,
			TabletAlias: sbc1.Tablet().Alias,
		}, {
			Target: &querypb.Target{
				Keyspace:   keyspace,
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			ReservedId:    1,
			TabletAlias:   sbc0.Tablet().Alias,
		}},
	}

	utils.MustMatch(t, &wantSession, session.Session, "Session")

	require.NoError(t,
		sc.txConn.Rollback(ctx, session))

	wantSession = vtgatepb.Session{
		InReservedConn: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   keyspace,
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  1,
			TabletAlias: sbc1.Tablet().Alias,
		}, {
			Target: &querypb.Target{
				Keyspace:   keyspace,
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  2,
			TabletAlias: sbc0.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbc0.RollbackCount.Get(), "sbc0.RollbackCount")
	assert.EqualValues(t, 0, sbc1.RollbackCount.Get(), "sbc1.RollbackCount")
}

func TestTxConnCommitOrderFailure1(t *testing.T) {
	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	queries := []*querypb.BoundQuery{{Sql: "query1"}}

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_PRE)
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_POST)
	sc.ExecuteMultiShard(ctx, rss1, queries, session, false, false)

	sbc0.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err := sc.txConn.Commit(ctx, session)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "INVALID_ARGUMENT error", "commit error")

	wantSession := vtgatepb.Session{}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbc0.CommitCount.Get(), "sbc0.CommitCount")
	// first commit failed so we don't try to commit the second shard
	assert.EqualValues(t, 0, sbc1.CommitCount.Get(), "sbc1.CommitCount")
	// When the commit fails, we try to clean up by issuing a rollback
	assert.EqualValues(t, 2, sbc0.ReleaseCount.Get(), "sbc0.ReleaseCount")
	assert.EqualValues(t, 1, sbc1.ReleaseCount.Get(), "sbc1.ReleaseCount")
}

func TestTxConnCommitOrderFailure2(t *testing.T) {
	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	queries := []*querypb.BoundQuery{{
		Sql: "query1",
	}}

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(context.Background(), rss1, queries, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_PRE)
	sc.ExecuteMultiShard(context.Background(), rss0, queries, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_POST)
	sc.ExecuteMultiShard(context.Background(), rss1, queries, session, false, false)

	sbc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err := sc.txConn.Commit(ctx, session)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "INVALID_ARGUMENT error", "Commit")

	wantSession := vtgatepb.Session{}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbc0.CommitCount.Get(), "sbc0.CommitCount")
	assert.EqualValues(t, 1, sbc1.CommitCount.Get(), "sbc1.CommitCount")
	// When the commit fails, we try to clean up by issuing a rollback
	assert.EqualValues(t, 0, sbc0.ReleaseCount.Get(), "sbc0.ReleaseCount")
	assert.EqualValues(t, 2, sbc1.ReleaseCount.Get(), "sbc1.ReleaseCount")
}

func TestTxConnCommitOrderFailure3(t *testing.T) {
	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	queries := []*querypb.BoundQuery{{
		Sql: "query1",
	}}

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_PRE)
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_POST)
	sc.ExecuteMultiShard(ctx, rss1, queries, session, false, false)

	sbc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	require.NoError(t,
		sc.txConn.Commit(ctx, session))

	// The last failed commit must generate a warning.
	expectErr := NewShardError(vterrors.New(
		vtrpcpb.Code_INVALID_ARGUMENT,
		fmt.Sprintf("%v error", vtrpcpb.Code_INVALID_ARGUMENT)),
		rss1[0].Target)

	wantSession := vtgatepb.Session{
		Warnings: []*querypb.QueryWarning{{
			Message: fmt.Sprintf("post-operation transaction had an error: %v", expectErr),
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 2, sbc0.CommitCount.Get(), "sbc0.CommitCount")
	assert.EqualValues(t, 1, sbc1.CommitCount.Get(), "sbc1.CommitCount")
	assert.EqualValues(t, 0, sbc0.RollbackCount.Get(), "sbc0.RollbackCount")
	assert.EqualValues(t, 0, sbc1.RollbackCount.Get(), "sbc1.RollbackCount")
}

func TestTxConnCommitOrderSuccess(t *testing.T) {
	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	queries := []*querypb.BoundQuery{{
		Sql: "query1",
	}}

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	wantSession := vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbc0.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	session.SetCommitOrder(vtgatepb.CommitOrder_PRE)
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	wantSession = vtgatepb.Session{
		InTransaction: true,
		PreSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 2,
			TabletAlias:   sbc0.Tablet().Alias,
		}},
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbc0.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	session.SetCommitOrder(vtgatepb.CommitOrder_POST)
	sc.ExecuteMultiShard(ctx, rss1, queries, session, false, false)
	wantSession = vtgatepb.Session{
		InTransaction: true,
		PreSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 2,
			TabletAlias:   sbc0.Tablet().Alias,
		}},
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbc0.Tablet().Alias,
		}},
		PostSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbc1.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	// Ensure nothing changes if we reuse a transaction.
	sc.ExecuteMultiShard(ctx, rss1, queries, session, false, false)
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	require.NoError(t,
		sc.txConn.Commit(ctx, session))
	wantSession = vtgatepb.Session{}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 2, sbc0.CommitCount.Get(), "sbc0.CommitCount")
	assert.EqualValues(t, 1, sbc1.CommitCount.Get(), "sbc1.CommitCount")
}

func TestTxConnReservedCommitOrderSuccess(t *testing.T) {
	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	queries := []*querypb.BoundQuery{{
		Sql: "query1",
	}}

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true, InReservedConn: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	wantSession := vtgatepb.Session{
		InTransaction:  true,
		InReservedConn: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			ReservedId:    1,
			TabletAlias:   sbc0.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	session.SetCommitOrder(vtgatepb.CommitOrder_PRE)
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	wantSession = vtgatepb.Session{
		InTransaction:  true,
		InReservedConn: true,
		PreSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 2,
			ReservedId:    2,
			TabletAlias:   sbc0.Tablet().Alias,
		}},
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			ReservedId:    1,
			TabletAlias:   sbc0.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	session.SetCommitOrder(vtgatepb.CommitOrder_POST)
	sc.ExecuteMultiShard(ctx, rss1, queries, session, false, false)
	wantSession = vtgatepb.Session{
		InTransaction:  true,
		InReservedConn: true,
		PreSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 2,
			ReservedId:    2,
			TabletAlias:   sbc0.Tablet().Alias,
		}},
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			ReservedId:    1,
			TabletAlias:   sbc0.Tablet().Alias,
		}},
		PostSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			ReservedId:    1,
			TabletAlias:   sbc1.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	// Ensure nothing changes if we reuse a transaction.
	sc.ExecuteMultiShard(ctx, rss1, queries, session, false, false)
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	require.NoError(t,
		sc.txConn.Commit(ctx, session))
	wantSession = vtgatepb.Session{
		InReservedConn: true,
		PreSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  3,
			TabletAlias: sbc0.Tablet().Alias,
		}},
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  4,
			TabletAlias: sbc0.Tablet().Alias,
		}},
		PostSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  2,
			TabletAlias: sbc1.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 2, sbc0.CommitCount.Get(), "sbc0.CommitCount")
	assert.EqualValues(t, 1, sbc1.CommitCount.Get(), "sbc1.CommitCount")

	require.NoError(t,
		sc.txConn.Release(ctx, session))
	wantSession = vtgatepb.Session{InReservedConn: true}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 2, sbc0.ReleaseCount.Get(), "sbc0.ReleaseCount")
	assert.EqualValues(t, 1, sbc1.ReleaseCount.Get(), "sbc1.ReleaseCount")
}

func TestTxConnCommit2PC(t *testing.T) {
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TestTxConnCommit2PC")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, rss01, twoQueries, session, false, false)
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	require.NoError(t,
		sc.txConn.Commit(ctx, session))
	assert.EqualValues(t, 1, sbc0.CreateTransactionCount.Get(), "sbc0.CreateTransactionCount")
	assert.EqualValues(t, 1, sbc1.PrepareCount.Get(), "sbc1.PrepareCount")
	assert.EqualValues(t, 1, sbc0.StartCommitCount.Get(), "sbc0.StartCommitCount")
	assert.EqualValues(t, 1, sbc1.CommitPreparedCount.Get(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 1, sbc0.ConcludeTransactionCount.Get(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnCommit2PCOneParticipant(t *testing.T) {
	sc, sbc0, _, rss0, _, _ := newTestTxConnEnv(t, "TestTxConnCommit2PCOneParticipant")
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	require.NoError(t,
		sc.txConn.Commit(ctx, session))
	assert.EqualValues(t, 1, sbc0.CommitCount.Get(), "sbc0.CommitCount")
}

func TestTxConnCommit2PCCreateTransactionFail(t *testing.T) {
	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, "TestTxConnCommit2PCCreateTransactionFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, rss1, queries, session, false, false)

	sbc0.MustFailCreateTransaction = 1
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	err := sc.txConn.Commit(ctx, session)
	want := "error: err"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Commit")
	assert.EqualValues(t, 1, sbc0.CreateTransactionCount.Get(), "sbc0.CreateTransactionCount")
	assert.EqualValues(t, 1, sbc0.RollbackCount.Get(), "sbc0.RollbackCount")
	assert.EqualValues(t, 1, sbc1.RollbackCount.Get(), "sbc1.RollbackCount")
	assert.EqualValues(t, 0, sbc1.PrepareCount.Get(), "sbc1.PrepareCount")
	assert.EqualValues(t, 0, sbc0.StartCommitCount.Get(), "sbc0.StartCommitCount")
	assert.EqualValues(t, 0, sbc1.CommitPreparedCount.Get(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 0, sbc0.ConcludeTransactionCount.Get(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnCommit2PCPrepareFail(t *testing.T) {
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TestTxConnCommit2PCPrepareFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, rss01, twoQueries, session, false, false)

	sbc1.MustFailPrepare = 1
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	err := sc.txConn.Commit(ctx, session)
	want := "error: err"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Commit")
	assert.EqualValues(t, 1, sbc0.CreateTransactionCount.Get(), "sbc0.CreateTransactionCount")
	assert.EqualValues(t, 1, sbc1.PrepareCount.Get(), "sbc1.PrepareCount")
	assert.EqualValues(t, 0, sbc0.StartCommitCount.Get(), "sbc0.StartCommitCount")
	assert.EqualValues(t, 0, sbc1.CommitPreparedCount.Get(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 0, sbc0.ConcludeTransactionCount.Get(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnCommit2PCStartCommitFail(t *testing.T) {
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TestTxConnCommit2PCStartCommitFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, rss01, twoQueries, session, false, false)

	sbc0.MustFailStartCommit = 1
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	err := sc.txConn.Commit(ctx, session)
	want := "error: err"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Commit")
	assert.EqualValues(t, 1, sbc0.CreateTransactionCount.Get(), "sbc0.CreateTransactionCount")
	assert.EqualValues(t, 1, sbc1.PrepareCount.Get(), "sbc1.PrepareCount")
	assert.EqualValues(t, 1, sbc0.StartCommitCount.Get(), "sbc0.StartCommitCount")
	assert.EqualValues(t, 0, sbc1.CommitPreparedCount.Get(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 0, sbc0.ConcludeTransactionCount.Get(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnCommit2PCCommitPreparedFail(t *testing.T) {
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TestTxConnCommit2PCCommitPreparedFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, rss01, twoQueries, session, false, false)

	sbc1.MustFailCommitPrepared = 1
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	err := sc.txConn.Commit(ctx, session)
	want := "error: err"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Commit")
	assert.EqualValues(t, 1, sbc0.CreateTransactionCount.Get(), "sbc0.CreateTransactionCount")
	assert.EqualValues(t, 1, sbc1.PrepareCount.Get(), "sbc1.PrepareCount")
	assert.EqualValues(t, 1, sbc0.StartCommitCount.Get(), "sbc0.StartCommitCount")
	assert.EqualValues(t, 1, sbc1.CommitPreparedCount.Get(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 0, sbc0.ConcludeTransactionCount.Get(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnCommit2PCConcludeTransactionFail(t *testing.T) {
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TestTxConnCommit2PCConcludeTransactionFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, rss01, twoQueries, session, false, false)

	sbc0.MustFailConcludeTransaction = 1
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	err := sc.txConn.Commit(ctx, session)
	want := "error: err"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Commit")
	assert.EqualValues(t, 1, sbc0.CreateTransactionCount.Get(), "sbc0.CreateTransactionCount")
	assert.EqualValues(t, 1, sbc1.PrepareCount.Get(), "sbc1.PrepareCount")
	assert.EqualValues(t, 1, sbc0.StartCommitCount.Get(), "sbc0.StartCommitCount")
	assert.EqualValues(t, 1, sbc1.CommitPreparedCount.Get(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 1, sbc0.ConcludeTransactionCount.Get(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnRollback(t *testing.T) {
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TxConnRollback")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, rss01, twoQueries, session, false, false)
	require.NoError(t,
		sc.txConn.Rollback(ctx, session))
	wantSession := vtgatepb.Session{}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbc0.RollbackCount.Get(), "sbc0.RollbackCount")
	assert.EqualValues(t, 1, sbc1.RollbackCount.Get(), "sbc1.RollbackCount")
}

func TestTxConnReservedRollback(t *testing.T) {
	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, "TxConnReservedRollback")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true, InReservedConn: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, rss01, twoQueries, session, false, false)
	require.NoError(t,
		sc.txConn.Rollback(ctx, session))
	wantSession := vtgatepb.Session{
		InReservedConn: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TxConnReservedRollback",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  2,
			TabletAlias: sbc0.Tablet().Alias,
		}, {
			Target: &querypb.Target{
				Keyspace:   "TxConnReservedRollback",
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			ReservedId:  2,
			TabletAlias: sbc1.Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbc0.RollbackCount.Get(), "sbc0.RollbackCount")
	assert.EqualValues(t, 1, sbc1.RollbackCount.Get(), "sbc1.RollbackCount")
	assert.EqualValues(t, 0, sbc0.ReleaseCount.Get(), "sbc0.ReleaseCount")
	assert.EqualValues(t, 0, sbc1.ReleaseCount.Get(), "sbc1.ReleaseCount")
}

func TestTxConnReservedRollbackFailure(t *testing.T) {
	sc, sbc0, sbc1, rss0, rss1, rss01 := newTestTxConnEnv(t, "TxConnReservedRollback")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true, InReservedConn: true})
	sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, rss01, twoQueries, session, false, false)

	sbc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	assert.Error(t,
		sc.txConn.Rollback(ctx, session))

	expectErr := NewShardError(vterrors.New(
		vtrpcpb.Code_INVALID_ARGUMENT,
		fmt.Sprintf("%v error", vtrpcpb.Code_INVALID_ARGUMENT)),
		rss1[0].Target)

	wantSession := vtgatepb.Session{
		InReservedConn: true,
		Warnings: []*querypb.QueryWarning{{
			Message: fmt.Sprintf("rollback encountered an error and connection to all shard for this session is released: %v", expectErr),
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbc0.RollbackCount.Get(), "sbc0.RollbackCount")
	assert.EqualValues(t, 1, sbc1.RollbackCount.Get(), "sbc1.RollbackCount")
	assert.EqualValues(t, 1, sbc0.ReleaseCount.Get(), "sbc0.ReleaseCount")
	assert.EqualValues(t, 1, sbc1.ReleaseCount.Get(), "sbc1.ReleaseCount")
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
			TabletType: topodatapb.TabletType_PRIMARY,
		}},
	}}
	err := sc.txConn.Resolve(ctx, dtid)
	require.NoError(t, err)
	assert.EqualValues(t, 1, sbc0.SetRollbackCount.Get(), "sbc0.SetRollbackCount")
	assert.EqualValues(t, 1, sbc1.RollbackPreparedCount.Get(), "sbc1.RollbackPreparedCount")
	assert.EqualValues(t, 0, sbc1.CommitPreparedCount.Get(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 1, sbc0.ConcludeTransactionCount.Get(), "sbc0.ConcludeTransactionCount")
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
			TabletType: topodatapb.TabletType_PRIMARY,
		}},
	}}
	require.NoError(t,
		sc.txConn.Resolve(ctx, dtid))
	assert.EqualValues(t, 0, sbc0.SetRollbackCount.Get(), "sbc0.SetRollbackCount")
	assert.EqualValues(t, 1, sbc1.RollbackPreparedCount.Get(), "sbc1.RollbackPreparedCount")
	assert.EqualValues(t, 0, sbc1.CommitPreparedCount.Get(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 1, sbc0.ConcludeTransactionCount.Get(), "sbc0.ConcludeTransactionCount")
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
			TabletType: topodatapb.TabletType_PRIMARY,
		}},
	}}
	require.NoError(t,
		sc.txConn.Resolve(ctx, dtid))
	assert.EqualValues(t, 0, sbc0.SetRollbackCount.Get(), "sbc0.SetRollbackCount")
	assert.EqualValues(t, 0, sbc1.RollbackPreparedCount.Get(), "sbc1.RollbackPreparedCount")
	assert.EqualValues(t, 1, sbc1.CommitPreparedCount.Get(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 1, sbc0.ConcludeTransactionCount.Get(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnResolveInvalidDTID(t *testing.T) {
	sc, _, _, _, _, _ := newTestTxConnEnv(t, "TestTxConn")

	err := sc.txConn.Resolve(ctx, "abcd")
	want := "invalid parts in dtid: abcd"
	require.EqualError(t, err, want, "Resolve")
}

func TestTxConnResolveReadTransactionFail(t *testing.T) {
	sc, sbc0, _, _, _, _ := newTestTxConnEnv(t, "TestTxConn")

	dtid := "TestTxConn:0:1234"
	sbc0.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err := sc.txConn.Resolve(ctx, dtid)
	want := "INVALID_ARGUMENT error"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Resolve")
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
			TabletType: topodatapb.TabletType_PRIMARY,
		}},
	}}
	err := sc.txConn.Resolve(ctx, dtid)
	want := "invalid state: UNKNOWN"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Resolve")
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
			TabletType: topodatapb.TabletType_PRIMARY,
		}},
	}}
	sbc0.MustFailSetRollback = 1
	err := sc.txConn.Resolve(ctx, dtid)
	want := "error: err"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Resolve")
	assert.EqualValues(t, 1, sbc0.SetRollbackCount.Get(), "sbc0.SetRollbackCount")
	assert.EqualValues(t, 0, sbc1.RollbackPreparedCount.Get(), "sbc1.RollbackPreparedCount")
	assert.EqualValues(t, 0, sbc1.CommitPreparedCount.Get(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 0, sbc0.ConcludeTransactionCount.Get(), "sbc0.ConcludeTransactionCount")
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
			TabletType: topodatapb.TabletType_PRIMARY,
		}},
	}}
	sbc1.MustFailRollbackPrepared = 1
	err := sc.txConn.Resolve(ctx, dtid)
	want := "error: err"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Resolve")
	assert.EqualValues(t, 0, sbc0.SetRollbackCount.Get(), "sbc0.SetRollbackCount")
	assert.EqualValues(t, 1, sbc1.RollbackPreparedCount.Get(), "sbc1.RollbackPreparedCount")
	assert.EqualValues(t, 0, sbc1.CommitPreparedCount.Get(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 0, sbc0.ConcludeTransactionCount.Get(), "sbc0.ConcludeTransactionCount")
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
			TabletType: topodatapb.TabletType_PRIMARY,
		}},
	}}
	sbc1.MustFailCommitPrepared = 1
	err := sc.txConn.Resolve(ctx, dtid)
	want := "error: err"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Resolve")
	assert.EqualValues(t, 0, sbc0.SetRollbackCount.Get(), "sbc0.SetRollbackCount")
	assert.EqualValues(t, 0, sbc1.RollbackPreparedCount.Get(), "sbc1.RollbackPreparedCount")
	assert.EqualValues(t, 1, sbc1.CommitPreparedCount.Get(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 0, sbc0.ConcludeTransactionCount.Get(), "sbc0.ConcludeTransactionCount")
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
			TabletType: topodatapb.TabletType_PRIMARY,
		}},
	}}
	sbc0.MustFailConcludeTransaction = 1
	err := sc.txConn.Resolve(ctx, dtid)
	want := "error: err"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Resolve")
	assert.EqualValues(t, 0, sbc0.SetRollbackCount.Get(), "sbc0.SetRollbackCount")
	assert.EqualValues(t, 0, sbc1.RollbackPreparedCount.Get(), "sbc1.RollbackPreparedCount")
	assert.EqualValues(t, 1, sbc1.CommitPreparedCount.Get(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 1, sbc0.ConcludeTransactionCount.Get(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnMultiGoSessions(t *testing.T) {
	txc := &TxConn{}

	input := []*vtgatepb.Session_ShardSession{{
		Target: &querypb.Target{
			Keyspace: "0",
		},
	}}
	err := txc.runSessions(ctx, input, nil, func(ctx context.Context, s *vtgatepb.Session_ShardSession, logger *executeLogger) error {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "err %s", s.Target.Keyspace)
	})
	want := "err 0"
	require.EqualError(t, err, want, "runSessions(1)")

	input = []*vtgatepb.Session_ShardSession{{
		Target: &querypb.Target{
			Keyspace: "0",
		},
	}, {
		Target: &querypb.Target{
			Keyspace: "1",
		},
	}}
	err = txc.runSessions(ctx, input, nil, func(ctx context.Context, s *vtgatepb.Session_ShardSession, logger *executeLogger) error {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "err %s", s.Target.Keyspace)
	})
	want = "err 0\nerr 1"
	require.EqualError(t, err, want, "runSessions(2)")
	wantCode := vtrpcpb.Code_INTERNAL
	assert.Equal(t, wantCode, vterrors.Code(err), "error code")

	err = txc.runSessions(ctx, input, nil, func(ctx context.Context, s *vtgatepb.Session_ShardSession, logger *executeLogger) error {
		return nil
	})
	require.NoError(t, err)
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
	require.EqualError(t, err, want, "runTargets(1)")

	input = []*querypb.Target{{
		Keyspace: "0",
	}, {
		Keyspace: "1",
	}}
	err = txc.runTargets(input, func(t *querypb.Target) error {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "err %s", t.Keyspace)
	})
	want = "err 0\nerr 1"
	require.EqualError(t, err, want, "runTargets(2)")
	wantCode := vtrpcpb.Code_INTERNAL
	assert.Equal(t, wantCode, vterrors.Code(err), "error code")

	err = txc.runTargets(input, func(t *querypb.Target) error {
		return nil
	})
	require.NoError(t, err)
}

func newTestTxConnEnv(t *testing.T, name string) (sc *ScatterConn, sbc0, sbc1 *sandboxconn.SandboxConn, rss0, rss1, rss01 []*srvtopo.ResolvedShard) {
	t.Helper()
	createSandbox(name)
	hc := discovery.NewFakeHealthCheck(nil)
	sc = newTestScatterConn(hc, newSandboxForCells([]string{"aa"}), "aa")
	sbc0 = hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "1", 1, name, "1", topodatapb.TabletType_PRIMARY, true, 1, nil)
	res := srvtopo.NewResolver(newSandboxForCells([]string{"aa"}), sc.gateway, "aa")
	var err error
	rss0, err = res.ResolveDestination(ctx, name, topodatapb.TabletType_PRIMARY, key.DestinationShard("0"))
	require.NoError(t, err)
	rss1, err = res.ResolveDestination(ctx, name, topodatapb.TabletType_PRIMARY, key.DestinationShard("1"))
	require.NoError(t, err)
	rss01, err = res.ResolveDestination(ctx, name, topodatapb.TabletType_PRIMARY, key.DestinationShards([]string{"0", "1"}))
	require.NoError(t, err)
	return sc, sbc0, sbc1, rss0, rss1, rss01
}
