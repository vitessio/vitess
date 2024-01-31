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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlerror"
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
var threeQueries = []*querypb.BoundQuery{{Sql: "query1"}, {Sql: "query1"}, {Sql: "query1"}}

func TestTxConnBegin(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, _, rss0, _, _ := newTestTxConnEnv(t, ctx, "TestTxConn")
	session := &vtgatepb.Session{}

	// begin
	safeSession := NewSafeSession(session)
	err := sc.txConn.Begin(ctx, safeSession, nil)
	require.NoError(t, err)
	wantSession := vtgatepb.Session{InTransaction: true}
	utils.MustMatch(t, &wantSession, session, "Session")
	_, errors := sc.ExecuteMultiShard(ctx, nil, rss0, queries, safeSession, false, false)
	require.Empty(t, errors)

	// Begin again should cause a commit and a new begin.
	require.NoError(t,
		sc.txConn.Begin(ctx, safeSession, nil))
	utils.MustMatch(t, &wantSession, session, "Session")
	assert.EqualValues(t, 1, sbc0.CommitCount.Load(), "sbc0.CommitCount")
}

func TestTxConnCommitFailure(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbcs, rssm, rssa := newTestTxConnEnvNShards(t, ctx, "TestTxConn", 3)
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI
	nonAtomicCommitCount := warnings.Counts()["NonAtomicCommit"]

	// Sequence the executes to ensure commit order

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, nil, rssm[0], queries, session, false, false)
	wantSession := vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbcs[0].Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	sc.ExecuteMultiShard(ctx, nil, rssm[1], queries, session, false, false)
	wantSession = vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbcs[0].Tablet().Alias,
		}, {
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbcs[1].Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	sc.ExecuteMultiShard(ctx, nil, rssa, threeQueries, session, false, false)
	wantSession = vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbcs[0].Tablet().Alias,
		}, {
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbcs[1].Tablet().Alias,
		}, {
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      "2",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbcs[2].Tablet().Alias,
		}},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	sbcs[2].MustFailCodes[vtrpcpb.Code_DEADLINE_EXCEEDED] = 1

	expectErr := NewShardError(vterrors.New(
		vtrpcpb.Code_DEADLINE_EXCEEDED,
		fmt.Sprintf("%v error", vtrpcpb.Code_DEADLINE_EXCEEDED)),
		rssm[2][0].Target)

	require.ErrorContains(t, sc.txConn.Commit(ctx, session), expectErr.Error())
	wantSession = vtgatepb.Session{
		Warnings: []*querypb.QueryWarning{
			{
				Code:    uint32(sqlerror.ERNonAtomicCommit),
				Message: "multi-db commit failed after committing to 2 shards: 0, 1",
			},
		},
	}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbcs[0].CommitCount.Load(), "sbc0.CommitCount")

	require.Equal(t, nonAtomicCommitCount+1, warnings.Counts()["NonAtomicCommit"])
}

func TestTxConnCommitFailureAfterNonAtomicCommitMaxShards(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbcs, rssm, _ := newTestTxConnEnvNShards(t, ctx, "TestTxConn", 18)
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI
	nonAtomicCommitCount := warnings.Counts()["NonAtomicCommit"]

	// Sequence the executes to ensure commit order

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	wantSession := vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{},
	}

	for i := 0; i < 18; i++ {
		sc.ExecuteMultiShard(ctx, nil, rssm[i], queries, session, false, false)
		wantSession.ShardSessions = append(wantSession.ShardSessions, &vtgatepb.Session_ShardSession{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      rssm[i][0].Target.Shard,
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbcs[i].Tablet().Alias,
		})
		utils.MustMatch(t, &wantSession, session.Session, "Session")
	}

	sbcs[17].MustFailCodes[vtrpcpb.Code_DEADLINE_EXCEEDED] = 1

	expectErr := NewShardError(vterrors.New(
		vtrpcpb.Code_DEADLINE_EXCEEDED,
		fmt.Sprintf("%v error", vtrpcpb.Code_DEADLINE_EXCEEDED)),
		rssm[17][0].Target)

	require.ErrorContains(t, sc.txConn.Commit(ctx, session), expectErr.Error())
	wantSession = vtgatepb.Session{
		Warnings: []*querypb.QueryWarning{
			{
				Code:    uint32(sqlerror.ERNonAtomicCommit),
				Message: "multi-db commit failed after committing to 17 shards: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, ...",
			},
		},
	}

	utils.MustMatch(t, &wantSession, session.Session, "Session")
	for i := 0; i < 17; i++ {
		assert.EqualValues(t, 1, sbcs[i].CommitCount.Load(), fmt.Sprintf("sbc%d.CommitCount", i))
	}

	require.Equal(t, nonAtomicCommitCount+1, warnings.Counts()["NonAtomicCommit"])
}

func TestTxConnCommitFailureBeforeNonAtomicCommitMaxShards(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbcs, rssm, _ := newTestTxConnEnvNShards(t, ctx, "TestTxConn", 17)
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI
	nonAtomicCommitCount := warnings.Counts()["NonAtomicCommit"]

	// Sequence the executes to ensure commit order

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	wantSession := vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{},
	}

	for i := 0; i < 17; i++ {
		sc.ExecuteMultiShard(ctx, nil, rssm[i], queries, session, false, false)
		wantSession.ShardSessions = append(wantSession.ShardSessions, &vtgatepb.Session_ShardSession{
			Target: &querypb.Target{
				Keyspace:   "TestTxConn",
				Shard:      rssm[i][0].Target.Shard,
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
			TabletAlias:   sbcs[i].Tablet().Alias,
		})
		utils.MustMatch(t, &wantSession, session.Session, "Session")
	}

	sbcs[16].MustFailCodes[vtrpcpb.Code_DEADLINE_EXCEEDED] = 1

	expectErr := NewShardError(vterrors.New(
		vtrpcpb.Code_DEADLINE_EXCEEDED,
		fmt.Sprintf("%v error", vtrpcpb.Code_DEADLINE_EXCEEDED)),
		rssm[16][0].Target)

	require.ErrorContains(t, sc.txConn.Commit(ctx, session), expectErr.Error())
	wantSession = vtgatepb.Session{
		Warnings: []*querypb.QueryWarning{
			{
				Code:    uint32(sqlerror.ERNonAtomicCommit),
				Message: "multi-db commit failed after committing to 16 shards: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15",
			},
		},
	}

	utils.MustMatch(t, &wantSession, session.Session, "Session")
	for i := 0; i < 16; i++ {
		assert.EqualValues(t, 1, sbcs[i].CommitCount.Load(), fmt.Sprintf("sbc%d.CommitCount", i))
	}

	require.Equal(t, nonAtomicCommitCount+1, warnings.Counts()["NonAtomicCommit"])
}

func TestTxConnCommitSuccess(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, ctx, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
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
	sc.ExecuteMultiShard(ctx, nil, rss01, twoQueries, session, false, false)
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
	assert.EqualValues(t, 1, sbc0.CommitCount.Load(), "sbc0.CommitCount")
	assert.EqualValues(t, 1, sbc1.CommitCount.Load(), "sbc1.CommitCount")
}

func TestTxConnReservedCommitSuccess(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, ctx, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true, InReservedConn: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
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
	sc.ExecuteMultiShard(ctx, nil, rss01, twoQueries, session, false, false)
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
	assert.EqualValues(t, 1, sbc0.CommitCount.Load(), "sbc0.CommitCount")
	assert.EqualValues(t, 1, sbc1.CommitCount.Load(), "sbc1.CommitCount")

	require.NoError(t,
		sc.txConn.Release(ctx, session))
	wantSession = vtgatepb.Session{InReservedConn: true}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbc0.ReleaseCount.Load(), "sbc0.ReleaseCount")
	assert.EqualValues(t, 1, sbc1.ReleaseCount.Load(), "sbc1.ReleaseCount")
}

func TestTxConnReservedOn2ShardTxOn1ShardAndCommit(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	keyspace := "TestTxConn"
	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, ctx, keyspace)
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	// Sequence the executes to ensure shard session order
	session := NewSafeSession(&vtgatepb.Session{InReservedConn: true})

	// this will create reserved connections against all tablets
	_, errs := sc.ExecuteMultiShard(ctx, nil, rss1, queries, session, false, false)
	require.Empty(t, errs)
	_, errs = sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
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
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
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
	assert.EqualValues(t, 1, sbc0.CommitCount.Load(), "sbc0.CommitCount")
	assert.EqualValues(t, 0, sbc1.CommitCount.Load(), "sbc1.CommitCount")
}

func TestTxConnReservedOn2ShardTxOn1ShardAndRollback(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	keyspace := "TestTxConn"
	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, ctx, keyspace)
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	// Sequence the executes to ensure shard session order
	session := NewSafeSession(&vtgatepb.Session{InReservedConn: true})

	// this will create reserved connections against all tablets
	_, errs := sc.ExecuteMultiShard(ctx, nil, rss1, queries, session, false, false)
	require.Empty(t, errs)
	_, errs = sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
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
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
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
	assert.EqualValues(t, 1, sbc0.RollbackCount.Load(), "sbc0.RollbackCount")
	assert.EqualValues(t, 0, sbc1.RollbackCount.Load(), "sbc1.RollbackCount")
}

func TestTxConnCommitOrderFailure1(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, ctx, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	queries := []*querypb.BoundQuery{{Sql: "query1"}}

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_PRE)
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_POST)
	sc.ExecuteMultiShard(ctx, nil, rss1, queries, session, false, false)

	sbc0.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err := sc.txConn.Commit(ctx, session)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "INVALID_ARGUMENT error", "commit error")

	wantSession := vtgatepb.Session{}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbc0.CommitCount.Load(), "sbc0.CommitCount")
	// first commit failed so we don't try to commit the second shard
	assert.EqualValues(t, 0, sbc1.CommitCount.Load(), "sbc1.CommitCount")
	// When the commit fails, we try to clean up by issuing a rollback
	assert.EqualValues(t, 2, sbc0.ReleaseCount.Load(), "sbc0.ReleaseCount")
	assert.EqualValues(t, 1, sbc1.ReleaseCount.Load(), "sbc1.ReleaseCount")
}

func TestTxConnCommitOrderFailure2(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, ctx, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	queries := []*querypb.BoundQuery{{
		Sql: "query1",
	}}

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(context.Background(), nil, rss1, queries, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_PRE)
	sc.ExecuteMultiShard(context.Background(), nil, rss0, queries, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_POST)
	sc.ExecuteMultiShard(context.Background(), nil, rss1, queries, session, false, false)

	sbc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err := sc.txConn.Commit(ctx, session)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "INVALID_ARGUMENT error", "Commit")

	wantSession := vtgatepb.Session{}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbc0.CommitCount.Load(), "sbc0.CommitCount")
	assert.EqualValues(t, 1, sbc1.CommitCount.Load(), "sbc1.CommitCount")
	// When the commit fails, we try to clean up by issuing a rollback
	assert.EqualValues(t, 0, sbc0.ReleaseCount.Load(), "sbc0.ReleaseCount")
	assert.EqualValues(t, 2, sbc1.ReleaseCount.Load(), "sbc1.ReleaseCount")
}

func TestTxConnCommitOrderFailure3(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, ctx, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	queries := []*querypb.BoundQuery{{
		Sql: "query1",
	}}

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_PRE)
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)

	session.SetCommitOrder(vtgatepb.CommitOrder_POST)
	sc.ExecuteMultiShard(ctx, nil, rss1, queries, session, false, false)

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
	assert.EqualValues(t, 2, sbc0.CommitCount.Load(), "sbc0.CommitCount")
	assert.EqualValues(t, 1, sbc1.CommitCount.Load(), "sbc1.CommitCount")
	assert.EqualValues(t, 0, sbc0.RollbackCount.Load(), "sbc0.RollbackCount")
	assert.EqualValues(t, 0, sbc1.RollbackCount.Load(), "sbc1.RollbackCount")
}

func TestTxConnCommitOrderSuccess(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, ctx, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	queries := []*querypb.BoundQuery{{
		Sql: "query1",
	}}

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
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
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
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
	sc.ExecuteMultiShard(ctx, nil, rss1, queries, session, false, false)
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
	sc.ExecuteMultiShard(ctx, nil, rss1, queries, session, false, false)
	utils.MustMatch(t, &wantSession, session.Session, "Session")

	require.NoError(t,
		sc.txConn.Commit(ctx, session))
	wantSession = vtgatepb.Session{}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 2, sbc0.CommitCount.Load(), "sbc0.CommitCount")
	assert.EqualValues(t, 1, sbc1.CommitCount.Load(), "sbc1.CommitCount")
}

func TestTxConnReservedCommitOrderSuccess(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, ctx, "TestTxConn")
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI

	queries := []*querypb.BoundQuery{{
		Sql: "query1",
	}}

	// Sequence the executes to ensure commit order
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true, InReservedConn: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
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
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
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
	sc.ExecuteMultiShard(ctx, nil, rss1, queries, session, false, false)
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
	sc.ExecuteMultiShard(ctx, nil, rss1, queries, session, false, false)
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
	assert.EqualValues(t, 2, sbc0.CommitCount.Load(), "sbc0.CommitCount")
	assert.EqualValues(t, 1, sbc1.CommitCount.Load(), "sbc1.CommitCount")

	require.NoError(t,
		sc.txConn.Release(ctx, session))
	wantSession = vtgatepb.Session{InReservedConn: true}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 2, sbc0.ReleaseCount.Load(), "sbc0.ReleaseCount")
	assert.EqualValues(t, 1, sbc1.ReleaseCount.Load(), "sbc1.ReleaseCount")
}

func TestTxConnCommit2PC(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, ctx, "TestTxConnCommit2PC")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, nil, rss01, twoQueries, session, false, false)
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	require.NoError(t,
		sc.txConn.Commit(ctx, session))
	assert.EqualValues(t, 1, sbc0.CreateTransactionCount.Load(), "sbc0.CreateTransactionCount")
	assert.EqualValues(t, 1, sbc1.PrepareCount.Load(), "sbc1.PrepareCount")
	assert.EqualValues(t, 1, sbc0.StartCommitCount.Load(), "sbc0.StartCommitCount")
	assert.EqualValues(t, 1, sbc1.CommitPreparedCount.Load(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 1, sbc0.ConcludeTransactionCount.Load(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnCommit2PCOneParticipant(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, _, rss0, _, _ := newTestTxConnEnv(t, ctx, "TestTxConnCommit2PCOneParticipant")
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	require.NoError(t,
		sc.txConn.Commit(ctx, session))
	assert.EqualValues(t, 1, sbc0.CommitCount.Load(), "sbc0.CommitCount")
}

func TestTxConnCommit2PCCreateTransactionFail(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, rss1, _ := newTestTxConnEnv(t, ctx, "TestTxConnCommit2PCCreateTransactionFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, nil, rss1, queries, session, false, false)

	sbc0.MustFailCreateTransaction = 1
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	err := sc.txConn.Commit(ctx, session)
	want := "error: err"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Commit")
	assert.EqualValues(t, 1, sbc0.CreateTransactionCount.Load(), "sbc0.CreateTransactionCount")
	assert.EqualValues(t, 1, sbc0.RollbackCount.Load(), "sbc0.RollbackCount")
	assert.EqualValues(t, 1, sbc1.RollbackCount.Load(), "sbc1.RollbackCount")
	assert.EqualValues(t, 0, sbc1.PrepareCount.Load(), "sbc1.PrepareCount")
	assert.EqualValues(t, 0, sbc0.StartCommitCount.Load(), "sbc0.StartCommitCount")
	assert.EqualValues(t, 0, sbc1.CommitPreparedCount.Load(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 0, sbc0.ConcludeTransactionCount.Load(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnCommit2PCPrepareFail(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, ctx, "TestTxConnCommit2PCPrepareFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, nil, rss01, twoQueries, session, false, false)

	sbc1.MustFailPrepare = 1
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	err := sc.txConn.Commit(ctx, session)
	want := "error: err"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Commit")
	assert.EqualValues(t, 1, sbc0.CreateTransactionCount.Load(), "sbc0.CreateTransactionCount")
	assert.EqualValues(t, 1, sbc1.PrepareCount.Load(), "sbc1.PrepareCount")
	assert.EqualValues(t, 0, sbc0.StartCommitCount.Load(), "sbc0.StartCommitCount")
	assert.EqualValues(t, 0, sbc1.CommitPreparedCount.Load(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 0, sbc0.ConcludeTransactionCount.Load(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnCommit2PCStartCommitFail(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, ctx, "TestTxConnCommit2PCStartCommitFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, nil, rss01, twoQueries, session, false, false)

	sbc0.MustFailStartCommit = 1
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	err := sc.txConn.Commit(ctx, session)
	want := "error: err"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Commit")
	assert.EqualValues(t, 1, sbc0.CreateTransactionCount.Load(), "sbc0.CreateTransactionCount")
	assert.EqualValues(t, 1, sbc1.PrepareCount.Load(), "sbc1.PrepareCount")
	assert.EqualValues(t, 1, sbc0.StartCommitCount.Load(), "sbc0.StartCommitCount")
	assert.EqualValues(t, 0, sbc1.CommitPreparedCount.Load(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 0, sbc0.ConcludeTransactionCount.Load(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnCommit2PCCommitPreparedFail(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, ctx, "TestTxConnCommit2PCCommitPreparedFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, nil, rss01, twoQueries, session, false, false)

	sbc1.MustFailCommitPrepared = 1
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	err := sc.txConn.Commit(ctx, session)
	want := "error: err"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Commit")
	assert.EqualValues(t, 1, sbc0.CreateTransactionCount.Load(), "sbc0.CreateTransactionCount")
	assert.EqualValues(t, 1, sbc1.PrepareCount.Load(), "sbc1.PrepareCount")
	assert.EqualValues(t, 1, sbc0.StartCommitCount.Load(), "sbc0.StartCommitCount")
	assert.EqualValues(t, 1, sbc1.CommitPreparedCount.Load(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 0, sbc0.ConcludeTransactionCount.Load(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnCommit2PCConcludeTransactionFail(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, ctx, "TestTxConnCommit2PCConcludeTransactionFail")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, nil, rss01, twoQueries, session, false, false)

	sbc0.MustFailConcludeTransaction = 1
	session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	err := sc.txConn.Commit(ctx, session)
	want := "error: err"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Commit")
	assert.EqualValues(t, 1, sbc0.CreateTransactionCount.Load(), "sbc0.CreateTransactionCount")
	assert.EqualValues(t, 1, sbc1.PrepareCount.Load(), "sbc1.PrepareCount")
	assert.EqualValues(t, 1, sbc0.StartCommitCount.Load(), "sbc0.StartCommitCount")
	assert.EqualValues(t, 1, sbc1.CommitPreparedCount.Load(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 1, sbc0.ConcludeTransactionCount.Load(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnRollback(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, ctx, "TxConnRollback")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, nil, rss01, twoQueries, session, false, false)
	require.NoError(t,
		sc.txConn.Rollback(ctx, session))
	wantSession := vtgatepb.Session{}
	utils.MustMatch(t, &wantSession, session.Session, "Session")
	assert.EqualValues(t, 1, sbc0.RollbackCount.Load(), "sbc0.RollbackCount")
	assert.EqualValues(t, 1, sbc1.RollbackCount.Load(), "sbc1.RollbackCount")
}

func TestTxConnReservedRollback(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, _, rss01 := newTestTxConnEnv(t, ctx, "TxConnReservedRollback")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true, InReservedConn: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, nil, rss01, twoQueries, session, false, false)
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
	assert.EqualValues(t, 1, sbc0.RollbackCount.Load(), "sbc0.RollbackCount")
	assert.EqualValues(t, 1, sbc1.RollbackCount.Load(), "sbc1.RollbackCount")
	assert.EqualValues(t, 0, sbc0.ReleaseCount.Load(), "sbc0.ReleaseCount")
	assert.EqualValues(t, 0, sbc1.ReleaseCount.Load(), "sbc1.ReleaseCount")
}

func TestTxConnReservedRollbackFailure(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, rss0, rss1, rss01 := newTestTxConnEnv(t, ctx, "TxConnReservedRollback")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true, InReservedConn: true})
	sc.ExecuteMultiShard(ctx, nil, rss0, queries, session, false, false)
	sc.ExecuteMultiShard(ctx, nil, rss01, twoQueries, session, false, false)

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
	assert.EqualValues(t, 1, sbc0.RollbackCount.Load(), "sbc0.RollbackCount")
	assert.EqualValues(t, 1, sbc1.RollbackCount.Load(), "sbc1.RollbackCount")
	assert.EqualValues(t, 1, sbc0.ReleaseCount.Load(), "sbc0.ReleaseCount")
	assert.EqualValues(t, 1, sbc1.ReleaseCount.Load(), "sbc1.ReleaseCount")
}

func TestTxConnResolveOnPrepare(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, _, _, _ := newTestTxConnEnv(t, ctx, "TestTxConn")

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
	assert.EqualValues(t, 1, sbc0.SetRollbackCount.Load(), "sbc0.SetRollbackCount")
	assert.EqualValues(t, 1, sbc1.RollbackPreparedCount.Load(), "sbc1.RollbackPreparedCount")
	assert.EqualValues(t, 0, sbc1.CommitPreparedCount.Load(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 1, sbc0.ConcludeTransactionCount.Load(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnResolveOnRollback(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, _, _, _ := newTestTxConnEnv(t, ctx, "TestTxConn")

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
	assert.EqualValues(t, 0, sbc0.SetRollbackCount.Load(), "sbc0.SetRollbackCount")
	assert.EqualValues(t, 1, sbc1.RollbackPreparedCount.Load(), "sbc1.RollbackPreparedCount")
	assert.EqualValues(t, 0, sbc1.CommitPreparedCount.Load(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 1, sbc0.ConcludeTransactionCount.Load(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnResolveOnCommit(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, _, _, _ := newTestTxConnEnv(t, ctx, "TestTxConn")

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
	assert.EqualValues(t, 0, sbc0.SetRollbackCount.Load(), "sbc0.SetRollbackCount")
	assert.EqualValues(t, 0, sbc1.RollbackPreparedCount.Load(), "sbc1.RollbackPreparedCount")
	assert.EqualValues(t, 1, sbc1.CommitPreparedCount.Load(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 1, sbc0.ConcludeTransactionCount.Load(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnResolveInvalidDTID(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, _, _, _, _, _ := newTestTxConnEnv(t, ctx, "TestTxConn")

	err := sc.txConn.Resolve(ctx, "abcd")
	want := "invalid parts in dtid: abcd"
	require.EqualError(t, err, want, "Resolve")
}

func TestTxConnResolveReadTransactionFail(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, _, _, _, _ := newTestTxConnEnv(t, ctx, "TestTxConn")

	dtid := "TestTxConn:0:1234"
	sbc0.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err := sc.txConn.Resolve(ctx, dtid)
	want := "INVALID_ARGUMENT error"
	require.Error(t, err)
	assert.Contains(t, err.Error(), want, "Resolve")
}

func TestTxConnResolveInternalError(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, _, _, _, _ := newTestTxConnEnv(t, ctx, "TestTxConn")

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
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, _, _, _ := newTestTxConnEnv(t, ctx, "TestTxConn")

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
	assert.EqualValues(t, 1, sbc0.SetRollbackCount.Load(), "sbc0.SetRollbackCount")
	assert.EqualValues(t, 0, sbc1.RollbackPreparedCount.Load(), "sbc1.RollbackPreparedCount")
	assert.EqualValues(t, 0, sbc1.CommitPreparedCount.Load(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 0, sbc0.ConcludeTransactionCount.Load(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnResolveRollbackPreparedFail(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, _, _, _ := newTestTxConnEnv(t, ctx, "TestTxConn")

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
	assert.EqualValues(t, 0, sbc0.SetRollbackCount.Load(), "sbc0.SetRollbackCount")
	assert.EqualValues(t, 1, sbc1.RollbackPreparedCount.Load(), "sbc1.RollbackPreparedCount")
	assert.EqualValues(t, 0, sbc1.CommitPreparedCount.Load(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 0, sbc0.ConcludeTransactionCount.Load(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnResolveCommitPreparedFail(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, _, _, _ := newTestTxConnEnv(t, ctx, "TestTxConn")

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
	assert.EqualValues(t, 0, sbc0.SetRollbackCount.Load(), "sbc0.SetRollbackCount")
	assert.EqualValues(t, 0, sbc1.RollbackPreparedCount.Load(), "sbc1.RollbackPreparedCount")
	assert.EqualValues(t, 1, sbc1.CommitPreparedCount.Load(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 0, sbc0.ConcludeTransactionCount.Load(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnResolveConcludeTransactionFail(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, sbc0, sbc1, _, _, _ := newTestTxConnEnv(t, ctx, "TestTxConn")

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
	assert.EqualValues(t, 0, sbc0.SetRollbackCount.Load(), "sbc0.SetRollbackCount")
	assert.EqualValues(t, 0, sbc1.RollbackPreparedCount.Load(), "sbc1.RollbackPreparedCount")
	assert.EqualValues(t, 1, sbc1.CommitPreparedCount.Load(), "sbc1.CommitPreparedCount")
	assert.EqualValues(t, 1, sbc0.ConcludeTransactionCount.Load(), "sbc0.ConcludeTransactionCount")
}

func TestTxConnMultiGoSessions(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

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

func TestTxConnAccessModeReset(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	sc, _, _, _, _, _ := newTestTxConnEnv(t, ctx, "TestTxConn")

	tcases := []struct {
		name string
		f    func(ctx context.Context, session *SafeSession) error
	}{{
		name: "begin-commit",
		f:    sc.txConn.Commit,
	}, {
		name: "begin-rollback",
		f:    sc.txConn.Rollback,
	}, {
		name: "begin-release",
		f:    sc.txConn.Release,
	}, {
		name: "begin-releaseAll",
		f:    sc.txConn.ReleaseAll,
	}}

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			safeSession := NewSafeSession(&vtgatepb.Session{
				Options: &querypb.ExecuteOptions{
					TransactionAccessMode: []querypb.ExecuteOptions_TransactionAccessMode{querypb.ExecuteOptions_READ_ONLY},
				},
			})

			// begin transaction
			require.NoError(t,
				sc.txConn.Begin(ctx, safeSession, nil))

			// resolve transaction
			require.NoError(t,
				tcase.f(ctx, safeSession))

			// check that the access mode is reset
			require.Nil(t, safeSession.Session.Options.TransactionAccessMode)
		})
	}
}

func newTestTxConnEnv(t *testing.T, ctx context.Context, name string) (sc *ScatterConn, sbc0, sbc1 *sandboxconn.SandboxConn, rss0, rss1, rss01 []*srvtopo.ResolvedShard) {
	t.Helper()
	createSandbox(name)
	sc, sbcs, rssl, rssa := newTestTxConnEnvNShards(t, ctx, name, 2)
	return sc, sbcs[0], sbcs[1], rssl[0], rssl[1], rssa
}

func newTestTxConnEnvNShards(t *testing.T, ctx context.Context, name string, n int) (
	sc *ScatterConn, sbcl []*sandboxconn.SandboxConn, rssl [][]*srvtopo.ResolvedShard, rssa []*srvtopo.ResolvedShard,
) {
	t.Helper()
	createSandbox(name)

	hc := discovery.NewFakeHealthCheck(nil)
	sc = newTestScatterConn(ctx, hc, newSandboxForCells(ctx, []string{"aa"}), "aa")

	sNames := make([]string, n)
	for i := 0; i < n; i++ {
		sNames[i] = strconv.FormatInt(int64(i), 10)
	}

	sbcl = make([]*sandboxconn.SandboxConn, len(sNames))
	for i, sName := range sNames {
		sbcl[i] = hc.AddTestTablet("aa", sName, int32(i)+1, name, sName, topodatapb.TabletType_PRIMARY, true, 1, nil)
	}

	res := srvtopo.NewResolver(newSandboxForCells(ctx, []string{"aa"}), sc.gateway, "aa")

	rssl = make([][]*srvtopo.ResolvedShard, len(sNames))
	for i, sName := range sNames {
		rss, err := res.ResolveDestination(ctx, name, topodatapb.TabletType_PRIMARY, key.DestinationShard(sName))
		require.NoError(t, err)
		rssl[i] = rss
	}

	rssa, err := res.ResolveDestination(ctx, name, topodatapb.TabletType_PRIMARY, key.DestinationShards(sNames))
	require.NoError(t, err)

	return sc, sbcl, rssl, rssa
}
