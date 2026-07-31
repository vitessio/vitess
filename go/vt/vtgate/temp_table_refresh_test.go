/*
Copyright 2026 The Vitess Authors.

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
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// TestTempTableActivityRefresh verifies that real activity on a session
// holding temporary tables is fanned out to the session's reserved
// connections that the query itself did not reach: an ordinary statement
// carrying the reserved id runs on the pinned tablet, resetting both the
// tablet's idle timer and mysqld's wait_timeout clock — so an active session
// keeps its temporary tables exactly as it would on a direct MySQL
// connection, no matter which shards its queries route to.
func TestTempTableActivityRefresh(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)
	sbclookup.RequireQueriesLocking()

	lookupTablet := sbclookup.Tablet()
	newSession := func(reservedID, transactionID int64) *econtext.SafeSession {
		return econtext.NewSafeSession(&vtgatepb.Session{
			Autocommit:   true,
			TargetString: "@primary",
			Options:      &querypb.ExecuteOptions{HasCreatedTempTables: true},
			ShardSessions: []*vtgatepb.Session_ShardSession{{
				Target: &querypb.Target{
					Keyspace:   lookupTablet.Keyspace,
					Shard:      lookupTablet.Shard,
					TabletType: lookupTablet.Type,
				},
				TabletAlias:   lookupTablet.Alias,
				ReservedId:    reservedID,
				TransactionId: transactionID,
			}},
		})
	}

	// A query routed to a different keyspace (TestExecutor) than the reserved
	// connection (TestUnsharded) must still refresh it, with the reserved id.
	session := newSession(42, 0)
	_, err := executor.Execute(ctx, nil, "TestTempTableActivityRefresh", session, "select id from `user` where id = 1", nil, false)
	require.NoError(t, err)

	refreshSent := func() bool {
		for _, q := range sbclookup.GetQueries() {
			if q.Sql == tempTableActivityRefreshQuery {
				return true
			}
		}
		return false
	}
	assert.Eventually(t, refreshSent, 30*time.Second, 10*time.Millisecond,
		"session activity must fan a refresh out to the idle temp-table reserved connection")
	assert.Eventually(t, func() bool {
		return slices.Contains(sbclookup.GetExecuteReservedIDs(), int64(42))
	}, 30*time.Second, 10*time.Millisecond,
		"the refresh must carry the reserved id so it runs on the reserved connection")

	// The fanout is rate-limited per reserved id: right after a refresh the
	// same connection is not due again.
	r := executor.tempTableRefresher
	require.Empty(t, r.dueTargets(session), "a just-refreshed connection must not be due again within the interval")

	// A shard session with an open transaction is excluded: the tablet does
	// not reset its transaction timer for in-transaction activity, so the
	// refresh would only inject a query into the user's transaction.
	require.Empty(t, r.dueTargets(newSession(43, 7)), "in-transaction shard sessions must not be refreshed")

	// Sessions without temporary tables are ignored entirely.
	noTemp := newSession(44, 0)
	noTemp.GetOrCreateOptions().HasCreatedTempTables = false
	require.Empty(t, r.dueTargets(noTemp), "sessions without temp tables must not be refreshed")

	// Shard sessions without a reserved connection are ignored.
	require.Empty(t, r.dueTargets(newSession(0, 0)), "shard sessions without a reserved connection must not be refreshed")

	// Reserved ids are generated independently by each tablet and can collide
	// across tablets: the rate-limit key includes the tablet, so the same id
	// on a different tablet is still due.
	otherTablet := &topodatapb.TabletAlias{Cell: "other", Uid: 999}
	sameIDOtherTablet := econtext.NewSafeSession(&vtgatepb.Session{
		Autocommit:   true,
		TargetString: "@primary",
		Options:      &querypb.ExecuteOptions{HasCreatedTempTables: true},
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   lookupTablet.Keyspace,
				Shard:      lookupTablet.Shard,
				TabletType: lookupTablet.Type,
			},
			TabletAlias: otherTablet,
			ReservedId:  42,
		}},
	})
	require.Len(t, r.dueTargets(sameIDOtherTablet), 1,
		"the same reserved id on a different tablet must not be suppressed by the rate limiter")

	// A non-positive heartbeat interval disables the activity fanout too: the
	// rate limiter could not suppress anything, so every query would launch a
	// refresh per reservation.
	origInterval := tempTableHeartbeatTime
	tempTableHeartbeatTime = 0
	require.Empty(t, r.dueTargets(newSession(45, 0)), "a zero interval must disable the activity fanout")
	tempTableHeartbeatTime = origInterval

	// Shard sessions arrive in the client-roundtripped session, so one
	// command's fanout is capped; entries beyond the cap are not stamped and
	// refresh progressively on later activity.
	fresh := newTempTableActivityRefresher(nil)
	wide := &vtgatepb.Session{
		Autocommit:   true,
		TargetString: "@primary",
		Options:      &querypb.ExecuteOptions{HasCreatedTempTables: true},
	}
	for i := range tempTableRefreshMaxPerCommand + 10 {
		wide.ShardSessions = append(wide.ShardSessions, &vtgatepb.Session_ShardSession{
			Target: &querypb.Target{
				Keyspace:   lookupTablet.Keyspace,
				Shard:      lookupTablet.Shard,
				TabletType: lookupTablet.Type,
			},
			TabletAlias: lookupTablet.Alias,
			ReservedId:  int64(1000 + i),
		})
	}
	wideSession := econtext.NewSafeSession(wide)
	require.Len(t, fresh.dueTargets(wideSession), tempTableRefreshMaxPerCommand,
		"one command's fanout must be capped")
	require.NotEmpty(t, fresh.dueTargets(wideSession),
		"entries beyond the cap must not be stamped, so later activity picks them up")

	// The global in-flight bound skips launches and un-stamps the skipped
	// targets so the next activity retries them.
	bounded := newTempTableActivityRefresher(nil)
	bounded.inFlight.Store(tempTableRefreshMaxInFlight)
	boundedSession := econtext.NewSafeSession(&vtgatepb.Session{
		Autocommit:   true,
		TargetString: "@primary",
		Options:      &querypb.ExecuteOptions{HasCreatedTempTables: true},
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   lookupTablet.Keyspace,
				Shard:      lookupTablet.Shard,
				TabletType: lookupTablet.Type,
			},
			TabletAlias: lookupTablet.Alias,
			ReservedId:  4242,
		}},
	})
	bounded.onSessionActivity(ctx, boundedSession)
	require.EqualValues(t, tempTableRefreshMaxInFlight, bounded.inFlight.Load(),
		"no refresh may launch beyond the in-flight bound")
	require.Len(t, bounded.dueTargets(boundedSession), 1,
		"a target skipped at the in-flight bound must be un-stamped so the next activity retries it")

	// COM_PING is session activity too: on a direct MySQL connection a ping
	// resets wait_timeout, so a ping-only client through vtgate must likewise
	// keep its temp-table reserved connections alive. The ping is answered
	// locally by the protocol layer; the handler hook fans the refresh out.
	vh := newVtgateHandler(&VTGate{executor: executor})
	pingConn := mysql.GetTestConn()
	pingConn.ClientData = newSession(52, 0).Session
	vh.ComPing(pingConn)
	assert.Eventually(t, func() bool {
		return slices.Contains(sbclookup.GetExecuteReservedIDs(), int64(52))
	}, 30*time.Second, 10*time.Millisecond,
		"a client ping must fan a refresh out to the session's temp-table reserved connections")

	// A ping on a connection that never ran a query has no session and must
	// be a no-op.
	vh.ComPing(mysql.GetTestConn())

	// The lease ticker reads shard-session state concurrently with the
	// command's own execution, which updates TransactionId and ReservedId in
	// place via AppendOrUpdate under the session mutex. dueTargets must read a
	// snapshot taken under that mutex, not the live protos: this section fails
	// under the race detector if it reads them directly.
	racy := newTempTableActivityRefresher(nil)
	racySession := newSession(4300, 0)
	racySession.Session.InReservedConn = true
	liveShardSession := racySession.ShardSessionsForCleanup()[0]
	target := liveShardSession.Target
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 2000 {
			err := racySession.AppendOrUpdate(target, &refreshTestActionInfo{
				transactionID: int64(i % 2 * 7),
				reservedID:    4300,
				alias:         lookupTablet.Alias,
			}, liveShardSession, vtgatepb.TransactionMode_MULTI)
			assert.NoError(t, err)
		}
	}()
	go func() {
		defer wg.Done()
		for range 2000 {
			racy.dueTargets(racySession)
		}
	}()
	wg.Wait()
}

// refreshTestActionInfo is a minimal econtext.ShardActionInfo for driving
// AppendOrUpdate in tests.
type refreshTestActionInfo struct {
	transactionID, reservedID int64
	alias                     *topodatapb.TabletAlias
}

func (i *refreshTestActionInfo) TransactionID() int64           { return i.transactionID }
func (i *refreshTestActionInfo) ReservedID() int64              { return i.reservedID }
func (i *refreshTestActionInfo) RowsAffected() bool             { return false }
func (i *refreshTestActionInfo) Alias() *topodatapb.TabletAlias { return i.alias }

// TestTempTableCommandLease verifies the command-scoped lease: an immediate
// refresh when the command starts, and a settling refresh when it stops —
// which also covers a session whose first temp table was created by the
// command itself. stop is idempotent.
func TestTempTableCommandLease(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)
	sbclookup.RequireQueriesLocking()
	lookupTablet := sbclookup.Tablet()

	session := econtext.NewSafeSession(&vtgatepb.Session{
		Autocommit:   true,
		TargetString: "@primary",
		Options:      &querypb.ExecuteOptions{HasCreatedTempTables: true},
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   lookupTablet.Keyspace,
				Shard:      lookupTablet.Shard,
				TabletType: lookupTablet.Type,
			},
			TabletAlias: lookupTablet.Alias,
			ReservedId:  77,
		}},
	})

	r := executor.tempTableRefresher
	stop := r.commandLease(ctx, session)
	refreshed := func() bool {
		for _, q := range sbclookup.GetQueries() {
			if q.Sql == tempTableActivityRefreshQuery {
				return true
			}
		}
		return false
	}
	assert.Eventually(t, refreshed, 30*time.Second, 10*time.Millisecond,
		"the lease must fire an immediate refresh at command start")
	stop()
	stop() // idempotent

	// A session without temp tables leases as a no-op.
	noTemp := econtext.NewSafeSession(&vtgatepb.Session{Autocommit: true, TargetString: "@primary"})
	stopNoop := r.commandLease(ctx, noTemp)
	stopNoop()
}
