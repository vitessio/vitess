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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
}
