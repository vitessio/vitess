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
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/log"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// tempTableActivityRefreshQuery is the statement the activity fanout runs on
// an idle temp-table reserved connection. Unlike the background keepalive
// touch — which deliberately never reaches mysqld — this is an ordinary query
// executed on the reserved connection itself, so it resets both the tablet's
// idle timer and mysqld's wait_timeout clock. That is intentional: the
// trigger is real client activity on the session, and on a direct MySQL
// connection session activity and connection activity are the same thing. A
// truly idle session sends nothing, so it still ages out at wait_timeout
// exactly as MySQL would.
const tempTableActivityRefreshQuery = "/* temp-table activity refresh */ select 1"

// tempTableActivityRefreshTimeout bounds one background fanout.
const tempTableActivityRefreshTimeout = 10 * time.Second

// tempTableRefreshPruneEvery bounds how many distinct reserved connections are
// tracked between cleanups of entries whose connections are gone.
const tempTableRefreshPruneEvery = 4096

type (
	// tempTableActivityRefresher fans real session activity out to the
	// session's temp-table reserved connections that the queries themselves
	// do not reach. A session's queries refresh only the connections they
	// route to, so a session that stays active against other shards would
	// still lose its temporary tables at the pinned connection's idle
	// timeout; on a direct MySQL connection that cannot happen, because any
	// activity is activity on the one connection. The fanout restores that
	// parity for both the MySQL protocol and the gRPC API, complementing the
	// connection-anchored background heartbeat (which covers connected but
	// completely idle MySQL-protocol clients) and the tablet-side temp-table
	// idle timeout (which bounds abandoned sessions).
	tempTableActivityRefresher struct {
		gw *TabletGateway

		// lastRefresh maps a reserved id to the unix-nano time of its last
		// refresh, bounding the fanout to one refresh per heartbeat interval
		// per connection regardless of the session's query rate. Entries for
		// connections that no longer exist are pruned in amortized batches.
		lastRefresh       sync.Map // int64 (reserved id) -> int64 (unix nanos)
		insertsSincePrune atomic.Int64
	}

	// tempTableRefreshTarget is one reserved connection due for a refresh.
	tempTableRefreshTarget struct {
		target     *querypb.Target
		alias      *topodatapb.TabletAlias
		reservedID int64
	}
)

func newTempTableActivityRefresher(gw *TabletGateway) *tempTableActivityRefresher {
	return &tempTableActivityRefresher{gw: gw}
}

// onSessionActivity fans the just-executed query's liveness signal out to the
// session's temp-table reserved connections that are due for a refresh.
// Fire-and-forget: it never blocks the caller and its failures are invisible
// to the user's query — a connection that cannot be refreshed is reclaimed by
// the tablet's idle timeout exactly as if the session had gone idle.
func (r *tempTableActivityRefresher) onSessionActivity(ctx context.Context, session *econtext.SafeSession) {
	if r == nil || session == nil {
		return
	}
	targets := r.dueTargets(session)
	if len(targets) == 0 {
		return
	}
	// The refresh must survive the request that triggered it:
	// context.WithoutCancel keeps the caller id and tracing values while
	// detaching from the request's cancellation.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), tempTableActivityRefreshTimeout)
	go func() {
		defer cancel()
		for _, t := range targets {
			r.refresh(rctx, t)
		}
	}()
}

// dueTargets snapshots the session's reserved connections that are due for an
// activity refresh, and stamps them, so concurrent callers do not double-send
// (a rare race sending one extra refresh is harmless). Shard sessions with an
// open transaction are excluded — the tablet does not reset its transaction
// timer for in-transaction activity, so a refresh would only inject a query
// into the user's transaction — as are sessions without temporary tables.
func (r *tempTableActivityRefresher) dueTargets(session *econtext.SafeSession) []tempTableRefreshTarget {
	if !session.GetOptions().GetHasCreatedTempTables() {
		return nil
	}
	interval := tempTableHeartbeatTime
	now := time.Now().UnixNano()
	var due []tempTableRefreshTarget
	for _, ss := range session.ShardSessionsForCleanup() {
		if ss.GetReservedId() == 0 || ss.GetTransactionId() != 0 {
			continue
		}
		if last, ok := r.lastRefresh.Load(ss.GetReservedId()); ok {
			if now-last.(int64) < interval.Nanoseconds() {
				continue
			}
		} else if r.insertsSincePrune.Add(1) >= tempTableRefreshPruneEvery {
			r.insertsSincePrune.Store(0)
			r.prune(now, interval)
		}
		r.lastRefresh.Store(ss.GetReservedId(), now)
		due = append(due, tempTableRefreshTarget{
			target:     ss.GetTarget(),
			alias:      ss.GetTabletAlias(),
			reservedID: ss.GetReservedId(),
		})
	}
	return due
}

// refresh runs the ordinary refresh statement on one reserved connection,
// best-effort. An error — connection reclaimed, tablet unreachable, the
// connection momentarily busy with a client command or keepalive — is only
// logged at the debug level the background beats use: the rate limiter
// retries on the session's next activity an interval later, and a connection
// that is genuinely gone errors on its next real use anyway.
func (r *tempTableActivityRefresher) refresh(ctx context.Context, t tempTableRefreshTarget) {
	qs, err := r.gw.QueryServiceByAlias(ctx, t.alias, t.target)
	if err == nil {
		_, err = qs.Execute(ctx, nil, t.target, tempTableActivityRefreshQuery, nil, 0 /* transactionID */, t.reservedID, nil /* options */)
	}
	if err != nil {
		log.V(2).Info("temp-table activity refresh failed",
			slog.Int64("reserved_id", t.reservedID),
			slog.Any("error", err))
	}
}

// prune drops rate-limiter entries idle for many intervals: their connections
// were either reclaimed or their sessions have gone quiet, and a stale entry's
// only cost on revival is one immediate (rather than deferred) refresh.
func (r *tempTableActivityRefresher) prune(now int64, interval time.Duration) {
	cutoff := now - 10*interval.Nanoseconds()
	r.lastRefresh.Range(func(k, v any) bool {
		if v.(int64) < cutoff {
			r.lastRefresh.Delete(k)
		}
		return true
	})
}
