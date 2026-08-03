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
	"vitess.io/vitess/go/vt/topo/topoproto"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

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

// tempTableActivityRefreshTimeout bounds one background refresh — a single
// trivial statement on one reserved connection. It is deliberately shorter
// than the foreground wait-out deadline in TxPool.GetAndLock, so a foreground
// command that collides with a refresh always outwaits it rather than
// returning an in-use error to the client.
const tempTableActivityRefreshTimeout = 1 * time.Second

// tempTableRefreshPruneEvery bounds how many distinct reserved connections are
// tracked between cleanups of entries whose connections are gone.
const tempTableRefreshPruneEvery = 4096

// tempTableRefreshMaxPerCommand caps how many reserved connections one command
// can schedule refreshes for. Shard sessions arrive in the client-roundtripped
// session, so their count is client-controlled; without a cap a hostile
// session with tens of thousands of entries would fan a goroutine and RPC out
// for each. Entries beyond the cap are not stamped, so a legitimate very-wide
// session still refreshes progressively across its subsequent commands.
const tempTableRefreshMaxPerCommand = 256

// tempTableRefreshMaxInFlight bounds refresh goroutines across all sessions.
// A target skipped at the bound has its rate-limiter stamp removed so the next
// activity retries it.
const tempTableRefreshMaxInFlight = 256

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

		// lastRefresh maps a reserved connection — keyed by tablet alias plus
		// reserved id, since reserved ids are generated independently by each
		// tablet and can collide across tablets — to the unix-nano time of
		// its last refresh, bounding the fanout to one refresh per heartbeat
		// interval per connection regardless of the session's query rate.
		// Entries for connections that no longer exist are pruned in
		// amortized batches.
		lastRefresh       sync.Map // tempTableRefreshKey -> int64 (unix nanos)
		insertsSincePrune atomic.Int64

		// inFlight counts running refresh goroutines, enforcing
		// tempTableRefreshMaxInFlight across all sessions.
		inFlight atomic.Int64
	}

	// tempTableRefreshKey identifies one reserved connection across tablets.
	tempTableRefreshKey struct {
		alias      string
		reservedID int64
	}

	// tempTableRefreshTarget is one reserved connection due for a refresh.
	tempTableRefreshTarget struct {
		target     *querypb.Target
		alias      *topodatapb.TabletAlias
		reservedID int64
		key        tempTableRefreshKey
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
	// Each target refreshes independently with its own timeout, so one slow
	// or unreachable tablet cannot starve the others' refreshes. Concurrency
	// is bounded per command by the dueTargets cap and globally by
	// tempTableRefreshMaxInFlight; a target skipped at the global bound has
	// its stamp removed so the next activity retries it. The refresh must
	// survive the request that triggered it: context.WithoutCancel keeps the
	// caller id and tracing values while detaching from the request's
	// cancellation.
	detached := context.WithoutCancel(ctx)
	for _, t := range targets {
		if r.inFlight.Add(1) > tempTableRefreshMaxInFlight {
			r.inFlight.Add(-1)
			r.lastRefresh.Delete(t.key)
			continue
		}
		rctx, cancel := context.WithTimeout(detached, tempTableActivityRefreshTimeout)
		go func(t tempTableRefreshTarget) {
			defer r.inFlight.Add(-1)
			defer cancel()
			r.refresh(rctx, t)
		}(t)
	}
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
	// A non-positive interval disables vtgate's temp-table keepalive
	// machinery entirely — the background sweeper and this activity fanout
	// alike (without an interval the rate limiter could not suppress
	// anything, and every query would launch a refresh per reservation).
	// The tablet-side idle timeout is unaffected.
	interval := tempTableHeartbeatTime
	if interval <= 0 {
		return nil
	}
	now := time.Now().UnixNano()
	var due []tempTableRefreshTarget
	// Snapshots, not the live shard-session protos: the lease ticker calls
	// this concurrently with the command's own execution, which updates
	// TransactionId and ReservedId on the live protos as it runs.
	for _, ss := range session.ShardSessionSnapshots() {
		if len(due) >= tempTableRefreshMaxPerCommand {
			break
		}
		if ss.ReservedID == 0 || ss.TransactionID != 0 ||
			ss.Target == nil || ss.TabletAlias == nil {
			continue
		}
		key := tempTableRefreshKey{
			alias:      topoproto.TabletAliasString(ss.TabletAlias),
			reservedID: ss.ReservedID,
		}
		if last, ok := r.lastRefresh.Load(key); ok {
			if now-last.(int64) < interval.Nanoseconds() {
				continue
			}
		} else if r.insertsSincePrune.Add(1) >= tempTableRefreshPruneEvery {
			r.insertsSincePrune.Store(0)
			r.prune(now, interval)
		}
		r.lastRefresh.Store(key, now)
		due = append(due, tempTableRefreshTarget{
			target:     ss.Target,
			alias:      ss.TabletAlias,
			reservedID: ss.ReservedID,
			key:        key,
		})
	}
	return due
}

// commandLease covers one client command with activity refreshes: one at the
// start, one per interval while the command runs, and one when it settles. A
// start-or-end-only refresh leaves a gap Graham pointed out: a query or
// stream running on shard B for longer than shard A's idle timeout would let
// A's temp table expire mid-command even though the session is visibly
// active. The rate limiter dedupes the start/tick/end calls, so a short
// command still costs at most one refresh per connection. The returned stop
// is idempotent; it joins the ticker goroutine before firing the settling
// refresh — each command wraps the shared session state in its own
// SafeSession, so a tick left running past stop would race the next
// command's session mutations — and the settling refresh also covers a
// session whose first temporary table was created by this very command.
func (r *tempTableActivityRefresher) commandLease(ctx context.Context, session *econtext.SafeSession) func() {
	if r == nil || session == nil {
		return func() {}
	}
	r.onSessionActivity(ctx, session)
	var done, exited chan struct{}
	if interval := tempTableHeartbeatTime; interval > 0 && session.GetOptions().GetHasCreatedTempTables() {
		done = make(chan struct{})
		exited = make(chan struct{})
		go func() {
			defer close(exited)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					r.onSessionActivity(ctx, session)
				case <-done:
					return
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			if done != nil {
				close(done)
				<-exited
			}
			r.onSessionActivity(ctx, session)
		})
	}
}

// refresh runs the ordinary refresh statement on one reserved connection,
// best-effort. An error — connection reclaimed, tablet unreachable, the
// connection momentarily busy with a client command or keepalive — is only
// logged at the debug level the background beats use: the rate limiter
// retries on the session's next activity an interval later, and a connection
// that is genuinely gone errors on its next real use anyway.
func (r *tempTableActivityRefresher) refresh(ctx context.Context, t tempTableRefreshTarget) {
	// The request-level marker makes the tablet lock the reserved connection
	// under a purpose that a colliding client command briefly waits out
	// instead of failing with an in-use error.
	ctx = queryservice.ContextWithReservedConnActivityRefresh(ctx)
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
