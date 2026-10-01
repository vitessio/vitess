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

package vreplication

import (
	"context"
	"errors"
	"io"
	"sync"
)

// errSchedulerAbandonedPendingWork is returned by nextReady when the scheduler
// is closed with pending work that can never become ready because nothing is
// inflight to advance the scheduler's state (lastCommittedSequence, writeset,
// inflight* counters). Workers surface this to their caller so the controller
// retries the stream from the last saved position rather than silently
// treating the abandoned pending work as "stream finished cleanly".
var errSchedulerAbandonedPendingWork = errors.New("parallel apply scheduler closed with unreachable pending transactions")

type applyTxn struct {
	// order is a monotonically increasing sequence number assigned by
	// scheduleLoop. The commitLoop commits transactions in strict order
	// so that the position saved to _vt.vreplication only moves forward.
	order int64
	// sequenceNumber is the source MySQL binlog sequence number from the
	// GTID event. Used to advance lastCommittedSequence after commit.
	sequenceNumber int64
	// commitParent is the source MySQL commit parent from the GTID event.
	// When the writeset is empty, the scheduler falls back to commit-parent
	// ordering: the transaction is ready only when commitParent <=
	// lastCommittedSequence.
	commitParent int64
	// hasCommitMeta is true when the GTID event carried non-zero
	// sequenceNumber or commitParent. Transactions with and without
	// commit metadata are never run concurrently (safety boundary).
	hasCommitMeta bool
	// forceGlobal is true for transactions that must serialize with
	// everything: non-row-only transactions (DDL, FIELD, OTHER, JOURNAL)
	// and copy-phase transactions.
	forceGlobal bool
	// noConflict is true for position-only saves and certain pass-through
	// events (OTHER, ignored DDL). These bypass all conflict checking and
	// are always ready, preventing deadlocks where an earlier-order
	// position save is blocked by later-order inflight data transactions.
	noConflict bool
	// writeset holds xxhash digests of PK-based keys (e.g. hash of "table:pk1,pk2").
	// Using uint64 hashes instead of strings eliminates per-txn heap allocations
	// in the scheduler hot path, reducing GC pressure at high TPS.
	writeset []uint64
	// mergedSequences tracks sequence numbers of source transactions that
	// were merged into this batched mega-transaction. They must be advanced
	// in lastCommittedSequence only after this txn actually commits, so that
	// later empty-writeset transactions whose commitParent references one of
	// these sequences don't become runnable before the batch commits.
	mergedSequences []int64
	// seq is the scheduler's enqueue sequence number, assigned by enqueue
	// starting at 1. Dependencies are expressed in seq rather than order so
	// the dependency clock does not rely on how callers number orders; in
	// production every order is enqueued exactly once and in sequence, so
	// seq == order.
	seq int64
	// dependsOn is the highest seq this transaction depends on, computed
	// once at enqueue time from the writesets and classification of the
	// transactions enqueued before it. The transaction is ready once every
	// seq up to and including dependsOn has committed (committedLWM >=
	// dependsOn). Because dependencies only ever point to earlier
	// transactions, a later transaction can never delay an earlier one.
	dependsOn int64
	// holdsSession is true while a dispatched worker transaction holds one of
	// the scheduler's accounted worker sessions (see sessionLimit). Released
	// by markCommitted.
	holdsSession bool
	// payload carries the transaction's events and DB connection info.
	// Pooled via applyTxnPayloadPool to reduce allocations.
	payload *applyTxnPayload
	// done is a buffered channel (cap 1) the commitLoop sends on after
	// committing a worker transaction, for observers of that commit. Workers
	// do not wait on it: they hand their session to the commitLoop and move
	// on. Always freshly allocated by acquireApplyTxn.
	done chan struct{}
}

type applyScheduler struct {
	// ctx is the parent context for the parallel applier. When cancelled,
	// all blocked nextReady/waitForIdle calls return immediately.
	ctx context.Context

	mu   sync.Mutex
	cond *sync.Cond
	// orderCond is a dedicated condition for the enqueue backpressure wait
	// (maxOutstandingOrders). The shared cond's Signal in markCommitted can
	// land on an idle worker that consumes the wakeup without re-signaling,
	// leaving the scheduleLoop asleep until the pipeline fully drains (the
	// allDrained Broadcast backstop). A dedicated cond makes the order-window
	// wakeup deterministic.
	orderCond *sync.Cond

	// pending is the queue of transactions waiting to be dispatched to
	// workers. Entries are set to nil when consumed; pendingOff tracks
	// how far into the slice consumed entries extend, and the slice is
	// compacted when half its capacity is nil entries.
	pending      []*applyTxn
	pendingOff   int // offset into pending slice; entries before this index are consumed
	pendingCount int // number of live (non-nil) entries in pending
	// lastCommittedSequence is the highest source MySQL sequence number
	// that has been committed. Used for commit-parent ordering: a
	// transaction whose writeset is empty is ready only when its
	// commitParent <= lastCommittedSequence.
	lastCommittedSequence int64
	// lastCommittedOrder is the highest transaction order number that
	// has been committed, used for diagnostics.
	lastCommittedOrder int64
	// maxOutstandingOrders caps how many ordered transactions may exist ahead
	// of durable commit progress. Zero disables the cap.
	maxOutstandingOrders int64

	// enqueueSeq is the seq assigned to the most recently enqueued txn.
	enqueueSeq int64
	// committedLWM is the commit low-water mark: every seq <= committedLWM
	// has committed. Since commitLoop commits in strict order this normally
	// advances one by one; committedAhead holds seqs committed beyond a gap
	// so the mark stays exact even if they are not. This is the logical
	// clock that dependsOn is compared against, mirroring the LWM clock of
	// MySQL's Change Stream Applier.
	committedLWM   int64
	committedAhead map[int64]struct{}
	// lastWriter maps a writeset key hash to the seq of the most recently
	// enqueued transaction whose writeset contains it. A new transaction
	// sharing a key depends on that seq. Depending on the last writer is
	// sufficient: it depended on any earlier writer of the key, and commits
	// happen in order. Entries are removed when that writer commits.
	lastWriter map[uint64]int64
	// lastBarrierSeq is the seq of the most recent transaction that must
	// serialize with everything (forceGlobal, or no commit metadata and no
	// writeset). Every later transaction depends on it.
	lastBarrierSeq int64
	// lastCommitMetaSeq and lastMissingMetaSeq are the seqs of the most
	// recently enqueued non-barrier transactions with and without commit
	// metadata. Transactions of the two classes never run concurrently, so
	// each depends on the latest transaction of the other class.
	lastCommitMetaSeq  int64
	lastMissingMetaSeq int64
	// sessionLimit is the number of worker sessions (MySQL connections)
	// shared by the workers; zero disables session accounting. A dispatched
	// worker transaction holds a session until it commits, so the scheduler
	// only dispatches worker transactions while a session is free, and keeps
	// the last free session for the transaction that is next to commit.
	// Without that reservation, out-of-order dispatch could park every
	// session on a later transaction waiting for its commit turn, leaving
	// no session for the earlier transaction they are all waiting on.
	sessionLimit  int
	sessionsInUse int
	// sessionHolders is the set of seqs that currently hold a session.
	sessionHolders map[int64]struct{}
	// The inflight counters below track dispatched-but-uncommitted
	// transactions by class. Readiness is decided by dependsOn against
	// committedLWM; these counters feed the idle/abandoned-work predicates.
	//
	// inflightGlobal counts inflight forceGlobal transactions and
	// no-metadata-no-writeset transactions.
	inflightGlobal int
	// inflightMissingMeta counts inflight transactions that lack commit
	// metadata.
	inflightMissingMeta int
	// inflightCommitMeta counts inflight transactions that have commit
	// metadata.
	inflightCommitMeta int
	// inflightNoConflict counts dispatched-but-uncommitted noConflict
	// transactions. They do not participate in conflict checking, but the
	// abandoned-pending-work check must not fire while one is in flight:
	// its markCommitted can advance lastCommittedSequence and unblock the
	// pending head.
	inflightNoConflict int

	// closed is set by close() to signal that no more transactions will
	// be enqueued. nextReady checks this to return io.EOF instead of
	// blocking forever on cond.Wait after the scheduler is shut down.
	closed bool
}

// newApplyScheduler creates a scheduler and starts a background goroutine
// that broadcasts on cond when ctx is cancelled, unblocking any workers
// waiting in nextReady.
func newApplyScheduler(ctx context.Context) *applyScheduler {
	s := &applyScheduler{
		ctx:            ctx,
		lastWriter:     make(map[uint64]int64),
		committedAhead: make(map[int64]struct{}),
		sessionHolders: make(map[int64]struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	s.orderCond = sync.NewCond(&s.mu)
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		defer s.mu.Unlock()
		s.cond.Broadcast()
		s.orderCond.Broadcast()
	}()
	return s
}

// enqueue adds a transaction to the pending queue and signals one waiting
// worker. On the first hasCommitMeta transaction, it seeds lastCommittedSequence
// from commitParent so that subsequent commit-parent checks have a baseline.
func (s *applyScheduler) enqueue(txn *applyTxn) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return io.EOF
	}
	for s.maxOutstandingOrders > 0 && txn.order > 0 && txn.order-s.lastCommittedOrder > s.maxOutstandingOrders {
		s.orderCond.Wait()
		if err := s.ctx.Err(); err != nil {
			return err
		}
		if s.closed {
			return io.EOF
		}
	}
	if txn.hasCommitMeta && s.lastCommittedSequence == 0 && s.inflightGlobal == 0 && s.inflightMissingMeta == 0 && s.inflightCommitMeta == 0 && s.pendingCount == 0 && txn.commitParent > 0 {
		s.lastCommittedSequence = txn.commitParent
	}
	s.computeDependsOnLocked(txn)
	s.pending = append(s.pending, txn)
	s.pendingCount++
	// Signal wakes one worker. enqueue adds at most one transaction, so at
	// most one worker can dequeue it via popReadyLocked. This avoids the
	// thundering-herd effect of Broadcast which wakes all N workers.
	s.cond.Signal()
	return nil
}

// nextReady blocks until a transaction in the pending queue passes the
// readiness check, marks it inflight, removes it from the queue, and returns
// it to the calling worker. Returns io.EOF when the scheduler is closed and
// there is no pending work left to drain.
func (s *applyScheduler) nextReady(ctx context.Context) (*applyTxn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := s.ctx.Err(); err != nil {
			return nil, err
		}
		txn := s.popReadyLocked()
		if txn != nil {
			if s.needsSessionLocked(txn) {
				txn.holdsSession = true
				s.sessionsInUse++
				s.sessionHolders[txn.seq] = struct{}{}
			}
			s.markInflightLocked(txn)
			// Pass the baton: one wakeup (e.g. a markCommitted that released
			// a multi-key writeset) can make several pending transactions
			// ready at once, but each waiter pops at most one. Signal the
			// next waiter while pending work remains so independent ready
			// transactions dispatch immediately instead of waiting for the
			// next commit event.
			if s.pendingCount > 0 {
				s.cond.Signal()
			}
			return txn, nil
		}
		// Check closed only after attempting to drain any queued work so
		// transactions already scheduled before shutdown still commit.
		if s.closed {
			if s.pendingCount == 0 {
				return nil, io.EOF
			}
			// A closed scheduler may still have blocked pending work that
			// becomes ready only after an inflight txn commits — in that
			// case we keep waiting so the blocked pending txns unblock.
			// But if nothing is inflight AND no pending txn is ready,
			// nothing will ever advance lastCommittedSequence or release
			// writeset/inflight counters, so workers would park forever.
			// Return a non-EOF error so the controller retries the stream
			// from the last saved position instead of silently abandoning
			// the pending work.
			if s.inflightGlobal == 0 && s.inflightMissingMeta == 0 && s.inflightCommitMeta == 0 && s.inflightNoConflict == 0 {
				return nil, errSchedulerAbandonedPendingWork
			}
		}
		s.cond.Wait()
	}
}

// markCommitted releases the transaction's inflight state and advances
// lastCommittedSequence. Uses Broadcast when a global/missingMeta counter
// drops to zero (multiple txns may unblock), Signal otherwise.
func (s *applyScheduler) markCommitted(txn *applyTxn) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if txn.hasCommitMeta && txn.sequenceNumber > s.lastCommittedSequence {
		s.lastCommittedSequence = txn.sequenceNumber
	}
	// Advance any sequences that were batched (merged away) into this txn.
	// These represent transactions whose events were merged into this batch
	// but whose GTID sequence numbers must still become visible in
	// lastCommittedSequence so that later empty-writeset commit-parent
	// dependents can unblock. Doing this here (after commit) instead of at
	// enqueue time preserves the invariant that commit-parent dependencies
	// are only satisfied after the parent has actually committed.
	for _, seq := range txn.mergedSequences {
		if seq > s.lastCommittedSequence {
			s.lastCommittedSequence = seq
		}
	}
	if txn.order > 0 && txn.order > s.lastCommittedOrder {
		s.lastCommittedOrder = txn.order
		// Wake the scheduleLoop if it is blocked on the order window; only
		// commits advance lastCommittedOrder, so this is the only wake site.
		s.orderCond.Signal()
	}
	s.advanceLWMLocked(txn.seq)
	// Forget this transaction as the last writer of its keys so lastWriter
	// only holds keys of uncommitted transactions. A key whose last writer
	// is a later transaction keeps that entry.
	for _, key := range txn.writeset {
		if s.lastWriter[key] == txn.seq {
			delete(s.lastWriter, key)
		}
	}
	if txn.holdsSession {
		txn.holdsSession = false
		s.sessionsInUse--
		delete(s.sessionHolders, txn.seq)
	}
	// Track pre-release state to decide between Signal and Broadcast.
	wasForceGlobal := txn.forceGlobal
	hadInflightGlobal := s.inflightGlobal > 0
	hadInflightMissingMeta := s.inflightMissingMeta > 0
	s.releaseInflightLocked(txn)
	// Use Broadcast when releasing a forceGlobal txn, when a global/
	// missingMeta counter drops to zero, or when all inflight work has
	// drained (so waitForIdle waiters are woken). Otherwise use Signal
	// to avoid thundering-herd wakeup of N workers when only one txn
	// can proceed.
	allDrained := s.inflightGlobal == 0 && s.inflightMissingMeta == 0 && s.inflightCommitMeta == 0
	if wasForceGlobal ||
		(hadInflightGlobal && s.inflightGlobal == 0) ||
		(hadInflightMissingMeta && s.inflightMissingMeta == 0) ||
		allDrained {
		s.cond.Broadcast()
	} else {
		s.cond.Signal()
	}
	return nil
}

// popReadyLocked returns the lowest-ordered dispatchable transaction in the
// pending queue, or nil if none is ready.
//
// Unlike a strict in-order dispatcher, it does not stop at a blocked
// transaction: a later transaction whose dependencies have committed is
// dispatched even when an earlier one is still waiting for its own. That is
// safe because readiness only depends on earlier transactions (dependsOn <
// txn.seq), so a dispatched later transaction can never be what an earlier
// one is waiting for, and the commitLoop's strict ordering cannot deadlock
// against it. Picking the lowest ready order first keeps the transaction
// that is next to commit at the front of the line.
func (s *applyScheduler) popReadyLocked() *applyTxn {
	for i := s.pendingOff; i < len(s.pending); i++ {
		txn := s.pending[i]
		if txn == nil {
			continue
		}
		if !s.isReadyLocked(txn) {
			continue
		}
		if s.needsSessionLocked(txn) && !s.sessionAvailableLocked(txn) {
			continue
		}
		s.removePendingLocked(i)
		return txn
	}
	return nil
}

// needsSessionLocked reports whether dispatching txn takes a worker session:
// worker transactions do when session accounting is enabled; commitOnly
// transactions run on the main connection and do not.
func (s *applyScheduler) needsSessionLocked(txn *applyTxn) bool {
	return s.sessionLimit > 0 && txn.payload != nil && !txn.payload.commitOnly
}

// sessionAvailableLocked reports whether a worker session may be handed to
// txn. The last free session is reserved for the transaction that is next to
// commit (committedLWM+1) unless that transaction already holds a session:
// every other in-use session may be parked on a later transaction waiting for
// exactly that commit, so it must always be able to run. Once it holds a
// session, its commit returns one to the pool, which the next transaction to
// commit can then use, so the last session need not be held back.
func (s *applyScheduler) sessionAvailableLocked(txn *applyTxn) bool {
	free := s.sessionLimit - s.sessionsInUse
	if free > 1 {
		return true
	}
	if free < 1 {
		return false
	}
	head := s.committedLWM + 1
	if txn.seq == head {
		return true
	}
	_, headHoldsSession := s.sessionHolders[head]
	return headHoldsSession
}

// removePendingLocked removes the element at index i by setting it to nil and
// advancing pendingOff if it's the head element. This avoids O(n) memory shifts
// from append-based removal. The slice is compacted when half or more of its
// capacity is consumed by nil entries.
func (s *applyScheduler) removePendingLocked(i int) {
	s.pending[i] = nil
	s.pendingCount--
	// Advance the offset past any leading nils.
	for s.pendingOff < len(s.pending) && s.pending[s.pendingOff] == nil {
		s.pendingOff++
	}
	// Compact when the offset has consumed half or more of the slice.
	if s.pendingOff > 0 && s.pendingOff >= len(s.pending)/2 {
		n := copy(s.pending, s.pending[s.pendingOff:])
		// Clear trailing pointers so GC can collect them.
		for j := n; j < len(s.pending); j++ {
			s.pending[j] = nil
		}
		s.pending = s.pending[:n]
		s.pendingOff = 0
	}
	// Shrink capacity after bursts to prevent permanent memory retention.
	// If the backing array is >64 slots and >4x the live element count,
	// allocate a right-sized slice and copy.
	n := len(s.pending)
	if cap(s.pending) > 64 && cap(s.pending) > 4*n {
		shrunk := make([]*applyTxn, n, 2*n+1)
		copy(shrunk, s.pending)
		s.pending = shrunk
	}
}

// computeDependsOnLocked assigns txn its seq, sets txn.dependsOn from the
// transactions enqueued before it, and records txn in the per-key and
// per-class trackers. Must be called under s.mu by enqueue.
//
// The rules reproduce the PR's original conflict classes as ordered
// dependencies:
//   - noConflict transactions depend on nothing.
//   - Barriers (forceGlobal, or no commit metadata and no writeset) depend on
//     every earlier transaction, and every later transaction depends on them.
//   - Transactions with and without commit metadata depend on the latest
//     transaction of the other class.
//   - A transaction with a writeset depends on the last writer of each of its
//     keys. With commit metadata, that is all: like MySQL's WRITESET
//     tracking, the source's commit parent is ignored when a writeset exists.
//   - A commit-metadata transaction with an empty writeset depends on the
//     latest commit-metadata transaction and, at readiness, on its commit
//     parent (see isReadyLocked).
func (s *applyScheduler) computeDependsOnLocked(txn *applyTxn) {
	s.enqueueSeq++
	txn.seq = s.enqueueSeq
	if txn.noConflict {
		txn.dependsOn = 0
		return
	}
	if txn.forceGlobal || (!txn.hasCommitMeta && len(txn.writeset) == 0) {
		txn.dependsOn = txn.seq - 1
		s.lastBarrierSeq = txn.seq
		return
	}
	dependsOn := s.lastBarrierSeq
	if txn.hasCommitMeta {
		dependsOn = max(dependsOn, s.lastMissingMetaSeq)
		if len(txn.writeset) == 0 {
			dependsOn = max(dependsOn, s.lastCommitMetaSeq)
		}
		s.lastCommitMetaSeq = txn.seq
	} else {
		dependsOn = max(dependsOn, s.lastCommitMetaSeq)
		s.lastMissingMetaSeq = txn.seq
	}
	for _, key := range txn.writeset {
		// A key repeated within the writeset must not make the transaction
		// depend on itself.
		if writer, ok := s.lastWriter[key]; ok && writer != txn.seq {
			dependsOn = max(dependsOn, writer)
		}
		s.lastWriter[key] = txn.seq
	}
	txn.dependsOn = dependsOn
}

// advanceLWMLocked records that seq committed and advances committedLWM over
// every consecutively committed seq. Must be called under s.mu.
func (s *applyScheduler) advanceLWMLocked(seq int64) {
	if seq <= s.committedLWM {
		return
	}
	if seq != s.committedLWM+1 {
		s.committedAhead[seq] = struct{}{}
		return
	}
	s.committedLWM = seq
	for {
		if _, ok := s.committedAhead[s.committedLWM+1]; !ok {
			return
		}
		delete(s.committedAhead, s.committedLWM+1)
		s.committedLWM++
	}
}

// isReadyLocked reports whether every dependency of txn has committed.
func (s *applyScheduler) isReadyLocked(txn *applyTxn) bool {
	// noConflict transactions (e.g., position-only saves) are always ready.
	// They have no data conflicts and must not block or be blocked by other
	// transactions.
	if txn.noConflict {
		return true
	}
	if txn.dependsOn > s.committedLWM {
		return false
	}
	// A commit-metadata transaction without a writeset falls back to the
	// source's commit-parent ordering as an additional safety net.
	//
	// NOTE: sequence_number/last_committed reset per binlog FILE on the
	// source, while lastCommittedSequence only advances (max). After a
	// binlog rotation the new file's small commitParent values compare
	// against the old file's high watermark, making this check vacuously
	// true. That is safe ONLY because such a transaction also depends on
	// the latest earlier commit-metadata transaction (see
	// computeDependsOnLocked): every earlier-ordered transaction of its
	// class has already committed, so the parent is durably applied
	// regardless of what this comparison says.
	if !txn.forceGlobal && txn.hasCommitMeta && len(txn.writeset) == 0 {
		return txn.commitParent <= s.lastCommittedSequence
	}
	return true
}

// markInflightLocked increments the appropriate inflight counters. Must be
// called under s.mu.
func (s *applyScheduler) markInflightLocked(txn *applyTxn) {
	if txn.noConflict {
		s.inflightNoConflict++
		return
	}
	if txn.forceGlobal {
		s.inflightGlobal++
		return
	}
	if txn.hasCommitMeta {
		s.inflightCommitMeta++
		return
	}
	if len(txn.writeset) == 0 {
		s.inflightGlobal++
	}
	s.inflightMissingMeta++
}

// releaseInflightLocked decrements the inflight counters. The inverse of markInflightLocked. Must be called under s.mu.
func (s *applyScheduler) releaseInflightLocked(txn *applyTxn) {
	if txn.noConflict {
		if s.inflightNoConflict > 0 {
			s.inflightNoConflict--
		}
		return
	}
	if txn.forceGlobal {
		if s.inflightGlobal > 0 {
			s.inflightGlobal--
		}
		return
	}
	if txn.hasCommitMeta {
		if s.inflightCommitMeta > 0 {
			s.inflightCommitMeta--
		}
		return
	}
	if len(txn.writeset) == 0 && s.inflightGlobal > 0 {
		s.inflightGlobal--
	}
	if s.inflightMissingMeta > 0 {
		s.inflightMissingMeta--
	}
}

// advanceCommittedSequence advances lastCommittedSequence for transactions
// that bypass the scheduler (e.g., empty transactions handled via unsavedEvent).
// Without this, hasCommitMeta transactions whose commitParent references a
// skipped empty transaction would be blocked forever because lastCommittedSequence
// would never reach their commitParent value.
func (s *applyScheduler) advanceCommittedSequence(seq int64) {
	if seq <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq > s.lastCommittedSequence {
		s.lastCommittedSequence = seq
		// Only wake waiters when there is pending work that could now be
		// ready. During catch-up on a filtered shard this is called for
		// every empty transaction (thousands/sec); an unconditional
		// Broadcast would wake all N workers each time just to rescan an
		// empty queue.
		if s.pendingCount > 0 {
			s.cond.Broadcast()
		}
	}
}

// idle reports whether the scheduler has no pending or inflight transactions
// of any class: everything enqueued so far has been durably committed. It is
// the non-blocking form of waitForIdle's predicate. scheduleItems uses it to
// gate out-of-band heartbeat writes: setting time_updated==time_heartbeat
// tells getVReplicationTrxLag that the stream is fully caught up, which is
// only true when no scheduled work remains uncommitted.
func (s *applyScheduler) idle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingCount == 0 && s.inflightGlobal == 0 && s.inflightMissingMeta == 0 &&
		s.inflightCommitMeta == 0 && s.inflightNoConflict == 0
}

// waitForIdle blocks until there are no pending or inflight transactions of
// any class. scheduleLoop calls it as a barrier after a DDL fetch so that the
// DDL, its FK-metadata refresh, and any FIELD events for DDL-affected tables
// are fully applied before the next fetch snapshots plans/FK refs. The idle
// predicate must therefore cover every inflight counter — including
// inflightNoConflict (position-only saves, OTHER/IGNORE stops) — so the barrier cannot return while any dispatched
// transaction is still uncommitted. This mirrors the fully-drained predicate
// in nextReady's abandoned-work check.
func (s *applyScheduler) waitForIdle(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.ctx.Err(); err != nil {
			return err
		}
		if s.pendingCount == 0 && s.inflightGlobal == 0 && s.inflightMissingMeta == 0 &&
			s.inflightCommitMeta == 0 && s.inflightNoConflict == 0 {
			return nil
		}
		s.cond.Wait()
	}
}

// close marks the scheduler as closed and broadcasts to wake blocked workers.
// Already-enqueued work remains available so callers can drain the scheduled
// prefix before observing io.EOF.
func (s *applyScheduler) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	s.closed = true
	s.cond.Broadcast()
	s.orderCond.Broadcast()
	return io.EOF
}
