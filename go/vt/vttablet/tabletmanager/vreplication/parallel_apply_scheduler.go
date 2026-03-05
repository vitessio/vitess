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
	"io"
	"sync"
)

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
	// everything: non-row-only transactions (DDL, FIELD, OTHER, JOURNAL),
	// copy-phase transactions, or transactions whose writeset build failed.
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
	// payload carries the transaction's events and DB connection info.
	// Pooled via applyTxnPayloadPool to reduce allocations.
	payload *applyTxnPayload
	// done is a buffered channel (cap 1) used to synchronize the commitLoop
	// with the worker that applied this transaction. The commitLoop sends on
	// done after committing, unblocking the worker to reuse its DB connection.
	// Pooled across sync.Pool cycles to avoid per-txn allocation.
	// Nil for commitOnly transactions (workers don't wait on them).
	done chan struct{}
}

type applyScheduler struct {
	// ctx is the parent context for the parallel applier. When cancelled,
	// all blocked nextReady/waitForIdle calls return immediately.
	ctx context.Context

	mu   sync.Mutex
	cond *sync.Cond

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

	// inflightWriteset maps writeset key hashes to reference counts.
	// A transaction is blocked if any of its writeset keys are present
	// in this map with count > 0.
	inflightWriteset map[uint64]int
	// inflightGlobal counts inflight forceGlobal transactions and
	// no-metadata-no-writeset transactions. When > 0, all non-noConflict
	// transactions are blocked.
	inflightGlobal int
	// inflightMissingMeta counts inflight transactions that lack commit
	// metadata. When > 0, hasCommitMeta transactions are blocked to
	// maintain the safety boundary between metadata modes.
	inflightMissingMeta int
	// inflightCommitMeta counts inflight transactions that have commit
	// metadata. When > 0, no-metadata transactions with writesets must
	// wait to prevent mixing metadata modes.
	inflightCommitMeta int

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
		ctx:              ctx,
		inflightWriteset: make(map[uint64]int),
	}
	s.cond = sync.NewCond(&s.mu)
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		defer s.mu.Unlock()
		s.cond.Broadcast()
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
	if txn.hasCommitMeta && s.lastCommittedSequence == 0 && s.inflightGlobal == 0 && s.inflightMissingMeta == 0 && s.inflightCommitMeta == 0 && s.pendingCount == 0 && txn.commitParent > 0 {
		s.lastCommittedSequence = txn.commitParent
	}
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
// it to the calling worker. Returns io.EOF when the scheduler is closed.
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
		// Check closed AFTER context checks so that workers exit
		// cleanly when close() is called without context cancellation
		// (e.g., scheduleLoop returns io.EOF for a relay log EOF).
		if s.closed {
			return nil, io.EOF
		}
		txn := s.popReadyLocked()
		if txn != nil {
			s.markInflightLocked(txn)
			return txn, nil
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
	if txn.hasCommitMeta {
		s.lastCommittedSequence = txn.sequenceNumber
	}
	if txn.order > 0 && txn.order > s.lastCommittedOrder {
		s.lastCommittedOrder = txn.order
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
	allDrained := s.inflightGlobal == 0 && s.inflightMissingMeta == 0 && s.inflightCommitMeta == 0 && len(s.inflightWriteset) == 0
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

// popReadyLocked scans the pending queue for the first ready transaction.
// It skips noConflict transactions (always ready) but stops at the first
// non-ready non-noConflict transaction to prevent head-of-line deadlocks
// with the commitLoop's strict ordering.
func (s *applyScheduler) popReadyLocked() *applyTxn {
	for i := s.pendingOff; i < len(s.pending); i++ {
		txn := s.pending[i]
		if txn.noConflict {
			// noConflict transactions are always ready and don't affect
			// inflight counters, so we can safely skip past them when
			// looking for the next ready transaction.
			if s.isReadyLocked(txn) {
				s.removePendingLocked(i)
				return txn
			}
			continue
		}
		if s.isReadyLocked(txn) {
			s.removePendingLocked(i)
			return txn
		}
		// A non-noConflict transaction is not ready. We must NOT skip past it
		// to dispatch a later-order transaction, because doing so could create
		// a deadlock: the later transaction's inflight state may prevent this
		// earlier transaction from ever becoming ready, while the commitLoop
		// (which requires strict ordering) waits for this earlier transaction
		// to be committed before it can commit the later one.
		return nil
	}
	return nil
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

// isReadyLocked checks whether a transaction can be dispatched to a worker
// based on its classification (noConflict, forceGlobal, hasCommitMeta) and
// the current inflight state. See the ready-check hierarchy in the PR docs.
func (s *applyScheduler) isReadyLocked(txn *applyTxn) bool {
	// noConflict transactions (e.g., position-only saves) are always ready.
	// They have no data conflicts and must not block or be blocked by other
	// transactions. This prevents deadlocks where forceGlobal position saves
	// (with earlier orders) are blocked by inflight data transactions (with
	// later orders), while the commitLoop waits for those earlier orders.
	if txn.noConflict {
		return true
	}
	if s.inflightGlobal > 0 {
		return false
	}
	if txn.forceGlobal {
		ready := s.inflightMissingMeta == 0 && s.inflightCommitMeta == 0 && len(s.inflightWriteset) == 0
		return ready
	}
	if txn.hasCommitMeta {
		if s.inflightMissingMeta > 0 {
			return false
		}
		for _, key := range txn.writeset {
			if s.inflightWriteset[key] > 0 {
				return false
			}
		}
		// When the transaction has a non-empty writeset, we use writeset-only
		// conflict detection and skip the commit-parent dependency check. This
		// is critical because the source MySQL may use COMMIT_ORDER dependency
		// tracking, which produces a strict serial chain where every
		// transaction's commitParent equals the immediately prior sequence
		// number. Under COMMIT_ORDER, the commit-parent check alone would
		// serialize ALL transactions regardless of whether their writesets
		// actually conflict. With a valid writeset, the writeset conflict
		// checks above are sufficient for correctness — the same approach
		// MySQL uses internally with WRITESET dependency tracking.
		//
		// When the writeset is empty (e.g., writeset build failed), we fall
		// back to the commit-parent ordering as the safety net.
		if len(txn.writeset) > 0 {
			return true
		}
		ready := txn.commitParent <= s.lastCommittedSequence
		return ready
	}
	if s.inflightCommitMeta > 0 {
		return false
	}
	if len(txn.writeset) == 0 {
		return s.inflightMissingMeta == 0 && len(s.inflightWriteset) == 0
	}
	for _, key := range txn.writeset {
		if s.inflightWriteset[key] > 0 {
			return false
		}
	}
	return true
}

// markInflightLocked increments the appropriate inflight counters and adds
// writeset keys to inflightWriteset. Must be called under s.mu.
func (s *applyScheduler) markInflightLocked(txn *applyTxn) {
	if txn.noConflict {
		return
	}
	if txn.forceGlobal {
		s.inflightGlobal++
		return
	}
	if txn.hasCommitMeta {
		s.inflightCommitMeta++
		for _, key := range txn.writeset {
			s.inflightWriteset[key]++
		}
		return
	}
	if len(txn.writeset) == 0 {
		s.inflightGlobal++
		s.inflightMissingMeta++
		return
	}
	s.inflightMissingMeta++
	for _, key := range txn.writeset {
		s.inflightWriteset[key]++
	}
}

// releaseInflightLocked decrements the inflight counters and removes
// writeset keys. The inverse of markInflightLocked. Must be called under s.mu.
func (s *applyScheduler) releaseInflightLocked(txn *applyTxn) {
	if txn.noConflict {
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
		for _, key := range txn.writeset {
			count := s.inflightWriteset[key]
			if count <= 1 {
				delete(s.inflightWriteset, key)
			} else {
				s.inflightWriteset[key] = count - 1
			}
		}
		return
	}
	if len(txn.writeset) == 0 {
		if s.inflightGlobal > 0 {
			s.inflightGlobal--
		}
		if s.inflightMissingMeta > 0 {
			s.inflightMissingMeta--
		}
		return
	}
	if s.inflightMissingMeta > 0 {
		s.inflightMissingMeta--
	}
	for _, key := range txn.writeset {
		count := s.inflightWriteset[key]
		if count <= 1 {
			delete(s.inflightWriteset, key)
		} else {
			s.inflightWriteset[key] = count - 1
		}
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
		s.cond.Broadcast()
	}
}

// waitForIdle blocks until there are no pending or inflight transactions.
// Used in tests to synchronize after enqueuing work.
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
		if s.pendingCount == 0 && s.inflightGlobal == 0 && s.inflightMissingMeta == 0 && s.inflightCommitMeta == 0 {
			return nil
		}
		s.cond.Wait()
	}
}

// close marks the scheduler as closed, clears the pending queue, and
// broadcasts to wake all blocked workers so they exit with io.EOF.
func (s *applyScheduler) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	s.closed = true
	s.pending = nil
	s.pendingOff = 0
	s.pendingCount = 0
	s.cond.Broadcast()
	return io.EOF
}
