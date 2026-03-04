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
	order          int64
	sequenceNumber int64
	commitParent   int64
	hasCommitMeta  bool
	forceGlobal    bool
	noConflict     bool
	// writeset holds FNV-1a hashes of PK-based keys (e.g. hash of "table:pk1,pk2").
	// Using uint64 hashes instead of strings eliminates per-txn heap allocations
	// in the scheduler hot path, reducing GC pressure at high TPS.
	writeset []uint64
	payload  *applyTxnPayload
	done     chan struct{}
}

type applyScheduler struct {
	ctx context.Context

	mu   sync.Mutex
	cond *sync.Cond

	pending               []*applyTxn
	pendingOff            int // offset into pending slice; entries before this index are consumed
	pendingCount          int // number of live (non-nil) entries in pending
	lastCommittedSequence int64
	lastCommittedOrder    int64

	inflightWriteset    map[uint64]int
	inflightGlobal      int
	inflightMissingMeta int
	inflightCommitMeta  int
}

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

func (s *applyScheduler) enqueue(txn *applyTxn) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
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
			s.markInflightLocked(txn)
			return txn, nil
		}
		s.cond.Wait()
	}
}

func (s *applyScheduler) markCommitted(txn *applyTxn) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	// if parallelDebugEnabled() {
	// 	parallelDebugLog(fmt.Sprintf("markCommitted: order=%d forceGlobal=%t inflightGlobal=%d inflightMissing=%d inflightCommit=%d writesetKeys=%d pending=%d", txn.order, txn.forceGlobal, s.inflightGlobal, s.inflightMissingMeta, s.inflightCommitMeta, len(s.inflightWriteset), len(s.pending)))
	// }
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
	// Use Broadcast when releasing a forceGlobal txn or when a global/
	// missingMeta counter drops to zero — multiple blocked txns may
	// become ready at once. Otherwise use Signal to avoid thundering-
	// herd wakeup of N workers when only one txn can proceed.
	if wasForceGlobal ||
		(hadInflightGlobal && s.inflightGlobal == 0) ||
		(hadInflightMissingMeta && s.inflightMissingMeta == 0) {
		s.cond.Broadcast()
	} else {
		s.cond.Signal()
	}
	return nil
}

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
		// if parallelDebugEnabled() {
		// 	parallelDebugLog(fmt.Sprintf("popReadyLocked STOPPED at non-ready order=%d forceGlobal=%t hasCommitMeta=%t pending=%d", txn.order, txn.forceGlobal, txn.hasCommitMeta, s.pendingCount))
		// }
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
		// if parallelDebugEnabled() {
		// 	parallelDebugLog(fmt.Sprintf("isReadyLocked BLOCKED by inflightGlobal: order=%d forceGlobal=%t inflightGlobal=%d inflightCommitMeta=%d inflightMissingMeta=%d writesetKeys=%d pending=%d", txn.order, txn.forceGlobal, s.inflightGlobal, s.inflightCommitMeta, s.inflightMissingMeta, len(s.inflightWriteset), len(s.pending)))
		// }
		return false
	}
	if txn.forceGlobal {
		ready := s.inflightMissingMeta == 0 && s.inflightCommitMeta == 0 && len(s.inflightWriteset) == 0
		// if parallelDebugEnabled() && !ready {
		// 	parallelDebugLog(fmt.Sprintf("isReadyLocked forceGlobal NOT READY: order=%d inflightMissingMeta=%d inflightCommitMeta=%d writesetKeys=%d", txn.order, s.inflightMissingMeta, s.inflightCommitMeta, len(s.inflightWriteset)))
		// }
		return ready
	}
	if txn.hasCommitMeta {
		if s.inflightMissingMeta > 0 {
			// if parallelDebugEnabled() {
			// 	parallelDebugLog(fmt.Sprintf("isReadyLocked hasCommitMeta BLOCKED by inflightMissingMeta: order=%d inflightMissingMeta=%d", txn.order, s.inflightMissingMeta))
			// }
			return false
		}
		for _, key := range txn.writeset {
			if s.inflightWriteset[key] > 0 {
				// if parallelDebugEnabled() {
				// 	parallelDebugLog(fmt.Sprintf("isReadyLocked hasCommitMeta BLOCKED by writeset key=%s: order=%d", key, txn.order))
				// }
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
		// if parallelDebugEnabled() && !ready {
		// 	parallelDebugLog(fmt.Sprintf("isReadyLocked hasCommitMeta BLOCKED by commitParent: order=%d commitParent=%d lastCommittedSequence=%d seq=%d", txn.order, txn.commitParent, s.lastCommittedSequence, txn.sequenceNumber))
		// }
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

func (s *applyScheduler) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	s.pending = nil
	s.pendingOff = 0
	s.pendingCount = 0
	s.cond.Broadcast()
	return io.EOF
}
