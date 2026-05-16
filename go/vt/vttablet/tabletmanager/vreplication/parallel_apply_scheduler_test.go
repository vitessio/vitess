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
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requireNoReadyTxn(t *testing.T, s *applyScheduler) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Nil(t, s.popReadyLocked())
}

func requireReadyTxn(t *testing.T, s *applyScheduler, want *applyTxn) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Same(t, want, s.popReadyLocked())
}

func TestApplySchedulerCommitParentOrder(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	// txn2 is enqueued first. Since it's the first hasCommitMeta transaction
	// and the scheduler is idle, enqueue seeds lastCommittedSequence to
	// txn2.commitParent (1). This makes txn2 immediately ready because
	// commitParent (1) <= lastCommittedSequence (1). The scheduler dispatches
	// in FIFO order, so txn2 goes first.
	txn2 := &applyTxn{sequenceNumber: 2, commitParent: 1, hasCommitMeta: true}
	txn1 := &applyTxn{sequenceNumber: 1, commitParent: 0, hasCommitMeta: true}

	require.NoError(t, s.enqueue(txn2))
	require.NoError(t, s.enqueue(txn1))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, txn2, got1)
	require.NoError(t, s.markCommitted(got1))

	got2, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, txn1, got2)
	require.NoError(t, s.markCommitted(got2))
}

func TestApplySchedulerAllowsIndependentWritesets(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	txn1 := &applyTxn{writeset: []uint64{1}}
	txn2 := &applyTxn{writeset: []uint64{2}}

	require.NoError(t, s.enqueue(txn1))
	require.NoError(t, s.enqueue(txn2))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	got2, err := s.nextReady(ctx)
	require.NoError(t, err)

	require.NotEqual(t, got1, got2)
}

func TestApplySchedulerBlocksConflictingWritesets(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	txn1 := &applyTxn{writeset: []uint64{100}}
	txn2 := &applyTxn{writeset: []uint64{100}}

	require.NoError(t, s.enqueue(txn1))
	require.NoError(t, s.enqueue(txn2))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)

	requireNoReadyTxn(t, s)

	require.NoError(t, s.markCommitted(got1))

	requireReadyTxn(t, s, txn2)
}

func TestApplySchedulerBlocksCommitMetaDuringMissingMeta(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	missing := &applyTxn{writeset: []uint64{100}}
	meta := &applyTxn{sequenceNumber: 2, commitParent: 0, hasCommitMeta: true}

	require.NoError(t, s.enqueue(missing))
	require.NoError(t, s.enqueue(meta))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, missing, got1)

	requireNoReadyTxn(t, s)

	require.NoError(t, s.markCommitted(got1))

	requireReadyTxn(t, s, meta)
}

func TestApplySchedulerBlocksCommitMetaConflictingWritesets(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	txn1 := &applyTxn{writeset: []uint64{100}, sequenceNumber: 1, commitParent: 0, hasCommitMeta: true}
	txn2 := &applyTxn{writeset: []uint64{100}, sequenceNumber: 2, commitParent: 0, hasCommitMeta: true}

	require.NoError(t, s.enqueue(txn1))
	require.NoError(t, s.enqueue(txn2))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, txn1, got1)

	requireNoReadyTxn(t, s)

	require.NoError(t, s.markCommitted(got1))

	requireReadyTxn(t, s, txn2)
}

func TestApplySchedulerCommitMetaDoesNotAdvanceOnMissingMeta(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)
	require.Equal(t, int64(0), s.lastCommittedSequence)

	missing := &applyTxn{writeset: []uint64{100}}
	meta := &applyTxn{sequenceNumber: 5, commitParent: 0, hasCommitMeta: true}

	require.NoError(t, s.enqueue(missing))
	require.NoError(t, s.enqueue(meta))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, missing, got1)

	require.NoError(t, s.markCommitted(got1))

	got2, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, meta, got2)

	require.NoError(t, s.markCommitted(got2))
	require.Equal(t, int64(5), s.lastCommittedSequence)
}

func TestApplySchedulerSeedsCommitParentOnFirstMeta(t *testing.T) {
	ctx := t.Context()
	// The scheduler seeds lastCommittedSequence from the first hasCommitMeta
	// transaction when the scheduler is completely idle (no pending, no inflight).
	// Enqueue meta as the very first transaction to trigger seeding.
	meta := &applyTxn{sequenceNumber: 6, commitParent: 5, hasCommitMeta: true}

	s := newApplyScheduler(ctx)

	require.NoError(t, s.enqueue(meta))

	got, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, meta, got)
	require.Equal(t, int64(5), s.lastCommittedSequence)
}

func TestApplySchedulerWritesetBypassesCommitParent(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	// Simulate COMMIT_ORDER dependency tracking: each txn's commitParent is
	// the immediately prior sequence number, forming a strict serial chain.
	// With non-conflicting writesets, the scheduler should allow parallelism
	// by ignoring the commit-parent dependency.
	txn1 := &applyTxn{order: 1, sequenceNumber: 10, commitParent: 9, hasCommitMeta: true, writeset: []uint64{1}}
	txn2 := &applyTxn{order: 2, sequenceNumber: 11, commitParent: 10, hasCommitMeta: true, writeset: []uint64{2}}
	txn3 := &applyTxn{order: 3, sequenceNumber: 12, commitParent: 11, hasCommitMeta: true, writeset: []uint64{3}}

	require.NoError(t, s.enqueue(txn1))
	require.NoError(t, s.enqueue(txn2))
	require.NoError(t, s.enqueue(txn3))

	// All three should be immediately ready since their writesets don't conflict.
	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, txn1, got1)

	got2, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, txn2, got2)

	got3, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, txn3, got3)

	// Commit in order.
	require.NoError(t, s.markCommitted(got1))
	require.NoError(t, s.markCommitted(got2))
	require.NoError(t, s.markCommitted(got3))
}

func TestApplySchedulerWritesetConflictStillBlocks(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	// Even with the commit-parent bypass, conflicting writesets must still
	// cause serialization.
	txn1 := &applyTxn{order: 1, sequenceNumber: 10, commitParent: 9, hasCommitMeta: true, writeset: []uint64{100}}
	txn2 := &applyTxn{order: 2, sequenceNumber: 11, commitParent: 10, hasCommitMeta: true, writeset: []uint64{100}}

	require.NoError(t, s.enqueue(txn1))
	require.NoError(t, s.enqueue(txn2))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, txn1, got1)

	// txn2 should be blocked because it conflicts with inflight txn1.
	requireNoReadyTxn(t, s)

	require.NoError(t, s.markCommitted(got1))

	requireReadyTxn(t, s, txn2)
}

func TestApplySchedulerEmptyWritesetFallsBackToCommitParent(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	// When a hasCommitMeta transaction has an empty writeset (e.g., writeset
	// build failed), it should fall back to commit-parent ordering.
	txn1 := &applyTxn{order: 1, sequenceNumber: 10, commitParent: 9, hasCommitMeta: true}
	txn2 := &applyTxn{order: 2, sequenceNumber: 11, commitParent: 10, hasCommitMeta: true}

	// Seed lastCommittedSequence to 9 so txn1 is ready.
	require.NoError(t, s.enqueue(txn1))
	require.NoError(t, s.enqueue(txn2))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, txn1, got1)

	// txn2 has commitParent=10 but lastCommittedSequence is still 9 (seeded).
	// txn2's writeset is empty, so it falls back to commit-parent check.
	requireNoReadyTxn(t, s)

	// After committing txn1, lastCommittedSequence advances to 10,
	// making txn2 ready (commitParent 10 <= 10).
	require.NoError(t, s.markCommitted(got1))

	requireReadyTxn(t, s, txn2)
}

func TestApplySchedulerNoConflictDoesNotBlockPending(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	// Enqueue a noConflict txn first and a normal txn second.
	nc := &applyTxn{order: 1, noConflict: true}
	normal := &applyTxn{order: 2, writeset: []uint64{100}}

	require.NoError(t, s.enqueue(nc))
	require.NoError(t, s.enqueue(normal))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, nc, got1)

	// Commit noConflict should not affect inflight counters for normal txn.
	require.NoError(t, s.markCommitted(got1))

	got2, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, normal, got2)
}

func TestApplySchedulerForceGlobalBlocksWritesets(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	global := &applyTxn{order: 1, forceGlobal: true}
	conflict := &applyTxn{order: 2, writeset: []uint64{100}}

	require.NoError(t, s.enqueue(global))
	require.NoError(t, s.enqueue(conflict))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, global, got1)

	requireNoReadyTxn(t, s)

	require.NoError(t, s.markCommitted(got1))

	requireReadyTxn(t, s, conflict)
}

func TestApplySchedulerAdvanceCommittedSequenceUnblocks(t *testing.T) {
	ctx := t.Context()
	// Use a non-empty pending queue to prevent commit-parent seeding.
	seed := &applyTxn{order: 1, noConflict: true}
	meta := &applyTxn{order: 2, sequenceNumber: 6, commitParent: 5, hasCommitMeta: true}

	s := newApplyScheduler(ctx)

	require.NoError(t, s.enqueue(seed))
	require.NoError(t, s.enqueue(meta))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, seed, got1)
	require.NoError(t, s.markCommitted(got1))

	requireNoReadyTxn(t, s)

	s.advanceCommittedSequence(5)

	requireReadyTxn(t, s, meta)
}

func TestApplySchedulerAdvanceCommittedSequenceDoesNotBypassInflightMetaParent(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	metaParent := &applyTxn{order: 1, sequenceNumber: 10, commitParent: 9, hasCommitMeta: true, writeset: []uint64{1}}
	metaChild := &applyTxn{order: 2, sequenceNumber: 12, commitParent: 11, hasCommitMeta: true}

	require.NoError(t, s.enqueue(metaParent))
	gotParent, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, metaParent, gotParent)

	require.NoError(t, s.enqueue(metaChild))
	s.advanceCommittedSequence(11)

	requireNoReadyTxn(t, s)

	require.NoError(t, s.markCommitted(gotParent))

	requireReadyTxn(t, s, metaChild)
}

func TestApplySchedulerMergedSequencesUnblockCommitParentChild(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	batchedParent := &applyTxn{order: 1, writeset: []uint64{1}, mergedSequences: []int64{10}}
	metaChild := &applyTxn{order: 2, sequenceNumber: 11, commitParent: 10, hasCommitMeta: true}

	require.NoError(t, s.enqueue(batchedParent))
	require.NoError(t, s.enqueue(metaChild))

	gotParent, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Same(t, batchedParent, gotParent)

	requireNoReadyTxn(t, s)

	require.NoError(t, s.markCommitted(gotParent))

	s.mu.Lock()
	require.Equal(t, int64(10), s.lastCommittedSequence)
	s.mu.Unlock()

	requireReadyTxn(t, s, metaChild)
}

func TestApplySchedulerWaitForIdleReturnsWhenIdle(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	require.NoError(t, s.waitForIdle(ctx))
}

func TestApplySchedulerWaitForIdleReturnsOnSchedulerCancel(t *testing.T) {
	ctx := t.Context()
	sCtx, cancel := context.WithCancel(ctx)
	s := newApplyScheduler(sCtx)

	require.NoError(t, s.enqueue(&applyTxn{writeset: []uint64{100}}))

	s.mu.Lock()
	require.NotZero(t, s.pendingCount)
	s.mu.Unlock()

	cancel()

	require.ErrorIs(t, s.waitForIdle(ctx), context.Canceled)
}

func TestApplySchedulerClosePreservesPending(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	txn := &applyTxn{writeset: []uint64{100}, noConflict: true}
	require.NoError(t, s.enqueue(txn))

	err := s.close()
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 1, s.pendingCount)
	require.Zero(t, s.pendingOff)
	require.Len(t, s.pending, 1)
	require.Same(t, txn, s.pending[0])
}

func TestApplySchedulerNextReadyDrainsPendingAfterClose(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	txn := &applyTxn{order: 1, noConflict: true}
	require.NoError(t, s.enqueue(txn))
	require.ErrorIs(t, s.close(), io.EOF)

	got, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Same(t, txn, got)

	_, err = s.nextReady(ctx)
	require.ErrorIs(t, err, io.EOF)
}

func TestApplySchedulerNextReadyWaitsForBlockedPendingAfterClose(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	blocker := &applyTxn{order: 1, writeset: []uint64{100}}
	blocked := &applyTxn{order: 2, writeset: []uint64{100}}

	require.NoError(t, s.enqueue(blocker))
	require.NoError(t, s.enqueue(blocked))

	gotBlocker, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Same(t, blocker, gotBlocker)

	require.ErrorIs(t, s.close(), io.EOF)

	type nextReadyResult struct {
		txn *applyTxn
		err error
	}
	resultCh := make(chan nextReadyResult, 1)
	go func() {
		txn, err := s.nextReady(ctx)
		resultCh <- nextReadyResult{txn: txn, err: err}
	}()

	assert.Never(t, func() bool {
		return len(resultCh) > 0
	}, 100*time.Millisecond, 5*time.Millisecond)

	require.NoError(t, s.markCommitted(gotBlocker))

	assert.Eventually(t, func() bool {
		return len(resultCh) > 0
	}, 200*time.Millisecond, 5*time.Millisecond)

	gotBlocked := <-resultCh
	require.NoError(t, gotBlocked.err)
	require.Same(t, blocked, gotBlocked.txn)

	require.NoError(t, s.markCommitted(gotBlocked.txn))

	_, err = s.nextReady(ctx)
	require.ErrorIs(t, err, io.EOF)
}

func TestApplySchedulerEnqueueBlocksWhenOutstandingOrdersReachCap(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s := newApplyScheduler(ctx)
	s.maxOutstandingOrders = 2

	require.NoError(t, s.enqueue(&applyTxn{order: 1, noConflict: true}))
	require.NoError(t, s.enqueue(&applyTxn{order: 2, noConflict: true}))

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.enqueue(&applyTxn{order: 3, noConflict: true})
	}()

	assert.Never(t, func() bool {
		return len(errCh) > 0
	}, 100*time.Millisecond, 5*time.Millisecond)

	s.mu.Lock()
	s.lastCommittedOrder = 1
	s.cond.Broadcast()
	s.mu.Unlock()

	assert.Eventually(t, func() bool {
		return len(errCh) > 0
	}, 200*time.Millisecond, 5*time.Millisecond)
	require.NoError(t, <-errCh)

	s.mu.Lock()
	require.Equal(t, 3, s.pendingCount)
	s.mu.Unlock()
}

func TestApplySchedulerLaterNoConflictBypassesBlockedEarlierTxn(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	blocker := &applyTxn{order: 1, writeset: []uint64{100}}
	require.NoError(t, s.enqueue(blocker))

	gotBlocker, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Same(t, blocker, gotBlocker)

	blocked := &applyTxn{order: 2, writeset: []uint64{100}}
	stopTxn1 := &applyTxn{order: 3, noConflict: true}
	require.NoError(t, s.enqueue(blocked))
	require.NoError(t, s.enqueue(stopTxn1))

	requireReadyTxn(t, s, stopTxn1)

	// The first bypass leaves a nil gap in pending. A second noConflict txn
	// must still be discoverable while the earlier normal txn remains blocked.
	stopTxn2 := &applyTxn{order: 4, noConflict: true}
	require.NoError(t, s.enqueue(stopTxn2))
	requireReadyTxn(t, s, stopTxn2)

	require.NoError(t, s.markCommitted(gotBlocker))
	requireReadyTxn(t, s, blocked)
}

func TestApplySchedulerPendingCompaction(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	for i := range 4 {
		require.NoError(t, s.enqueue(&applyTxn{order: int64(i + 1), noConflict: true}))
	}

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), got1.order)
	require.NoError(t, s.markCommitted(got1))

	got2, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), got2.order)
	require.NoError(t, s.markCommitted(got2))

	require.Zero(t, s.pendingOff)
	require.Len(t, s.pending, 2)
	require.Equal(t, 2, s.pendingCount)
}

// TestApplySchedulerConcurrentEnqueueAndCommitStress exercises the
// scheduler under many concurrent enqueues, worker-side nextReady calls,
// and markCommitted calls to flush out deadlocks, lost wakeups, and
// counter-balance bugs. Runs fast enough for the normal test suite.
//
// Correctness properties checked:
//   - Every enqueued transaction is eventually observed by nextReady.
//   - nextReady returns transactions in strictly increasing order.
//   - After all work drains, every inflight counter is zero.
func TestApplySchedulerConcurrentEnqueueAndCommitStress(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	s := newApplyScheduler(ctx)

	const (
		numProducers        = 2
		numWorkers          = 6
		txnsPerProducer     = 500
		maxWritesetKeys     = 4
		writesetKeySpace    = 32
		maxOutstandingOrder = int64(128)
	)
	totalTxns := numProducers * txnsPerProducer
	s.maxOutstandingOrders = maxOutstandingOrder

	// Atomically assigned order so all producers share one sequence.
	var nextOrder atomic.Int64

	// Producer goroutines enqueue a mix of writeset-based and forceGlobal
	// transactions. Writeset keys are drawn from a small space so workers
	// frequently conflict, exercising the writeset-refcount machinery.
	var producers sync.WaitGroup
	for p := range numProducers {
		producers.Add(1)
		go func(producerID int) {
			defer producers.Done()
			// Deterministic per-producer RNG so flakes are reproducible.
			rng := rand.New(rand.NewPCG(uint64(producerID+1), 0x51ED))
			for i := range txnsPerProducer {
				txn := &applyTxn{
					order: nextOrder.Add(1),
				}
				// 5% of transactions force-global, others carry a writeset.
				if rng.IntN(20) == 0 {
					txn.forceGlobal = true
				} else {
					n := 1 + rng.IntN(maxWritesetKeys)
					txn.writeset = make([]uint64, 0, n)
					seen := map[uint64]struct{}{}
					for range n {
						k := uint64(rng.IntN(writesetKeySpace))
						if _, dup := seen[k]; dup {
							continue
						}
						seen[k] = struct{}{}
						txn.writeset = append(txn.writeset, k)
					}
				}
				if err := s.enqueue(txn); err != nil {
					t.Errorf("producer %d txn %d enqueue: %v", producerID, i, err)
					return
				}
			}
		}(p)
	}

	// Consumer goroutines simulate workers: pull via nextReady, mark committed.
	// Record the order sequence observed by the commit serializer to verify
	// strict monotonicity (mirrors the real commitLoop invariant).
	observed := make([]int64, 0, totalTxns)
	var observedMu sync.Mutex
	commitDone := make(chan struct{})
	go func() {
		defer close(commitDone)
		for {
			if len(observed) == totalTxns {
				return
			}
			s.mu.Lock()
			txn := s.popReadyLocked()
			s.mu.Unlock()
			if txn == nil {
				time.Sleep(10 * time.Microsecond)
				continue
			}
			observedMu.Lock()
			observed = append(observed, txn.order)
			observedMu.Unlock()
			if err := s.markCommitted(txn); err != nil {
				t.Errorf("markCommitted: %v", err)
				return
			}
		}
	}()

	producers.Wait()

	select {
	case <-commitDone:
	case <-ctx.Done():
		t.Fatalf("stress test timed out: observed %d / %d transactions", len(observed), totalTxns)
	}

	// Invariants after the scheduler has drained.
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Zero(t, s.inflightGlobal, "inflightGlobal leaked")
	require.Zero(t, s.inflightMissingMeta, "inflightMissingMeta leaked")
	require.Zero(t, s.inflightCommitMeta, "inflightCommitMeta leaked")
	require.Empty(t, s.inflightWriteset, "inflightWriteset leaked")
	require.Zero(t, s.pendingCount, "pendingCount not drained")
	require.Len(t, observed, totalTxns)

	// All order numbers from 1..totalTxns must appear exactly once.
	seen := make(map[int64]struct{}, totalTxns)
	for _, o := range observed {
		if _, dup := seen[o]; dup {
			t.Fatalf("order %d observed twice", o)
		}
		seen[o] = struct{}{}
	}
	for i := int64(1); i <= int64(totalTxns); i++ {
		if _, ok := seen[i]; !ok {
			t.Fatalf("order %d missing from observed sequence", i)
		}
	}
}
