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
	"math/rand/v2"
	"runtime"
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

	// Both have commit metadata and empty writesets. The scheduler dispatches
	// in FIFO order, so txn2, enqueued first, goes first, and txn1 waits until
	// txn2 has committed.
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

func TestApplySchedulerCommitMetaAfterMissingMeta(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

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

func TestApplySchedulerEmptyWritesetWaitsForInflightCommitMeta(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	// A hasCommitMeta transaction with an empty writeset (e.g., the writeset
	// build failed) waits until no transaction with commit metadata is
	// inflight.
	txn1 := &applyTxn{order: 1, sequenceNumber: 10, commitParent: 9, hasCommitMeta: true}
	txn2 := &applyTxn{order: 2, sequenceNumber: 11, commitParent: 10, hasCommitMeta: true}

	require.NoError(t, s.enqueue(txn1))
	require.NoError(t, s.enqueue(txn2))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, txn1, got1)

	// txn1 is inflight, so txn2 waits.
	requireNoReadyTxn(t, s)

	// Once txn1 has committed, txn2 is ready.
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

// TestApplySchedulerCommitMetaWithEmptyWritesetReadyWhenIdle pins that a
// transaction with commit metadata and an empty writeset becomes ready as soon
// as nothing earlier is inflight, whether or not its commit parent's sequence
// number was ever seen. Everything ordered before it has committed by then, so
// its parent has too, and waiting for the parent's sequence number could wait
// forever: a source transaction that ends in a Query COMMIT (e.g. a MyISAM
// one) reaches the applier without commit metadata, so its sequence number is
// never known.
func TestApplySchedulerCommitMetaWithEmptyWritesetReadyWhenIdle(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	withMeta := &applyTxn{order: 1, sequenceNumber: 10, commitParent: 9, hasCommitMeta: true, writeset: []uint64{1}}
	// Sequence 11 has no commit metadata.
	withoutMeta := &applyTxn{order: 2, writeset: []uint64{2}}
	// Its child has commit metadata and an empty writeset.
	child := &applyTxn{order: 3, sequenceNumber: 12, commitParent: 11, hasCommitMeta: true}

	require.NoError(t, s.enqueue(withMeta))
	require.NoError(t, s.enqueue(withoutMeta))
	require.NoError(t, s.enqueue(child))

	got, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Same(t, withMeta, got)
	require.NoError(t, s.markCommitted(got))
	got, err = s.nextReady(ctx)
	require.NoError(t, err)
	require.Same(t, withoutMeta, got)
	// The child waits while an earlier transaction is inflight...
	requireNoReadyTxn(t, s)
	require.NoError(t, s.markCommitted(got))

	// ...and is ready once nothing is.
	requireReadyTxn(t, s, child)
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

	// Advance durable progress through the real path: markCommitted bumps
	// lastCommittedOrder and wakes the order-window waiter (orderCond).
	require.NoError(t, s.markCommitted(&applyTxn{order: 1, noConflict: true}))

	assert.Eventually(t, func() bool {
		return len(errCh) > 0
	}, 30*time.Second, 5*time.Millisecond)
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

// TestApplySchedulerConcurrentEnqueueAndCommitStress exercises the scheduler
// under concurrent producers and real worker goroutines (nextReady +
// markCommitted, so inflight state and the writeset-refcount machinery are
// genuinely engaged) to flush out deadlocks, lost wakeups, counter-balance
// bugs, and — most importantly — conflicting dispatches.
//
// Correctness properties checked:
//   - No two concurrently-dispatched transactions share a writeset key, and
//     forceGlobal transactions run exclusively (verified by an external
//     conflict tracker, independent of the scheduler's own bookkeeping).
//   - Every enqueued transaction is dispatched exactly once.
//   - After all work drains, every inflight counter is zero.
func TestApplySchedulerConcurrentEnqueueAndCommitStress(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
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

	// External conflict tracker: validates, independently of the scheduler's
	// own counters, that no two dispatched-and-uncommitted transactions
	// conflict. Registration is atomic with the check under one mutex.
	var (
		trackerMu    sync.Mutex
		activeKeys   = map[uint64]int64{} // key -> holding txn order
		activeGlobal int64                // order of the active forceGlobal txn, 0 = none
		activeCount  int
	)
	dispatch := func(txn *applyTxn) {
		trackerMu.Lock()
		defer trackerMu.Unlock()
		if activeGlobal != 0 {
			t.Errorf("txn %d dispatched while forceGlobal txn %d active", txn.order, activeGlobal)
		}
		if txn.forceGlobal {
			if activeCount != 0 {
				t.Errorf("forceGlobal txn %d dispatched with %d txns active", txn.order, activeCount)
			}
			activeGlobal = txn.order
		}
		for _, k := range txn.writeset {
			if holder, conflict := activeKeys[k]; conflict {
				t.Errorf("txn %d dispatched with writeset key %d held by active txn %d", txn.order, k, holder)
			}
			activeKeys[k] = txn.order
		}
		activeCount++
	}
	finish := func(txn *applyTxn) {
		trackerMu.Lock()
		defer trackerMu.Unlock()
		if txn.forceGlobal {
			activeGlobal = 0
		}
		for _, k := range txn.writeset {
			delete(activeKeys, k)
		}
		activeCount--
	}

	// Worker goroutines: the REAL dispatch path. nextReady marks inflight;
	// markCommitted releases it. The tracker unregisters BEFORE
	// markCommitted, mirroring the real pipeline where a conflicting txn may
	// dispatch the instant the commit releases the scheduler state.
	observed := make([]int64, 0, totalTxns)
	var observedMu sync.Mutex
	var workers sync.WaitGroup
	for range numWorkers {
		workers.Go(func() {
			for {
				txn, err := s.nextReady(ctx)
				if err != nil {
					if !errors.Is(err, io.EOF) && ctx.Err() == nil {
						t.Errorf("nextReady: %v", err)
					}
					return
				}
				dispatch(txn)
				if txn.order%7 == 0 {
					runtime.Gosched() // widen the race window a little
				}
				observedMu.Lock()
				observed = append(observed, txn.order)
				observedMu.Unlock()
				finish(txn)
				if err := s.markCommitted(txn); err != nil {
					t.Errorf("markCommitted: %v", err)
					return
				}
			}
		})
	}

	producers.Wait()
	s.close()
	workersDone := make(chan struct{})
	go func() { workers.Wait(); close(workersDone) }()
	select {
	case <-workersDone:
	case <-ctx.Done():
		observedMu.Lock()
		n := len(observed)
		observedMu.Unlock()
		t.Fatalf("stress test timed out: observed %d / %d transactions", n, totalTxns)
	}

	// Invariants after the scheduler has drained.
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Zero(t, s.inflightGlobal, "inflightGlobal leaked")
	require.Zero(t, s.inflightMissingMeta, "inflightMissingMeta leaked")
	require.Zero(t, s.inflightCommitMeta, "inflightCommitMeta leaked")
	require.Zero(t, s.inflightNoConflict, "inflightNoConflict leaked")
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
	require.Len(t, seen, totalTxns)
}

func TestApplySchedulerMultiKeyReleaseWakesAllReadyWaiters(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	// megaTxn holds two writeset keys; blocker keeps a third key inflight for
	// the whole test so markCommitted(megaTxn) does not take the
	// all-drained Broadcast path.
	megaTxn := &applyTxn{order: 1, sequenceNumber: 10, commitParent: 9, hasCommitMeta: true, writeset: []uint64{100, 200}}
	blocker := &applyTxn{order: 2, sequenceNumber: 11, commitParent: 9, hasCommitMeta: true, writeset: []uint64{900}}
	require.NoError(t, s.enqueue(megaTxn))
	require.NoError(t, s.enqueue(blocker))

	got, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, megaTxn, got)
	got, err = s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, blocker, got)

	// Two pending transactions, each conflicting with a different one of
	// megaTxn's keys.
	waiterA := &applyTxn{order: 3, sequenceNumber: 12, commitParent: 9, hasCommitMeta: true, writeset: []uint64{100}}
	waiterB := &applyTxn{order: 4, sequenceNumber: 13, commitParent: 9, hasCommitMeta: true, writeset: []uint64{200}}
	require.NoError(t, s.enqueue(waiterA))
	require.NoError(t, s.enqueue(waiterB))

	// Two workers block in nextReady before the commit.
	results := make(chan *applyTxn, 2)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			txn, err := s.nextReady(ctx)
			if err != nil {
				errs <- err
				return
			}
			results <- txn
		}()
	}
	// Wait until both goroutines are parked in cond.Wait. There is no direct
	// hook for "waiter count", so poll the scheduler state: both pending txns
	// are still queued and neither result has arrived.
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.pendingCount == 2
	}, 30*time.Second, time.Millisecond)

	// One commit releases both keys; both waiters must be dispatched without
	// any further commit happening (blocker stays inflight throughout).
	require.NoError(t, s.markCommitted(megaTxn))

	dispatched := make(map[int64]bool)
	for range 2 {
		select {
		case txn := <-results:
			dispatched[txn.order] = true
		case err := <-errs:
			t.Fatalf("nextReady returned error: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out waiting for both ready transactions to be dispatched; got %v", dispatched)
		}
	}
	require.True(t, dispatched[waiterA.order])
	require.True(t, dispatched[waiterB.order])
}

// TestApplySchedulerNoMetaNoWritesetIsGlobal pins ready-check case 7: a
// transaction without commit metadata and without a writeset must serialize
// as global, with BOTH inflightGlobal and inflightMissingMeta held and then
// released in balance. An unbalanced release here would silently wedge the
// scheduler (counter stuck > 0) or unsafely unblock it (counter goes
// negative-equivalent via early zero).
func TestApplySchedulerNoMetaNoWritesetIsGlobal(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	opaque := &applyTxn{order: 1} // no meta, no writeset
	other := &applyTxn{order: 2, sequenceNumber: 10, commitParent: 0, hasCommitMeta: true, writeset: []uint64{100}}
	require.NoError(t, s.enqueue(opaque))
	require.NoError(t, s.enqueue(other))

	got, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, opaque, got)
	s.mu.Lock()
	require.Equal(t, 1, s.inflightGlobal, "no-meta/no-writeset must count as global")
	require.Equal(t, 1, s.inflightMissingMeta, "no-meta/no-writeset must count as missing-meta")
	s.mu.Unlock()

	// While the opaque txn is inflight, everything else is blocked.
	requireNoReadyTxn(t, s)

	require.NoError(t, s.markCommitted(opaque))
	s.mu.Lock()
	require.Zero(t, s.inflightGlobal, "release must balance the global count")
	require.Zero(t, s.inflightMissingMeta, "release must balance the missing-meta count")
	s.mu.Unlock()
	requireReadyTxn(t, s, other)
}

// TestApplySchedulerNoMetaWritesetBlockedByInflightCommitMeta pins the
// blocked direction of ready-check case 8: a transaction without commit
// metadata (even with a non-conflicting writeset) must not run alongside an
// inflight transaction that has metadata — the two metadata modes never mix.
func TestApplySchedulerNoMetaWritesetBlockedByInflightCommitMeta(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	withMeta := &applyTxn{order: 1, sequenceNumber: 10, commitParent: 9, hasCommitMeta: true, writeset: []uint64{100}}
	noMeta := &applyTxn{order: 2, writeset: []uint64{200}} // disjoint writeset, no metadata
	require.NoError(t, s.enqueue(withMeta))
	require.NoError(t, s.enqueue(noMeta))

	got, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, withMeta, got)

	requireNoReadyTxn(t, s)

	require.NoError(t, s.markCommitted(withMeta))
	requireReadyTxn(t, s, noMeta)
}

// TestApplySchedulerForceGlobalWaitsForInflightAndThenBlocksAll pins the
// blocked direction of ready-check case 3: a forceGlobal transaction (e.g. a
// DDL) must wait until ALL inflight work drains, and while it is inflight it
// must block everything behind it. A regression here would let a DDL execute
// concurrently with inflight row transactions on other connections.
func TestApplySchedulerForceGlobalWaitsForInflightAndThenBlocksAll(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	row := &applyTxn{order: 1, sequenceNumber: 10, commitParent: 9, hasCommitMeta: true, writeset: []uint64{100}}
	global := &applyTxn{order: 2, forceGlobal: true}
	row2 := &applyTxn{order: 3, sequenceNumber: 11, commitParent: 9, hasCommitMeta: true, writeset: []uint64{200}}
	require.NoError(t, s.enqueue(row))
	require.NoError(t, s.enqueue(global))
	require.NoError(t, s.enqueue(row2))

	got, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, row, got)

	// The DDL must not be dispatchable while the row txn is inflight, and
	// head-of-line blocking must also keep row2 queued behind it.
	requireNoReadyTxn(t, s)

	require.NoError(t, s.markCommitted(row))
	// Dispatch via nextReady so the DDL is actually marked inflight
	// (requireReadyTxn only pops, without marking).
	got, err = s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, global, got)

	// While the DDL is inflight, nothing else may start.
	requireNoReadyTxn(t, s)

	require.NoError(t, s.markCommitted(global))
	requireReadyTxn(t, s, row2)
}

// TestApplySchedulerEmptyWritesetDoesNotWaitForInflightNoConflict pins that a
// transaction with commit metadata and an empty writeset does not wait for an
// inflight noConflict position save, even when the save is its commit parent:
// the save changes no data, and the commitLoop still commits the two in order.
func TestApplySchedulerEmptyWritesetDoesNotWaitForInflightNoConflict(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	save := &applyTxn{order: 1, sequenceNumber: 99, commitParent: 0, hasCommitMeta: true, noConflict: true}
	child := &applyTxn{order: 2, sequenceNumber: 100, commitParent: 99, hasCommitMeta: true}
	require.NoError(t, s.enqueue(save))
	require.NoError(t, s.enqueue(child))

	got, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Same(t, save, got)

	requireReadyTxn(t, s, child)
}

// TestApplySchedulerWaitForTurn pins the wait a transaction does when it has
// to apply alone, after every earlier transaction has committed: it returns
// once the order right before it commits, and on cancellation.
func TestApplySchedulerWaitForTurn(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := newApplyScheduler(ctx)

	txns := []*applyTxn{
		{order: 1, writeset: []uint64{1}},
		{order: 2, writeset: []uint64{2}},
		{order: 3, writeset: []uint64{3}},
	}
	for _, txn := range txns {
		require.NoError(t, s.enqueue(txn))
	}
	for range txns {
		_, err := s.nextReady(ctx)
		require.NoError(t, err)
	}
	require.True(t, s.isNext(1))
	require.False(t, s.isNext(3))

	turn := make(chan error, 1)
	go func() { turn <- s.waitForTurn(ctx, 3) }()
	require.NoError(t, s.markCommitted(txns[0]))
	assert.Never(t, func() bool { return len(turn) > 0 }, 200*time.Millisecond, 10*time.Millisecond)
	require.NoError(t, s.markCommitted(txns[1]))
	select {
	case err := <-turn:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "waitForTurn did not return once the previous order committed")
	}
	require.True(t, s.isNext(3))

	waiting := make(chan error, 1)
	go func() { waiting <- s.waitForTurn(ctx, 10) }()
	cancel()
	select {
	case err := <-waiting:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "waitForTurn did not return on cancellation")
	}
}

// TestApplySchedulerPauseDispatchAfterAbort pins the pause that follows a
// commit-order deadlock abort: until the transaction that requested it
// commits, no later ordered transaction is dispatched, so none can take the
// locks it is about to retry; noConflict transactions, which take none, still
// go through.
func TestApplySchedulerPauseDispatchAfterAbort(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	head := &applyTxn{order: 1, writeset: []uint64{1}}
	later := &applyTxn{order: 2, writeset: []uint64{2}}
	save := &applyTxn{order: 3, noConflict: true}
	require.NoError(t, s.enqueue(head))
	got, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Same(t, head, got)

	s.requestAbortAfter(1)
	require.NoError(t, s.enqueue(later))
	require.NoError(t, s.enqueue(save))
	requireReadyTxn(t, s, save)
	requireNoReadyTxn(t, s)

	require.NoError(t, s.markCommitted(head))
	requireReadyTxn(t, s, later)
}

// TestApplySchedulerCommitOrderAborts pins the bookkeeping behind
// commit-order deadlock aborts: an abort requested by the transaction with
// order h covers every transaction ordered after h that began applying before
// it, closes the notification channel handed out before it, and pauses
// dispatch after h.
func TestApplySchedulerCommitOrderAborts(t *testing.T) {
	ctx := t.Context()
	s := newApplyScheduler(ctx)

	gen, notify := s.abortState()
	aborted, _ := s.abortedSince(gen, 5)
	require.False(t, aborted)

	s.requestAbortAfter(3)
	select {
	case <-notify:
	default:
		require.FailNow(t, "requestAbortAfter must close the notification channel handed out before it")
	}
	aborted, notifyAfter := s.abortedSince(gen, 5)
	require.True(t, aborted, "a transaction after the requester that began applying before the abort is covered")
	aborted, _ = s.abortedSince(gen, 3)
	require.False(t, aborted, "the requester is not covered by its own abort")
	aborted, _ = s.abortedSince(gen, 2)
	require.False(t, aborted, "a transaction before the requester is not covered")

	newGen, _ := s.abortState()
	aborted, _ = s.abortedSince(newGen, 5)
	require.False(t, aborted, "a transaction that began applying after the abort is not covered")
	select {
	case <-notifyAfter:
		require.FailNow(t, "the notification channel handed out after the abort must stay open until the next one")
	default:
	}

	s.mu.Lock()
	require.Equal(t, int64(3), s.dispatchPausedAfter)
	s.mu.Unlock()
}
