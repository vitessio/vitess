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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplySchedulerCommitParentOrder(t *testing.T) {
	ctx := context.Background()
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
	ctx := context.Background()
	s := newApplyScheduler(ctx)

	txn1 := &applyTxn{writeset: []string{"t1:1"}}
	txn2 := &applyTxn{writeset: []string{"t1:2"}}

	require.NoError(t, s.enqueue(txn1))
	require.NoError(t, s.enqueue(txn2))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	got2, err := s.nextReady(ctx)
	require.NoError(t, err)

	assert.NotEqual(t, got1, got2)
}

func TestApplySchedulerBlocksConflictingWritesets(t *testing.T) {
	ctx := context.Background()
	s := newApplyScheduler(ctx)

	txn1 := &applyTxn{writeset: []string{"t1:1"}}
	txn2 := &applyTxn{writeset: []string{"t1:1"}}

	require.NoError(t, s.enqueue(txn1))
	require.NoError(t, s.enqueue(txn2))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)

	readyCh := make(chan *applyTxn, 1)
	go func() {
		txn, err := s.nextReady(ctx)
		if err == nil {
			readyCh <- txn
		}
	}()

	assert.Never(t, func() bool {
		return len(readyCh) > 0
	}, 50*time.Millisecond, 5*time.Millisecond)

	require.NoError(t, s.markCommitted(got1))

	assert.Eventually(t, func() bool {
		return len(readyCh) > 0
	}, 200*time.Millisecond, 5*time.Millisecond)
}

func TestApplySchedulerBlocksCommitMetaDuringMissingMeta(t *testing.T) {
	ctx := context.Background()
	s := newApplyScheduler(ctx)

	missing := &applyTxn{writeset: []string{"t1:1"}}
	meta := &applyTxn{sequenceNumber: 2, commitParent: 0, hasCommitMeta: true}

	require.NoError(t, s.enqueue(missing))
	require.NoError(t, s.enqueue(meta))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, missing, got1)

	readyCh := make(chan *applyTxn, 1)
	go func() {
		txn, err := s.nextReady(ctx)
		if err == nil {
			readyCh <- txn
		}
	}()

	assert.Never(t, func() bool {
		return len(readyCh) > 0
	}, 50*time.Millisecond, 5*time.Millisecond)

	require.NoError(t, s.markCommitted(got1))

	assert.Eventually(t, func() bool {
		return len(readyCh) > 0
	}, 200*time.Millisecond, 5*time.Millisecond)
}

func TestApplySchedulerBlocksCommitMetaConflictingWritesets(t *testing.T) {
	ctx := context.Background()
	s := newApplyScheduler(ctx)

	txn1 := &applyTxn{writeset: []string{"t1:1"}, sequenceNumber: 1, commitParent: 0, hasCommitMeta: true}
	txn2 := &applyTxn{writeset: []string{"t1:1"}, sequenceNumber: 2, commitParent: 0, hasCommitMeta: true}

	require.NoError(t, s.enqueue(txn1))
	require.NoError(t, s.enqueue(txn2))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, txn1, got1)

	readyCh := make(chan *applyTxn, 1)
	go func() {
		txn, err := s.nextReady(ctx)
		if err == nil {
			readyCh <- txn
		}
	}()

	assert.Never(t, func() bool {
		return len(readyCh) > 0
	}, 50*time.Millisecond, 5*time.Millisecond)

	require.NoError(t, s.markCommitted(got1))

	assert.Eventually(t, func() bool {
		return len(readyCh) > 0
	}, 200*time.Millisecond, 5*time.Millisecond)
}

func TestApplySchedulerCommitMetaDoesNotAdvanceOnMissingMeta(t *testing.T) {
	ctx := context.Background()
	s := newApplyScheduler(ctx)
	require.Equal(t, int64(0), s.lastCommittedSequence)

	missing := &applyTxn{writeset: []string{"t1:1"}}
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
	assert.Equal(t, int64(5), s.lastCommittedSequence)
}

func TestApplySchedulerSeedsCommitParentOnFirstMeta(t *testing.T) {
	ctx := context.Background()
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
	ctx := context.Background()
	s := newApplyScheduler(ctx)

	// Simulate COMMIT_ORDER dependency tracking: each txn's commitParent is
	// the immediately prior sequence number, forming a strict serial chain.
	// With non-conflicting writesets, the scheduler should allow parallelism
	// by ignoring the commit-parent dependency.
	txn1 := &applyTxn{order: 1, sequenceNumber: 10, commitParent: 9, hasCommitMeta: true, writeset: []string{"t1:1"}}
	txn2 := &applyTxn{order: 2, sequenceNumber: 11, commitParent: 10, hasCommitMeta: true, writeset: []string{"t1:2"}}
	txn3 := &applyTxn{order: 3, sequenceNumber: 12, commitParent: 11, hasCommitMeta: true, writeset: []string{"t1:3"}}

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
	ctx := context.Background()
	s := newApplyScheduler(ctx)

	// Even with the commit-parent bypass, conflicting writesets must still
	// cause serialization.
	txn1 := &applyTxn{order: 1, sequenceNumber: 10, commitParent: 9, hasCommitMeta: true, writeset: []string{"t1:1"}}
	txn2 := &applyTxn{order: 2, sequenceNumber: 11, commitParent: 10, hasCommitMeta: true, writeset: []string{"t1:1"}}

	require.NoError(t, s.enqueue(txn1))
	require.NoError(t, s.enqueue(txn2))

	got1, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, txn1, got1)

	// txn2 should be blocked because it conflicts with inflight txn1.
	readyCh := make(chan *applyTxn, 1)
	go func() {
		txn, err := s.nextReady(ctx)
		if err == nil {
			readyCh <- txn
		}
	}()

	assert.Never(t, func() bool {
		return len(readyCh) > 0
	}, 50*time.Millisecond, 5*time.Millisecond)

	require.NoError(t, s.markCommitted(got1))

	assert.Eventually(t, func() bool {
		return len(readyCh) > 0
	}, 200*time.Millisecond, 5*time.Millisecond)
}

func TestApplySchedulerEmptyWritesetFallsBackToCommitParent(t *testing.T) {
	ctx := context.Background()
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
	readyCh := make(chan *applyTxn, 1)
	go func() {
		txn, err := s.nextReady(ctx)
		if err == nil {
			readyCh <- txn
		}
	}()

	assert.Never(t, func() bool {
		return len(readyCh) > 0
	}, 50*time.Millisecond, 5*time.Millisecond)

	// After committing txn1, lastCommittedSequence advances to 10,
	// making txn2 ready (commitParent 10 <= 10).
	require.NoError(t, s.markCommitted(got1))

	assert.Eventually(t, func() bool {
		return len(readyCh) > 0
	}, 200*time.Millisecond, 5*time.Millisecond)
}
