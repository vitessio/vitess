/*
Copyright 2025 The Vitess Authors.

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

package vdiff

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// TestWorkflowLockRetriesWithMultipleShards tests that multiple concurrent
// VDiff operations (simulating one per shard) can all successfully acquire
// the workflow lock using exponential backoff retry logic.
func TestWorkflowLockRetriesWithMultipleShards(t *testing.T) {
	numShards := 64
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()
	keyspace := "testkeyspace"
	workflow := "testwf"
	lockName := fmt.Sprintf("%s/%s", keyspace, workflow)
	var successCount atomic.Int32
	var wg sync.WaitGroup

	// Simulate multiple concurrent table diffs (one per shard) all trying to acquire the same named lock.
	for shard := range numShards {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()
			retryDelay := 10 * time.Millisecond
			maxRetryDelay := 100 * time.Millisecond
			backoffFactor := 1.5

			for {
				attemptCtx, attemptCancel := context.WithTimeout(ctx, topo.LockTimeout)
				_, unlock, lockErr := ts.LockName(attemptCtx, lockName, "vdiff")
				if lockErr == nil {
					time.Sleep(1 * time.Millisecond) // Simulate initialization work
					var unlockErr error
					unlock(&unlockErr)
					attemptCancel()
					successCount.Add(1)
					return
				}

				// Retry with exponential backoff.
				attemptCancel()

				select {
				case <-ctx.Done():
					return
				case <-time.After(retryDelay):
					if retryDelay < maxRetryDelay {
						retryDelay = min(time.Duration(float64(retryDelay)*backoffFactor), maxRetryDelay)
					}
					continue
				}
			}
		}(shard)
	}
	wg.Wait()

	// All shards should have successfully acquired the lock.
	require.Equal(t, int32(numShards), successCount.Load(),
		"all shards should successfully acquire the lock with retry logic")
}

// TestWorkflowLockExponentialBackoff tests that the exponential backoff
// retry logic works correctly when acquiring workflow locks.
func TestWorkflowLockExponentialBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()
	keyspace := "testkeyspace"
	workflow := "testwf"
	lockName := fmt.Sprintf("%s/%s", keyspace, workflow)
	var (
		retryCount atomic.Int32
		gotLock    atomic.Bool
		wg         sync.WaitGroup
	)

	// First, acquire the lock and hold it.
	_, unlock, err := ts.LockName(ctx, lockName, "test")
	require.NoError(t, err)

	// Start a goroutine that will try to acquire the lock that is held.
	wg.Go(func() {
		retryDelay := 10 * time.Millisecond
		maxRetryDelay := 100 * time.Millisecond
		backoffFactor := 1.5

		for {
			attemptCtx, attemptCancel := context.WithTimeout(ctx, maxRetryDelay)
			_, attemptUnlock, attemptErr := ts.LockName(attemptCtx, lockName, "test")
			if attemptErr == nil {
				gotLock.Store(true)
				defer attemptUnlock(&attemptErr)
				defer attemptCancel()
				return
			}

			attemptCancel()
			retryCount.Add(1)

			select {
			case <-ctx.Done():
				return
			case <-time.After(retryDelay):
				if retryDelay < maxRetryDelay {
					retryDelay = min(time.Duration(float64(retryDelay)*backoffFactor), maxRetryDelay)
				}
				continue
			}
		}
	})

	// Wait for multiple retries.
	require.Eventually(t, func() bool {
		return retryCount.Load() > 1
	}, 5*time.Second, 50*time.Millisecond)

	// Release the lock.
	var unlockErr error
	unlock(&unlockErr)
	require.NoError(t, unlockErr)

	// Wait for the goroutine to get the lock now and finish.
	wg.Wait()

	// Verify the lock was eventually acquired and there were retries.
	require.True(t, gotLock.Load(), "the lock should have been acquired after retries")
	require.Greater(t, retryCount.Load(), int32(1), "we should have retried at least twice")
}
