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

package grpctabletconn

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamPool_GetCreatesNew(t *testing.T) {
	var created atomic.Int32
	pool := newStreamPool(1, 0, func() (int, context.CancelFunc, error) {
		return int(created.Add(1)), func() {}, nil
	})
	defer pool.close()

	ctx := context.Background()
	s, err := pool.get(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, s.stream)
	assert.Equal(t, int32(1), created.Load())
}

func TestStreamPool_PutAndReuse(t *testing.T) {
	var created atomic.Int32
	pool := newStreamPool(1, 0, func() (int, context.CancelFunc, error) {
		return int(created.Add(1)), func() {}, nil
	})
	defer pool.close()

	ctx := context.Background()
	s1, err := pool.get(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, s1.stream)

	pool.put(s1)

	s2, err := pool.get(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, s2.stream, "should reuse the pooled stream")
	assert.Equal(t, int32(1), created.Load(), "should not create a new stream")
}

func TestStreamPool_MaxSize(t *testing.T) {
	var created atomic.Int32
	pool := newStreamPool(2, 0, func() (int, context.CancelFunc, error) {
		return int(created.Add(1)), func() {}, nil
	})
	defer pool.close()

	ctx := context.Background()
	s1, err := pool.get(ctx)
	require.NoError(t, err)
	s2, err := pool.get(ctx)
	require.NoError(t, err)

	pool.put(s1)
	pool.put(s2)

	// Both should be in the pool now (maxSize=2). addBagItem should not create
	// a third entry because len(entries) == maxSize.
	assert.Equal(t, int32(2), created.Load())
	entries := pool.loadEntries()
	assert.Len(t, entries, 2)
}

func TestStreamPool_Discard(t *testing.T) {
	var cancelled atomic.Int32
	pool := newStreamPool(2, 0, func() (int, context.CancelFunc, error) {
		return 0, func() { cancelled.Add(1) }, nil
	})
	defer pool.close()

	ctx := context.Background()
	s, err := pool.get(ctx)
	require.NoError(t, err)

	pool.discard(s)
	assert.Equal(t, int32(1), cancelled.Load())
	assert.Equal(t, stateRemoved, s.state.Load(), "discarded entry should be REMOVED")
}

func TestStreamPool_Close(t *testing.T) {
	var cancelled atomic.Int32
	pool := newStreamPool(2, 0, func() (int, context.CancelFunc, error) {
		return 0, func() { cancelled.Add(1) }, nil
	})

	ctx := context.Background()
	s1, _ := pool.get(ctx)
	s2, _ := pool.get(ctx)
	pool.put(s1)
	pool.put(s2)

	pool.close()
	assert.Equal(t, int32(2), cancelled.Load(), "close should cancel all idle entries")

	_, err := pool.get(ctx)
	assert.ErrorIs(t, err, ErrPoolClosed)
}

func TestStreamPool_PutAfterClose(t *testing.T) {
	var cancelled atomic.Int32
	pool := newStreamPool(2, 0, func() (int, context.CancelFunc, error) {
		return 0, func() { cancelled.Add(1) }, nil
	})

	ctx := context.Background()
	s, _ := pool.get(ctx)
	pool.close()

	pool.put(s)
	assert.Equal(t, stateRemoved, s.state.Load(), "entry should be REMOVED after put-after-close")
	assert.GreaterOrEqual(t, cancelled.Load(), int32(1), "put after close should cancel the stream")
}

func TestStreamPool_DirectHandoff(t *testing.T) {
	createGate := make(chan struct{})
	var created atomic.Int32
	pool := newStreamPool(4, 0, func() (int, context.CancelFunc, error) {
		v := int(created.Add(1))
		if v > 1 {
			// Block subsequent creates so only put() can satisfy the waiter.
			<-createGate
		}
		return v, func() {}, nil
	})
	defer close(createGate)
	defer pool.close()

	ctx := context.Background()

	// Pre-populate one entry and borrow it so the pool is empty.
	s1, err := pool.get(ctx)
	require.NoError(t, err)

	// Start a goroutine that will block on get().
	gotCh := make(chan *poolEntry[int], 1)
	go func() {
		s, err := pool.get(ctx)
		if err == nil {
			gotCh <- s
		}
	}()

	// Give the goroutine time to register as waiter.
	assert.Eventually(t, func() bool {
		return pool.waiters.Load() > 0
	}, time.Second, time.Millisecond)

	// Return s1 — should hand off directly to the blocked get().
	pool.put(s1)

	select {
	case s := <-gotCh:
		assert.Equal(t, s1.stream, s.stream, "should receive the same entry via handoff")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for handoff")
	}
}

func TestStreamPool_ScanBeatsHandoff(t *testing.T) {
	pool := newStreamPool(4, 0, func() (int, context.CancelFunc, error) {
		return 42, func() {}, nil
	})
	defer pool.close()

	ctx := context.Background()

	// Create an entry, return it so it's NOT_IN_USE in the list.
	s1, err := pool.get(ctx)
	require.NoError(t, err)
	pool.put(s1)

	// Next get() should find it by scan, not by handoff (no waiters needed).
	s2, err := pool.get(ctx)
	require.NoError(t, err)
	assert.Equal(t, 42, s2.stream)
	assert.Equal(t, int32(0), pool.waiters.Load(), "should not have registered as waiter")
}

func TestStreamPool_ContextTimeout(t *testing.T) {
	pool := newStreamPool(1, 0, func() (int, context.CancelFunc, error) {
		return 0, func() {}, errors.New("create fails")
	})
	defer pool.close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := pool.get(ctx)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "expected deadline exceeded, got: %v", err)
}

func TestStreamPool_CreateFailureRetries(t *testing.T) {
	var attempts atomic.Int32
	const failUntil = int32(3)

	pool := newStreamPool(2, 0, func() (int, context.CancelFunc, error) {
		n := attempts.Add(1)
		if n <= failUntil {
			return 0, nil, errors.New("transient failure")
		}
		return int(n), func() {}, nil
	})
	defer pool.close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// get() should eventually succeed despite initial create failures.
	s, err := pool.get(ctx)
	require.NoError(t, err)
	assert.NotNil(t, s)
	assert.True(t, attempts.Load() > failUntil, "should have retried past the failing attempts")

	pool.put(s)
}

func TestStreamPool_CreateFailureBackoff(t *testing.T) {
	var attempts atomic.Int32
	pool := newStreamPool(2, 0, func() (int, context.CancelFunc, error) {
		attempts.Add(1)
		return 0, nil, errors.New("permanent failure")
	})
	defer pool.close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := pool.get(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// With 5ms backoff and 50ms timeout, we expect a bounded number of
	// attempts. Without backoff this would be thousands.
	assert.LessOrEqual(t, attempts.Load(), int32(20),
		"create attempts should be bounded by backoff")
}

func TestStreamPool_DiscardTriggersReplacement(t *testing.T) {
	var created atomic.Int32
	pool := newStreamPool(2, 0, func() (int, context.CancelFunc, error) {
		return int(created.Add(1)), func() {}, nil
	})
	defer pool.close()

	ctx := context.Background()

	// Create and borrow two entries (at capacity).
	s1, err := pool.get(ctx)
	require.NoError(t, err)
	s2, err := pool.get(ctx)
	require.NoError(t, err)

	// Return one so there's an idle entry.
	pool.put(s1)

	// Start a goroutine that borrows the idle entry, leaving pool empty.
	s3, err := pool.get(ctx)
	require.NoError(t, err)
	assert.Equal(t, s1.stream, s3.stream)

	// Now start a blocking get().
	gotCh := make(chan *poolEntry[int], 1)
	go func() {
		s, err := pool.get(ctx)
		if err == nil {
			gotCh <- s
		}
	}()

	assert.Eventually(t, func() bool {
		return pool.waiters.Load() > 0
	}, time.Second, time.Millisecond)

	// Discard s2 — should trigger replacement creation since there's a waiter.
	pool.discard(s2)

	select {
	case s := <-gotCh:
		assert.NotNil(t, s)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replacement entry")
	}

	pool.put(s3)
}

func TestStreamPool_ConcurrentStress(t *testing.T) {
	var created atomic.Int32
	pool := newStreamPool(8, 0, func() (int, context.CancelFunc, error) {
		return int(created.Add(1)), func() {}, nil
	})

	ctx := context.Background()
	const goroutines = 16
	const iterations = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for i := range iterations {
				s, err := pool.get(ctx)
				if err != nil {
					continue
				}
				// Simulate some work.
				if i%10 == 0 {
					pool.discard(s)
				} else {
					pool.put(s)
				}
			}
		}()
	}
	wg.Wait()

	pool.close()
}

func TestStreamPool_PutNoWaiters(t *testing.T) {
	pool := newStreamPool(4, 0, func() (int, context.CancelFunc, error) {
		return 1, func() {}, nil
	})
	defer pool.close()

	ctx := context.Background()
	s, err := pool.get(ctx)
	require.NoError(t, err)

	// put() with no waiters should just flip state — zero-cost return.
	assert.Equal(t, int32(0), pool.waiters.Load())
	pool.put(s)

	assert.Equal(t, stateNotInUse, s.state.Load(), "entry should be NOT_IN_USE after put")
	assert.GreaterOrEqual(t, len(pool.loadEntries()), 1, "entry should remain in shared list")
}

func TestStreamPool_ColdStartBurst(t *testing.T) {
	const maxSize = 4
	const goroutines = 20

	var created atomic.Int32
	pool := newStreamPool(maxSize, 0, func() (int, context.CancelFunc, error) {
		return int(created.Add(1)), func() {}, nil
	})
	defer pool.close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Launch many goroutines competing for a small pool.
	// The creator loop should fill the pool up to maxSize, never beyond.
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			s, err := pool.get(ctx)
			if err != nil {
				return
			}
			runtime.Gosched()
			pool.put(s)
		}()
	}
	wg.Wait()

	assert.LessOrEqual(t, created.Load(), int32(maxSize),
		"should never create more streams than maxSize")
}

func BenchmarkStreamPool_GetPut(b *testing.B) {
	pool := newStreamPool(16, 0, func() (int, context.CancelFunc, error) {
		return 0, func() {}, nil
	})

	ctx := context.Background()
	// Pre-populate one entry.
	s, _ := pool.get(ctx)
	pool.put(s)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, _ := pool.get(ctx)
		pool.put(s)
	}
}

func BenchmarkStreamPool_GetPut_Parallel(b *testing.B) {
	pool := newStreamPool(64, 0, func() (int, context.CancelFunc, error) {
		return 0, func() {}, nil
	})

	ctx := context.Background()
	// Pre-populate entries.
	for range 16 {
		s, _ := pool.get(ctx)
		pool.put(s)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, _ := pool.get(ctx)
			if s != nil {
				pool.put(s)
			}
		}
	})
}

// BenchmarkStreamPool_Contention_Scarce benchmarks the handoff path:
// pool has only 2 entries but GOMAXPROCS goroutines competing for them.
// Most get() calls must block on the handoff channel.
func BenchmarkStreamPool_Contention_Scarce(b *testing.B) {
	pool := newStreamPool(2, 0, func() (int, context.CancelFunc, error) {
		return 0, func() {}, nil
	})

	ctx := context.Background()
	// Pre-populate exactly 2 entries.
	for range 2 {
		s, _ := pool.get(ctx)
		pool.put(s)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, _ := pool.get(ctx)
			if s != nil {
				pool.put(s)
			}
		}
	})
}

// BenchmarkStreamPool_Contention_WithWork simulates real usage where each
// goroutine holds the entry for some work (100ns busy loop) before returning it.
// Pool has 4 entries with GOMAXPROCS goroutines competing.
func BenchmarkStreamPool_Contention_WithWork(b *testing.B) {
	pool := newStreamPool(4, 0, func() (int, context.CancelFunc, error) {
		return 0, func() {}, nil
	})

	ctx := context.Background()
	for range 4 {
		s, _ := pool.get(ctx)
		pool.put(s)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, _ := pool.get(ctx)
			if s != nil {
				// Simulate ~100ns of work (Send/Recv).
				start := time.Now()
				for time.Since(start) < 100*time.Nanosecond {
				}
				pool.put(s)
			}
		}
	})
}

// BenchmarkStreamPool_Contention_MixedOps benchmarks a realistic mix:
// 90% get/put, 10% get/discard (simulating stream errors that force replacement).
func BenchmarkStreamPool_Contention_MixedOps(b *testing.B) {
	pool := newStreamPool(8, 0, func() (int, context.CancelFunc, error) {
		return 0, func() {}, nil
	})

	ctx := context.Background()
	for range 8 {
		s, _ := pool.get(ctx)
		pool.put(s)
	}

	b.ResetTimer()
	var iter atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, _ := pool.get(ctx)
			if s == nil {
				continue
			}
			if iter.Add(1)%10 == 0 {
				pool.discard(s)
			} else {
				pool.put(s)
			}
		}
	})
}

// BenchmarkStreamPool_Contention_MixedOps_SlowCreate is like MixedOps but
// with a 1ms create latency, simulating real gRPC stream establishment.
func BenchmarkStreamPool_Contention_MixedOps_SlowCreate(b *testing.B) {
	pool := newStreamPool(8, 0, func() (int, context.CancelFunc, error) {
		time.Sleep(time.Millisecond)
		return 0, func() {}, nil
	})

	ctx := context.Background()
	for range 8 {
		s, _ := pool.get(ctx)
		pool.put(s)
	}

	b.ResetTimer()
	var iter atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, _ := pool.get(ctx)
			if s == nil {
				continue
			}
			if iter.Add(1)%10 == 0 {
				pool.discard(s)
			} else {
				pool.put(s)
			}
		}
	})
}

// BenchmarkStreamPool_Latency_SlowCreate measures get() latency distribution
// with 1ms create cost. maxSize is set to 64 (well above GOMAXPROCS) so that
// capacity is never the bottleneck — the only waits come from discards needing
// stream creation. This isolates the "rescue by put()" effect: can a waiter
// blocked on slow creation be unblocked early by a returned stream?
func BenchmarkStreamPool_Latency_SlowCreate(b *testing.B) {
	const poolSize = 64
	pool := newStreamPool(poolSize, 0, func() (int, context.CancelFunc, error) {
		time.Sleep(time.Millisecond)
		return 0, func() {}, nil
	})

	ctx := context.Background()
	for range poolSize {
		s, _ := pool.get(ctx)
		pool.put(s)
	}

	var (
		mu        sync.Mutex
		latencies []time.Duration
	)

	b.ResetTimer()
	var iter atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		var local []time.Duration
		for pb.Next() {
			start := time.Now()
			s, _ := pool.get(ctx)
			elapsed := time.Since(start)
			local = append(local, elapsed)
			if s == nil {
				continue
			}
			if iter.Add(1)%10 == 0 {
				pool.discard(s)
			} else {
				pool.put(s)
			}
		}
		mu.Lock()
		latencies = append(latencies, local...)
		mu.Unlock()
	})

	b.StopTimer()
	slices.Sort(latencies)
	n := len(latencies)
	if n == 0 {
		return
	}
	b.ReportMetric(float64(latencies[n/2].Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(latencies[n*95/100].Nanoseconds()), "p95-ns")
	b.ReportMetric(float64(latencies[n*99/100].Nanoseconds()), "p99-ns")
	b.ReportMetric(float64(latencies[n-1].Nanoseconds()), "max-ns")

	var over1ms int
	for _, l := range latencies {
		if l > time.Millisecond {
			over1ms++
		}
	}
	b.ReportMetric(100*float64(over1ms)/float64(n), "%>1ms")
}

func TestStreamPool_MaxLifetime_IdleEviction(t *testing.T) {
	var cancelled atomic.Int32
	// maxSize=1 prevents the creator goroutine from speculatively creating extra entries.
	pool := newStreamPool(1, 50*time.Millisecond, func() (int, context.CancelFunc, error) {
		return 1, func() { cancelled.Add(1) }, nil
	})
	defer pool.close()

	ctx := context.Background()
	s, err := pool.get(ctx)
	require.NoError(t, err)

	// Return entry so it's idle (NOT_IN_USE).
	pool.put(s)

	// Wait for the max lifetime timer to fire and soft-evict the idle entry.
	assert.Eventually(t, func() bool {
		return s.state.Load() == stateRemoved
	}, time.Second, time.Millisecond)
	assert.Equal(t, int32(1), cancelled.Load())
}

func TestStreamPool_MaxLifetime_ActiveEviction(t *testing.T) {
	var cancelled atomic.Int32
	// maxSize=1 prevents extra entries from the creator goroutine.
	pool := newStreamPool(1, 50*time.Millisecond, func() (int, context.CancelFunc, error) {
		return 1, func() { cancelled.Add(1) }, nil
	})
	defer pool.close()

	ctx := context.Background()
	s, err := pool.get(ctx)
	require.NoError(t, err)

	// Hold the entry past its max lifetime.
	time.Sleep(80 * time.Millisecond)

	// The entry should be marked evicted but still IN_USE (timer can't CAS it).
	assert.True(t, s.evicted.Load(), "entry should be marked evicted")
	assert.Equal(t, stateInUse, s.state.Load(), "entry should still be IN_USE")
	assert.Equal(t, int32(0), cancelled.Load(), "cancel should not have been called yet")

	// put() should discard instead of returning to pool.
	pool.put(s)
	assert.Equal(t, stateRemoved, s.state.Load())
	assert.Equal(t, int32(1), cancelled.Load())
}

func TestStreamPool_MaxLifetime_Jitter(t *testing.T) {
	const lifetime = 200 * time.Millisecond
	const n = 5

	var mu sync.Mutex
	var evictionTimes []time.Time

	pool := newStreamPool(n, lifetime, func() (int, context.CancelFunc, error) {
		return 0, func() {
			mu.Lock()
			evictionTimes = append(evictionTimes, time.Now())
			mu.Unlock()
		}, nil
	})
	defer pool.close()

	ctx := context.Background()

	// Hold all entries simultaneously to force n distinct creates.
	held := make([]*poolEntry[int], n)
	for i := range n {
		s, err := pool.get(ctx)
		require.NoError(t, err)
		held[i] = s
	}
	// Return them all so they're idle and subject to eviction.
	for _, s := range held {
		pool.put(s)
	}

	// Wait for all pool entries to be evicted.
	totalEntries := len(pool.loadEntries())
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(evictionTimes) >= totalEntries
	}, 2*time.Second, time.Millisecond)

	// Check that not all evictions happened at the exact same time.
	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(evictionTimes), 2, "need at least 2 entries for jitter check")
	earliest := evictionTimes[0]
	latest := evictionTimes[0]
	for _, et := range evictionTimes[1:] {
		if et.Before(earliest) {
			earliest = et
		}
		if et.After(latest) {
			latest = et
		}
	}
	spread := latest.Sub(earliest)
	assert.Greater(t, spread, time.Duration(0), "eviction times should have jitter (spread=%v)", spread)
}

func TestStreamPool_MaxLifetime_Replacement(t *testing.T) {
	createGate := make(chan struct{})
	var created atomic.Int32

	pool := newStreamPool(2, 50*time.Millisecond, func() (int, context.CancelFunc, error) {
		v := int(created.Add(1))
		if v > 1 {
			// Block subsequent creates so we can control timing.
			<-createGate
		}
		return v, func() {}, nil
	})
	defer close(createGate)
	defer pool.close()

	ctx := context.Background()

	// Get and hold one entry.
	s, err := pool.get(ctx)
	require.NoError(t, err)

	// Hold it past max lifetime, then put it (triggers eviction + requestCreate).
	time.Sleep(80 * time.Millisecond)
	pool.put(s)

	// Start a waiter that needs a new entry.
	gotCh := make(chan *poolEntry[int], 1)
	go func() {
		entry, err := pool.get(ctx)
		if err == nil {
			gotCh <- entry
		}
	}()

	// Wait for the waiter to register.
	assert.Eventually(t, func() bool {
		return pool.waiters.Load() > 0
	}, time.Second, time.Millisecond)

	// Unblock the creator.
	createGate <- struct{}{}

	select {
	case entry := <-gotCh:
		assert.NotNil(t, entry)
		pool.put(entry)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replacement entry")
	}
}

func TestStreamPool_MaxLifetime_Disabled(t *testing.T) {
	pool := newStreamPool(4, 0, func() (int, context.CancelFunc, error) {
		return 1, func() {}, nil
	})
	defer pool.close()

	ctx := context.Background()
	s, err := pool.get(ctx)
	require.NoError(t, err)
	pool.put(s)

	// With maxLifetime=0, the entry should stay idle indefinitely.
	time.Sleep(50 * time.Millisecond)
	assert.False(t, s.evicted.Load(), "entry should not be evicted with maxLifetime=0")
	assert.Equal(t, stateNotInUse, s.state.Load())
}

func TestStreamPool_MaxLifetime_GetSkipsEvicted(t *testing.T) {
	var created atomic.Int32
	pool := newStreamPool(4, 0, func() (int, context.CancelFunc, error) {
		return int(created.Add(1)), func() {}, nil
	})
	defer pool.close()

	ctx := context.Background()

	// Create an entry and return it to the pool.
	s, err := pool.get(ctx)
	require.NoError(t, err)
	pool.put(s)

	// Mark all pool entries as evicted to simulate fired timers.
	for _, e := range pool.loadEntries() {
		e.evicted.Store(true)
	}
	createdBefore := created.Load()

	// get() should skip all evicted entries and create a new one.
	// The evicted slots are discarded (REMOVED), then the creator
	// recycles one of them with a fresh stream.
	s2, err := pool.get(ctx)
	require.NoError(t, err)
	assert.False(t, s2.evicted.Load(), "new entry should not be evicted")
	assert.Greater(t, created.Load(), createdBefore, "should have created a new entry")

	pool.put(s2)
}
