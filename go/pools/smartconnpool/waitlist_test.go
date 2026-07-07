/*
Copyright 2024 The Vitess Authors.

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

package smartconnpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/list"
)

func TestWaitlistPoolCloseWithMultipleWaiters(t *testing.T) {
	wait := waitlist[*TestConn]{}
	wait.init()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	poolClose := make(chan struct{})

	waiterCount := 2
	expireCount := atomic.Int32{}

	for range waiterCount {
		go func() {
			_, err := wait.waitForConn(ctx, nil, poolClose, 0)
			if err != nil {
				expireCount.Add(1)
			}
		}()
	}

	close(poolClose)

	// Wait for the context to expire
	<-ctx.Done()

	// Wait for the notified goroutines to finish
	timeout := time.After(1 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for expireCount.Load() != int32(waiterCount) {
		select {
		case <-timeout:
			require.Failf(t, "Timed out waiting for all waiters to expire", "Wanted %d, got %d", waiterCount, expireCount.Load())
		case <-ticker.C:
			// try again
		}
	}

	assert.Equal(t, int32(waiterCount), expireCount.Load())
}

func TestWaitlistWaiterCap(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init()

	poolClose := make(chan struct{})

	const maxWaiters = 3

	errs := make(chan error, maxWaiters)
	for i := 1; i <= maxWaiters; i++ {
		go func() {
			_, err := wl.waitForConn(t.Context(), nil, poolClose, maxWaiters)
			errs <- err
		}()

		assert.Eventually(t, func() bool {
			return wl.waiting() == i
		}, time.Second, 5*time.Millisecond)
	}

	_, err := wl.waitForConn(t.Context(), nil, poolClose, maxWaiters)
	require.ErrorIs(t, err, ErrPoolWaiterCapReached)
	assert.Equal(t, maxWaiters, wl.waiting())

	close(poolClose)

	for range maxWaiters {
		assert.NotErrorIs(t, <-errs, ErrPoolWaiterCapReached)
	}
}

// pushWaiter injects a synthetic waitlist entry with no goroutine behind it.
// The conn channel is buffered so a handoff to it can't block the test.
func pushWaiter(wl *waitlist[*TestConn], ctx context.Context) *list.Element[waiter[*TestConn]] {
	elem := &list.Element[waiter[*TestConn]]{
		Value: waiter[*TestConn]{
			ctx:  ctx,
			conn: make(chan *Pooled[*TestConn], 1),
		},
	}
	wl.mu.Lock()
	wl.list.PushBackValue(elem)
	wl.mu.Unlock()
	return elem
}

func TestWaitlistTryReturnConnEvictsExpiredWaiters(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init()

	expiredCtx, cancel := context.WithCancel(t.Context())
	cancel()

	expired := pushWaiter(&wl, expiredCtx)
	live := pushWaiter(&wl, t.Context())

	conn := &Pooled[*TestConn]{Conn: &TestConn{}}
	require.True(t, wl.tryReturnConn(conn))

	// the live waiter behind the expired one received the connection
	select {
	case got := <-live.Value.conn:
		require.Same(t, conn, got)
	default:
		require.Fail(t, "live waiter did not receive the returned connection")
	}

	// the expired waiter was evicted from the list and woken with a nil, so it
	// stops waiting and returns its context error
	require.Equal(t, 0, wl.waiting())
	select {
	case got := <-expired.Value.conn:
		require.Nil(t, got, "expired waiter must be woken with a nil, not a connection")
	default:
		require.Fail(t, "expired waiter was not woken")
	}
}

func TestWaitlistTryReturnConnAllWaitersExpired(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init()

	expiredCtx, cancel := context.WithCancel(t.Context())
	cancel()

	const waiterCount = 3
	waiters := make([]*list.Element[waiter[*TestConn]], 0, waiterCount)
	for range waiterCount {
		waiters = append(waiters, pushWaiter(&wl, expiredCtx))
	}

	conn := &Pooled[*TestConn]{Conn: &TestConn{}}
	require.False(t, wl.tryReturnConn(conn))

	// every waiter was expired, so the returner evicted them all (waking each
	// with a nil) and kept the connection (nothing live to hand it to)
	require.Equal(t, 0, wl.waiting())
	for _, w := range waiters {
		select {
		case got := <-w.Value.conn:
			require.Nil(t, got, "expired waiter must be woken with a nil, not a connection")
		default:
			require.Fail(t, "expired waiter was not woken")
		}
	}
}

func TestWaitlistMaybeStarvingCountSkipsExpiredWaiters(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init()

	expiredCtx, cancel := context.WithCancel(t.Context())
	cancel()

	pushWaiter(&wl, expiredCtx)
	pushWaiter(&wl, t.Context())

	// only the live waiter is maybe starving; the expired waiter cannot use
	// a connection and must not cause the starving worker to hand one out
	require.Equal(t, 1, wl.maybeStarvingCount())
}

func TestWaitlistHandoverToLiveWaiter(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init()

	poolClose := make(chan struct{})

	results := make(chan *Pooled[*TestConn], 1)
	go func() {
		conn, err := wl.waitForConn(t.Context(), nil, poolClose, 0)
		assert.NoError(t, err)
		results <- conn
	}()

	require.Eventually(t, func() bool {
		return wl.waiting() == 1
	}, 30*time.Second, time.Millisecond)

	conn := &Pooled[*TestConn]{Conn: &TestConn{}}
	require.True(t, wl.tryReturnConn(conn))

	var got *Pooled[*TestConn]
	require.Eventually(t, func() bool {
		select {
		case got = <-results:
			return true
		default:
			return false
		}
	}, 30*time.Second, time.Millisecond)
	require.Same(t, conn, got)
}

// TestWaitlistClaimedWaiterStillReceivesAfterExpiry pins the handoff
// protocol's claim window: a returner removes a live waiter from the list and
// is then committed to the handoff, even if the waiter's context expires
// before the send lands. The waiter must take the connection instead of
// erroring out, or the returner would be stranded mid-send.
func TestWaitlistClaimedWaiterStillReceivesAfterExpiry(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init()

	poolClose := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	type result struct {
		conn *Pooled[*TestConn]
		err  error
	}
	results := make(chan result, 1)
	go func() {
		conn, err := wl.waitForConn(ctx, nil, poolClose, 0)
		results <- result{conn: conn, err: err}
	}()

	require.Eventually(t, func() bool {
		return wl.waiting() == 1
	}, 30*time.Second, time.Millisecond)

	// Simulate a returner that claimed the waiter while it was live, racing
	// with the context expiring before the handoff send: remove the element
	// and cancel under the mutex, so the waiter can't self-remove first.
	wl.mu.Lock()
	elem := wl.list.Front()
	wl.list.Remove(elem)
	cancel()
	wl.mu.Unlock()

	conn := &Pooled[*TestConn]{Conn: &TestConn{}}
	elem.Value.conn <- conn

	var res result
	require.Eventually(t, func() bool {
		select {
		case res = <-results:
			return true
		default:
			return false
		}
	}, 30*time.Second, time.Millisecond)
	require.NoError(t, res.err)
	require.Same(t, conn, res.conn)
}

// TestWaitlistConvoyDrainsUnderConcurrentReturns is a bounded reproduction of
// the production timeout-storm convoy: a large prefix of expired waiters sits
// at the head of the waitlist while live waiters queue behind it, and several
// returners hand connections back concurrently. The fix requires a returner to
// evict the dead prefix as it scans, so the list drains to empty and every
// live waiter is served. On the unpatched skip-and-leave code the expired
// waiters are left in place, so waiting() never reaches zero and this fails.
func TestWaitlistConvoyDrainsUnderConcurrentReturns(t *testing.T) {
	const (
		expiredWaiters = 4000
		liveWaiters    = 64
		returners      = 8
	)

	wl := waitlist[*TestConn]{}
	wl.init()

	poolClose := make(chan struct{})

	// dead prefix: synthetic waiters whose context already expired, with no
	// goroutine behind them, so they linger in the list exactly like the
	// timed-out waiters that pile up under a real timeout storm.
	expiredCtx, cancel := context.WithCancel(t.Context())
	cancel()
	for range expiredWaiters {
		pushWaiter(&wl, expiredCtx)
	}

	// live waiters queue behind the dead prefix
	var served atomic.Int64
	var waiters sync.WaitGroup
	for range liveWaiters {
		waiters.Go(func() {
			conn, err := wl.waitForConn(t.Context(), nil, poolClose, 0)
			if err == nil && conn != nil {
				served.Add(1)
			}
		})
	}
	require.Eventually(t, func() bool {
		return wl.waiting() == expiredWaiters+liveWaiters
	}, 30*time.Second, time.Millisecond, "waiters did not all enqueue")

	// concurrent returners hand back exactly liveWaiters connections; the first
	// returner to win the mutex also evicts the whole dead prefix as it scans.
	var toIssue atomic.Int64
	toIssue.Store(liveWaiters)
	var returnersWG sync.WaitGroup
	for range returners {
		returnersWG.Go(func() {
			for toIssue.Add(-1) >= 0 {
				wl.tryReturnConn(&Pooled[*TestConn]{Conn: &TestConn{}})
			}
		})
	}
	returnersWG.Wait()
	waiters.Wait()

	require.Equal(t, int64(liveWaiters), served.Load(), "every live waiter should receive a connection")
	require.Equal(t, 0, wl.waiting(), "the expired prefix must be evicted, leaving an empty waitlist")
}
