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
	assert.ErrorIs(t, err, ErrPoolWaiterCapReached)
	assert.Equal(t, maxWaiters, wl.waiting())

	close(poolClose)

	for range maxWaiters {
		assert.NotErrorIs(t, <-errs, ErrPoolWaiterCapReached)
	}
}

func TestWaitlistTryReturnConnDoesNotBlockWhenWaiterIsNotReceiving(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init()

	// After tryReturnConn chooses a live waiter, that waiter can stop receiving
	// before the handoff send happens. The return path must still complete
	// instead of blocking.
	elem := wl.nodes.Get().(*list.Element[waiter[*TestConn]])
	defer wl.nodes.Put(elem)

	elem.Value = waiter[*TestConn]{conn: elem.Value.conn, ctx: t.Context()}
	wl.list.PushBackValue(elem)

	returned := make(chan bool, 1)
	conn := &Pooled[*TestConn]{
		Conn: &TestConn{
			counts: &TestState{},
		},
	}

	go func() {
		returned <- wl.tryReturnConn(conn)
	}()

	require.Eventually(t, func() bool {
		return len(returned) == 1
	}, time.Second, 5*time.Millisecond)
	assert.True(t, <-returned)
	assert.Same(t, conn, <-elem.Value.conn)
}

func TestWaitlistTryReturnConnSkipsCanceledWaiters(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init()

	// Canceled waiters can remain in the waitlist while returners are trying to
	// hand off connections. Returning a connection must skip those canceled
	// waiters, wake them, and deliver the connection only to a live waiter.
	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()

	canceled := wl.nodes.Get().(*list.Element[waiter[*TestConn]])
	defer wl.nodes.Put(canceled)
	canceled.Value = waiter[*TestConn]{conn: canceled.Value.conn, ctx: canceledCtx}
	wl.list.PushBackValue(canceled)

	live := wl.nodes.Get().(*list.Element[waiter[*TestConn]])
	defer wl.nodes.Put(live)
	live.Value = waiter[*TestConn]{conn: live.Value.conn, ctx: t.Context()}
	wl.list.PushBackValue(live)

	conn := &Pooled[*TestConn]{
		Conn: &TestConn{
			counts: &TestState{},
		},
	}

	assert.True(t, wl.tryReturnConn(conn))
	assert.Zero(t, wl.waiting())
	assert.Nil(t, <-canceled.Value.conn)
	assert.Same(t, conn, <-live.Value.conn)
}

func TestWaitlistTryReturnConnReturnsFalseWhenOnlyCanceledWaitersRemain(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init()

	// This test covers the case where every remaining waiter has already
	// canceled. tryReturnConn must clean and wake those waiters, then report
	// that no handoff happened so the caller can keep the connection.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	elem := wl.nodes.Get().(*list.Element[waiter[*TestConn]])
	defer wl.nodes.Put(elem)
	elem.Value = waiter[*TestConn]{conn: elem.Value.conn, ctx: ctx}
	wl.list.PushBackValue(elem)

	conn := &Pooled[*TestConn]{
		Conn: &TestConn{
			counts: &TestState{},
		},
	}

	assert.False(t, wl.tryReturnConn(conn))
	assert.Zero(t, wl.waiting())
	assert.Nil(t, <-elem.Value.conn)
}
