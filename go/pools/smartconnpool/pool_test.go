/*
Copyright 2023 The Vitess Authors.

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
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	sFoo = &Setting{queryApply: "set foo=1"}
	sBar = &Setting{queryApply: "set bar=1"}
)

type TestState struct {
	lastID, open, close, reset atomic.Int64
	mu                         sync.Mutex
	waits                      []time.Time
	chaos                      struct {
		delayConnect time.Duration
		failConnect  atomic.Bool
		failApply    bool
	}
}

func (ts *TestState) LogWait(start time.Time) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.waits = append(ts.waits, start)
}

type TestConn struct {
	counts  *TestState
	onClose chan struct{}

	setting     *Setting
	num         int64
	timeCreated time.Time
	closed      bool
	failApply   bool
	onSetting   func()
}

func (tr *TestConn) waitForClose() chan struct{} {
	tr.onClose = make(chan struct{})
	return tr.onClose
}

func (tr *TestConn) IsClosed() bool {
	return tr.closed
}

func (tr *TestConn) Setting() *Setting {
	if tr.onSetting != nil {
		tr.onSetting()
	}
	return tr.setting
}

func (tr *TestConn) ResetSetting(ctx context.Context) error {
	tr.counts.reset.Add(1)
	tr.setting = nil
	return nil
}

func (tr *TestConn) ApplySetting(ctx context.Context, setting *Setting) error {
	if tr.failApply {
		return errors.New("ApplySetting failed")
	}
	tr.setting = setting
	return nil
}

func (tr *TestConn) Close() {
	if !tr.closed {
		if tr.onClose != nil {
			close(tr.onClose)
		}
		tr.counts.open.Add(-1)
		tr.counts.close.Add(1)
		tr.closed = true
	}
}

var _ Connection = (*TestConn)(nil)

func newConnector(state *TestState) Connector[*TestConn] {
	return func(ctx context.Context) (*TestConn, error) {
		state.open.Add(1)
		if state.chaos.delayConnect != 0 {
			time.Sleep(state.chaos.delayConnect)
		}
		if state.chaos.failConnect.Load() {
			return nil, errors.New("failed to connect: forced failure")
		}
		return &TestConn{
			num:         state.lastID.Add(1),
			timeCreated: time.Now(),
			counts:      state,
			failApply:   state.chaos.failApply,
		}, nil
	}
}

func TestOpen(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	var resources [10]*Pooled[*TestConn]
	var r *Pooled[*TestConn]
	var err error

	// Test Get
	for i := range 5 {
		if i%2 == 0 {
			r, err = p.Get(ctx, nil)
		} else {
			r, err = p.Get(ctx, sFoo)
		}
		require.NoError(t, err)
		resources[i] = r
		assert.EqualValues(t, 5-i-1, p.Available())
		assert.Zero(t, p.Metrics.WaitCount())
		assert.Zero(t, len(state.waits))
		assert.Zero(t, p.Metrics.WaitTime())
		assert.EqualValues(t, i+1, state.lastID.Load())
		assert.EqualValues(t, i+1, state.open.Load())
	}

	// Test that Get waits
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 5 {
			if i%2 == 0 {
				r, err = p.Get(ctx, nil)
			} else {
				r, err = p.Get(ctx, sFoo)
			}
			if !assert.NoError(t, err) {
				return
			}
			resources[i] = r
		}
		for i := range 5 {
			p.put(resources[i])
		}
	}()
	for i := range 5 {
		// block until we have a client wait for a connection, then offer it
		for p.wait.waiting() == 0 {
			time.Sleep(time.Millisecond)
		}
		p.put(resources[i])
	}
	<-done
	assert.EqualValues(t, 5, p.Metrics.WaitCount())
	assert.Equal(t, 5, len(state.waits))
	// verify start times are monotonic increasing
	for i := 1; i < len(state.waits); i++ {
		assert.False(t, state.waits[i].Before(state.waits[i-1]), "Expecting monotonic increasing start times")
	}
	assert.NotZero(t, p.Metrics.WaitTime())
	assert.EqualValues(t, 5, state.lastID.Load())
	// Test Close resource
	r, err = p.Get(ctx, nil)
	require.NoError(t, err)
	r.Close()
	// A nil Put should cause the resource to be reopened.
	p.put(nil)
	assert.EqualValues(t, 5, state.open.Load())
	assert.EqualValues(t, 6, state.lastID.Load())

	for i := range 5 {
		if i%2 == 0 {
			r, err = p.Get(ctx, nil)
		} else {
			r, err = p.Get(ctx, sFoo)
		}
		require.NoError(t, err)
		resources[i] = r
	}
	for i := range 5 {
		p.put(resources[i])
	}
	assert.EqualValues(t, 5, state.open.Load())
	assert.EqualValues(t, 6, state.lastID.Load())

	// SetCapacity
	err = p.SetCapacity(ctx, 3)
	require.NoError(t, err)
	assert.EqualValues(t, 3, state.open.Load())
	assert.EqualValues(t, 6, state.lastID.Load())
	assert.EqualValues(t, 3, p.Capacity())
	assert.EqualValues(t, 3, p.Available())

	err = p.SetCapacity(ctx, 6)
	require.NoError(t, err)
	assert.EqualValues(t, 6, p.Capacity())
	assert.EqualValues(t, 6, p.Available())

	for i := range 6 {
		if i%2 == 0 {
			r, err = p.Get(ctx, nil)
		} else {
			r, err = p.Get(ctx, sFoo)
		}
		require.NoError(t, err)
		resources[i] = r
	}
	for i := range 6 {
		p.put(resources[i])
	}
	assert.EqualValues(t, 6, state.open.Load())
	assert.EqualValues(t, 9, state.lastID.Load())

	// Close
	p.Close()
	assert.EqualValues(t, 0, p.Capacity())
	assert.EqualValues(t, 0, p.Available())
	assert.EqualValues(t, 0, state.open.Load())
}

func TestShrinking(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	var resources [10]*Pooled[*TestConn]
	// Leave one empty slot in the pool
	for i := range 4 {
		var r *Pooled[*TestConn]
		var err error
		if i%2 == 0 {
			r, err = p.Get(ctx, nil)
		} else {
			r, err = p.Get(ctx, sFoo)
		}
		require.NoError(t, err)
		resources[i] = r
	}
	done := make(chan bool)
	go func() {
		defer func() { done <- true }()
		err := p.SetCapacity(ctx, 3)
		if !assert.NoError(t, err) {
			return
		}
	}()
	expected := map[string]any{
		"Capacity":          3,
		"Available":         -1, // negative because we've borrowed past our capacity
		"Active":            4,
		"InUse":             4,
		"WaitCount":         0,
		"WaitTime":          time.Duration(0),
		"IdleTimeout":       1 * time.Second,
		"IdleClosed":        0,
		"MaxLifetimeClosed": 0,
	}
	for i := range 10 {
		time.Sleep(10 * time.Millisecond)
		stats := p.StatsJSON()
		if reflect.DeepEqual(expected, stats) {
			break
		}
		if i == 9 {
			assert.Equal(t, expected, stats)
		}
	}
	// There are already 2 resources available in the pool.
	// So, returning one should be enough for SetCapacity to complete.
	p.put(resources[3])
	<-done
	// Return the rest of the resources
	for i := range 3 {
		p.put(resources[i])
	}
	stats := p.StatsJSON()
	expected = map[string]any{
		"Capacity":          3,
		"Available":         3,
		"Active":            3,
		"InUse":             0,
		"WaitCount":         0,
		"WaitTime":          time.Duration(0),
		"IdleTimeout":       1 * time.Second,
		"IdleClosed":        0,
		"MaxLifetimeClosed": 0,
	}
	assert.Equal(t, expected, stats)
	assert.EqualValues(t, 3, state.open.Load())

	// Ensure no deadlock if SetCapacity is called after we start
	// waiting for a resource
	var err error
	for i := range 3 {
		var r *Pooled[*TestConn]
		if i%2 == 0 {
			r, err = p.Get(ctx, nil)
		} else {
			r, err = p.Get(ctx, sFoo)
		}
		require.NoError(t, err)
		resources[i] = r
	}
	// This will wait because pool is empty
	go func() {
		defer func() { done <- true }()
		r, err := p.Get(ctx, nil)
		if !assert.NoError(t, err) {
			return
		}
		p.put(r)
	}()

	// This will also wait
	go func() {
		defer func() { done <- true }()
		err := p.SetCapacity(ctx, 2)
		if !assert.NoError(t, err) {
			return
		}
	}()
	time.Sleep(10 * time.Millisecond)

	// This should not hang
	for i := range 3 {
		p.put(resources[i])
	}
	<-done
	<-done
	assert.EqualValues(t, 2, p.Capacity())
	assert.EqualValues(t, 2, p.Available())
	assert.EqualValues(t, 1, p.Metrics.WaitCount())
	assert.EqualValues(t, p.Metrics.WaitCount(), len(state.waits))
	assert.EqualValues(t, 2, state.open.Load())

	// Test race condition of SetCapacity with itself
	err = p.SetCapacity(ctx, 3)
	require.NoError(t, err)
	for i := range 3 {
		var r *Pooled[*TestConn]
		var err error
		if i%2 == 0 {
			r, err = p.Get(ctx, nil)
		} else {
			r, err = p.Get(ctx, sFoo)
		}
		require.NoError(t, err)
		resources[i] = r
	}
	// This will wait because pool is empty
	go func() {
		defer func() { done <- true }()
		r, err := p.Get(ctx, nil)
		if !assert.NoError(t, err) {
			return
		}
		p.put(r)
	}()
	time.Sleep(10 * time.Millisecond)

	// This will wait till we Put
	go func() {
		err := p.SetCapacity(ctx, 2)
		if !assert.NoError(t, err) {
			return
		}
	}()
	time.Sleep(10 * time.Millisecond)
	go func() {
		err := p.SetCapacity(ctx, 4)
		if !assert.NoError(t, err) {
			return
		}
	}()
	time.Sleep(10 * time.Millisecond)

	// This should not hang
	for i := range 3 {
		p.put(resources[i])
	}
	<-done

	assert.Panics(t, func() {
		_ = p.SetCapacity(ctx, -1)
	})

	assert.EqualValues(t, 4, p.Capacity())
	assert.EqualValues(t, 4, p.Available())
}

func TestClosing(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	var resources [10]*Pooled[*TestConn]
	for i := range 5 {
		var r *Pooled[*TestConn]
		var err error
		if i%2 == 0 {
			r, err = p.Get(ctx, nil)
		} else {
			r, err = p.Get(ctx, sFoo)
		}
		require.NoError(t, err)
		resources[i] = r
	}
	ch := make(chan bool)
	go func() {
		p.Close()
		ch <- true
	}()

	// Wait for goroutine to call Close
	time.Sleep(10 * time.Millisecond)
	stats := p.StatsJSON()
	expected := map[string]any{
		"Capacity":          0,
		"Available":         -5,
		"Active":            5,
		"InUse":             5,
		"WaitCount":         0,
		"WaitTime":          time.Duration(0),
		"IdleTimeout":       1 * time.Second,
		"IdleClosed":        0,
		"MaxLifetimeClosed": 0,
	}
	assert.Equal(t, expected, stats)

	// Put is allowed when closing
	for i := range 5 {
		p.put(resources[i])
	}

	// Wait for Close to return
	<-ch

	stats = p.StatsJSON()
	expected = map[string]any{
		"Capacity":          0,
		"Available":         0,
		"Active":            0,
		"InUse":             0,
		"WaitCount":         0,
		"WaitTime":          time.Duration(0),
		"IdleTimeout":       1 * time.Second,
		"IdleClosed":        0,
		"MaxLifetimeClosed": 0,
	}
	assert.Equal(t, expected, stats)
	assert.EqualValues(t, 5, state.lastID.Load())
	assert.EqualValues(t, 0, state.open.Load())
}

func TestReopen(t *testing.T) {
	var state TestState
	var refreshed atomic.Bool

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:        5,
		IdleTimeout:     time.Second,
		LogWait:         state.LogWait,
		RefreshInterval: 500 * time.Millisecond,
	}).Open(newConnector(&state), func() (bool, error) {
		refreshed.Store(true)
		return true, nil
	})
	t.Cleanup(p.Close)

	var resources [10]*Pooled[*TestConn]
	for i := range 5 {
		var r *Pooled[*TestConn]
		var err error
		if i%2 == 0 {
			r, err = p.Get(ctx, nil)
		} else {
			r, err = p.Get(ctx, sFoo)
		}
		require.NoError(t, err)
		resources[i] = r
	}

	time.Sleep(10 * time.Millisecond)
	stats := p.StatsJSON()
	expected := map[string]any{
		"Capacity":          5,
		"Available":         0,
		"Active":            5,
		"InUse":             5,
		"WaitCount":         0,
		"WaitTime":          time.Duration(0),
		"IdleTimeout":       1 * time.Second,
		"IdleClosed":        0,
		"MaxLifetimeClosed": 0,
	}
	assert.Equal(t, expected, stats)

	time.Sleep(1 * time.Second)
	assert.Truef(t, refreshed.Load(), "did not refresh")

	for i := range 5 {
		p.put(resources[i])
	}
	time.Sleep(50 * time.Millisecond)
	stats = p.StatsJSON()
	expected = map[string]any{
		"Capacity":          5,
		"Available":         5,
		"Active":            0,
		"InUse":             0,
		"WaitCount":         0,
		"WaitTime":          time.Duration(0),
		"IdleTimeout":       1 * time.Second,
		"IdleClosed":        0,
		"MaxLifetimeClosed": 0,
	}
	assert.Equal(t, expected, stats)
	assert.EqualValues(t, 5, state.lastID.Load())
	assert.EqualValues(t, 0, state.open.Load())
}

func TestRefreshWorkerContinuesAfterTriggeredReopen(t *testing.T) {
	var state TestState
	var refreshCount atomic.Int32

	// The refresh callback returns true exactly once to trigger a reopen. If
	// the worker exits after the trigger (the bug we're guarding against),
	// refreshCount stops at 1. With the worker still ticking we expect many
	// more calls within a reasonable window.
	p := NewPool(&Config[*TestConn]{
		Capacity:        2,
		RefreshInterval: 50 * time.Millisecond,
	}).Open(newConnector(&state), func() (bool, error) {
		count := refreshCount.Add(1)
		return count == 1, nil
	})
	t.Cleanup(p.Close)

	require.Eventually(t, func() bool {
		return refreshCount.Load() >= 3
	}, 30*time.Second, 50*time.Millisecond,
		"refresh worker stopped ticking after first triggered reopen")
}

func TestSetCapacityOnUnopenedPoolArmsCapacity(t *testing.T) {
	// SetCapacity is allowed before Open so callers can pre-configure the
	// pool. The value is just stored; Get still refuses until the pool is
	// opened (the close pointer is what gates lifecycle, not capacity).
	p := NewPool(&Config[*TestConn]{Capacity: 5})

	require.NoError(t, p.SetCapacity(t.Context(), 1))
	require.EqualValues(t, 1, p.Capacity())
	require.False(t, p.IsOpen())

	_, err := p.Get(t.Context(), nil)
	require.ErrorIs(t, err, ErrConnPoolClosed)
}

func TestTryReturnConnClosesOnClosedPool(t *testing.T) {
	// put() guards against Recycles that arrive after Close at the top,
	// but it only checks pool.close once. A Recycle that passes the check
	// then proceeds through tryReturnConn can race a CloseWithContext that
	// fires before the push lands — the conn would otherwise end up in an
	// idle stack of a closed pool (especially when SetCapacity has armed
	// idleCount > 0).
	//
	// This test exercises that race directly by invoking tryReturnConn on
	// a closed pool with a still-fresh conn — exactly the state a racing
	// Recycle would reach after put's initial close check.
	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 3}).Open(newConnector(&state), nil)

	held, err := p.Get(t.Context(), nil)
	require.NoError(t, err)

	// Close with a tight ctx so the close returns despite the held conn.
	closeCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_ = p.CloseWithContext(closeCtx)
	require.False(t, p.IsOpen())
	require.EqualValues(t, 1, p.Active())

	// Arm idleCount > 0 so closeOnIdleLimitReached would otherwise push
	// rather than close.
	require.NoError(t, p.SetCapacity(t.Context(), 3))
	require.EqualValues(t, 3, p.IdleCount())

	// Mimic put()'s borrowed-- and then enter tryReturnConn directly,
	// matching the state of a racing Recycle that passed put's close
	// check but reached tryReturnConn after close cleared pool.close.
	p.borrowed.Add(-1)
	p.tryReturnConn(held)

	require.EqualValues(t, 0, state.open.Load(), "tryReturnConn must close the conn when pool is closed")
	require.EqualValues(t, 0, p.Active())
	require.Nil(t, p.clean.Peek(), "no conn should remain in the stack of a closed pool")
}

func TestRecycleOnClosedPoolClosesConn(t *testing.T) {
	// On a closed pool, a conn returned via Recycle must be physically
	// closed — pushing it back onto an idle stack with no workers running
	// and Get refusing requests would leak the underlying DB connection.
	// The scenario surfaces specifically when SetCapacity has armed
	// capacity / idleCount > 0 after Close, which makes
	// closeOnIdleLimitReached decide to push rather than close.

	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 3}).Open(newConnector(&state), nil)

	held, err := p.Get(t.Context(), nil)
	require.NoError(t, err)

	// Close with a tight ctx so the close returns despite the held conn.
	closeCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_ = p.CloseWithContext(closeCtx)
	require.False(t, p.IsOpen())
	require.EqualValues(t, 1, p.Active())

	// Arm idleCount > 0 on the closed pool. This is what would make a
	// returning conn get pushed to the stack instead of closed.
	require.NoError(t, p.SetCapacity(t.Context(), 3))
	require.EqualValues(t, 3, p.IdleCount())

	held.Recycle()

	require.EqualValues(t, 0, state.open.Load(),
		"recycle on a closed pool must close the conn, not push it to the idle stack")
	require.EqualValues(t, 0, p.Active())
	require.Nil(t, p.clean.Peek(), "no conn should sit in the idle stack of a closed pool")
}

func TestSetCapacityOnClosedPoolDoesNotDrain(t *testing.T) {
	// SetCapacity on a closed pool must update the capacity field without
	// entering setCapacity's drain loop. If a prior CloseWithContext timed
	// out with conns still borrowed, the drain would otherwise block on
	// those conns — but the pool is no longer serving connections, so
	// draining is lifecycle teardown's job, not capacity configuration's.

	old := PoolCloseTimeout
	PoolCloseTimeout = 50 * time.Millisecond
	t.Cleanup(func() { PoolCloseTimeout = old })

	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 5}).Open(newConnector(&state), nil)

	var held []*Pooled[*TestConn]
	for range 3 {
		c, err := p.Get(t.Context(), nil)
		require.NoError(t, err)
		held = append(held, c)
	}

	// Close with a short timeout; the held conns keep active > 0 so close's
	// drain times out. The pool ends up logically closed but with active
	// slots still booked.
	p.Close()
	require.False(t, p.IsOpen())
	require.EqualValues(t, 3, p.Active(), "held conns still count as active after timed-out close")

	// SetCapacity(1) with active=3 would otherwise enter setCapacity's drain
	// loop and block until the held conns return (or the ctx fires). On a
	// closed pool we should just set the field and return.
	done := make(chan error, 1)
	go func() {
		done <- p.SetCapacity(t.Context(), 1)
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		require.Fail(t, "SetCapacity on closed pool blocked on drain loop")
	}

	require.EqualValues(t, 1, p.Capacity())

	// Release held conns so the test exits cleanly.
	for _, c := range held {
		c.Recycle()
	}
}

func TestSetCapacityAfterCloseArmsCapacity(t *testing.T) {
	// SetCapacity after Close is also allowed: the capacity field is config,
	// not lifecycle. Gets still return ErrConnPoolClosed because the close
	// pointer is nil.
	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 5}).Open(newConnector(&state), nil)
	p.Close()

	require.NoError(t, p.SetCapacity(t.Context(), 1))
	require.EqualValues(t, 1, p.Capacity())
	require.False(t, p.IsOpen())

	_, err := p.Get(t.Context(), nil)
	require.ErrorIs(t, err, ErrConnPoolClosed)
}

func TestCloseAfterSetCapacityZeroStopsPool(t *testing.T) {
	// SetCapacity(0) drains an open pool but does not close it; a subsequent
	// Close() must actually shut the pool down. The bug treats capacity==0
	// as "already closed" in CloseWithContext and returns immediately,
	// leaving workers running.
	var state TestState
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
	}).Open(newConnector(&state), nil)

	require.NoError(t, p.SetCapacity(t.Context(), 0))
	require.True(t, p.IsOpen(), "pool with capacity 0 should still be open")

	p.Close()
	require.False(t, p.IsOpen(), "pool should be closed after Close()")
}

func TestCloseWithExpiredCtxStillClosesIdleConns(t *testing.T) {
	// CloseWithContext calls setCapacity(ctx, 0) to drain the pool. If ctx
	// is already expired, the drain loop's ctx.Err() check used to bail out
	// before even popping idle conns from the stacks, leaving them
	// physically open. Closing those idle conns is unconditional work and
	// shouldn't depend on the caller's deadline — only waiting for borrowed
	// conns to return needs to honor ctx.
	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 3}).Open(newConnector(&state), nil)

	var conns []*Pooled[*TestConn]
	for range 3 {
		c, err := p.Get(t.Context(), nil)
		require.NoError(t, err)
		conns = append(conns, c)
	}
	for _, c := range conns {
		c.Recycle()
	}
	require.EqualValues(t, 3, state.open.Load())

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_ = p.CloseWithContext(ctx)

	require.EqualValues(t, 0, state.open.Load(),
		"idle conns must be physically closed even when the close ctx is already expired")
}

func TestGetAtZeroCapacityReturnsTimeout(t *testing.T) {
	// An open pool whose capacity has been drained to 0 is paused, not closed.
	// Get must distinguish these states: closed pools return ErrConnPoolClosed;
	// paused pools return ErrTimeout (the same signal callers see when nobody
	// returns a conn within the deadline).
	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 5}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	require.NoError(t, p.SetCapacity(t.Context(), 0))
	require.True(t, p.IsOpen())

	_, err := p.Get(t.Context(), nil)
	require.ErrorIs(t, err, ErrTimeout)
	require.NotErrorIs(t, err, ErrConnPoolClosed)
}

func TestSetCapacityZeroWakesExistingWaiters(t *testing.T) {
	// A waiter parked in the waitlist must be woken when capacity drops to 0,
	// otherwise it stays blocked until ctx fires. The wake-up should surface
	// as ErrTimeout because the pool is paused, not closed.
	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 1}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	held, err := p.Get(t.Context(), nil)
	require.NoError(t, err)

	waiterErr := make(chan error, 1)
	go func() {
		_, err := p.Get(t.Context(), nil)
		waiterErr <- err
	}()

	// Give the waiter time to enter the waitlist before draining.
	require.Eventually(t, func() bool {
		return p.wait.waiting() == 1
	}, time.Second, time.Millisecond)

	// SetCapacity(0) blocks until active drains; the held conn keeps it
	// blocked. We only care that the waiter wakes up — release the held conn
	// afterwards so SetCapacity can finish.
	setCapDone := make(chan struct{})
	go func() {
		defer close(setCapDone)
		_ = p.SetCapacity(t.Context(), 0)
	}()

	select {
	case err := <-waiterErr:
		require.ErrorIs(t, err, ErrTimeout)
		require.NotErrorIs(t, err, ErrConnPoolClosed)
	case <-time.After(2 * time.Second):
		require.Fail(t, "waiter was not woken when capacity dropped to 0")
	}

	held.Recycle()
	<-setCapDone
}

func TestSetCapacityResumesAfterZero(t *testing.T) {
	// SetCapacity(0) pauses the pool; SetCapacity to a positive value must
	// resume it. Gets should succeed again with no Close/Open dance required.
	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 3}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	require.NoError(t, p.SetCapacity(t.Context(), 0))

	_, err := p.Get(t.Context(), nil)
	require.ErrorIs(t, err, ErrTimeout, "paused pool should refuse Gets")

	require.NoError(t, p.SetCapacity(t.Context(), 2))
	require.EqualValues(t, 2, p.Capacity())

	conn, err := p.Get(t.Context(), nil)
	require.NoError(t, err)
	conn.Recycle()
}

func TestGetAfterCloseReturnsErrConnPoolClosed(t *testing.T) {
	// After Close(), the pool is no longer open and Get must surface that
	// explicitly so callers (which can recover from ErrTimeout) don't confuse
	// a terminal close with a transient pause.
	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 1}).Open(newConnector(&state), nil)
	p.Close()

	_, err := p.Get(t.Context(), nil)
	require.ErrorIs(t, err, ErrConnPoolClosed)
}

func TestWaitTimeRecordedOnTimeout(t *testing.T) {
	// A Get that enters the waitlist and times out should still count toward
	// WaitTime. Otherwise pool stress is invisible to monitoring exactly when
	// it matters most.
	var state TestState
	p := NewPool(&Config[*TestConn]{
		Capacity: 1,
		LogWait:  state.LogWait,
	}).Open(newConnector(&state), nil)

	held, err := p.Get(t.Context(), nil)
	require.NoError(t, err)

	// Cleanups run LIFO: Recycle first, then Close — keeps Close from waiting
	// the full PoolCloseTimeout for the held conn.
	t.Cleanup(p.Close)
	t.Cleanup(held.Recycle)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	_, err = p.Get(ctx, nil)
	require.ErrorIs(t, err, ErrTimeout)

	require.EqualValues(t, 1, p.Metrics.WaitCount(), "WaitCount should reflect the timed-out wait")
	require.Greater(t, p.Metrics.WaitTime(), time.Duration(0), "WaitTime should reflect the timed-out wait")
}

func TestWaitForConnHonorsCtxOnRetryPath(t *testing.T) {
	// waitForConn's shouldRetry path removes the waiter and returns (nil,
	// nil) so the outer get loop can re-evaluate state changes that
	// happened during enqueue. If ctx has been canceled in the meantime,
	// the retry path would otherwise pop a conn and hand it to a caller
	// whose ctx is dead — the get loop doesn't re-check ctx per iteration.
	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 1}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	held, err := p.Get(t.Context(), nil)
	require.NoError(t, err)

	// Replace onWait with a gate: hold the waiter just before push so we
	// can stage the race deterministically — recycle a conn (so the
	// stack is non-empty, making shouldRetry fire) and cancel ctx before
	// the waiter actually enqueues.
	blocked := make(chan struct{})
	unblock := make(chan struct{})
	p.wait.onWait = func() {
		close(blocked)
		<-unblock
	}

	ctx, cancel := context.WithCancel(t.Context())

	type result struct {
		conn *Pooled[*TestConn]
		err  error
	}
	done := make(chan result, 1)
	go func() {
		c, err := p.Get(ctx, nil)
		done <- result{c, err}
	}()

	select {
	case <-blocked:
	case <-time.After(time.Second):
		require.Fail(t, "waiter never reached onWait")
	}

	held.Recycle() // populates the clean stack so shouldRetryWait fires
	cancel()
	close(unblock)

	res := <-done
	if res.conn != nil {
		res.conn.Recycle()
	}
	require.ErrorIs(t, res.err, ErrTimeout, "Get must honor canceled ctx via the retry path")
	require.Nil(t, res.conn)
}

func TestReopenRetiresStaleConnections(t *testing.T) {
	// reopen() is invoked when the pool needs to retire all open connections
	// (e.g., after a DNS change). A borrowed conn outlives reopen by definition;
	// the test confirms that the conn is retired (not pushed back into the
	// pool) when its holder eventually recycles it.
	var state TestState
	p := NewPool(&Config[*TestConn]{
		Capacity: 1,
	}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	held, err := p.Get(t.Context(), nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, state.open.Load())

	// Trigger a reopen synchronously. The held conn survives in the user's
	// hands; the gen check on Recycle retires it.
	p.reopen()

	require.EqualValues(t, 1, p.Capacity())
	require.EqualValues(t, 1, p.Active())
	require.EqualValues(t, 1, state.open.Load(), "stale conn is still in flight after reopen")

	// Recycle the now-stale conn. It must not return to the pool — the whole
	// point of the reopen was to retire conns of this generation.
	held.Recycle()

	require.EqualValues(t, 0, state.open.Load(), "stale conn should be closed when returned")
	require.EqualValues(t, 0, p.Active(), "stale conn slot should be released")
}

func TestReopenViaCloseAndOpenRetiresStaleConnections(t *testing.T) {
	// Same generation invariant as TestReopenRetiresStaleConnections, but the
	// reset happens via Close+Open instead of reopen(). A conn that survived
	// a timed-out Close must not return to the re-opened pool.

	old := PoolCloseTimeout
	PoolCloseTimeout = 100 * time.Millisecond
	t.Cleanup(func() { PoolCloseTimeout = old })

	var state TestState
	p := NewPool(&Config[*TestConn]{
		Capacity: 1,
	}).Open(newConnector(&state), nil)

	held, err := p.Get(t.Context(), nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, state.open.Load())

	// Close times out because the held conn isn't returned; it survives in
	// the user's hands while the pool transitions through closed back to open.
	p.Close()
	p.Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	held.Recycle()

	require.EqualValues(t, 0, state.open.Load(), "stale conn should be closed when returned")
}

func TestCloseHonorsContextDuringIdleReopen(t *testing.T) {
	// The idle worker's reopen path used context.Background, so a slow MySQL
	// connect would pin workers.Wait() inside CloseWithContext, no matter how
	// tight the caller's deadline was. The fix derives the connect context
	// from the pool's close channel so in-flight reconnects abort on close.

	var state TestState
	var connectCount atomic.Int32

	reopenStartedCh := make(chan struct{})
	var reopenStartedOnce sync.Once
	blockConnect := make(chan struct{})
	var blockOnce sync.Once
	unblockConnect := func() { blockOnce.Do(func() { close(blockConnect) }) }
	defer unblockConnect()

	connector := func(ctx context.Context) (*TestConn, error) {
		// First call is the seed; succeed quickly so we have an idle conn to
		// expire.
		if connectCount.Add(1) == 1 {
			state.open.Add(1)
			return &TestConn{counts: &state, num: state.lastID.Add(1)}, nil
		}
		// Subsequent calls are the idle worker's reopen. Block until our ctx
		// is cancelled (the fix) or the test unblocks us as a fallback.
		reopenStartedOnce.Do(func() { close(reopenStartedCh) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-blockConnect:
			return nil, errors.New("test fallback unblock")
		}
	}

	p := NewPool(&Config[*TestConn]{
		Capacity:    1,
		IdleTimeout: 50 * time.Millisecond,
	}).Open(connector, nil)

	seed, err := p.Get(t.Context(), nil)
	require.NoError(t, err)
	seed.Recycle()

	// Wait for the idle worker to begin its (blocked) reopen.
	select {
	case <-reopenStartedCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "idle worker did not start its reopen within 5s")
	}

	closeCtx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	closeDone := make(chan time.Duration, 1)
	start := time.Now()
	go func() {
		_ = p.CloseWithContext(closeCtx)
		closeDone <- time.Since(start)
	}()

	var elapsed time.Duration
	select {
	case elapsed = <-closeDone:
	case <-time.After(3 * time.Second):
		// Close is stuck on the worker's reconnect; release it so the test
		// can shut down cleanly, then fail.
		unblockConnect()
		elapsed = <-closeDone
		require.Failf(t, "close did not return", "took %s", elapsed)
	}

	require.Less(t, elapsed, 2*time.Second,
		"close should abort the worker's reconnect on ctx; took %s", elapsed)
}

func TestTryReturnConnRejectsStaleGeneration(t *testing.T) {
	// tryReturnConn is the single ownership gate for stale-generation conns.
	// put's initial check catches the common case; this test exercises the
	// secondary check that catches a reopen racing between put's first check
	// and the actual handoff.
	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 1}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	held, err := p.Get(t.Context(), nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, state.open.Load())
	require.EqualValues(t, 1, p.Active())

	// Simulate a reopen that races with put() by bumping generation directly.
	p.generation.Add(1)

	// Mirror put()'s borrowed-- step, then hand the now-stale conn straight
	// to tryReturnConn.
	p.borrowed.Add(-1)
	returned := p.tryReturnConn(held)

	require.False(t, returned, "tryReturnConn should refuse a stale conn")
	require.EqualValues(t, 0, state.open.Load(), "tryReturnConn must close the stale conn itself")
	require.EqualValues(t, 0, p.Active(), "tryReturnConn must release the slot")
}

func TestReopenPreservesCapacity(t *testing.T) {
	// reopen() retires the pool's connections without temporarily setting
	// capacity to 0. Capacity must be observable as the original value
	// before and after, and idle conns must be physically closed.
	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 5}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	var seeds []*Pooled[*TestConn]
	for range 5 {
		c, err := p.Get(t.Context(), nil)
		require.NoError(t, err)
		seeds = append(seeds, c)
	}
	for _, c := range seeds {
		c.Recycle()
	}

	require.EqualValues(t, 5, p.Capacity())
	require.EqualValues(t, 5, p.Active())
	require.EqualValues(t, 5, state.open.Load())

	p.reopen()

	require.EqualValues(t, 5, p.Capacity(), "reopen must not change capacity")
	require.EqualValues(t, 0, p.Active(), "stale idle conn slots must be released")
	require.EqualValues(t, 0, state.open.Load(), "old idle conns must be physically closed")
}

func TestReopenPreservesFreshGenConnsInStack(t *testing.T) {
	// reopen() bumps the generation then sweeps the stacks. A fresh-gen conn
	// can land on a stack between the bump and the sweep — e.g.
	// closeIdleResources's connReopen captures the new generation, succeeds,
	// then calls tryReturnConn which pushes the new conn onto the stack.
	// reopen's sweep must not drop those: they're valid post-reopen conns,
	// and dropping them leaks an active slot.
	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 2}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	require.EqualValues(t, 1, p.generation.Load())

	// Craft a conn whose generation matches what pool.generation will be
	// after the upcoming reopen's bump (1 -> 2). This simulates a fresh
	// conn pushed onto the stack between reopen's bump and its sweep.
	fresh, err := p.connNew(t.Context())
	require.NoError(t, err)
	p.active.Add(1)
	fresh.generation = 2
	p.clean.Push(fresh)

	require.EqualValues(t, 1, state.open.Load())
	require.EqualValues(t, 1, p.Active())

	p.reopen()

	require.EqualValues(t, 1, state.open.Load(), "fresh-gen conn must not be closed by reopen")
	require.EqualValues(t, 1, p.Active(), "fresh-gen conn's active slot must be preserved")
	require.NotNil(t, p.clean.Peek(), "fresh-gen conn must remain reachable in the stack")
}

func TestReopenViaPopClosesStaleStackedConns(t *testing.T) {
	// After reopen bumps the generation, idle conns left in the stacks are
	// stale. A racing Get's pop() must close them rather than surface them to
	// the caller. This exercises the generation guard on the acquisition path
	// that lets reopen safely "bump gen first, sweep stacks after".
	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 2}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	var seeds []*Pooled[*TestConn]
	for range 2 {
		c, err := p.Get(t.Context(), nil)
		require.NoError(t, err)
		seeds = append(seeds, c)
	}
	initialIDs := map[int64]bool{seeds[0].Conn.num: true, seeds[1].Conn.num: true}
	for _, c := range seeds {
		c.Recycle()
	}
	require.EqualValues(t, 2, state.open.Load())

	// Simulate the post-gen-bump pre-drain state: bump generation directly so
	// the stacked conns are now stale. A racing Get's pop must close them.
	p.generation.Add(1)

	conn := p.pop(&p.clean)
	require.Nil(t, conn, "pop must close stale-gen conns and return nil for an empty fresh stack")
	require.EqualValues(t, 0, p.Active(), "stale conns must release their active slot")
	require.EqualValues(t, 0, state.open.Load(), "stale conns must be physically closed")

	fresh, err := p.Get(t.Context(), nil)
	require.NoError(t, err)
	require.False(t, initialIDs[fresh.Conn.num], "Get after reopen must open a fresh conn")
	fresh.Recycle()
}

func TestReopenDoesNotWakeWaiters(t *testing.T) {
	// reopen() must not perturb capacity, so parked waiters should not be
	// artificially woken. The previous implementation called setCapacity(0)
	// + setCapacity(original), which woke all waiters via the cap-zero
	// notification loop.
	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 1}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	held, err := p.Get(t.Context(), nil)
	require.NoError(t, err)
	defer held.Recycle()

	waiterReturned := make(chan struct{})
	go func() {
		_, _ = p.Get(t.Context(), nil)
		close(waiterReturned)
	}()

	require.Eventually(t, func() bool {
		return p.wait.waiting() == 1
	}, time.Second, time.Millisecond)

	p.reopen()

	select {
	case <-waiterReturned:
		require.Fail(t, "reopen must not wake parked waiters")
	case <-time.After(100 * time.Millisecond):
		// Expected: waiter still parked.
	}
}

func TestSetCapacityUpdatesIdleCountBeforeDraining(t *testing.T) {
	// setIdleCount must run synchronously with the capacity Swap, not as a
	// deferred trailer that fires only when setCapacity returns. Otherwise a
	// borrowed conn returning during the drain loop sees the OLD idleCount in
	// closeOnIdleLimitReached, gets pushed back to the stack instead of being
	// closed, and — if drain times out — strands there with the pre-reopen
	// generation. A later Get's fast-path pop then hands a stale-gen conn to
	// the caller.

	var state TestState
	p := NewPool(&Config[*TestConn]{Capacity: 3}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	require.EqualValues(t, 3, p.IdleCount())

	// Hold every conn so SetCapacity(0) blocks in its drain loop.
	var held []*Pooled[*TestConn]
	for range 3 {
		c, err := p.Get(t.Context(), nil)
		require.NoError(t, err)
		held = append(held, c)
	}

	setCapacityDone := make(chan struct{})
	go func() {
		defer close(setCapacityDone)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_ = p.SetCapacity(ctx, 0)
	}()

	// Wait until SetCapacity has swapped capacity to 0 (drain phase started).
	require.Eventually(t, func() bool {
		return p.Capacity() == 0
	}, time.Second, time.Millisecond)

	// idleCount must already reflect the new capacity while SetCapacity is
	// still in its drain loop. The held conns keep SetCapacity blocked, so
	// any non-synchronous setIdleCount would still observe the old value.
	require.Eventually(t, func() bool {
		return p.IdleCount() == 0
	}, time.Second, time.Millisecond,
		"idleCount must be updated synchronously with capacity; otherwise a returning conn during the drain can be pushed back to the stack")

	for _, c := range held {
		c.Recycle()
	}
	<-setCapacityDone
}

func TestUserClosing(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	var resources [5]*Pooled[*TestConn]
	for i := range 5 {
		var err error
		resources[i], err = p.Get(ctx, nil)
		require.NoError(t, err)
	}

	for _, r := range resources[:4] {
		r.Recycle()
	}

	ch := make(chan error)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
		defer cancel()

		err := p.CloseWithContext(ctx)
		ch <- err
		close(ch)
	}()

	select {
	case <-time.After(5 * time.Second):
		require.Fail(t, "Pool did not shutdown after 5s")
	case err := <-ch:
		require.Error(t, err)
		t.Logf("Shutdown error: %v", err)
	}
}

func TestConnReopen(t *testing.T) {
	var state TestState

	p := NewPool(&Config[*TestConn]{
		Capacity:    1,
		IdleTimeout: 200 * time.Millisecond,
		MaxLifetime: 10 * time.Millisecond,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	defer p.Close()

	conn, err := p.Get(t.Context(), nil)
	require.NoError(t, err)
	assert.EqualValues(t, 1, state.lastID.Load())
	assert.EqualValues(t, 1, p.Active())

	// wait enough to reach maxlifetime.
	time.Sleep(50 * time.Millisecond)

	p.put(conn)
	assert.EqualValues(t, 2, state.lastID.Load())
	assert.EqualValues(t, 1, p.Active())

	// wait enough to reach idle timeout.
	time.Sleep(300 * time.Millisecond)
	assert.GreaterOrEqual(t, state.lastID.Load(), int64(3))
	assert.EqualValues(t, 1, p.Active())
	assert.GreaterOrEqual(t, p.Metrics.IdleClosed(), int64(1))

	// mark connect to fail
	state.chaos.failConnect.Store(true)
	// wait enough to reach idle timeout and connect to fail.
	time.Sleep(300 * time.Millisecond)
	// no active connection should be left.
	assert.Zero(t, p.Active())
}

func TestIdleTimeout(t *testing.T) {
	testTimeout := func(t *testing.T, setting *Setting) {
		var state TestState

		ctx := t.Context()
		p := NewPool(&Config[*TestConn]{
			Capacity:    5,
			IdleTimeout: 10 * time.Millisecond,
			LogWait:     state.LogWait,
		}).Open(newConnector(&state), nil)

		defer p.Close()

		var conns []*Pooled[*TestConn]
		for i := range 5 {
			r, err := p.Get(ctx, setting)
			require.NoError(t, err)
			assert.EqualValues(t, i+1, state.open.Load())
			assert.EqualValues(t, 0, p.Metrics.IdleClosed())

			conns = append(conns, r)
		}
		assert.GreaterOrEqual(t, state.open.Load(), int64(5))

		// wait a long while; ensure that none of the conns have been closed
		time.Sleep(1 * time.Second)

		var closers []chan struct{}
		for _, conn := range conns {
			assert.Falsef(t, conn.Conn.IsClosed(), "connection was idle-closed while outside the pool")
			closers = append(closers, conn.Conn.waitForClose())
			p.put(conn)
		}

		time.Sleep(1 * time.Second)

		for _, closed := range closers {
			select {
			case <-closed:
			default:
				require.Fail(t, "Connections remain open after 1 second")
			}
		}
		// At least 5 connections should have been closed by now.
		assert.GreaterOrEqual(t, p.Metrics.IdleClosed(), int64(5), "At least 5 connections should have been closed by now.")

		// At any point, at least 4 connections should be open, with 1 either in the process of opening or already opened.
		// The idle connection closer shuts down one connection at a time.
		assert.GreaterOrEqual(t, state.open.Load(), int64(4))

		// The number of available connections in the pool should remain at 5.
		assert.EqualValues(t, 5, p.Available())
	}

	t.Run("WithoutSettings", func(t *testing.T) { testTimeout(t, nil) })
	t.Run("WithSettings", func(t *testing.T) { testTimeout(t, sFoo) })
}

func TestIdleTimeoutCreateFail(t *testing.T) {
	var state TestState
	connector := newConnector(&state)

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:    1,
		IdleTimeout: 10 * time.Millisecond,
		LogWait:     state.LogWait,
	}).Open(connector, nil)

	defer p.Close()

	for _, setting := range []*Setting{nil, sFoo} {
		r, err := p.Get(ctx, setting)
		require.NoError(t, err)
		// Change the factory before putting back
		// to prevent race with the idle closer, who will
		// try to use it.
		state.chaos.failConnect.Store(true)
		p.put(r)
		timeout := time.After(1 * time.Second)
		for p.Active() != 0 {
			select {
			case <-timeout:
				assert.Fail(t, "Timed out waiting for resource to be closed by idle timeout")
			default:
			}
		}
		// reset factory for next run.
		state.chaos.failConnect.Store(false)
	}
}

func TestMaxLifetime(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:    1,
		IdleTimeout: 10 * time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	r, err := p.Get(ctx, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 1, state.open.Load())
	assert.EqualValues(t, 0, p.Metrics.MaxLifetimeClosed())

	time.Sleep(10 * time.Millisecond)

	p.put(r)
	assert.EqualValues(t, 1, state.lastID.Load())
	assert.EqualValues(t, 1, state.open.Load())
	assert.EqualValues(t, 0, p.Metrics.MaxLifetimeClosed())

	p.Close()

	// maxLifetime > 0
	state.lastID.Store(0)
	state.open.Store(0)

	p = NewPool(&Config[*TestConn]{
		Capacity:    1,
		IdleTimeout: 10 * time.Second,
		MaxLifetime: 10 * time.Millisecond,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	r, err = p.Get(ctx, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 1, state.open.Load())
	assert.EqualValues(t, 0, p.Metrics.MaxLifetimeClosed())

	time.Sleep(5 * time.Millisecond)

	p.put(r)
	assert.EqualValues(t, 1, state.lastID.Load())
	assert.EqualValues(t, 1, state.open.Load())
	assert.EqualValues(t, 0, p.Metrics.MaxLifetimeClosed())

	r, err = p.Get(ctx, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 1, state.open.Load())
	assert.EqualValues(t, 0, p.Metrics.MaxLifetimeClosed())

	time.Sleep(10 * time.Millisecond * 2)

	p.put(r)
	assert.EqualValues(t, 2, state.lastID.Load())
	assert.EqualValues(t, 1, state.open.Load())
	assert.EqualValues(t, 1, p.Metrics.MaxLifetimeClosed())
}

func TestExtendedLifetimeTimeout(t *testing.T) {
	var state TestState
	connector := newConnector(&state)
	config := &Config[*TestConn]{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxLifetime: 0,
		LogWait:     state.LogWait,
	}

	// maxLifetime 0
	p := NewPool(config).Open(connector, nil)
	assert.Zero(t, p.extendedMaxLifetime())
	p.Close()

	// maxLifetime > 0
	config.MaxLifetime = 10 * time.Millisecond
	for range 10 {
		p = NewPool(config).Open(connector, nil)
		assert.LessOrEqual(t, config.MaxLifetime, p.extendedMaxLifetime())
		assert.Greater(t, 2*config.MaxLifetime, p.extendedMaxLifetime())
		p.Close()
	}
}

func TestExtendedMaxLifetimeJitter(t *testing.T) {
	var state TestState
	connector := newConnector(&state)

	// 30 minutes is well above 2^32 ns (~4.29s). A uint32(maxLifetime) cast
	// would cap jitter at ~408ms, so no sample could ever reach the upper half
	// of [M, 2M). A single sample above M + M/2 proves the full 64-bit range
	// is in use; the loop bound just guards against the ~5e-20 chance that all
	// uniform draws land in the lower half.
	config := &Config[*TestConn]{
		Capacity:    1,
		MaxLifetime: 30 * time.Minute,
		LogWait:     state.LogWait,
	}

	p := NewPool(config).Open(connector, nil)
	t.Cleanup(p.Close)

	threshold := config.MaxLifetime + config.MaxLifetime/2
	const maxAttempts = 64

	for range maxAttempts {
		s := p.extendedMaxLifetime()
		require.LessOrEqual(t, config.MaxLifetime, s)
		require.Greater(t, 2*config.MaxLifetime, s)

		if s > threshold {
			return
		}
	}

	require.Failf(t, "jitter never reached upper half of range",
		"no sample in %d tries exceeded %s; jitter appears truncated", maxAttempts, threshold)
}

func TestExtendedMaxLifetimeNegativeDisables(t *testing.T) {
	// MaxLifetime is a time.Duration sourced from user config. A negative
	// value is a misconfiguration, but it must not panic the process —
	// rand.Int64N panics on non-positive bounds. Treat it like the zero
	// case: lifetime tracking disabled.
	var state TestState
	p := NewPool(&Config[*TestConn]{
		Capacity:    1,
		MaxLifetime: -1 * time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	require.EqualValues(t, 0, p.extendedMaxLifetime())
}

// TestMaxIdleCount tests the MaxIdleCount setting, to check if the pool closes
// the idle connections when the number of idle connections exceeds the limit.
func TestMaxIdleCount(t *testing.T) {
	testMaxIdleCount := func(t *testing.T, setting *Setting, maxIdleCount int64, expClosedConn int) {
		var state TestState

		ctx := t.Context()
		p := NewPool(&Config[*TestConn]{
			Capacity:     5,
			MaxIdleCount: maxIdleCount,
			LogWait:      state.LogWait,
		}).Open(newConnector(&state), nil)

		defer p.Close()

		var conns []*Pooled[*TestConn]
		for i := range 5 {
			r, err := p.Get(ctx, setting)
			require.NoError(t, err)
			assert.EqualValues(t, i+1, state.open.Load())
			assert.EqualValues(t, 0, p.Metrics.IdleClosed())

			conns = append(conns, r)
		}

		for _, conn := range conns {
			p.put(conn)
		}

		closedConn := 0
		for _, conn := range conns {
			if conn.Conn.IsClosed() {
				closedConn++
			}
		}
		assert.EqualValues(t, expClosedConn, closedConn)
		assert.EqualValues(t, expClosedConn, p.Metrics.IdleClosed())
	}

	t.Run("WithoutSettings", func(t *testing.T) { testMaxIdleCount(t, nil, 2, 3) })
	t.Run("WithSettings", func(t *testing.T) { testMaxIdleCount(t, sFoo, 2, 3) })
	t.Run("WithoutSettings-MaxIdleCount-Zero", func(t *testing.T) { testMaxIdleCount(t, nil, 0, 0) })
	t.Run("WithSettings-MaxIdleCount-Zero", func(t *testing.T) { testMaxIdleCount(t, sFoo, 0, 0) })
}

func TestCreateFail(t *testing.T) {
	var state TestState
	state.chaos.failConnect.Store(true)

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	for _, setting := range []*Setting{nil, sFoo} {
		_, err := p.Get(ctx, setting)
		require.EqualError(t, err, "failed to connect: forced failure")
		stats := p.StatsJSON()
		expected := map[string]any{
			"Capacity":          5,
			"Available":         5,
			"Active":            0,
			"InUse":             0,
			"WaitCount":         0,
			"WaitTime":          time.Duration(0),
			"IdleTimeout":       1 * time.Second,
			"IdleClosed":        0,
			"MaxLifetimeClosed": 0,
		}
		assert.Equal(t, expected, stats)
	}
}

func TestCreateFailOnPut(t *testing.T) {
	var state TestState
	connector := newConnector(&state)

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(connector, nil)

	defer p.Close()

	for _, setting := range []*Setting{nil, sFoo} {
		_, err := p.Get(ctx, setting)
		require.NoError(t, err)

		// change factory to fail the put.
		state.chaos.failConnect.Store(true)
		p.put(nil)
		assert.Zero(t, p.Active())

		// change back for next iteration.
		state.chaos.failConnect.Store(false)
	}
}

func TestSlowCreateFail(t *testing.T) {
	var state TestState
	state.chaos.delayConnect = 10 * time.Millisecond

	ctx := t.Context()
	ch := make(chan *Pooled[*TestConn])

	for _, setting := range []*Setting{nil, sFoo} {
		p := NewPool(&Config[*TestConn]{
			Capacity:    2,
			IdleTimeout: time.Second,
			LogWait:     state.LogWait,
		}).Open(newConnector(&state), nil)

		state.chaos.failConnect.Store(true)

		for range 3 {
			go func() {
				conn, _ := p.Get(ctx, setting)
				ch <- conn
			}()
		}
		assert.Nil(t, <-ch)
		assert.Nil(t, <-ch)
		assert.Equalf(t, p.Capacity(), int64(2), "pool should not be out of capacity")
		assert.Equalf(t, p.Available(), int64(2), "pool should not be out of availability")

		select {
		case <-ch:
			assert.Fail(t, "there should be no capacity for a third connection")
		default:
		}

		state.chaos.failConnect.Store(false)
		conn, err := p.Get(ctx, setting)
		require.NoError(t, err)

		p.put(conn)
		conn = <-ch
		assert.NotNil(t, conn)
		p.put(conn)
		p.Close()
	}
}

func TestWaiterRetriesWhenCapacityFreedByFailedReplacement(t *testing.T) {
	var state TestState
	var connectAttempts atomic.Int64

	connector := func(ctx context.Context) (*TestConn, error) {
		attempt := connectAttempts.Add(1)
		if attempt == 2 {
			return nil, errors.New("forced replacement failure")
		}
		return &TestConn{
			num:    attempt,
			counts: &state,
		}, nil
	}

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity: 1,
		LogWait:  state.LogWait,
	}).Open(connector, nil)
	defer p.Close()

	conn, err := p.Get(ctx, nil)
	require.NoError(t, err)

	getCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	type getResult struct {
		conn *Pooled[*TestConn]
		err  error
	}
	results := make(chan getResult, 1)
	go func() {
		conn, err := p.Get(getCtx, nil)
		results <- getResult{conn: conn, err: err}
	}()

	require.Eventually(t, func() bool {
		return p.wait.waiting() == 1
	}, 5*time.Second, 10*time.Millisecond)

	conn.Taint()
	require.GreaterOrEqual(t, connectAttempts.Load(), int64(2))
	require.EqualValues(t, 1, p.Capacity())

	var result getResult
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		select {
		case result = <-results:
			assert.NoError(c, result.err)
			assert.NotNil(c, result.conn)
		default:
			assert.Fail(c, "waiting Get did not retry after replacement failure freed capacity")
		}
	}, 5*time.Second, 10*time.Millisecond)

	result.conn.Recycle()
}

func TestWaiterRetriesWhenCapacityIncreases(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity: 1,
		LogWait:  state.LogWait,
	}).Open(newConnector(&state), nil)

	conn, err := p.Get(ctx, nil)
	require.NoError(t, err)

	getCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	type getResult struct {
		conn *Pooled[*TestConn]
		err  error
	}
	results := make(chan getResult, 1)
	go func() {
		conn, err := p.Get(getCtx, nil)
		results <- getResult{conn: conn, err: err}
	}()

	defer func() {
		cancel()
		if conn != nil {
			conn.Recycle()
		}
		select {
		case result := <-results:
			if result.conn != nil {
				result.conn.Recycle()
			}
		default:
		}

		closeCtx, closeCancel := context.WithTimeout(ctx, PoolCloseTimeout)
		defer closeCancel()
		require.NoError(t, p.CloseWithContext(closeCtx))
	}()

	require.Eventually(t, func() bool {
		return p.wait.waiting() == 1
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, p.SetCapacity(ctx, 2))
	require.EqualValues(t, 2, p.Capacity())

	var result getResult
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		select {
		case result = <-results:
			assert.NoError(c, result.err)
			assert.NotNil(c, result.conn)
		default:
			assert.Fail(c, "waiting Get did not retry after capacity increased")
		}
	}, 5*time.Second, 10*time.Millisecond)

	result.conn.Recycle()
	conn.Recycle()
	result.conn = nil
	conn = nil
}

func TestGetRetriesWhenConnectionReturnedBeforeWaiterEnqueues(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity: 1,
		LogWait:  state.LogWait,
	})
	p.config.connect = newConnector(&state)
	p.capacity.Store(1)
	p.setIdleCount()

	closeChan := make(chan struct{})
	p.close.Store(&closeChan)

	var (
		conn   *Pooled[*TestConn]
		result struct {
			conn *Pooled[*TestConn]
			err  error
		}
		allowWaiterToEnqueue sync.Once
		waiterReadyOnce      sync.Once
	)
	releaseWaiter := make(chan struct{})
	defer func() {
		allowWaiterToEnqueue.Do(func() {
			close(releaseWaiter)
		})
		if conn != nil {
			conn.Recycle()
		}
		if result.conn != nil {
			result.conn.Recycle()
		}

		closeCtx, cancel := context.WithTimeout(ctx, PoolCloseTimeout)
		defer cancel()
		require.NoError(t, p.CloseWithContext(closeCtx))
	}()

	var err error
	conn, err = p.Get(ctx, nil)
	require.NoError(t, err)

	waiterReady := make(chan struct{})
	oldOnWait := p.wait.onWait
	p.wait.onWait = func() {
		if oldOnWait != nil {
			oldOnWait()
		}
		waiterReadyOnce.Do(func() {
			close(waiterReady)
		})
		<-releaseWaiter
	}

	getCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	results := make(chan struct {
		conn *Pooled[*TestConn]
		err  error
	}, 1)
	go func() {
		conn, err := p.Get(getCtx, nil)
		results <- struct {
			conn *Pooled[*TestConn]
			err  error
		}{conn: conn, err: err}
	}()

	require.Eventually(t, func() bool {
		select {
		case <-waiterReady:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)

	conn.Recycle()
	conn = nil
	allowWaiterToEnqueue.Do(func() {
		close(releaseWaiter)
	})

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		select {
		case result = <-results:
			assert.NoError(c, result.err)
			assert.NotNil(c, result.conn)
		default:
			assert.Fail(c, "waiting Get did not retry after connection was returned before enqueue")
		}
	}, 5*time.Second, 10*time.Millisecond)
}

func TestGetRetriesWhenConnectionReturnedAfterWaiterEnqueues(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity: 1,
		LogWait:  state.LogWait,
	})
	p.config.connect = newConnector(&state)
	p.capacity.Store(1)
	p.setIdleCount()

	closeChan := make(chan struct{})
	p.close.Store(&closeChan)

	var result struct {
		conn *Pooled[*TestConn]
		err  error
	}
	getCtx, cancelGet := context.WithCancel(ctx)
	releaseReturn := make(chan struct{})
	releaseReturnOnce := sync.Once{}
	defer func() {
		cancelGet()
		releaseReturnOnce.Do(func() {
			close(releaseReturn)
		})
		if result.conn != nil {
			result.conn.Recycle()
		}

		closeCtx, cancelClose := context.WithTimeout(ctx, PoolCloseTimeout)
		defer cancelClose()
		require.NoError(t, p.CloseWithContext(closeCtx))
	}()

	conn, err := p.Get(ctx, nil)
	require.NoError(t, err)

	returnReady := make(chan struct{})
	var returnReadyOnce sync.Once
	conn.Conn.onSetting = func() {
		returnReadyOnce.Do(func() {
			close(returnReady)
			<-releaseReturn
		})
	}

	recycleDone := make(chan struct{})
	go func() {
		defer close(recycleDone)
		conn.Recycle()
	}()

	require.Eventually(t, func() bool {
		select {
		case <-returnReady:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)

	results := make(chan struct {
		conn *Pooled[*TestConn]
		err  error
	}, 1)
	go func() {
		conn, err := p.Get(getCtx, nil)
		results <- struct {
			conn *Pooled[*TestConn]
			err  error
		}{conn: conn, err: err}
	}()

	require.Eventually(t, func() bool {
		return p.wait.waiting() == 1
	}, 5*time.Second, 10*time.Millisecond)

	releaseReturnOnce.Do(func() {
		close(releaseReturn)
	})

	require.Eventually(t, func() bool {
		select {
		case <-recycleDone:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		select {
		case result = <-results:
			assert.NoError(c, result.err)
			assert.NotNil(c, result.conn)
		default:
			assert.Fail(c, "waiting Get did not retry after connection was returned after enqueue")
		}
	}, 5*time.Second, 10*time.Millisecond)
}

func TestTimeout(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:    1,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	defer p.Close()

	// take the only connection available
	r, err := p.Get(ctx, nil)
	require.NoError(t, err)

	for _, setting := range []*Setting{nil, sFoo} {
		// trying to get the connection without a timeout.
		newctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		_, err = p.Get(newctx, setting)
		cancel()
		assert.EqualError(t, err, "connection pool timed out")
	}

	// put the connection take was taken initially.
	p.put(r)
}

func TestExpired(t *testing.T) {
	var state TestState

	p := NewPool(&Config[*TestConn]{
		Capacity:    1,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	defer p.Close()

	for _, setting := range []*Setting{nil, sFoo} {
		// expired context
		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-1*time.Second))
		_, err := p.Get(ctx, setting)
		cancel()
		require.EqualError(t, err, "connection pool context already expired")
	}
}

func TestMultiSettings(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	var resources [10]*Pooled[*TestConn]
	var r *Pooled[*TestConn]
	var err error

	settings := []*Setting{nil, sFoo, sBar, sBar, sFoo}

	// Test Get
	for i := range 5 {
		r, err = p.Get(ctx, settings[i])
		require.NoError(t, err)
		resources[i] = r
		assert.EqualValues(t, 5-i-1, p.Available())
		assert.Zero(t, p.Metrics.WaitCount())
		assert.Zero(t, len(state.waits))
		assert.Zero(t, p.Metrics.WaitTime())
		assert.EqualValues(t, i+1, state.lastID.Load())
		assert.EqualValues(t, i+1, state.open.Load())
	}

	// Test that Get waits
	ch := make(chan bool)
	go func() {
		defer func() { ch <- true }()
		for i := range 5 {
			r, err = p.Get(ctx, settings[i])
			if !assert.NoError(t, err) {
				return
			}
			resources[i] = r
		}
		for i := range 5 {
			p.put(resources[i])
		}
	}()
	for i := range 5 {
		// Sleep to ensure the goroutine waits
		time.Sleep(10 * time.Millisecond)
		p.put(resources[i])
	}
	<-ch
	assert.EqualValues(t, 5, p.Metrics.WaitCount())
	assert.Equal(t, 5, len(state.waits))
	// verify start times are monotonic increasing
	for i := 1; i < len(state.waits); i++ {
		assert.False(t, state.waits[i].Before(state.waits[i-1]), "Expecting monotonic increasing start times")
	}
	assert.NotZero(t, p.Metrics.WaitTime())
	assert.EqualValues(t, 5, state.lastID.Load())

	// Close
	p.Close()
	assert.EqualValues(t, 0, p.Capacity())
	assert.EqualValues(t, 0, p.Available())
	assert.EqualValues(t, 0, state.open.Load())
}

func TestMultiSettingsWithReset(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	var resources [10]*Pooled[*TestConn]
	var r *Pooled[*TestConn]
	var err error

	settings := []*Setting{nil, sFoo, sBar, sBar, sFoo}

	// Test Get
	for i := range 5 {
		r, err = p.Get(ctx, settings[i])
		require.NoError(t, err)
		resources[i] = r
		assert.EqualValues(t, 5-i-1, p.Available())
		assert.EqualValues(t, i+1, state.lastID.Load())
		assert.EqualValues(t, i+1, state.open.Load())
	}

	// Put all of them back
	for i := range 5 {
		p.put(resources[i])
	}

	// Getting all with same setting.
	for i := range 5 {
		r, err = p.Get(ctx, settings[1]) // {foo}
		require.NoError(t, err)
		assert.Truef(t, r.Conn.setting == settings[1], "setting was not properly applied")
		resources[i] = r
	}
	assert.EqualValues(t, 2, state.reset.Load()) // when setting was {bar} and getting for {foo}
	assert.EqualValues(t, 0, p.Available())
	assert.EqualValues(t, 5, state.lastID.Load())
	assert.EqualValues(t, 5, state.open.Load())

	for i := range 5 {
		p.put(resources[i])
	}

	// Close
	p.Close()
	assert.EqualValues(t, 0, p.Capacity())
	assert.EqualValues(t, 0, p.Available())
	assert.EqualValues(t, 0, state.open.Load())
}

func TestApplySettingsFailure(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	var resources []*Pooled[*TestConn]
	var r *Pooled[*TestConn]
	var err error

	settings := []*Setting{nil, sFoo, sBar, sBar, sFoo}
	// get the resource and mark for failure
	for i := range 5 {
		r, err = p.Get(ctx, settings[i])
		require.NoError(t, err)
		r.Conn.failApply = true
		resources = append(resources, r)
	}
	// put them back
	for _, r = range resources {
		p.put(r)
	}

	// any new connection created will fail to apply setting
	state.chaos.failApply = true

	// Get the resource with "foo" setting
	// For an applied connection if the setting are same it will be returned as-is.
	// Otherwise, will fail to get the resource.
	var failCount int
	resources = nil
	for range 5 {
		r, err = p.Get(ctx, settings[1])
		if err != nil {
			failCount++
			assert.EqualError(t, err, "ApplySetting failed")
			continue
		}
		resources = append(resources, r)
	}
	// put them back
	for _, r = range resources {
		p.put(r)
	}
	require.Equal(t, 3, failCount)

	// should be able to get all the resource with no setting
	resources = nil
	for range 5 {
		r, err = p.Get(ctx, nil)
		require.NoError(t, err)
		resources = append(resources, r)
	}
	// put them back
	for _, r = range resources {
		p.put(r)
	}
}

func TestGetSpike(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	var resources [10]*Pooled[*TestConn]

	// Ensure we have a pool with 5 available resources
	for i := range 5 {
		r, err := p.Get(ctx, nil)

		require.NoError(t, err)
		resources[i] = r
		assert.EqualValues(t, 5-i-1, p.Available())
		assert.Zero(t, p.Metrics.WaitCount())
		assert.Zero(t, len(state.waits))
		assert.Zero(t, p.Metrics.WaitTime())
		assert.EqualValues(t, i+1, state.lastID.Load())
		assert.EqualValues(t, i+1, state.open.Load())
	}

	for i := range 5 {
		p.put(resources[i])
	}

	assert.EqualValues(t, 5, p.Available())
	assert.EqualValues(t, 5, p.Active())
	assert.EqualValues(t, 0, p.InUse())

	for range 2000 {
		wg := sync.WaitGroup{}

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		errs := make(chan error, 80)

		for range 80 {
			wg.Go(func() {
				r, err := p.Get(ctx, nil)
				defer p.put(r)

				if err != nil {
					errs <- err
				}
			})
		}
		wg.Wait()

		if len(errs) > 0 {
			assert.Failf(t, "Error getting connection", "Error getting connection: %v", <-errs)
		}

		close(errs)
	}
}

// TestCloseDuringWaitForConn confirms that we do not get hung when the pool gets
// closed while we are waiting for a connection from it.
func TestCloseDuringWaitForConn(t *testing.T) {
	ctx := t.Context()
	goRoutineCnt := 50
	getTimeout := 2000 * time.Millisecond

	for range 50 {
		hung := make(chan (struct{}), goRoutineCnt)
		var state TestState
		p := NewPool(&Config[*TestConn]{
			Capacity:     1,
			MaxIdleCount: 1,
			IdleTimeout:  time.Second,
			LogWait:      state.LogWait,
		}).Open(newConnector(&state), nil)

		closed := atomic.Bool{}
		wg := sync.WaitGroup{}
		var count atomic.Int64

		fmt.Println("Starting TestCloseDuringWaitForConn")

		// Spawn multiple goroutines to perform Get and Put operations, but only
		// allow connections to be checked out until `closed` has been set to true.
		for range goRoutineCnt {
			wg.Go(func() {
				for !closed.Load() {
					timeout := time.After(getTimeout)
					getCtx, getCancel := context.WithTimeout(ctx, getTimeout/3)
					defer getCancel()
					done := make(chan struct{})
					go func() {
						defer close(done)
						r, err := p.Get(getCtx, nil)
						if err != nil {
							return
						}
						count.Add(1)
						r.Recycle()
					}()
					select {
					case <-timeout:
						hung <- struct{}{}
						return
					case <-done:
					}
				}
			})
		}

		// Let the go-routines get up and running.
		for count.Load() < 5000 {
			time.Sleep(1 * time.Millisecond)
		}

		// Close the pool, which should allow all goroutines to finish.
		closeCtx, closeCancel := context.WithTimeout(ctx, 1*time.Second)
		defer closeCancel()
		err := p.CloseWithContext(closeCtx)
		closed.Store(true)
		require.NoError(t, err, "Failed to close pool")

		// Wait for all goroutines to finish.
		wg.Wait()
		select {
		case <-hung:
			require.FailNow(t, "Race encountered and deadlock detected")
		default:
		}

		fmt.Println("Count of connections checked out:", count.Load())
		// Check that the pool is closed and no connections are available.
		require.EqualValues(t, 0, p.Capacity())
		require.EqualValues(t, 0, p.Available())
		require.EqualValues(t, 0, state.open.Load())
	}
}

func TestCloseDoesNotReturnConnToWaitingGet(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity: 1,
		LogWait:  state.LogWait,
	}).Open(newConnector(&state), nil)

	conn, err := p.Get(ctx, nil)
	require.NoError(t, err)

	type getResult struct {
		conn *Pooled[*TestConn]
		err  error
	}
	results := make(chan getResult, 1)
	getCtx, cancelGet := context.WithTimeout(ctx, 30*time.Second)
	defer cancelGet()
	go func() {
		conn, err := p.Get(getCtx, nil)
		results <- getResult{conn: conn, err: err}
	}()

	require.Eventually(t, func() bool {
		return p.wait.waiting() == 1
	}, 5*time.Second, 10*time.Millisecond)

	closeCtx, cancelClose := context.WithTimeout(ctx, 30*time.Second)
	defer cancelClose()
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- p.CloseWithContext(closeCtx)
	}()

	require.Eventually(t, func() bool {
		return p.Capacity() == 0
	}, 5*time.Second, 10*time.Millisecond)

	conn.Recycle()

	var result getResult
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		select {
		case result = <-results:
			assert.ErrorIs(c, result.err, ErrConnPoolClosed)
			assert.Nil(c, result.conn)
		default:
			assert.Fail(c, "waiting Get did not return after close")
		}
	}, 5*time.Second, 10*time.Millisecond)

	if result.conn != nil {
		result.conn.Recycle()
	}

	require.NoError(t, <-closeDone)
}

// TestIdleTimeoutConnectionLeak checks for leaked connections after idle timeout
func TestIdleTimeoutConnectionLeak(t *testing.T) {
	var state TestState

	// Slow connection creation to ensure idle timeout happens during reopening
	state.chaos.delayConnect = 300 * time.Millisecond

	p := NewPool(&Config[*TestConn]{
		Capacity:    2,
		IdleTimeout: 50 * time.Millisecond,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	getCtx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	// Get and return two connections
	conn1, err := p.Get(getCtx, nil)
	require.NoError(t, err)

	conn2, err := p.Get(getCtx, nil)
	require.NoError(t, err)

	p.put(conn1)
	p.put(conn2)

	// At this point: Active=2, InUse=0, Available=2
	require.EqualValues(t, 2, p.Active())
	require.EqualValues(t, 0, p.InUse())
	require.EqualValues(t, 2, p.Available())

	// Wait for idle timeout to kick in and start expiring connections
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		// Check the actual number of currently open connections
		assert.Equal(c, int64(2), state.open.Load())
		// Check the total number of closed connections
		assert.Equal(c, int64(1), state.close.Load())
	}, 100*time.Millisecond, 10*time.Millisecond)

	// Keep this test focused on the in-flight reopen above. Further idle
	// cleanup ticks can legitimately close additional connections and make the
	// leak assertions below depend on background timing.
	p.SetIdleTimeout(0)

	// At this point, the idle timeout worker has expired the connections
	// and is trying to reopen them (which takes 300ms due to delayConnect)

	// Try to get connections while they're being reopened
	// This should trigger the bug where connections get discarded
	wg := sync.WaitGroup{}

	for range 2 {
		wg.Go(func() {
			getCtx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()

			conn, err := p.Get(getCtx, nil)
			require.NoError(t, err)

			p.put(conn)
		})
	}

	wg.Wait()

	// Wait a moment for all reopening to complete
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int64(2), state.open.Load())
		assert.Equal(c, int64(2), p.Active())
	}, 400*time.Millisecond, 10*time.Millisecond)
	assert.GreaterOrEqual(t, state.close.Load(), int64(1))

	// Check the pool state
	assert.Equal(t, int64(2), p.Active())
	assert.Equal(t, int64(0), p.InUse())
	assert.Equal(t, int64(2), p.Available())
	assert.GreaterOrEqual(t, p.Metrics.IdleClosed(), int64(1))

	// Try to close the pool - if there are leaked connections, this will timeout
	closeCtx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	err = p.CloseWithContext(closeCtx)
	require.NoError(t, err)

	// Pool should be completely closed now
	assert.Equal(t, int64(0), p.Active())
	assert.Equal(t, int64(0), p.InUse())
	assert.Equal(t, int64(0), p.Available())
	assert.GreaterOrEqual(t, p.Metrics.IdleClosed(), int64(1))

	assert.Equal(t, int64(0), state.open.Load())
	assert.Equal(t, state.lastID.Load(), state.close.Load())
}

func TestIdleTimeoutReopenDoesNotBlockGetsDespiteAvailableCapacity(t *testing.T) {
	var state TestState
	var blockReconnect atomic.Bool
	reconnectStarted := make(chan struct{})
	releaseReconnect := make(chan struct{})
	releaseReconnectOnce := sync.Once{}
	reconnectAttempts := atomic.Int64{}
	release := func() {
		blockReconnect.Store(false)
		releaseReconnectOnce.Do(func() {
			close(releaseReconnect)
		})
	}

	connector := func(ctx context.Context) (*TestConn, error) {
		if blockReconnect.Load() && reconnectAttempts.Add(1) == 1 {
			close(reconnectStarted)
			select {
			case <-releaseReconnect:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		state.open.Add(1)
		return &TestConn{
			num:    state.lastID.Add(1),
			counts: &state,
		}, nil
	}

	ctx := context.Background()
	p := NewPool(&Config[*TestConn]{
		Capacity: 4,
		LogWait:  state.LogWait,
	}).Open(connector, nil)
	defer p.Close()
	defer release()

	var conns []*Pooled[*TestConn]
	for range 4 {
		conn, err := p.Get(ctx, nil)
		require.NoError(t, err)
		conns = append(conns, conn)
	}

	for _, conn := range conns {
		p.put(conn)
		conn.timeUsed.set(monotonicNow() - 2*time.Millisecond)
	}

	require.EqualValues(t, 4, p.Active())
	require.EqualValues(t, 4, p.Available())

	p.SetIdleTimeout(time.Millisecond)
	blockReconnect.Store(true)

	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		p.closeIdleResources(time.Now())
	}()

	require.Eventually(t, func() bool {
		select {
		case <-reconnectStarted:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	conn1, err := p.Get(ctx, nil)
	require.NoError(t, err)
	defer p.put(conn1)

	conn2, err := p.Get(ctx, nil)
	require.NoError(t, err)
	defer p.put(conn2)

	require.EqualValues(t, 2, p.Available())

	getCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()

	conn, err := p.Get(getCtx, nil)
	require.NoError(t, err)
	defer p.put(conn)

	release()

	require.Eventually(t, func() bool {
		select {
		case <-cleanupDone:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestIdleTimeoutDoesntLeaveLingeringConnection(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:    10,
		IdleTimeout: 50 * time.Millisecond,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	defer p.Close()

	var conns []*Pooled[*TestConn]
	for range 10 {
		conn, err := p.Get(ctx, nil)
		require.NoError(t, err)
		conns = append(conns, conn)
	}

	for _, conn := range conns {
		p.put(conn)
	}

	require.EqualValues(t, 10, p.Active())
	require.EqualValues(t, 10, p.Available())

	// Wait for the idle worker to actually rotate connections.
	require.Eventually(t, func() bool {
		return p.Metrics.IdleClosed() > 10
	}, time.Second, 5*time.Millisecond, "expected at least 10 idle closes")

	// Wait for the pool to settle back to steady state before asserting on
	// Active/Available. closeIdleResources intentionally drops pool.active
	// during the close-then-reopen sequence so concurrent Gets can claim
	// the freed slot (see closeIdleResources's doc and
	// TestIdleTimeoutReopenDoesNotBlockGetsDespiteAvailableCapacity). A naive
	// `Active() == 10` check right after the first Eventually could observe
	// that dip mid-cycle.
	require.Eventually(t, func() bool {
		return p.Active() == 10 && p.Available() == 10
	}, time.Second, 5*time.Millisecond, "pool didn't settle back to capacity")

	// Count how many connections in the stack are closed
	totalInStack := 0
	for conn := p.clean.Peek(); conn != nil; conn = conn.next.Load() {
		totalInStack++
	}

	require.LessOrEqual(t, totalInStack, 10)
}

func BenchmarkPoolCleanupIdleConnectionsPerformanceNoIdleConnections(b *testing.B) {
	var state TestState

	capacity := 1000

	p := NewPool(&Config[*TestConn]{
		Capacity:    int64(capacity),
		IdleTimeout: 30 * time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)
	defer p.Close()

	// Fill the pool
	connections := make([]*Pooled[*TestConn], 0, capacity)
	for range capacity {
		conn, err := p.Get(context.Background(), nil)
		if err != nil {
			b.Fatal(err)
		}

		connections = append(connections, conn)
	}

	// Return all connections to the pool
	for _, conn := range connections {
		conn.Recycle()
	}

	b.ResetTimer()

	for b.Loop() {
		p.closeIdleResources(time.Now())
	}
}
