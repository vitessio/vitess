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
	"runtime"
	"slices"
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
}

func (tr *TestConn) waitForClose() chan struct{} {
	tr.onClose = make(chan struct{})
	return tr.onClose
}

func (tr *TestConn) IsClosed() bool {
	return tr.closed
}

func (tr *TestConn) Setting() *Setting {
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
		assert.Empty(t, state.waits)
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
	assert.Len(t, state.waits, 5)
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
	// A nil Put causes the resource to be reopened so any queued waiter
	// can be served.
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
	assert.Len(t, state.waits, int(p.Metrics.WaitCount()))
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
	}, 30*time.Second, 50*time.Millisecond)
}

func TestRefreshWorkerDoesNotQueueReopens(t *testing.T) {
	old := PoolCloseTimeout
	PoolCloseTimeout = 500 * time.Millisecond
	t.Cleanup(func() { PoolCloseTimeout = old })

	var state TestState
	var refreshCount atomic.Int32

	p := NewPool(&Config[*TestConn]{
		Capacity:        1,
		RefreshInterval: 20 * time.Millisecond,
	}).Open(newConnector(&state), func() (bool, error) {
		refreshCount.Add(1)
		return true, nil
	})
	t.Cleanup(p.Close)

	held, err := p.Get(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if held != nil {
			held.Recycle()
		}
	})

	require.Eventually(t, func() bool {
		return refreshCount.Load() == 1
	}, 5*time.Second, time.Millisecond)

	require.Never(t, func() bool {
		return refreshCount.Load() > 1
	}, 100*time.Millisecond, 5*time.Millisecond)

	held.Recycle()
	held = nil

	require.Eventually(t, func() bool {
		return refreshCount.Load() >= 2
	}, 5*time.Second, 5*time.Millisecond)
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

func TestCloseWithContextAfterIdleCleanupPopDoesNotLeak(t *testing.T) {
	var state TestState

	p := NewPool(&Config[*TestConn]{
		Capacity: 2,
		LogWait:  state.LogWait,
	}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	held, err := p.Get(t.Context(), nil)
	require.NoError(t, err)

	idle, err := p.Get(t.Context(), nil)
	require.NoError(t, err)
	idle.Recycle()

	cleanupConn, ok := p.clean.Pop()
	require.True(t, ok)
	require.NotNil(t, cleanupConn)

	var waiterGotConn atomic.Bool
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)

		conn, err := p.Get(t.Context(), nil)
		if err == nil {
			waiterGotConn.Store(true)
			conn.Recycle()
		}
	}()

	require.Eventually(t, func() bool {
		return p.wait.waiting() == 1
	}, 30*time.Second, time.Millisecond)

	closeCtx, cancelClose := context.WithTimeout(t.Context(), 30*time.Second)
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- p.CloseWithContext(closeCtx)
	}()

	require.Eventually(t, func() bool {
		return p.Capacity() == 0
	}, 30*time.Second, time.Millisecond)

	p.tryReturnConn(cleanupConn, false)
	held.Recycle()

	var closeErr error
	require.Eventually(t, func() bool {
		select {
		case closeErr = <-closeDone:
			return true
		default:
			return false
		}
	}, 30*time.Second, time.Millisecond)
	cancelClose()
	require.NoError(t, closeErr)

	require.Eventually(t, func() bool {
		select {
		case <-waiterDone:
			return true
		default:
			return false
		}
	}, 30*time.Second, time.Millisecond)

	require.False(t, waiterGotConn.Load(), "close must not hand an idle-cleanup conn to a waiter after capacity reaches 0")
	require.EqualValues(t, 0, p.Active())
	require.EqualValues(t, 0, p.InUse())
	require.EqualValues(t, 0, state.open.Load())
}

func TestCloseWithContextAfterSetCapacityZeroClosesPool(t *testing.T) {
	var state TestState

	p := NewPool(&Config[*TestConn]{
		Capacity: 1,
		LogWait:  state.LogWait,
	}).Open(newConnector(&state), nil)

	t.Cleanup(func() {
		if closeChan := p.close.Swap(nil); closeChan != nil {
			close(*closeChan)
			p.workers.Wait()
		}
	})

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	require.NoError(t, p.SetCapacity(ctx, 0))
	require.True(t, p.IsOpen())

	require.NoError(t, p.CloseWithContext(ctx))
	require.False(t, p.IsOpen())
}

// TestCloseWithContextDrainsAfterTimedOutSetCapacityZero guards against an
// early return in setCapacity that previously caused CloseWithContext to
// return nil despite active conns being out, when a prior SetCapacity(0)
// had timed out with conns still borrowed. The path is:
//
//   - SetCapacity(0) times out: capacity is swapped to 0 first, then the
//     drain loop hits the deadline and returns an error.
//   - The next CloseWithContext calls setCapacity(0) again. With the bug,
//     oldcap == newcap == 0 short-circuited the drain loop and Close
//     returned nil even though pool.active was still > 0.
//
// CloseWithContext must instead attempt to drain and surface a timeout
// when borrowed conns aren't returned.
func TestCloseWithContextDrainsAfterTimedOutSetCapacityZero(t *testing.T) {
	var state TestState

	p := NewPool(&Config[*TestConn]{
		Capacity: 2,
		LogWait:  state.LogWait,
	}).Open(newConnector(&state), nil)

	t.Cleanup(func() {
		if closeChan := p.close.Swap(nil); closeChan != nil {
			close(*closeChan)
			p.workers.Wait()
		}
	})

	conn, err := p.Get(t.Context(), nil)
	require.NoError(t, err)

	// SetCapacity(0) must time out: a conn is still borrowed.
	cancelledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	require.Error(t, p.SetCapacity(cancelledCtx, 0))
	require.EqualValues(t, 0, p.Capacity())
	require.GreaterOrEqual(t, p.Active(), int64(1))

	// CloseWithContext must still attempt to drain — without the fix it
	// returns nil immediately because setCapacity short-circuits on
	// oldcap == newcap.
	require.Error(t, p.CloseWithContext(cancelledCtx))

	// Release the held conn so it is properly closed during teardown.
	conn.Recycle()
}

// TestSetCapacityRejectedOnClosedPool pins the contract that SetCapacity must
// fail fast (rather than silently writing into pool.capacity) when the pool
// is not open. This covers both states: never-opened and previously-opened-
// then-closed. Without this guard, a SetCapacity call queued on capacityMu
// during CloseWithContext can race through after Close releases the mutex
// and bump capacity back up — leaving the pool closed with non-zero capacity.
func TestSetCapacityRejectedOnClosedPool(t *testing.T) {
	var state TestState

	// Never-opened pool: SetCapacity must reject.
	p := NewPool(&Config[*TestConn]{Capacity: 4})
	err := p.SetCapacity(t.Context(), 2)
	require.ErrorIs(t, err, ErrConnPoolClosed)

	// Opened pool: SetCapacity succeeds.
	p.Open(newConnector(&state), nil)
	require.NoError(t, p.SetCapacity(t.Context(), 2))
	require.EqualValues(t, 2, p.Capacity())

	// Closed pool: SetCapacity must reject and must not change capacity.
	p.Close()
	require.False(t, p.IsOpen())
	err = p.SetCapacity(t.Context(), 8)
	require.ErrorIs(t, err, ErrConnPoolClosed)
	require.EqualValues(t, 0, p.Capacity())
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
	config := &Config[*TestConn]{
		Capacity:    1,
		MaxLifetime: 30 * time.Minute,
		LogWait:     state.LogWait,
	}

	p := NewPool(config).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	threshold := config.MaxLifetime + config.MaxLifetime/2
	const maxAttempts = 64

	for range maxAttempts {
		extended := p.extendedMaxLifetime()
		require.LessOrEqual(t, config.MaxLifetime, extended)
		require.Greater(t, 2*config.MaxLifetime, extended)

		if extended > threshold {
			return
		}
	}

	require.Failf(t, "jitter never reached upper half of range",
		"no sample in %d tries exceeded %s", maxAttempts, threshold)
}

func TestExtendedMaxLifetimeNegativeDisables(t *testing.T) {
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
		assert.Equal(t, expClosedConn, closedConn)
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
		assert.Equalf(t, int64(2), p.Capacity(), "pool should not be out of capacity")
		assert.Equalf(t, int64(2), p.Available(), "pool should not be out of availability")

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
		require.EqualError(t, err, "connection pool timed out")
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
		assert.Empty(t, state.waits)
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
	assert.Len(t, state.waits, 5)
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
		assert.Samef(t, r.Conn.setting, settings[1], "setting was not properly applied")
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
			require.EqualError(t, err, "ApplySetting failed")
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
		assert.Empty(t, state.waits)
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

	errs := make([]error, 2)
	for i := range 2 {
		wg.Go(func() {
			getCtx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()

			conn, err := p.Get(getCtx, nil)
			if err != nil {
				errs[i] = err
				return
			}

			p.put(conn)
		})
	}

	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}

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

	ctx := t.Context()
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

func TestIdleTimeoutBoundsExpiredSweep(t *testing.T) {
	var state TestState

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity: 6,
		LogWait:  state.LogWait,
	}).Open(newConnector(&state), nil)
	var borrowedConns []*Pooled[*TestConn]
	defer func() {
		for _, conn := range borrowedConns {
			p.put(conn)
		}
		p.Close()
	}()

	var idleConns []*Pooled[*TestConn]
	for range 2 {
		conn, err := p.Get(ctx, nil)
		require.NoError(t, err)
		idleConns = append(idleConns, conn)
	}

	for range 4 {
		conn, err := p.Get(ctx, nil)
		require.NoError(t, err)
		borrowedConns = append(borrowedConns, conn)
	}

	p.SetIdleTimeout(time.Millisecond)
	for _, conn := range idleConns {
		p.put(conn)
		conn.timeUsed.set(monotonicNow() - 2*time.Millisecond)
	}
	oldestClosed := idleConns[0].Conn.waitForClose()
	newestClosed := idleConns[1].Conn.waitForClose()

	p.closeIdleResources(time.Now())

	assert.EqualValues(t, 1, p.Metrics.IdleClosed())
	assert.EqualValues(t, 1, state.close.Load())
	assert.EqualValues(t, 6, p.Active())
	assert.EqualValues(t, 2, p.Available())
	assert.True(t, channelClosed(oldestClosed))
	assert.False(t, channelClosed(newestClosed))

	p.closeIdleResources(time.Now())

	assert.EqualValues(t, 2, p.Metrics.IdleClosed())
	assert.EqualValues(t, 2, state.close.Load())
	assert.EqualValues(t, 6, p.Active())
	assert.EqualValues(t, 2, p.Available())
	assert.True(t, channelClosed(newestClosed))
}

func channelClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestIdleTimeoutStopsReopeningWhenPoolCloses(t *testing.T) {
	var state TestState
	var reconnectAttempts atomic.Int64
	reconnectStarted := make(chan struct{})
	releaseReconnect := make(chan struct{})
	releaseReconnectOnce := sync.Once{}
	release := func() {
		releaseReconnectOnce.Do(func() {
			close(releaseReconnect)
		})
	}
	t.Cleanup(release)

	p := NewPool(&Config[*TestConn]{
		Capacity:    4,
		IdleTimeout: time.Millisecond,
		LogWait:     state.LogWait,
	})
	p.config.connect = func(ctx context.Context) (*TestConn, error) {
		if reconnectAttempts.Add(1) == 1 {
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

	closeChan := make(chan struct{})
	p.close.Store(&closeChan)
	lifetimeCtx, lifetimeCancel := context.WithCancel(context.Background())
	t.Cleanup(lifetimeCancel)
	p.lifetime.Store(&lifetime{ctx: lifetimeCtx, cancel: lifetimeCancel})
	p.capacity.Store(4)
	p.idleCount.Store(4)
	p.active.Store(4)

	now := monotonicNow()
	for range 4 {
		conn := &Pooled[*TestConn]{
			pool: p,
			Conn: &TestConn{
				num:    state.lastID.Add(1),
				counts: &state,
			},
		}
		state.open.Add(1)
		conn.timeCreated.set(now)
		conn.timeUsed.set(now - 2*time.Millisecond)
		p.clean.Push(conn)
	}

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

	closePtr := p.close.Swap(nil)
	require.NotNil(t, closePtr)
	close(*closePtr)
	release()

	require.Eventually(t, func() bool {
		select {
		case <-cleanupDone:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	assert.EqualValues(t, 1, reconnectAttempts.Load())
	assert.EqualValues(t, 3, state.close.Load())

	if conn, ok := p.clean.PopAll(); ok {
		for conn != nil {
			next := conn.next.Load()
			conn.Close()
			conn = next
		}
	}
}

func TestIdleTimeoutCloseInStackDoesNotAllocate(t *testing.T) {
	var state TestState
	const capacity = 1000

	ctx := t.Context()
	p := NewPool(&Config[*TestConn]{
		Capacity:    capacity,
		IdleTimeout: time.Hour,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)
	defer p.Close()

	conns := make([]*Pooled[*TestConn], 0, capacity)
	for range capacity {
		conn, err := p.Get(ctx, nil)
		require.NoError(t, err)
		conns = append(conns, conn)
	}

	for _, conn := range conns {
		p.put(conn)
	}

	now := time.Now()
	allocs := testing.AllocsPerRun(100, func() {
		p.closeIdleResources(now)
	})

	assert.Zero(t, allocs)
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

	// Wait for the idle worker to refresh more than the initial 10 conns
	// AND for the reopen loop to bring Active back to capacity. These need
	// to hold simultaneously, so they go in one EventuallyWithT — the
	// idle worker briefly drops Active between closing expired conns and
	// re-acquiring slots for the reopened ones.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Greater(c, p.Metrics.IdleClosed(), int64(10))
		assert.EqualValues(c, 10, p.Active())
		assert.EqualValues(c, 10, p.Available())
	}, time.Second, 10*time.Millisecond)
}

// TestCloseDoesNotHandOffToWaiters verifies that once Close has started
// (capacity has been driven to 0), connections being Recycled by clients
// are closed immediately rather than handed off to waiters that are still
// queued in the waitlist. Without this guarantee, Close can be delayed
// indefinitely as Recycled conns keep getting passed waiter-to-waiter
// instead of landing on a stack where the close loop can drain them.
func TestCloseDoesNotHandOffToWaiters(t *testing.T) {
	var state TestState
	p := NewPool(&Config[*TestConn]{
		Capacity: 1,
	}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	ctx := t.Context()

	c, err := p.Get(ctx, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, state.open.Load())

	var waiterGotConn atomic.Bool
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		conn, err := p.Get(ctx, nil)
		if err == nil {
			waiterGotConn.Store(true)
			conn.Recycle()
		}
	}()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, p.wait.waiting())
	}, time.Second, time.Millisecond)

	closeDone := make(chan error, 1)
	go func() {
		closeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		closeDone <- p.CloseWithContext(closeCtx)
	}()

	// Wait until Close has set the capacity to 0 — only then is the
	// "no more handoffs" guarantee in effect.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.EqualValues(c, 0, p.Capacity())
	}, time.Second, time.Millisecond)

	c.Recycle()

	select {
	case err = <-closeDone:
	case <-time.After(30 * time.Second):
		require.Fail(t, "Close did not complete in time")
	}
	require.NoError(t, err)

	select {
	case <-waiterDone:
	case <-time.After(30 * time.Second):
		require.Fail(t, "waiter did not finish")
	}

	require.False(t, waiterGotConn.Load(),
		"waiter must not be handed a connection after Close has set capacity to 0")
	require.EqualValues(t, 0, state.open.Load(),
		"recycled conn must be closed when capacity is 0, not pushed back to a stack")
	require.EqualValues(t, 0, p.Active())
}

// TestSetCapacityReductionDrainsWithWaiters verifies that SetCapacity reducing
// capacity from N to M (0 < M < N) completes promptly while waiters are
// queued for conns. tryReturnConn must eagerly close conns whenever
// active > capacity; otherwise Recycles are handed off to waiters who hold
// the conn, the wait list drains without conns ever hitting a stack, and
// setCapacity's drain loop spins until ctx expires.
func TestSetCapacityReductionDrainsWithWaiters(t *testing.T) {
	var state TestState
	pool := NewPool(&Config[*TestConn]{
		Capacity: 4,
	}).Open(newConnector(&state), nil)

	ctx := t.Context()

	// Hold all conns directly.
	conns := make([]*Pooled[*TestConn], 4)
	for i := range conns {
		c, err := pool.Get(ctx, nil)
		require.NoError(t, err)
		conns[i] = c
	}

	// Queue NumWaiters callers that Get a conn and then hold it until release.
	// Holding (instead of immediately recycling) keeps every handed-off conn
	// out of the clean stack — the exact scenario where setCapacity's drain
	// loop has nothing to pop and spins.
	const NumWaiters = 4
	release := make(chan struct{})
	var wg sync.WaitGroup
	for range NumWaiters {
		wg.Go(func() {
			c, err := pool.Get(ctx, nil)
			if err != nil {
				return
			}
			<-release
			c.Recycle()
		})
	}
	t.Cleanup(func() {
		close(release)
		pool.Close()
		wg.Wait()
	})

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, pool.wait.waiting(), NumWaiters)
	}, time.Second, time.Millisecond)

	setDone := make(chan error, 1)
	go func() {
		setCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		setDone <- pool.SetCapacity(setCtx, 1)
	}()

	// Wait for capacity to actually drop before recycling, so every
	// Recycle observes active > capacity.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.EqualValues(c, 1, pool.Capacity())
	}, time.Second, time.Millisecond)

	for _, c := range conns {
		c.Recycle()
	}

	var err error
	select {
	case err = <-setDone:
	case <-time.After(5 * time.Second):
		require.Fail(t, "SetCapacity did not return")
	}
	require.NoError(t, err, "SetCapacity reduction must not stall while waiters are queued")
	require.EqualValues(t, 1, pool.Capacity())
	require.LessOrEqual(t, pool.Active(), int64(1))
}

// TestIdleWorkerConnectCancelsOnClose verifies that an idle-worker reopen
// blocked inside the user-supplied connect callback unblocks when Close is
// called. Without a cancellable context, the idle worker would extend Close
// by the full backend connect timeout per expired connection.
func TestIdleWorkerConnectCancelsOnClose(t *testing.T) {
	var state TestState

	ctx := t.Context()

	var connectCalls atomic.Int64
	customConnector := func(connectCtx context.Context) (*TestConn, error) {
		call := connectCalls.Add(1)
		if call > 1 {
			// Block until connectCtx is cancelled (the behavior under test)
			// or the test exits. t.Context is cancelled just before t.Cleanup
			// runs, so a failed assertion still unblocks us.
			select {
			case <-connectCtx.Done():
				return nil, connectCtx.Err()
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		state.open.Add(1)
		return &TestConn{
			num:         state.lastID.Add(1),
			timeCreated: time.Now(),
			counts:      &state,
		}, nil
	}

	p := NewPool(&Config[*TestConn]{
		Capacity:    1,
		IdleTimeout: 20 * time.Millisecond,
	}).Open(customConnector, nil)
	t.Cleanup(p.Close)
	c, err := p.Get(ctx, nil)
	require.NoError(t, err)
	c.Recycle()

	// Wait until the idle worker has expired the conn and called into the
	// connector a second time (which is now blocked).
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, connectCalls.Load(), int64(2))
	}, 500*time.Millisecond, 5*time.Millisecond)

	closeDone := make(chan error, 1)
	go func() {
		closeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		closeDone <- p.CloseWithContext(closeCtx)
	}()

	select {
	case err = <-closeDone:
	case <-time.After(30 * time.Second):
		require.Fail(t, "Close did not return; idle-worker connect was not cancelled")
	}
	require.NoError(t, err)
}

// TestTaintConnectCancelsOnClose verifies that Pooled.Taint's synchronous
// reopen (via put(nil)) unblocks when the pool is closed. Without using
// the pool's lifetime context for the reopen, Taint against an unreachable
// backend would block for the full MySQL connect timeout — even when the
// pool is already shutting down.
func TestTaintConnectCancelsOnClose(t *testing.T) {
	var state TestState
	ctx := t.Context()

	var connectCalls atomic.Int64
	customConnector := func(connectCtx context.Context) (*TestConn, error) {
		call := connectCalls.Add(1)
		if call > 1 {
			select {
			case <-connectCtx.Done():
				return nil, connectCtx.Err()
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		state.open.Add(1)
		return &TestConn{
			num:         state.lastID.Add(1),
			timeCreated: time.Now(),
			counts:      &state,
		}, nil
	}

	p := NewPool(&Config[*TestConn]{
		Capacity: 1,
	}).Open(customConnector, nil)
	t.Cleanup(p.Close)

	c, err := p.Get(ctx, nil)
	require.NoError(t, err)

	taintDone := make(chan struct{})
	go func() {
		defer close(taintDone)
		c.Taint()
	}()

	// Wait until Taint's reopen is parked inside the connector.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, connectCalls.Load(), int64(2))
	}, 500*time.Millisecond, 5*time.Millisecond)

	closeDone := make(chan error, 1)
	go func() {
		closeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		closeDone <- p.CloseWithContext(closeCtx)
	}()

	select {
	case err = <-closeDone:
	case <-time.After(30 * time.Second):
		require.Fail(t, "Close did not complete; Taint's reopen connect was not cancelled")
	}
	require.NoError(t, err)

	select {
	case <-taintDone:
	case <-time.After(30 * time.Second):
		require.Fail(t, "Taint did not return")
	}
}

// TestTaintWakesWaiter verifies that Taint, which frees a pool slot,
// still results in a queued waiter being served. The replacement conn
// is opened synchronously on Taint's goroutine and must hand itself to
// the waiter rather than letting the waiter wait for its own ctx to
// expire.
func TestTaintWakesWaiter(t *testing.T) {
	var state TestState
	ctx := t.Context()

	p := NewPool(&Config[*TestConn]{
		Capacity: 1,
	}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	c, err := p.Get(ctx, nil)
	require.NoError(t, err)

	waiterGot := make(chan *Pooled[*TestConn], 1)
	go func() {
		conn, err := p.Get(ctx, nil)
		if err == nil {
			waiterGot <- conn
		}
	}()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, p.wait.waiting())
	}, time.Second, time.Millisecond)

	c.Taint()

	var conn *Pooled[*TestConn]
	select {
	case conn = <-waiterGot:
	case <-time.After(30 * time.Second):
		require.Fail(t, "waiter was not served after Taint freed the slot")
	}
	conn.Recycle()
}

// TestRecycleMaxLifetimeWakesWaiter mirrors TestTaintWakesWaiter for the
// other put() branch that frees a slot — a Recycle on a conn whose
// maxLifetime has elapsed.
func TestRecycleMaxLifetimeWakesWaiter(t *testing.T) {
	var state TestState
	ctx := t.Context()

	p := NewPool(&Config[*TestConn]{
		Capacity:    1,
		MaxLifetime: time.Millisecond,
	}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	c, err := p.Get(ctx, nil)
	require.NoError(t, err)

	time.Sleep(2 * time.Duration(p.config.maxLifetime.Load()))

	waiterGot := make(chan *Pooled[*TestConn], 1)
	go func() {
		conn, err := p.Get(ctx, nil)
		if err == nil {
			waiterGot <- conn
		}
	}()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, p.wait.waiting())
	}, time.Second, time.Millisecond)

	c.Recycle()

	var conn *Pooled[*TestConn]
	select {
	case conn = <-waiterGot:
	case <-time.After(30 * time.Second):
		require.Fail(t, "waiter was not served after maxLifetime put freed the slot")
	}
	conn.Recycle()
}

// TestRecycleMaxLifetimePreservesSetting verifies that a maxLifetime
// reopen retains the original conn's Setting so the replacement lands
// in the same settings stack rather than silently migrating to clean.
func TestRecycleMaxLifetimePreservesSetting(t *testing.T) {
	var state TestState
	ctx := t.Context()

	p := NewPool(&Config[*TestConn]{
		Capacity:    1,
		MaxLifetime: time.Millisecond,
	}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	c, err := p.Get(ctx, sFoo)
	require.NoError(t, err)
	require.Equal(t, sFoo, c.Conn.Setting())

	time.Sleep(2 * time.Duration(p.config.maxLifetime.Load()))

	c.Recycle()

	require.EventuallyWithT(t, func(check *assert.CollectT) {
		assert.EqualValues(check, 1, p.Metrics.MaxLifetimeClosed())
		assert.NotNil(check, p.settings[sFoo.bucket&stackMask].Peek(),
			"reopened conn should live in settings[sFoo.bucket]")
		assert.Nil(check, p.clean.Peek(),
			"reopened conn must not migrate to the clean stack")
	}, time.Second, 10*time.Millisecond)
}

// TestIdleWorkerReopenPreservesSetting verifies the idle worker's
// closeIdleResources path keeps the original Setting on reopen.
func TestIdleWorkerReopenPreservesSetting(t *testing.T) {
	var state TestState
	ctx := t.Context()

	p := NewPool(&Config[*TestConn]{
		Capacity:    1,
		IdleTimeout: 20 * time.Millisecond,
	}).Open(newConnector(&state), nil)
	t.Cleanup(p.Close)

	c, err := p.Get(ctx, sFoo)
	require.NoError(t, err)
	require.Equal(t, sFoo, c.Conn.Setting())
	c.Recycle()

	require.EventuallyWithT(t, func(check *assert.CollectT) {
		assert.GreaterOrEqual(check, p.Metrics.IdleClosed(), int64(1))
		assert.NotNil(check, p.settings[sFoo.bucket&stackMask].Peek(),
			"idle-reopened conn should remain in settings[sFoo.bucket]")
		assert.Nil(check, p.clean.Peek(),
			"idle-reopened conn must not migrate to the clean stack")
	}, time.Second, 10*time.Millisecond)
}

// TestRecycleMaxLifetimeReopenCancelsOnClose verifies that the maxLifetime
// reopen inside put (synchronous, on the Recycle caller's goroutine)
// unblocks when the pool is closed.
func TestRecycleMaxLifetimeReopenCancelsOnClose(t *testing.T) {
	var state TestState
	ctx := t.Context()

	var connectCalls atomic.Int64
	customConnector := func(connectCtx context.Context) (*TestConn, error) {
		call := connectCalls.Add(1)
		if call > 1 {
			select {
			case <-connectCtx.Done():
				return nil, connectCtx.Err()
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		state.open.Add(1)
		return &TestConn{
			num:         state.lastID.Add(1),
			timeCreated: time.Now(),
			counts:      &state,
		}, nil
	}

	p := NewPool(&Config[*TestConn]{
		Capacity:    1,
		MaxLifetime: time.Millisecond,
	}).Open(customConnector, nil)
	t.Cleanup(p.Close)

	c, err := p.Get(ctx, nil)
	require.NoError(t, err)

	time.Sleep(2 * time.Duration(p.config.maxLifetime.Load()))

	recycleDone := make(chan struct{})
	go func() {
		defer close(recycleDone)
		c.Recycle()
	}()

	// Wait until Recycle's reopen is parked inside the connector.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, connectCalls.Load(), int64(2))
	}, 500*time.Millisecond, 5*time.Millisecond)

	closeDone := make(chan error, 1)
	go func() {
		closeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		closeDone <- p.CloseWithContext(closeCtx)
	}()

	select {
	case err = <-closeDone:
	case <-time.After(30 * time.Second):
		require.Fail(t, "Close did not complete; maxLifetime reopen connect was not cancelled")
	}
	require.NoError(t, err)

	select {
	case <-recycleDone:
	case <-time.After(30 * time.Second):
		require.Fail(t, "Recycle did not return")
	}
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

func BenchmarkPoolCleanupIdleConnectionsPerformanceHighConcurrencyNoIdleConnections(b *testing.B) {
	var state TestState

	const capacity = 1000

	p := NewPool(&Config[*TestConn]{
		Capacity:    capacity,
		IdleTimeout: 30 * time.Second,
	}).Open(newConnector(&state), nil)
	defer p.Close()

	connections := make([]*Pooled[*TestConn], 0, capacity)
	for range capacity {
		conn, err := p.Get(b.Context(), nil)
		require.NoError(b, err)
		connections = append(connections, conn)
	}

	for _, conn := range connections {
		conn.Recycle()
	}

	workerCount := runtime.GOMAXPROCS(0) * 8
	latencies := make([][]int64, workerCount)
	var stop atomic.Bool
	var getErrors atomic.Int64
	var sampleBudget atomic.Int64
	var wg sync.WaitGroup
	sampleBudget.Store(200_000)
	samplesPerWorker := 200_000/workerCount + 1024

	for i := range workerCount {
		wg.Go(func() {
			localLatencies := make([]int64, 0, samplesPerWorker)
			for !stop.Load() {
				start := time.Now()
				conn, err := p.Get(b.Context(), nil)
				elapsed := time.Since(start)
				if err != nil {
					getErrors.Add(1)
					continue
				}

				if sampleBudget.Add(-1) >= 0 {
					localLatencies = append(localLatencies, elapsed.Nanoseconds())
				}
				conn.Recycle()
			}
			latencies[i] = localLatencies
		})
	}

	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	for b.Loop() {
		p.closeIdleResources(time.Now())
	}
	b.StopTimer()

	stop.Store(true)
	wg.Wait()

	require.Zero(b, getErrors.Load())
	reportGetLatencyMetrics(b, latencies)
}

func reportGetLatencyMetrics(b *testing.B, latencies [][]int64) {
	total := 0
	for _, localLatencies := range latencies {
		total += len(localLatencies)
	}
	if total == 0 {
		return
	}

	merged := make([]int64, 0, total)
	for _, localLatencies := range latencies {
		merged = append(merged, localLatencies...)
	}
	slices.Sort(merged)

	b.ReportMetric(float64(total), "get-ops")
	b.ReportMetric(float64(merged[percentileIndex(total, 50)]), "get-p50-ns")
	b.ReportMetric(float64(merged[percentileIndex(total, 95)]), "get-p95-ns")
	b.ReportMetric(float64(merged[percentileIndex(total, 99)]), "get-p99-ns")
	b.ReportMetric(float64(merged[total-1]), "get-max-ns")
}

func percentileIndex(total int, percentile int) int {
	return ((total - 1) * percentile) / 100
}
