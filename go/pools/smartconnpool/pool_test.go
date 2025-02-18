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
	waits                      []time.Time
	mu                         sync.Mutex

	chaos struct {
		delayConnect time.Duration
		failConnect  bool
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
		return fmt.Errorf("ApplySetting failed")
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
		if state.chaos.failConnect {
			return nil, fmt.Errorf("failed to connect: forced failure")
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

	ctx := context.Background()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	var resources [10]*Pooled[*TestConn]
	var r *Pooled[*TestConn]
	var err error

	// Test Get
	for i := 0; i < 5; i++ {
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
		for i := 0; i < 5; i++ {
			if i%2 == 0 {
				r, err = p.Get(ctx, nil)
			} else {
				r, err = p.Get(ctx, sFoo)
			}
			require.NoError(t, err)
			resources[i] = r
		}
		for i := 0; i < 5; i++ {
			p.put(resources[i])
		}
		close(done)
	}()
	for i := 0; i < 5; i++ {
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

	for i := 0; i < 5; i++ {
		if i%2 == 0 {
			r, err = p.Get(ctx, nil)
		} else {
			r, err = p.Get(ctx, sFoo)
		}
		require.NoError(t, err)
		resources[i] = r
	}
	for i := 0; i < 5; i++ {
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

	for i := 0; i < 6; i++ {
		if i%2 == 0 {
			r, err = p.Get(ctx, nil)
		} else {
			r, err = p.Get(ctx, sFoo)
		}
		require.NoError(t, err)
		resources[i] = r
	}
	for i := 0; i < 6; i++ {
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

	ctx := context.Background()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	var resources [10]*Pooled[*TestConn]
	// Leave one empty slot in the pool
	for i := 0; i < 4; i++ {
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
		err := p.SetCapacity(ctx, 3)
		require.NoError(t, err)

		done <- true
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
	for i := 0; i < 10; i++ {
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
	for i := 0; i < 3; i++ {
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
	for i := 0; i < 3; i++ {
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
		r, err := p.Get(ctx, nil)
		require.NoError(t, err)
		p.put(r)
		done <- true
	}()

	// This will also wait
	go func() {
		err := p.SetCapacity(ctx, 2)
		require.NoError(t, err)
		done <- true
	}()
	time.Sleep(10 * time.Millisecond)

	// This should not hang
	for i := 0; i < 3; i++ {
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
	for i := 0; i < 3; i++ {
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
		r, err := p.Get(ctx, nil)
		require.NoError(t, err)
		p.put(r)
		done <- true
	}()
	time.Sleep(10 * time.Millisecond)

	// This will wait till we Put
	go func() {
		err := p.SetCapacity(ctx, 2)
		require.NoError(t, err)
	}()
	time.Sleep(10 * time.Millisecond)
	go func() {
		err := p.SetCapacity(ctx, 4)
		require.NoError(t, err)
	}()
	time.Sleep(10 * time.Millisecond)

	// This should not hang
	for i := 0; i < 3; i++ {
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

	ctx := context.Background()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	var resources [10]*Pooled[*TestConn]
	for i := 0; i < 5; i++ {
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
	for i := 0; i < 5; i++ {
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

	ctx := context.Background()
	p := NewPool(&Config[*TestConn]{
		Capacity:        5,
		IdleTimeout:     time.Second,
		LogWait:         state.LogWait,
		RefreshInterval: 500 * time.Millisecond,
	}).Open(newConnector(&state), func() (bool, error) {
		refreshed.Store(true)
		return true, nil
	})

	var resources [10]*Pooled[*TestConn]
	for i := 0; i < 5; i++ {
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

	for i := 0; i < 5; i++ {
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

func TestUserClosing(t *testing.T) {
	var state TestState

	ctx := context.Background()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	var resources [5]*Pooled[*TestConn]
	for i := 0; i < 5; i++ {
		var err error
		resources[i], err = p.Get(ctx, nil)
		require.NoError(t, err)
	}

	for _, r := range resources[:4] {
		r.Recycle()
	}

	ch := make(chan error)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err := p.CloseWithContext(ctx)
		ch <- err
		close(ch)
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatalf("Pool did not shutdown after 5s")
	case err := <-ch:
		require.Error(t, err)
		t.Logf("Shutdown error: %v", err)
	}
}

func TestIdleTimeout(t *testing.T) {
	testTimeout := func(t *testing.T, setting *Setting) {
		var state TestState

		ctx := context.Background()
		p := NewPool(&Config[*TestConn]{
			Capacity:    5,
			IdleTimeout: 10 * time.Millisecond,
			LogWait:     state.LogWait,
		}).Open(newConnector(&state), nil)

		defer p.Close()

		var conns []*Pooled[*TestConn]
		for i := 0; i < 5; i++ {
			r, err := p.Get(ctx, setting)
			require.NoError(t, err)
			assert.EqualValues(t, i+1, state.open.Load())
			assert.EqualValues(t, 0, p.Metrics.IdleClosed())

			conns = append(conns, r)
		}

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
				t.Fatalf("Connections remain open after 1 second")
			}
		}

		// no need to assert anything: all the connections in the pool should are idle-closed
		// now and if they're not the test will timeout and fail
	}

	t.Run("WithoutSettings", func(t *testing.T) { testTimeout(t, nil) })
	t.Run("WithSettings", func(t *testing.T) { testTimeout(t, sFoo) })
}

func TestIdleTimeoutCreateFail(t *testing.T) {
	var state TestState
	var connector = newConnector(&state)

	ctx := context.Background()
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
		state.chaos.failConnect = true
		p.put(r)
		timeout := time.After(1 * time.Second)
		for p.Active() != 0 {
			select {
			case <-timeout:
				t.Errorf("Timed out waiting for resource to be closed by idle timeout")
			default:
			}
		}
		// reset factory for next run.
		state.chaos.failConnect = false
	}
}

func TestMaxLifetime(t *testing.T) {
	var state TestState

	ctx := context.Background()
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
	var connector = newConnector(&state)
	var config = &Config[*TestConn]{
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
	for i := 0; i < 10; i++ {
		p = NewPool(config).Open(connector, nil)
		assert.LessOrEqual(t, config.MaxLifetime, p.extendedMaxLifetime())
		assert.Greater(t, 2*config.MaxLifetime, p.extendedMaxLifetime())
		p.Close()
	}
}

// TestMaxIdleCount tests the MaxIdleCount setting, to check if the pool closes
// the idle connections when the number of idle connections exceeds the limit.
func TestMaxIdleCount(t *testing.T) {
	testMaxIdleCount := func(t *testing.T, setting *Setting, maxIdleCount int64, expClosedConn int) {
		var state TestState

		ctx := context.Background()
		p := NewPool(&Config[*TestConn]{
			Capacity:     5,
			MaxIdleCount: maxIdleCount,
			LogWait:      state.LogWait,
		}).Open(newConnector(&state), nil)

		defer p.Close()

		var conns []*Pooled[*TestConn]
		for i := 0; i < 5; i++ {
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
	state.chaos.failConnect = true

	ctx := context.Background()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	for _, setting := range []*Setting{nil, sFoo} {
		if _, err := p.Get(ctx, setting); err.Error() != "failed to connect: forced failure" {
			t.Errorf("Expecting Failed, received %v", err)
		}
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
	var connector = newConnector(&state)

	ctx := context.Background()
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
		state.chaos.failConnect = true
		p.put(nil)
		assert.Zero(t, p.Active())

		// change back for next iteration.
		state.chaos.failConnect = false
	}
}

func TestSlowCreateFail(t *testing.T) {
	var state TestState
	state.chaos.delayConnect = 10 * time.Millisecond

	ctx := context.Background()
	ch := make(chan *Pooled[*TestConn])

	for _, setting := range []*Setting{nil, sFoo} {
		p := NewPool(&Config[*TestConn]{
			Capacity:    2,
			IdleTimeout: time.Second,
			LogWait:     state.LogWait,
		}).Open(newConnector(&state), nil)

		state.chaos.failConnect = true

		for i := 0; i < 3; i++ {
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

		state.chaos.failConnect = false
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

	ctx := context.Background()
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
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Second))
		_, err := p.Get(ctx, setting)
		cancel()
		require.EqualError(t, err, "connection pool context already expired")
	}
}

func TestMultiSettings(t *testing.T) {
	var state TestState

	ctx := context.Background()
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
	for i := 0; i < 5; i++ {
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
		for i := 0; i < 5; i++ {
			r, err = p.Get(ctx, settings[i])
			require.NoError(t, err)
			resources[i] = r
		}
		for i := 0; i < 5; i++ {
			p.put(resources[i])
		}
		ch <- true
	}()
	for i := 0; i < 5; i++ {
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

	ctx := context.Background()
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
	for i := 0; i < 5; i++ {
		r, err = p.Get(ctx, settings[i])
		require.NoError(t, err)
		resources[i] = r
		assert.EqualValues(t, 5-i-1, p.Available())
		assert.EqualValues(t, i+1, state.lastID.Load())
		assert.EqualValues(t, i+1, state.open.Load())
	}

	// Put all of them back
	for i := 0; i < 5; i++ {
		p.put(resources[i])
	}

	// Getting all with same setting.
	for i := 0; i < 5; i++ {
		r, err = p.Get(ctx, settings[1]) // {foo}
		require.NoError(t, err)
		assert.Truef(t, r.Conn.setting == settings[1], "setting was not properly applied")
		resources[i] = r
	}
	assert.EqualValues(t, 2, state.reset.Load()) // when setting was {bar} and getting for {foo}
	assert.EqualValues(t, 0, p.Available())
	assert.EqualValues(t, 5, state.lastID.Load())
	assert.EqualValues(t, 5, state.open.Load())

	for i := 0; i < 5; i++ {
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

	ctx := context.Background()
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
	for i := 0; i < 5; i++ {
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
	for i := 0; i < 5; i++ {
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
	for i := 0; i < 5; i++ {
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

	ctx := context.Background()
	p := NewPool(&Config[*TestConn]{
		Capacity:    5,
		IdleTimeout: time.Second,
		LogWait:     state.LogWait,
	}).Open(newConnector(&state), nil)

	var resources [10]*Pooled[*TestConn]

	// Ensure we have a pool with 5 available resources
	for i := 0; i < 5; i++ {
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

	for i := 0; i < 5; i++ {
		p.put(resources[i])
	}

	assert.EqualValues(t, 5, p.Available())
	assert.EqualValues(t, 5, p.Active())
	assert.EqualValues(t, 0, p.InUse())

	for i := 0; i < 2000; i++ {
		wg := sync.WaitGroup{}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		errs := make(chan error, 80)

		for j := 0; j < 80; j++ {
			wg.Add(1)

			go func() {
				defer wg.Done()
				r, err := p.Get(ctx, nil)
				defer p.put(r)

				if err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()

		if len(errs) > 0 {
			t.Errorf("Error getting connection: %v", <-errs)
		}

		close(errs)
	}
}
