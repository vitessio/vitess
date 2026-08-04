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
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type StressConn struct {
	setting *Setting
	owner   atomic.Int32
	closed  atomic.Bool
}

func (b *StressConn) Expired(_ time.Duration) bool {
	return false
}

func (b *StressConn) IsSettingApplied() bool {
	return b.setting != nil
}

func (b *StressConn) IsSameSetting(setting string) bool {
	return b.setting != nil && b.setting.ApplyQuery() == setting
}

var _ Connection = (*StressConn)(nil)

func (b *StressConn) ApplySetting(ctx context.Context, setting *Setting) error {
	b.setting = setting
	return nil
}

func (b *StressConn) ResetSetting(ctx context.Context) error {
	b.setting = nil
	return nil
}

func (b *StressConn) Setting() *Setting {
	return b.setting
}

func (b *StressConn) IsClosed() bool {
	return b.closed.Load()
}

func (b *StressConn) Close() {
	b.closed.Store(true)
}

func TestStackRace(t *testing.T) {
	const Count = 64
	const Procs = 32

	var wg sync.WaitGroup
	var stack connStack[*StressConn]
	var done atomic.Bool

	for range Count {
		stack.Push(&Pooled[*StressConn]{Conn: &StressConn{}})
	}

	for i := range Procs {
		wg.Add(1)
		go func(tid int32) {
			defer wg.Done()
			for !done.Load() {
				if conn, ok := stack.Pop(); ok {
					previousOwner := conn.Conn.owner.Swap(tid)
					if previousOwner != 0 {
						panic(fmt.Errorf("owner race: %d with %d", tid, previousOwner))
					}
					runtime.Gosched()
					previousOwner = conn.Conn.owner.Swap(0)
					if previousOwner != tid {
						panic(fmt.Errorf("owner race: %d with %d", previousOwner, tid))
					}
					stack.Push(conn)
				}
			}
		}(int32(i + 1))
	}

	time.Sleep(5 * time.Second)
	done.Store(true)
	wg.Wait()

	for range Count {
		conn, ok := stack.Pop()
		require.NotNil(t, conn)
		require.True(t, ok)
	}
}

func TestStress(t *testing.T) {
	const Capacity = 64
	const P = 8

	connect := func(ctx context.Context) (*StressConn, error) {
		return &StressConn{}, nil
	}

	pool := NewPool[*StressConn](&Config[*StressConn]{
		Capacity: Capacity,
	}).Open(connect, nil)

	var wg errgroup.Group
	var stop atomic.Bool

	for p := range P {
		tid := int32(p + 1)
		wg.Go(func() error {
			ctx := t.Context()
			for !stop.Load() {
				conn, err := pool.get(ctx)
				if err != nil {
					return err
				}

				previousOwner := conn.Conn.owner.Swap(tid)
				if previousOwner != 0 {
					return fmt.Errorf("owner race: %d with %d", tid, previousOwner)
				}
				runtime.Gosched()
				previousOwner = conn.Conn.owner.Swap(0)
				if previousOwner != tid {
					return fmt.Errorf("owner race: %d with %d", previousOwner, tid)
				}
				conn.Recycle()
			}
			return nil
		})
	}

	time.Sleep(5 * time.Second)
	stop.Store(true)
	require.NoError(t, wg.Wait())
}

func TestStressFull(t *testing.T) {
	const (
		MaxCapacity      = 32
		NumWorkers       = 16
		CoverageTimeout  = 30 * time.Second
		ShutdownTimeout  = 30 * time.Second
		MinSuccessfulGet = 2000
	)

	var (
		connsMu        sync.Mutex
		allConns       []*StressConn
		successfulGets atomic.Int64
		capacitySets   atomic.Int64
		refreshChecks  atomic.Int64
	)

	connect := func(_ context.Context) (*StressConn, error) {
		c := &StressConn{}
		connsMu.Lock()
		allConns = append(allConns, c)
		connsMu.Unlock()
		return c, nil
	}
	connCount := func() int {
		connsMu.Lock()
		defer connsMu.Unlock()
		return len(allConns)
	}

	refresh := func() (bool, error) {
		return refreshChecks.Add(1)%4 == 0, nil
	}

	pool := NewPool[*StressConn](&Config[*StressConn]{
		Capacity:        MaxCapacity,
		IdleTimeout:     10 * time.Millisecond,
		RefreshInterval: 25 * time.Millisecond,
	}).Open(connect, refresh)

	settings := []*Setting{
		nil,
		NewSetting("set a=1", "set a=0"),
		NewSetting("set b=1", "set b=0"),
		NewSetting("set c=1", "set c=0"),
	}

	var (
		wg   errgroup.Group
		stop atomic.Bool
	)

	for i := range NumWorkers {
		worker := i
		tid := int32(worker + 1)
		wg.Go(func() error {
			rng := rand.New(rand.NewPCG(uint64(worker+1), uint64(worker+101)))

			for !stop.Load() {
				ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
				conn, err := pool.Get(ctx, settings[rng.IntN(len(settings))])
				cancel()
				if err != nil {
					runtime.Gosched()
					continue
				}

				previousOwner := conn.Conn.owner.Swap(tid)
				if previousOwner != 0 {
					return fmt.Errorf("conn handed out concurrently: %d still owned it when %d acquired", previousOwner, tid)
				}
				runtime.Gosched()
				previousOwner = conn.Conn.owner.Swap(0)
				if previousOwner != tid {
					return fmt.Errorf("conn owner overwritten under us: expected %d, got %d", tid, previousOwner)
				}
				successfulGets.Add(1)

				switch rng.IntN(50) {
				case 0:
					conn.Conn.closed.Store(true)
					conn.Recycle()
				case 1:
					conn.Conn.closed.Store(true)
					conn.Taint()
				default:
					conn.Recycle()
				}
			}
			return nil
		})
	}

	wg.Go(func() error {
		rng := rand.New(rand.NewPCG(1, 2))

		for !stop.Load() {
			ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
			_ = pool.SetCapacity(ctx, int64(rng.IntN(MaxCapacity+1)))
			cancel()
			capacitySets.Add(1)
			runtime.Gosched()
		}
		return nil
	})

	status := func() string {
		return fmt.Sprintf("capacity=%d active=%d borrowed=%d open=%d isOpen=%v successfulGets=%d capacitySets=%d refreshChecks=%d",
			pool.Capacity(), pool.Active(), pool.InUse(), connCount(), pool.IsOpen(), successfulGets.Load(), capacitySets.Load(), refreshChecks.Load())
	}

	coverageReached := assert.Eventuallyf(t, func() bool {
		return successfulGets.Load() >= MinSuccessfulGet &&
			capacitySets.Load() >= 10 &&
			refreshChecks.Load() >= 4
	}, CoverageTimeout, time.Millisecond, "stress coverage was not reached: %s", status())

	stop.Store(true)
	waitForStressTraffic(t, -1, &wg, ShutdownTimeout, status)

	restoreCtx, restoreCancel := context.WithTimeout(t.Context(), ShutdownTimeout)
	restoreErr := pool.SetCapacity(restoreCtx, MaxCapacity)
	restoreCancel()
	require.NoError(t, restoreErr)

	closeCtx, closeCancel := context.WithTimeout(t.Context(), ShutdownTimeout)
	closeErr := pool.CloseWithContext(closeCtx)
	closeCancel()
	require.NoError(t, closeErr)

	if !coverageReached {
		require.FailNowf(t, "stress coverage was not reached", "%s", status())
	}

	require.EqualValues(t, 0, pool.active.Load(), "active should be 0 after Close")
	require.EqualValues(t, 0, pool.borrowed.Load(), "borrowed should be 0 after Close")

	connsMu.Lock()
	defer connsMu.Unlock()

	var leaked int
	for _, c := range allConns {
		if !c.IsClosed() {
			leaked++
		}
	}
	require.Equalf(t, 0, leaked, "leaked %d connections out of %d ever opened", leaked, len(allConns))
	t.Logf("stress test exercised %d connections", len(allConns))
}

func TestStressRefreshReopenSetCapacityClose(t *testing.T) {
	const Cycles = 50

	for cycle := range Cycles {
		if !t.Run(fmt.Sprintf("cycle-%03d", cycle), func(t *testing.T) {
			runStressRefreshReopenSetCapacityCloseCycle(t, cycle)
		}) {
			return
		}
	}
}

func runStressRefreshReopenSetCapacityCloseCycle(t *testing.T, cycle int) {
	t.Helper()

	const (
		MaxCapacity   = 12
		NumWorkers    = 16
		NumCapWorkers = 3
		StartupWait   = 30 * time.Second
		CloseTimeout  = 30 * time.Second
		Watchdog      = 30 * time.Second
	)

	var (
		connsMu            sync.Mutex
		allConns           []*StressConn
		liveGetWorkers     atomic.Int64
		liveCapWorkers     atomic.Int64
		successfulGets     atomic.Int64
		refreshChecks      atomic.Int64
		connectsAfterClose atomic.Int64
		closeStarted       atomic.Bool
		capacityInProgress atomic.Bool
	)

	connect := func(_ context.Context) (*StressConn, error) {
		if closeStarted.Load() {
			connectsAfterClose.Add(1)
		}

		c := &StressConn{}
		connsMu.Lock()
		allConns = append(allConns, c)
		connsMu.Unlock()
		return c, nil
	}
	connCount := func() int {
		connsMu.Lock()
		defer connsMu.Unlock()
		return len(allConns)
	}

	refresh := func() (bool, error) {
		refreshChecks.Add(1)
		return true, nil
	}

	pool := NewPool[*StressConn](&Config[*StressConn]{
		Capacity:        MaxCapacity,
		IdleTimeout:     10 * time.Millisecond,
		RefreshInterval: time.Millisecond,
	}).Open(connect, refresh)

	settings := []*Setting{
		nil,
		NewSetting("set refresh a=1", "set refresh a=0"),
		NewSetting("set refresh b=1", "set refresh b=0"),
		NewSetting("set refresh c=1", "set refresh c=0"),
	}

	var (
		wg   errgroup.Group
		stop atomic.Bool
	)

	for i := range NumWorkers {
		worker := i
		tid := int32(worker + 1)
		wg.Go(func() error {
			liveGetWorkers.Add(1)
			defer liveGetWorkers.Add(-1)

			rng := rand.New(rand.NewPCG(uint64(cycle+1), uint64(worker+201)))

			for !stop.Load() {
				ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
				conn, err := pool.Get(ctx, settings[rng.IntN(len(settings))])
				cancel()
				if err != nil {
					runtime.Gosched()
					continue
				}
				if conn.Conn.IsClosed() {
					return fmt.Errorf("cycle %d: closed conn handed out to worker %d", cycle, tid)
				}

				previousOwner := conn.Conn.owner.Swap(tid)
				if previousOwner != 0 {
					return fmt.Errorf("cycle %d: conn handed out concurrently: %d still owned it when %d acquired", cycle, previousOwner, tid)
				}
				if rng.IntN(3) == 0 {
					runtime.Gosched()
				}
				previousOwner = conn.Conn.owner.Swap(0)
				if previousOwner != tid {
					return fmt.Errorf("cycle %d: conn owner overwritten under us: expected %d, got %d", cycle, tid, previousOwner)
				}
				successfulGets.Add(1)

				if rng.IntN(20) == 0 {
					conn.Conn.closed.Store(true)
					conn.Recycle()
					continue
				}
				conn.Recycle()
			}
			return nil
		})
	}

	for i := range NumCapWorkers {
		worker := i
		wg.Go(func() error {
			liveCapWorkers.Add(1)
			defer liveCapWorkers.Add(-1)

			rng := rand.New(rand.NewPCG(uint64(cycle+1), uint64(worker+301)))

			for !stop.Load() {
				ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
				capacityInProgress.Store(true)
				_ = pool.SetCapacity(ctx, int64(rng.IntN(MaxCapacity+1)))
				capacityInProgress.Store(false)
				cancel()
				runtime.Gosched()
			}
			return nil
		})
	}

	status := func() string {
		return fmt.Sprintf("capacity=%d active=%d borrowed=%d open=%d isOpen=%v liveGetWorkers=%d liveCapWorkers=%d successfulGets=%d refreshChecks=%d connectsAfterClose=%d capacityInProgress=%v",
			pool.Capacity(), pool.Active(), pool.InUse(), connCount(), pool.IsOpen(), liveGetWorkers.Load(), liveCapWorkers.Load(), successfulGets.Load(), refreshChecks.Load(), connectsAfterClose.Load(), capacityInProgress.Load())
	}

	started := assert.Eventuallyf(t, func() bool {
		return liveGetWorkers.Load() == NumWorkers &&
			liveCapWorkers.Load() == NumCapWorkers &&
			successfulGets.Load() >= NumWorkers &&
			refreshChecks.Load() > 0
	}, StartupWait, time.Millisecond, "cycle %d: stress workers did not start: %s", cycle, status())
	if !started {
		stop.Store(true)
		waitForStressTraffic(t, cycle, &wg, Watchdog, status)
		closeCtx, closeCancel := context.WithTimeout(t.Context(), CloseTimeout)
		_ = pool.CloseWithContext(closeCtx)
		closeCancel()
		require.FailNowf(t, "stress workers did not start", "cycle %d: %s", cycle, status())
	}

	closeCtx, cancelClose := context.WithTimeout(t.Context(), CloseTimeout)
	closeDone := make(chan error, 1)
	closeStarted.Store(true)
	go func() {
		closeDone <- pool.CloseWithContext(closeCtx)
	}()

	var closeErr error
	closeReturned := assert.Eventuallyf(t, func() bool {
		select {
		case closeErr = <-closeDone:
			return true
		default:
			return false
		}
	}, Watchdog, time.Millisecond, "cycle %d: CloseWithContext stalled during refresh/capacity churn: %s", cycle, status())
	cancelClose()

	stop.Store(true)

	if !closeReturned {
		require.FailNowf(t, "CloseWithContext stalled during refresh/capacity churn", "cycle %d: %s", cycle, status())
	}
	waitForStressTraffic(t, cycle, &wg, Watchdog, status)

	require.NoErrorf(t, closeErr, "cycle %d: CloseWithContext failed during refresh/capacity churn: %s", cycle, status())
	require.Falsef(t, pool.IsOpen(), "cycle %d: pool should be closed", cycle)
	require.EqualValuesf(t, 0, pool.Capacity(), "cycle %d: capacity should be 0 after Close: %s", cycle, status())
	require.EqualValuesf(t, 0, pool.Active(), "cycle %d: active should be 0 after Close: %s", cycle, status())
	require.EqualValuesf(t, 0, pool.InUse(), "cycle %d: borrowed should be 0 after Close: %s", cycle, status())

	finalStatus := status()
	connsMu.Lock()
	defer connsMu.Unlock()

	var leaked int
	for _, c := range allConns {
		if !c.IsClosed() {
			leaked++
		}
	}
	require.Equalf(t, 0, leaked, "cycle %d: leaked %d connections out of %d ever opened; %s",
		cycle, leaked, len(allConns), finalStatus)
}

// TestStressCloseDuringReconnectStorm exercises CloseWithContext racing
// against in-flight reconnects. Workers churn conns (Get / Recycle, plus
// occasional Taint), and a storm of Taints is launched with the test's
// connect() blocked, so several background reconnects are parked inside
// connNew when Close is called. The test verifies that:
//
//   - Close returns (does not deadlock waiting on parked reconnects),
//   - no closed conn is handed to a worker,
//   - no conn opened by the pool is leaked (every StressConn the connect
//     callback ever produced ends up Closed),
//   - capacity / active / inUse all settle to zero.
//
// Each cycle is a fresh pool; the loop runs many cycles to surface
// scheduling-dependent races.
func TestStressCloseDuringReconnectStorm(t *testing.T) {
	const Cycles = 25

	for cycle := range Cycles {
		if !t.Run(fmt.Sprintf("cycle-%03d", cycle), func(t *testing.T) {
			runStressCloseDuringReconnectStormCycle(t, cycle)
		}) {
			return
		}
	}
}

// TestStressWaiterStormDuringDrain exercises CloseWithContext while a crowd
// of goroutines are queued on the waitlist. The pool is filled to capacity
// by the test (held conns), NumWaiters callers pile up on pool.Get, and
// then Close is invoked. The held conns are Recycled mid-drain. The test
// verifies that:
//
//   - Close completes during the drain,
//   - the drain does not hand any returned conn to a queued waiter
//     (waiterHandoffs stays 0 — waiters must error out instead),
//   - no conn opened by the pool is leaked,
//   - capacity / active / inUse all settle to zero.
//
// Each cycle is a fresh pool; the loop runs many cycles to surface
// scheduling-dependent races.
func TestStressWaiterStormDuringDrain(t *testing.T) {
	const Cycles = 25

	for cycle := range Cycles {
		if !t.Run(fmt.Sprintf("cycle-%03d", cycle), func(t *testing.T) {
			runStressWaiterStormDuringDrainCycle(t, cycle)
		}) {
			return
		}
	}
}

func runStressWaiterStormDuringDrainCycle(t *testing.T, cycle int) {
	t.Helper()

	const (
		Capacity     = 4
		NumWaiters   = 32
		CloseTimeout = 30 * time.Second
		Watchdog     = 30 * time.Second
	)

	var (
		connsMu        sync.Mutex
		allConns       []*StressConn
		liveWaiters    atomic.Int64
		waiterErrors   atomic.Int64
		waiterHandoffs atomic.Int64
	)

	connect := func(_ context.Context) (*StressConn, error) {
		c := &StressConn{}
		connsMu.Lock()
		allConns = append(allConns, c)
		connsMu.Unlock()
		return c, nil
	}
	connCount := func() int {
		connsMu.Lock()
		defer connsMu.Unlock()
		return len(allConns)
	}

	pool := NewPool[*StressConn](&Config[*StressConn]{
		Capacity: Capacity,
	}).Open(connect, nil)

	var held []*Pooled[*StressConn]
	for range Capacity {
		conn, err := pool.Get(t.Context(), nil)
		require.NoError(t, err)
		held = append(held, conn)
	}

	releaseWaiters := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseWaiters)
		})
	}
	t.Cleanup(release)

	var wg errgroup.Group
	for i := range NumWaiters {
		tid := int32(i + 1)
		wg.Go(func() error {
			liveWaiters.Add(1)
			defer liveWaiters.Add(-1)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			conn, err := pool.Get(ctx, nil)
			cancel()
			if err != nil {
				waiterErrors.Add(1)
				return nil
			}

			waiterHandoffs.Add(1)
			previousOwner := conn.Conn.owner.Swap(tid)
			if previousOwner != 0 {
				return fmt.Errorf("cycle %d: waiter handoff gave conn owned by %d to %d", cycle, previousOwner, tid)
			}

			<-releaseWaiters

			previousOwner = conn.Conn.owner.Swap(0)
			if previousOwner != tid {
				return fmt.Errorf("cycle %d: waiter owner overwritten under us: expected %d, got %d", cycle, tid, previousOwner)
			}
			conn.Recycle()
			return nil
		})
	}

	status := func() string {
		return fmt.Sprintf("capacity=%d active=%d borrowed=%d open=%d isOpen=%v waiting=%d liveWaiters=%d waiterErrors=%d waiterHandoffs=%d",
			pool.Capacity(), pool.Active(), pool.InUse(), connCount(), pool.IsOpen(), pool.wait.waiting(), liveWaiters.Load(), waiterErrors.Load(), waiterHandoffs.Load())
	}

	// abort tears down on a watchdog trip. If closeCancel is non-nil a close
	// goroutine is already running and we unblock it via cancellation;
	// otherwise we drive a fresh CloseWithContext.
	abort := func(closeCancel context.CancelFunc) {
		if closeCancel != nil {
			closeCancel()
		}
		release()
		for _, conn := range held {
			conn.Recycle()
		}
		if closeCancel == nil {
			closeCtx, cancel := context.WithTimeout(t.Context(), Watchdog)
			_ = pool.CloseWithContext(closeCtx)
			cancel()
		}
		waitForStressTraffic(t, cycle, &wg, Watchdog, status)
	}

	waitersQueued := assert.Eventuallyf(t, func() bool {
		return pool.wait.waiting() == NumWaiters
	}, Watchdog, time.Millisecond, "cycle %d: waiters did not queue before drain: %s", cycle, status())
	if !waitersQueued {
		abort(nil)
		require.FailNowf(t, "waiters did not queue before drain", "cycle %d: %s", cycle, status())
	}

	closeCtx, cancelClose := context.WithTimeout(t.Context(), CloseTimeout)
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- pool.CloseWithContext(closeCtx)
	}()

	closeStarted := assert.Eventuallyf(t, func() bool {
		return pool.Capacity() == 0
	}, Watchdog, time.Millisecond, "cycle %d: close did not start draining: %s", cycle, status())
	if !closeStarted {
		abort(cancelClose)
		require.FailNowf(t, "close did not start draining", "cycle %d: %s", cycle, status())
	}

	for _, conn := range held {
		conn.Recycle()
	}

	var closeErr error
	tryReceiveClose := func() bool {
		select {
		case closeErr = <-closeDone:
			return true
		default:
			return false
		}
	}

	closeReturned := assert.Eventuallyf(t, tryReceiveClose,
		Watchdog, time.Millisecond, "cycle %d: CloseWithContext stalled during waiter drain: %s", cycle, status())
	if !closeReturned {
		cancelClose()
		release()
		closeReturnedAfterRelease := assert.Eventuallyf(t, tryReceiveClose,
			Watchdog, time.Millisecond, "cycle %d: CloseWithContext did not unblock after releasing waiter handoffs: %s", cycle, status())
		waitForStressTraffic(t, cycle, &wg, Watchdog, status)
		if !closeReturnedAfterRelease {
			require.FailNowf(t, "CloseWithContext did not unblock after releasing waiter handoffs", "cycle %d: %s", cycle, status())
		}
		require.FailNowf(t, "CloseWithContext stalled during waiter drain", "cycle %d: closeErr=%v %s", cycle, closeErr, status())
	}
	cancelClose()

	release()
	waitForStressTraffic(t, cycle, &wg, Watchdog, status)

	require.NoErrorf(t, closeErr, "cycle %d: CloseWithContext failed during waiter drain: %s", cycle, status())
	require.Falsef(t, pool.IsOpen(), "cycle %d: pool should be closed", cycle)
	require.EqualValuesf(t, 0, pool.Capacity(), "cycle %d: capacity should be 0 after Close", cycle)
	require.EqualValuesf(t, 0, pool.Active(), "cycle %d: active should be 0 after Close", cycle)
	require.EqualValuesf(t, 0, pool.InUse(), "cycle %d: borrowed should be 0 after Close", cycle)
	require.EqualValuesf(t, 0, waiterHandoffs.Load(), "cycle %d: close drain should not hand returned conns to waiters", cycle)

	finalStatus := status()
	connsMu.Lock()
	defer connsMu.Unlock()

	var leaked int
	for _, c := range allConns {
		if !c.IsClosed() {
			leaked++
		}
	}
	require.Equalf(t, 0, leaked, "cycle %d: leaked %d connections out of %d ever opened; %s",
		cycle, leaked, len(allConns), finalStatus)
}

func runStressCloseDuringReconnectStormCycle(t *testing.T, cycle int) {
	t.Helper()

	const (
		MaxCapacity  = 8
		NumWorkers   = 8
		CloseTimeout = 30 * time.Second
		Watchdog     = 30 * time.Second
	)

	var (
		connsMu                   sync.Mutex
		allConns                  []*StressConn
		blockReconnect            atomic.Bool
		closeStarted              atomic.Bool
		liveGetWorkers            atomic.Int64
		liveReconnectWorkers      atomic.Int64
		successfulGets            atomic.Int64
		blockedConnects           atomic.Int64
		blockedBackgroundConnects atomic.Int64
		canceledConnects          atomic.Int64
		completedBlockedConnects  atomic.Int64
		connectsAfterClose        atomic.Int64
	)

	releaseReconnect := make(chan struct{})
	var releaseReconnectOnce sync.Once
	release := func() {
		releaseReconnectOnce.Do(func() {
			close(releaseReconnect)
		})
	}
	t.Cleanup(release)

	connect := func(ctx context.Context) (*StressConn, error) {
		if closeStarted.Load() {
			connectsAfterClose.Add(1)
		}
		if blockReconnect.Load() {
			blockedConnects.Add(1)
			if _, ok := ctx.Deadline(); !ok {
				blockedBackgroundConnects.Add(1)
			}
			select {
			case <-releaseReconnect:
			case <-ctx.Done():
				canceledConnects.Add(1)
				completedBlockedConnects.Add(1)
				return nil, ctx.Err()
			}
			completedBlockedConnects.Add(1)
		}

		c := &StressConn{}
		connsMu.Lock()
		allConns = append(allConns, c)
		connsMu.Unlock()
		return c, nil
	}
	connCount := func() int {
		connsMu.Lock()
		defer connsMu.Unlock()
		return len(allConns)
	}

	pool := NewPool[*StressConn](&Config[*StressConn]{
		Capacity:    MaxCapacity,
		IdleTimeout: 10 * time.Millisecond,
		MaxLifetime: 10 * time.Millisecond,
	}).Open(connect, nil)

	settings := []*Setting{
		nil,
		NewSetting("set reconnect a=1", "set reconnect a=0"),
		NewSetting("set reconnect b=1", "set reconnect b=0"),
	}

	var (
		wg   errgroup.Group
		stop atomic.Bool
	)

	stormConns := make([]*Pooled[*StressConn], 0, MaxCapacity/2)
	for range MaxCapacity / 2 {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		conn, err := pool.Get(ctx, nil)
		cancel()
		require.NoError(t, err)
		stormConns = append(stormConns, conn)
	}

	for i := range NumWorkers {
		tid := int32(i + 1)
		wg.Go(func() error {
			liveGetWorkers.Add(1)
			defer liveGetWorkers.Add(-1)

			rng := rand.New(rand.NewPCG(uint64(cycle+1), uint64(i+101)))

			for !stop.Load() {
				ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
				conn, err := pool.Get(ctx, settings[rng.IntN(len(settings))])
				cancel()
				if err != nil {
					runtime.Gosched()
					continue
				}
				if conn.Conn.IsClosed() {
					return fmt.Errorf("cycle %d: closed conn handed out to worker %d", cycle, tid)
				}
				successfulGets.Add(1)

				previousOwner := conn.Conn.owner.Swap(tid)
				if previousOwner != 0 {
					return fmt.Errorf("cycle %d: conn handed out concurrently: %d still owned it when %d acquired", cycle, previousOwner, tid)
				}
				if rng.IntN(3) == 0 {
					runtime.Gosched()
				}
				previousOwner = conn.Conn.owner.Swap(0)
				if previousOwner != tid {
					return fmt.Errorf("cycle %d: conn owner overwritten under us: expected %d, got %d", cycle, tid, previousOwner)
				}

				switch rng.IntN(10) {
				case 0:
					conn.Conn.closed.Store(true)
					conn.Taint()
				case 1:
					conn.Conn.closed.Store(true)
					conn.Recycle()
				default:
					conn.Recycle()
				}
			}
			return nil
		})
	}

	status := func() string {
		return fmt.Sprintf("capacity=%d active=%d borrowed=%d open=%d isOpen=%v liveGetWorkers=%d liveReconnectWorkers=%d blockedConnects=%d blockedBackgroundConnects=%d canceledConnects=%d completedBlockedConnects=%d connectsAfterClose=%d",
			pool.Capacity(), pool.Active(), pool.InUse(), connCount(), pool.IsOpen(), liveGetWorkers.Load(), liveReconnectWorkers.Load(), blockedConnects.Load(), blockedBackgroundConnects.Load(), canceledConnects.Load(), completedBlockedConnects.Load(), connectsAfterClose.Load())
	}

	abort := func() {
		stop.Store(true)
		release()
		closeCtx, cancel := context.WithTimeout(t.Context(), Watchdog)
		_ = pool.CloseWithContext(closeCtx)
		cancel()
		waitForStressTraffic(t, cycle, &wg, Watchdog, status)
	}

	trafficStarted := assert.Eventuallyf(t, func() bool {
		return liveGetWorkers.Load() == NumWorkers && successfulGets.Load() >= NumWorkers
	}, Watchdog, time.Millisecond, "cycle %d: traffic did not start before reconnect storm: %s", cycle, status())
	if !trafficStarted {
		abort()
		require.FailNowf(t, "traffic did not start before reconnect storm", "cycle %d: %s", cycle, status())
	}

	blockReconnect.Store(true)

	// Taint() synchronously calls pool.put(nil) → connNew → connect, which
	// blocks on releaseReconnect once blockReconnect is set. Fan the storm
	// out into goroutines so the main goroutine isn't pinned in the first
	// Taint() and can proceed to start the close. release() (deferred via
	// t.Cleanup, also called on the success path) unblocks them.
	for _, conn := range stormConns {
		wg.Go(func() error {
			liveReconnectWorkers.Add(1)
			defer liveReconnectWorkers.Add(-1)

			conn.Conn.closed.Store(true)
			conn.Taint()
			return nil
		})
	}

	reconnectBlocked := assert.Eventuallyf(t, func() bool {
		return blockedBackgroundConnects.Load() > 0
	}, Watchdog, time.Millisecond, "cycle %d: reconnect storm did not block any no-deadline reconnect: %s", cycle, status())
	if !reconnectBlocked {
		abort()
		require.FailNowf(t, "reconnect storm did not block any no-deadline reconnect", "cycle %d: %s", cycle, status())
	}

	closeCtx, cancelClose := context.WithTimeout(t.Context(), CloseTimeout)
	closeDone := make(chan error, 1)
	closeStarted.Store(true)
	go func() {
		closeDone <- pool.CloseWithContext(closeCtx)
	}()

	var closeErr error
	tryReceiveClose := func() bool {
		select {
		case closeErr = <-closeDone:
			return true
		default:
			return false
		}
	}

	closeReturned := assert.Eventuallyf(t, tryReceiveClose,
		Watchdog, time.Millisecond, "cycle %d: CloseWithContext stalled during reconnect storm: %s", cycle, status())
	if !closeReturned {
		cancelClose()
		stop.Store(true)
		release()
		closeReturnedAfterRelease := assert.Eventuallyf(t, tryReceiveClose,
			Watchdog, time.Millisecond, "cycle %d: CloseWithContext did not unblock after releasing reconnects: %s", cycle, status())
		waitForStressTraffic(t, cycle, &wg, Watchdog, status)
		if !closeReturnedAfterRelease {
			require.FailNowf(t, "CloseWithContext did not unblock after releasing reconnects", "cycle %d: %s", cycle, status())
		}
		require.FailNowf(t, "CloseWithContext stalled during reconnect storm", "cycle %d: closeErr=%v %s", cycle, closeErr, status())
	}
	cancelClose()

	stop.Store(true)
	release()
	waitForStressTraffic(t, cycle, &wg, Watchdog, status)

	require.NoErrorf(t, closeErr, "cycle %d: CloseWithContext failed during reconnect storm: %s", cycle, status())
	require.Falsef(t, pool.IsOpen(), "cycle %d: pool should be closed", cycle)
	require.EqualValuesf(t, 0, pool.Capacity(), "cycle %d: capacity should be 0 after Close", cycle)
	require.EqualValuesf(t, 0, pool.Active(), "cycle %d: active should be 0 after Close", cycle)
	require.EqualValuesf(t, 0, pool.InUse(), "cycle %d: borrowed should be 0 after Close", cycle)

	finalStatus := status()
	connsMu.Lock()
	defer connsMu.Unlock()

	var leaked int
	for _, c := range allConns {
		if !c.IsClosed() {
			leaked++
		}
	}
	require.Equalf(t, 0, leaked, "cycle %d: leaked %d connections out of %d ever opened; %s",
		cycle, leaked, len(allConns), finalStatus)
}

// TestStressCloseDuringTraffic exercises CloseWithContext racing against
// active workload: NumWorkers goroutines churn conns (Get / Recycle, plus
// occasional Taint), a separate goroutine repeatedly resizes the pool via
// SetCapacity, and then Close is invoked. The test verifies that:
//
//   - Close returns (does not deadlock),
//   - no closed conn is handed to a worker,
//   - no conn opened by the pool is leaked,
//   - active / inUse settle to zero.
//
// Each cycle is a fresh pool; the loop runs many cycles to surface
// scheduling-dependent races.
func TestStressCloseDuringTraffic(t *testing.T) {
	const Cycles = 100

	for cycle := range Cycles {
		if !t.Run(fmt.Sprintf("cycle-%03d", cycle), func(t *testing.T) {
			runStressCloseDuringTrafficCycle(t, cycle)
		}) {
			return
		}
	}
}

func runStressCloseDuringTrafficCycle(t *testing.T, cycle int) {
	t.Helper()

	const (
		MaxCapacity   = 16
		NumWorkers    = 24
		WarmupTimeout = 30 * time.Second
		CloseTimeout  = 30 * time.Second
		Watchdog      = 30 * time.Second
	)

	var (
		connsMu            sync.Mutex
		allConns           []*StressConn
		closeStarted       atomic.Bool
		liveGetWorkers     atomic.Int64
		successfulGets     atomic.Int64
		connectsAfterClose atomic.Int64
		capacityInProgress atomic.Bool
	)

	connect := func(_ context.Context) (*StressConn, error) {
		if closeStarted.Load() {
			connectsAfterClose.Add(1)
		}

		c := &StressConn{}
		connsMu.Lock()
		allConns = append(allConns, c)
		connsMu.Unlock()
		return c, nil
	}
	connCount := func() int {
		connsMu.Lock()
		defer connsMu.Unlock()
		return len(allConns)
	}

	var refreshCount atomic.Int32
	refresh := func() (bool, error) {
		return refreshCount.Add(1)%2 == 0, nil
	}

	pool := NewPool[*StressConn](&Config[*StressConn]{
		Capacity:        MaxCapacity,
		IdleTimeout:     10 * time.Millisecond,
		RefreshInterval: 2 * time.Millisecond,
	}).Open(connect, refresh)

	settings := []*Setting{
		nil,
		NewSetting("set a=1", "set a=0"),
		NewSetting("set b=1", "set b=0"),
		NewSetting("set c=1", "set c=0"),
	}

	var (
		wg   errgroup.Group
		stop atomic.Bool
	)

	for i := range NumWorkers {
		tid := int32(i + 1)
		wg.Go(func() error {
			liveGetWorkers.Add(1)
			defer liveGetWorkers.Add(-1)

			rng := rand.New(rand.NewPCG(uint64(cycle+1), uint64(i+1)))

			for !stop.Load() {
				ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
				conn, err := pool.Get(ctx, settings[rng.IntN(len(settings))])
				cancel()
				if err != nil {
					runtime.Gosched()
					continue
				}
				if conn.Conn.IsClosed() {
					return fmt.Errorf("cycle %d: closed conn handed out to worker %d", cycle, tid)
				}
				successfulGets.Add(1)

				previousOwner := conn.Conn.owner.Swap(tid)
				if previousOwner != 0 {
					return fmt.Errorf("cycle %d: conn handed out concurrently: %d still owned it when %d acquired", cycle, previousOwner, tid)
				}
				if rng.IntN(4) == 0 {
					runtime.Gosched()
				}
				previousOwner = conn.Conn.owner.Swap(0)
				if previousOwner != tid {
					return fmt.Errorf("cycle %d: conn owner overwritten under us: expected %d, got %d", cycle, tid, previousOwner)
				}

				switch rng.IntN(50) {
				case 0:
					conn.Conn.closed.Store(true)
					conn.Recycle()
				case 1:
					conn.Conn.closed.Store(true)
					conn.Taint()
				default:
					conn.Recycle()
				}
			}
			return nil
		})
	}

	wg.Go(func() error {
		rng := rand.New(rand.NewPCG(uint64(cycle+1), uint64(NumWorkers+1)))

		for !stop.Load() {
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			capacityInProgress.Store(true)
			_ = pool.SetCapacity(ctx, int64(rng.IntN(MaxCapacity)+1))
			capacityInProgress.Store(false)
			cancel()
			time.Sleep(time.Duration(rng.IntN(5)) * time.Millisecond)
		}
		return nil
	})

	status := func() string {
		return fmt.Sprintf("capacity=%d active=%d borrowed=%d open=%d isOpen=%v liveGetWorkers=%d capacityInProgress=%v connectsAfterClose=%d",
			pool.Capacity(), pool.Active(), pool.InUse(), connCount(), pool.IsOpen(), liveGetWorkers.Load(), capacityInProgress.Load(), connectsAfterClose.Load())
	}

	// abort tears down on a watchdog trip. If closeCancel is non-nil a close
	// goroutine is already running and we unblock it via cancellation;
	// otherwise we drive a fresh CloseWithContext.
	abort := func(closeCancel context.CancelFunc) {
		if closeCancel != nil {
			closeCancel()
		}
		stop.Store(true)
		if closeCancel == nil {
			ctx, cancel := context.WithTimeout(t.Context(), Watchdog)
			_ = pool.CloseWithContext(ctx)
			cancel()
		}
		waitForStressTraffic(t, cycle, &wg, Watchdog, status)
	}

	trafficStarted := assert.Eventuallyf(t, func() bool {
		return liveGetWorkers.Load() == NumWorkers && successfulGets.Load() >= NumWorkers
	}, WarmupTimeout, time.Millisecond, "cycle %d: traffic did not start: %s", cycle, status())
	if !trafficStarted {
		abort(nil)
		require.FailNowf(t, "traffic did not start", "cycle %d: %s", cycle, status())
	}

	closeCtx, cancelClose := context.WithTimeout(t.Context(), CloseTimeout)
	closeDone := make(chan error, 1)
	closeStarted.Store(true)
	go func() {
		closeDone <- pool.CloseWithContext(closeCtx)
	}()

	var closeErr error
	closeReturned := assert.Eventuallyf(t, func() bool {
		select {
		case closeErr = <-closeDone:
			return true
		default:
			return false
		}
	}, Watchdog, time.Millisecond, "cycle %d: CloseWithContext stalled: %s", cycle, status())
	if !closeReturned {
		abort(cancelClose)
		require.FailNowf(t, "CloseWithContext stalled", "cycle %d: %s", cycle, status())
	}
	cancelClose()

	stop.Store(true)
	waitForStressTraffic(t, cycle, &wg, Watchdog, status)

	require.NoErrorf(t, closeErr, "cycle %d: CloseWithContext failed", cycle)
	require.Falsef(t, pool.IsOpen(), "cycle %d: pool should be closed", cycle)
	require.EqualValuesf(t, 0, pool.Active(), "cycle %d: active should be 0 after Close", cycle)
	require.EqualValuesf(t, 0, pool.InUse(), "cycle %d: borrowed should be 0 after Close", cycle)

	connsMu.Lock()
	defer connsMu.Unlock()

	var leaked int
	for _, c := range allConns {
		if !c.IsClosed() {
			leaked++
		}
	}
	require.Equalf(t, 0, leaked, "cycle %d: leaked %d connections out of %d ever opened; connectsAfterClose=%d",
		cycle, leaked, len(allConns), connectsAfterClose.Load())
}

// TestStressWaiterTimeoutStorm reproduces a production vttablet stall. The
// anatomy of the incident, taken from a goroutine dump of the stalled
// tablet:
//
//   - the stream pool was at capacity and a large backlog of requests was
//     queued on the waitlist; their deadlines expired while they waited.
//   - an expired waiter cannot leave the waitlist instantly: it must
//     reacquire the contended waitlist mutex to remove itself, so at any
//     given moment the list held thousands of expired-but-still-listed
//     waiters.
//   - every returned connection was handed to the front-most (expired)
//     waiter; the returner then sat blocked in the unbuffered channel send
//     until its dead target crawled through the mutex queue to perform the
//     fallback receive. The dead request then "completed" holding a
//     connection it could not use, failed, and recycled it to the next
//     expired waiter.
//   - net effect: connections churned through dead requests while live
//     requests starved; useful throughput was zero.
//
// The test recreates that state with real waiters and real returners. The
// waitlist mutex, held by the test, plays the role of the convoyed mutex:
// the returners are parked on it first, then the queued waiters' contexts
// are cancelled so they pile up behind. When the test releases the mutex,
// the returners scan a waitlist full of expired-but-listed waiters — the
// exact moment the production bug fired.
//
// The assertions are the customer-visible contract and do not depend on
// timing: no request whose context has already expired may be handed a
// connection (wastedHandoffs must stay zero), and every live request must
// be served. Each cycle is a fresh pool; the loop runs several cycles to
// surface scheduling-dependent races.
func TestStressWaiterTimeoutStorm(t *testing.T) {
	const Cycles = 25

	for cycle := range Cycles {
		if !t.Run(fmt.Sprintf("cycle-%03d", cycle), func(t *testing.T) {
			runStressWaiterTimeoutStormCycle(t, cycle)
		}) {
			return
		}
	}
}

func runStressWaiterTimeoutStormCycle(t *testing.T, cycle int) {
	t.Helper()

	const (
		Capacity     = 4
		NumExpired   = 64
		NumLive      = 8
		GetsPerLive  = 4
		LiveTimeout  = 30 * time.Second
		Watchdog     = 30 * time.Second
		CloseTimeout = 30 * time.Second
	)

	var (
		connsMu         sync.Mutex
		allConns        []*StressConn
		wastedHandoffs  atomic.Int64
		expiredTimedOut atomic.Int64
		liveSuccesses   atomic.Int64
	)

	connect := func(_ context.Context) (*StressConn, error) {
		c := &StressConn{}
		connsMu.Lock()
		allConns = append(allConns, c)
		connsMu.Unlock()
		return c, nil
	}
	connCount := func() int {
		connsMu.Lock()
		defer connsMu.Unlock()
		return len(allConns)
	}

	pool := NewPool[*StressConn](&Config[*StressConn]{
		Capacity: Capacity,
	}).Open(connect, nil)

	// Hold every conn so all subsequent Gets queue on the waitlist.
	var held []*Pooled[*StressConn]
	for range Capacity {
		conn, err := pool.Get(t.Context(), nil)
		require.NoError(t, err)
		held = append(held, conn)
	}

	var wg errgroup.Group

	// The backlog: real requests whose deadline will expire while they are
	// queued. Until the held conns are recycled nothing can be handed to
	// them, so any successful Get here is a connection delivered to a
	// request whose context had already expired — the production bug.
	expiredCtx, cancelExpired := context.WithCancel(t.Context())
	defer cancelExpired()
	for range NumExpired {
		wg.Go(func() error {
			conn, err := pool.Get(expiredCtx, nil)
			if err != nil {
				expiredTimedOut.Add(1)
				return nil
			}
			if expiredCtx.Err() != nil {
				wastedHandoffs.Add(1)
			}
			// The production query path fails on the dead context and
			// recycles; do the same so the connection churns onward.
			conn.Recycle()
			return nil
		})
	}

	status := func() string {
		return fmt.Sprintf("capacity=%d active=%d borrowed=%d open=%d isOpen=%v waiting=%d wastedHandoffs=%d expiredTimedOut=%d liveSuccesses=%d",
			pool.Capacity(), pool.Active(), pool.InUse(), connCount(), pool.IsOpen(), pool.wait.waiting(), wastedHandoffs.Load(), expiredTimedOut.Load(), liveSuccesses.Load())
	}

	backlogQueued := assert.Eventuallyf(t, func() bool {
		return pool.wait.waiting() == NumExpired
	}, Watchdog, time.Millisecond, "cycle %d: backlog did not queue: %s", cycle, status())
	if !backlogQueued {
		cancelExpired()
		for _, conn := range held {
			conn.Recycle()
		}
		waitForStressTraffic(t, cycle, &wg, Watchdog, status)
		require.FailNowf(t, "backlog did not queue", "cycle %d: %s", cycle, status())
	}

	// Live traffic queued behind the backlog, with deadlines generous
	// enough to never expire during the test.
	for i := range NumLive {
		tid := int32(i + 1)
		wg.Go(func() error {
			for range GetsPerLive {
				ctx, cancel := context.WithTimeout(t.Context(), LiveTimeout)
				conn, err := pool.Get(ctx, nil)
				cancel()
				if err != nil {
					return fmt.Errorf("cycle %d: live request starved by the expired backlog: %w", cycle, err)
				}

				previousOwner := conn.Conn.owner.Swap(tid)
				if previousOwner != 0 {
					return fmt.Errorf("cycle %d: conn handed out concurrently: %d still owned it when %d acquired", cycle, previousOwner, tid)
				}
				runtime.Gosched()
				previousOwner = conn.Conn.owner.Swap(0)
				if previousOwner != tid {
					return fmt.Errorf("cycle %d: conn owner overwritten under us: expected %d, got %d", cycle, tid, previousOwner)
				}
				liveSuccesses.Add(1)
				conn.Recycle()
			}
			return nil
		})
	}

	liveQueued := assert.Eventuallyf(t, func() bool {
		return pool.wait.waiting() == NumExpired+NumLive
	}, Watchdog, time.Millisecond, "cycle %d: live traffic did not queue behind the backlog: %s", cycle, status())
	if !liveQueued {
		cancelExpired()
		for _, conn := range held {
			conn.Recycle()
		}
		waitForStressTraffic(t, cycle, &wg, Watchdog, status)
		require.FailNowf(t, "live traffic did not queue behind the backlog", "cycle %d: %s", cycle, status())
	}

	// The convoy. With the waitlist mutex held by the test, park the
	// returners on it, then expire the entire backlog so it piles up
	// behind them. Releasing the mutex lets the returners scan first,
	// while every backlog waiter is still expired-but-listed.
	pool.wait.mu.Lock()

	for _, conn := range held {
		wg.Go(func() error {
			conn.Recycle()
			return nil
		})
	}

	// Recycle decrements the borrowed count before reaching for the
	// waitlist mutex, so once InUse hits zero every returner is parked on
	// (or a few instructions away from) the mutex queue.
	returnersParked := assert.Eventuallyf(t, func() bool {
		return pool.InUse() == 0
	}, Watchdog, time.Millisecond, "cycle %d: returners did not park on the waitlist mutex: %s", cycle, status())
	if !returnersParked {
		pool.wait.mu.Unlock()
		cancelExpired()
		waitForStressTraffic(t, cycle, &wg, Watchdog, status)
		require.FailNowf(t, "returners did not park on the waitlist mutex", "cycle %d: %s", cycle, status())
	}
	for range 100 {
		runtime.Gosched()
	}

	cancelExpired()
	// Give the expired waiters a moment to wake and pile up on the mutex.
	// This only biases the interleaving towards the production one; the
	// assertions below hold for every interleaving.
	for range 100 {
		runtime.Gosched()
	}

	pool.wait.mu.Unlock()

	// Everything settles on its own: the backlog drains (timing out or —
	// on broken code — receiving wasted handoffs), then the live traffic
	// is served.
	waitForStressTraffic(t, cycle, &wg, Watchdog, status)

	require.Zerof(t, wastedHandoffs.Load(),
		"cycle %d: connections were handed to requests whose context had already expired: %s", cycle, status())
	require.EqualValuesf(t, NumLive*GetsPerLive, liveSuccesses.Load(),
		"cycle %d: live traffic was not fully served: %s", cycle, status())

	closeCtx, cancelClose := context.WithTimeout(t.Context(), CloseTimeout)
	closeErr := pool.CloseWithContext(closeCtx)
	cancelClose()
	require.NoErrorf(t, closeErr, "cycle %d: CloseWithContext failed: %s", cycle, status())

	require.EqualValuesf(t, 0, pool.Active(), "cycle %d: active should be 0 after Close", cycle)
	require.EqualValuesf(t, 0, pool.InUse(), "cycle %d: borrowed should be 0 after Close", cycle)

	finalStatus := status()
	connsMu.Lock()
	defer connsMu.Unlock()

	var leaked int
	for _, c := range allConns {
		if !c.IsClosed() {
			leaked++
		}
	}
	require.Equalf(t, 0, leaked, "cycle %d: leaked %d connections out of %d ever opened; %s",
		cycle, leaked, len(allConns), finalStatus)
}

func waitForStressTraffic(t *testing.T, cycle int, wg *errgroup.Group, timeout time.Duration, status func() string) {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		done <- wg.Wait()
	}()

	var err error
	trafficStopped := assert.Eventuallyf(t, func() bool {
		select {
		case err = <-done:
			return true
		default:
			return false
		}
	}, timeout, time.Millisecond, "cycle %d: traffic workers did not stop: %s", cycle, status())
	if !trafficStopped {
		require.FailNowf(t, "traffic workers did not stop", "cycle %d: %s", cycle, status())
	}
	require.NoErrorf(t, err, "cycle %d: traffic worker failed", cycle)
}
