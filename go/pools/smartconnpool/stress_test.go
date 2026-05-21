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
