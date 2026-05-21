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
		WatchdogDelay = 30 * time.Second
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
		worker := i
		tid := int32(worker + 1)
		wg.Go(func() error {
			liveGetWorkers.Add(1)
			defer liveGetWorkers.Add(-1)

			rng := rand.New(rand.NewPCG(uint64(cycle+1), uint64(worker+1)))

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
			_ = pool.SetCapacity(ctx, int64(rng.IntN(MaxCapacity+1)))
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

	trafficStarted := assert.Eventuallyf(t, func() bool {
		return liveGetWorkers.Load() == NumWorkers && successfulGets.Load() >= NumWorkers
	}, WarmupTimeout, time.Millisecond, "cycle %d: traffic did not start: %s", cycle, status())
	if !trafficStarted {
		stop.Store(true)
		ctx, cancel := context.WithTimeout(t.Context(), CloseTimeout)
		_ = pool.CloseWithContext(ctx)
		cancel()
		waitForStressTraffic(t, cycle, &wg, WatchdogDelay, status)
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
	}, WatchdogDelay, time.Millisecond, "cycle %d: CloseWithContext stalled: %s", cycle, status())
	if !closeReturned {
		cancelClose()
		stop.Store(true)
		waitForStressTraffic(t, cycle, &wg, WatchdogDelay, status)
		require.FailNowf(t, "CloseWithContext stalled", "cycle %d: %s", cycle, status())
	}
	cancelClose()

	stop.Store(true)
	waitForStressTraffic(t, cycle, &wg, WatchdogDelay, status)

	require.NoErrorf(t, closeErr, "cycle %d: CloseWithContext failed", cycle)
	require.Falsef(t, pool.IsOpen(), "cycle %d: pool should be closed", cycle)
	require.EqualValuesf(t, 0, pool.Capacity(), "cycle %d: capacity should be 0 after Close", cycle)
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
