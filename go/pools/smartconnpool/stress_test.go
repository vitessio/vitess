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
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

type stressAccounting struct {
	nextID, connectAttempts, open, closed atomic.Int64
}

type accountingStressConn struct {
	mu      sync.Mutex
	setting *Setting
	owner   atomic.Int32
	closed  atomic.Bool
	state   *stressAccounting
	id      int64
}

var _ Connection = (*accountingStressConn)(nil)

func (c *accountingStressConn) ApplySetting(ctx context.Context, setting *Setting) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setting = setting
	return nil
}

func (c *accountingStressConn) ResetSetting(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setting = nil
	return nil
}

func (c *accountingStressConn) Setting() *Setting {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setting
}

func (c *accountingStressConn) IsClosed() bool {
	return c.closed.Load()
}

func (c *accountingStressConn) Close() {
	if c.closed.CompareAndSwap(false, true) {
		c.state.open.Add(-1)
		c.state.closed.Add(1)
	}
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

func TestStressLifecycleCapacityAccounting(t *testing.T) {
	const (
		maxCapacity = int64(8)
		clients     = 16
		runFor      = 2 * time.Second
	)

	var state stressAccounting
	connect := func(ctx context.Context) (*accountingStressConn, error) {
		attempt := state.connectAttempts.Add(1)
		delay := time.Duration(attempt%4) * time.Millisecond
		if delay != 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return nil, context.Cause(ctx)
			case <-timer.C:
			}
		}

		state.open.Add(1)
		return &accountingStressConn{
			state: &state,
			id:    state.nextID.Add(1),
		}, nil
	}

	pool := NewPool[*accountingStressConn](&Config[*accountingStressConn]{
		Capacity:     maxCapacity,
		MaxIdleCount: maxCapacity / 2,
		MaxWaiters:   uint(clients * 2),
	}).Open(connect, nil)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	defer pool.Close()

	settings := []*Setting{
		nil,
		NewSetting("set stress_a=1", "reset stress_a"),
		NewSetting("set stress_b=1", "reset stress_b"),
	}

	var stop atomic.Bool
	var wg errgroup.Group

	for client := range clients {
		tid := int32(client + 1)
		wg.Go(func() error {
			rng := rand.New(rand.NewPCG(uint64(tid), uint64(tid)+1))
			for !stop.Load() {
				setting := settings[rng.IntN(len(settings))]
				conn, err := pool.Get(ctx, setting)
				if err != nil {
					if ctx.Err() != nil ||
						errors.Is(err, ErrConnPoolClosed) ||
						errors.Is(err, ErrTimeout) ||
						errors.Is(err, ErrPoolWaiterCapReached) {
						runtime.Gosched()
						continue
					}
					return err
				}

				previousOwner := conn.Conn.owner.Swap(tid)
				if previousOwner != 0 {
					conn.Recycle()
					return fmt.Errorf("owner race on conn %d: %d with %d", conn.Conn.id, tid, previousOwner)
				}
				if got := conn.Conn.Setting(); got != setting {
					conn.Conn.owner.Store(0)
					conn.Recycle()
					return fmt.Errorf("conn %d setting mismatch: got %p want %p", conn.Conn.id, got, setting)
				}

				for range rng.IntN(3) + 1 {
					runtime.Gosched()
				}

				previousOwner = conn.Conn.owner.Swap(0)
				if previousOwner != tid {
					conn.Recycle()
					return fmt.Errorf("owner race on conn %d: %d released by %d", conn.Conn.id, previousOwner, tid)
				}
				conn.Recycle()
			}
			return nil
		})
	}

	wg.Go(func() error {
		rng := rand.New(rand.NewPCG(0x5151, 0x9191))
		for !stop.Load() {
			switch rng.IntN(10) {
			case 0:
				pool.Close()
				if !stop.Load() {
					pool.Open(connect, nil)
				}
			default:
				if err := pool.SetCapacity(ctx, int64(rng.IntN(int(maxCapacity+1)))); err != nil {
					return err
				}
			}

			timer := time.NewTimer(time.Duration(rng.IntN(5)+1) * time.Millisecond)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return nil
			case <-timer.C:
			}
		}
		return nil
	})

	time.Sleep(runFor)
	stop.Store(true)
	cancel()
	require.NoError(t, wg.Wait())

	require.Eventually(t, func() bool {
		return pool.InUse() == 0 &&
			pool.pendingOpen.Load() == 0 &&
			pool.connecting.Load() == 0 &&
			pool.wait.waiting() == 0 &&
			pool.Active() == state.open.Load()
	}, 30*time.Second, 10*time.Millisecond)

	pool.Close()
	require.EqualValues(t, 0, pool.InUse())
	require.EqualValues(t, 0, pool.Active())
	require.EqualValues(t, 0, pool.pendingOpen.Load())
	require.EqualValues(t, 0, pool.connecting.Load())
	require.EqualValues(t, 0, pool.wait.waiting())

	// Leak check: every connection successfully returned by the connector must
	// have been closed exactly once after the final pool close.
	created := state.nextID.Load()
	closed := state.closed.Load()
	require.EqualValues(t, 0, state.open.Load())
	require.Equal(t, created, closed)
}
