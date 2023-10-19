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

func TestStackRace(t *testing.T) {
	const Count = 64
	const Procs = 32

	var wg sync.WaitGroup
	var stack connStack[*StressConn]
	var done atomic.Bool

	for c := 0; c < Count; c++ {
		stack.Push(&Pooled[*StressConn]{Conn: &StressConn{}})
	}

	for i := 0; i < Procs; i++ {
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

	for c := 0; c < Count; c++ {
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

	for p := 0; p < P; p++ {
		tid := int32(p + 1)
		wg.Go(func() error {
			ctx := context.Background()
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
	if err := wg.Wait(); err != nil {
		t.Fatal(err)
	}
}
