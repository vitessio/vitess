//go:build !race

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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCloseDuringWaitForConn confirms that we do not get hung when the pool gets
// closed while we are waiting for a connection from it.
func TestCloseDuringWaitForConn(t *testing.T) {
	ctx := context.Background()
	goRoutineCnt := 50
	getTimeout := 300 * time.Millisecond

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

		fmt.Println("Starting TestCloseDuringGetAndPut")

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
