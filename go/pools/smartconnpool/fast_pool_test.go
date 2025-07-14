package smartconnpool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFastPoolGetAndPut(t *testing.T) {
	var state TestState

	ctx := context.Background()
	p := NewFastPool(&Config[*TestConn]{
		Capacity:     1,
		MaxIdleCount: 1,
		IdleTimeout:  time.Second,
		LogWait:      state.LogWait,
	}).Open(newConnector(&state), nil)

	var closed = atomic.Bool{}

	wg := sync.WaitGroup{}

	fmt.Println("Starting TestCloseDuringGetAndPut")
	var count atomic.Int64

	// Spawn multiple goroutines to perform Get and Put operations, but only
	// allow connections to be checked out until `closed` has been set to true.
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			for !closed.Load() {
				r, err := p.Get(ctx, nil)
				if err != nil {
					break
				}

				count.Add(1)

				r.Recycle()
			}

			wg.Done()
		}()
	}

	// Allow some time for goroutines to start and attempt to get connections.
	time.Sleep(1000 * time.Millisecond)

	// Close the pool, which should allow all goroutines to finish.
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()

	// err := p.CloseWithContext(ctx)
	// closed.Store(true)

	// fmt.Println("Count of connections checked out:", count.Load())
	// require.NoError(t, err, "Failed to close pool")

	fmt.Println("Count of connections checked out:", count.Load())
	fmt.Println("Number of waits", len(state.waits))

	// // Wait for a short time to ensure all goroutines have completed.
	wg.Wait()

	// // time.Sleep(1000 * time.Millisecond)

	// // Check that the pool is closed and no connections are available.
	// assert.EqualValues(t, 0, p.Capacity())
	// assert.EqualValues(t, 0, p.Available())

	// if state.open.Load() != 0 {
	// 	t.Errorf("Expected no open connections, but found %d", state.open.Load())
	// }

	// require.EqualValues(t, 0, state.open.Load())
}

func TestFastPoolWithDifferentSettings(t *testing.T) {
	var state TestState

	ctx := context.Background()
	p := NewFastPool(&Config[*TestConn]{
		Capacity:     30,
		MaxIdleCount: 5,
		IdleTimeout:  time.Second,
		LogWait:      state.LogWait,
	}).Open(newConnector(&state), nil)

	var closed = atomic.Bool{}
	wg := sync.WaitGroup{}

	fmt.Println("Starting TestFastPoolWithDifferentSettings")
	var count atomic.Int64
	// Spawn multiple goroutines to perform Get and Put operations, but only
	// allow connections to be checked out until `closed` has been set to true.
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			for !closed.Load() {
				var setting *Setting
				switch i % 3 {
				case 0:
					setting = sFoo
				case 1:
					setting = sBar
				default:
					setting = nil
				}

				r, err := p.Get(ctx, setting)
				if err != nil {
					break
				}

				count.Add(1)

				r.Recycle()
			}
			wg.Done()
		}()
	}

	// Allow some time for goroutines to start and attempt to get connections.
	time.Sleep(1000 * time.Millisecond)

	// Close the pool, which should allow all goroutines to finish.
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()

	// err := p.CloseWithContext(ctx)
	closed.Store(true)

	fmt.Println("Count of connections checked out:", count.Load())
	// require.NoError(t, err, "Failed to close pool")

	fmt.Println("Number of waits", len(state.waits))

	fmt.Println("Count of setting changes:", p.Metrics.diffSetting.Load())

	fmt.Println("Count of connections opened:", state.open.Load())

	// Wait for a short time to ensure all goroutines have completed.
	wg.Wait()

	// // Check that the pool is closed and no connections are available.
	// assert.EqualValues(t, 0, p.Capacity())
	// assert.EqualValues(t, 0, p.Available())
}
