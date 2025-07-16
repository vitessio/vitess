package smartconnpool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/loov/hrtime"
)

type ConnStats struct {
	Requests int
	Reset    int
	Apply    int
}

type BenchConn struct {
	Stats ConnStats

	setting *Setting
	latency time.Duration
	closed  bool
}

func (b *BenchConn) Expired(_ time.Duration) bool {
	return false
}

func (b *BenchConn) IsSettingApplied() bool {
	return b.setting != nil
}

func (b *BenchConn) IsSameSetting(setting string) bool {
	return b.setting != nil && b.setting.ApplyQuery() == setting
}

var _ Connection = (*BenchConn)(nil)

func (b *BenchConn) ApplySetting(ctx context.Context, setting *Setting) error {
	time.Sleep(b.latency)
	b.setting = setting
	b.Stats.Apply++
	return nil
}

func (b *BenchConn) ResetSetting(ctx context.Context) error {
	time.Sleep(b.latency)
	b.setting = nil
	b.Stats.Reset++
	return nil
}

func (b *BenchConn) Setting() *Setting {
	return b.setting
}

func (b *BenchConn) IsClosed() bool {
	return b.closed
}

func (b *BenchConn) Close() {
	b.closed = true
}

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

func BenchmarkPoolWithDifferentSettings(b *testing.B) {
	var state TestState

	latency := 500 * time.Microsecond
	var mu sync.Mutex
	connstats := make([]*ConnStats, 0, 64)

	benchConnector := func(ctx context.Context) (*BenchConn, error) {
		conn := &BenchConn{latency: latency}

		mu.Lock()
		connstats = append(connstats, &conn.Stats)
		mu.Unlock()

		return conn, nil
	}

	ctx := context.Background()
	p := NewPool(&Config[*BenchConn]{
		Capacity:     30,
		MaxIdleCount: 5,
		IdleTimeout:  time.Second,
		LogWait:      state.LogWait,
	}).Open(benchConnector, nil)

	b.Cleanup(func() {
		p.Close()
	})

	b.ReportAllocs()

	b.SetParallelism(128)

	var m sync.Map
	var i atomic.Int64

	b.RunParallel(func(pb *testing.PB) {
		var setting *Setting

		run := i.Add(1)
		switch run % 3 {
		case 0:
			setting = sFoo
		case 1:
			setting = sBar
		default:
			setting = nil
		}

		var laps []time.Duration

		for pb.Next() {
			start := time.Now()
			r, err := p.Get(ctx, setting)
			laps = append(laps, time.Since(start))
			if err != nil {
				b.Fatalf("Failed to get connection: %v", err)
			}

			time.Sleep(2 * time.Millisecond) // Simulate some work
			r.Recycle()
		}

		m.Store(run, laps)
	})

	b.ReportMetric(float64(state.open.Load()), "open_connections")
	b.ReportMetric(float64(len(state.waits)), "waits")
	b.ReportMetric(float64(p.Metrics.diffSetting.Load()), "setting_changes")

	var durations []time.Duration
	m.Range(func(key, value any) bool {
		durations = append(durations, value.([]time.Duration)...)
		return true
	})

	histogram := hrtime.NewDurationHistogram(durations, &hrtime.HistogramOptions{
		BinCount:        20,
		NiceRange:       true,
		ClampMaximum:    0,
		ClampPercentile: 0.999,
	})

	fmt.Println("Histogram:", histogram)
}

func BenchmarkFastPoolWithDifferentSettings(b *testing.B) {
	var state TestState

	latency := 500 * time.Microsecond
	var mu sync.Mutex
	connstats := make([]*ConnStats, 0, 64)

	benchConnector := func(ctx context.Context) (*BenchConn, error) {
		conn := &BenchConn{latency: latency}

		mu.Lock()
		connstats = append(connstats, &conn.Stats)
		mu.Unlock()

		return conn, nil
	}

	ctx := context.Background()
	p := NewFastPool(&Config[*BenchConn]{
		Capacity:     30,
		MaxIdleCount: 5,
		IdleTimeout:  time.Second,
		LogWait:      state.LogWait,
	}).Open(benchConnector, nil)

	b.Cleanup(func() {
		p.Close()
	})

	b.ReportAllocs()

	b.SetParallelism(128)

	var m sync.Map
	var i atomic.Int64

	b.RunParallel(func(pb *testing.PB) {
		var setting *Setting

		run := i.Add(1)
		switch run % 3 {
		case 0:
			setting = sFoo
		case 1:
			setting = sBar
		default:
			setting = nil
		}

		var laps []time.Duration

		for pb.Next() {
			start := time.Now()
			r, err := p.Get(ctx, setting)
			laps = append(laps, time.Since(start))
			if err != nil {
				b.Fatalf("Failed to get connection: %v", err)
			}

			time.Sleep(2 * time.Millisecond) // Simulate some work

			r.Recycle()
		}

		m.Store(run, laps)
	})

	b.ReportMetric(float64(state.open.Load()), "open_connections")
	b.ReportMetric(float64(len(state.waits)), "waits")
	b.ReportMetric(float64(p.Metrics.diffSetting.Load()), "setting_changes")

	var durations []time.Duration
	m.Range(func(key, value any) bool {
		durations = append(durations, value.([]time.Duration)...)
		return true
	})

	histogram := hrtime.NewDurationHistogram(durations, &hrtime.HistogramOptions{
		BinCount:        20,
		NiceRange:       true,
		ClampMaximum:    0,
		ClampPercentile: 0.999,
	})

	fmt.Println("Histogram:", histogram)
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
