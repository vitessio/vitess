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

package benchmarking_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat/distuv"

	"vitess.io/vitess/go/pools/smartconnpool"
	pools "vitess.io/vitess/go/pools/smartconnpool/benchmarking/legacy"
)

type Request struct {
	Delay    time.Duration
	Duration time.Duration
	Setting  int
}

type ConnStats struct {
	Requests int
	Reset    int
	Apply    int
}

type BenchConn struct {
	Stats ConnStats

	setting *smartconnpool.Setting
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

var _ smartconnpool.Connection = (*BenchConn)(nil)
var _ pools.Resource = (*BenchConn)(nil)

func (b *BenchConn) ApplySetting(ctx context.Context, setting *smartconnpool.Setting) error {
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

func (b *BenchConn) Setting() *smartconnpool.Setting {
	return b.setting
}

func (b *BenchConn) IsClosed() bool {
	return b.closed
}

func (b *BenchConn) Close() {
	b.closed = true
}

type Trace []Request
type Perform func(ctx context.Context, setting *smartconnpool.Setting, delay time.Duration)

type Benchmark struct {
	t        testing.TB
	name     string
	trace    Trace
	settings []*smartconnpool.Setting
	latency  time.Duration

	wg          sync.WaitGroup
	progress    atomic.Int64
	concurrent  atomic.Int64
	concurrency []int64

	mu        sync.Mutex
	waits     []time.Duration
	connstats []*ConnStats
}

func NewBenchmark(t testing.TB, name string, opts *TraceOptions) *Benchmark {
	bench := &Benchmark{
		t:       t,
		name:    name,
		trace:   opts.Generate(),
		latency: opts.Latency,
	}

	bench.settings = append(bench.settings, nil)
	for i := 1; i < len(opts.Settings); i++ {
		bench.settings = append(bench.settings, smartconnpool.NewSetting(fmt.Sprintf("set setting%d=1", i), ""))
	}

	return bench
}

func (b *Benchmark) displayProgress(done <-chan struct{}, total int) {
	tick1 := time.NewTicker(time.Second)
	defer tick1.Stop()

	tick2 := time.NewTicker(100 * time.Millisecond)
	defer tick2.Stop()

	for {
		select {
		case <-done:
			return
		case <-tick1.C:
			count := b.progress.Load()
			b.t.Logf("benchmark: %d/%d (%.02f%%), concurrency = %v", count, total, 100*float64(count)/float64(total), b.concurrency[len(b.concurrency)-1])

		case <-tick2.C:
			b.concurrency = append(b.concurrency, b.concurrent.Load())
		}
	}
}

func (b *Benchmark) run(perform Perform) {
	trace := b.trace

	b.progress.Store(0)
	b.concurrent.Store(0)
	b.waits = make([]time.Duration, 0, len(trace))
	b.connstats = make([]*ConnStats, 0, 64)
	b.concurrency = nil

	done := make(chan struct{})
	go b.displayProgress(done, len(trace))

	b.wg.Add(len(trace))

	for _, req := range trace {
		b.progress.Add(1)
		time.Sleep(req.Delay)

		go func(req Request) {
			b.concurrent.Add(1)
			defer func() {
				b.concurrent.Add(-1)
				b.wg.Done()
			}()

			start := time.Now()
			perform(context.Background(), b.settings[req.Setting], req.Duration)
			wait := time.Since(start) - req.Duration

			b.mu.Lock()
			b.waits = append(b.waits, wait)
			b.mu.Unlock()
		}(req)
	}

	b.wg.Wait()
	close(done)
}

func (b *Benchmark) waitTotal() (t time.Duration) {
	for _, w := range b.waits {
		t += w
	}
	return
}

type InternalStatistics struct {
	Capacity   int
	WaitCount  int64
	WaitTime   time.Duration
	DiffCount  int64
	ResetCount int64
}

type Statistics struct {
	Connections []*ConnStats
	Waits       []time.Duration
	Trace       []Request

	Settings int
	Internal InternalStatistics
}

func (b *Benchmark) serialize(suffix string, internal *InternalStatistics) {
	stats := &Statistics{
		Connections: b.connstats,
		Waits:       b.waits,
		Trace:       b.trace,
		Settings:    len(b.settings),
		Internal:    *internal,
	}

	f, err := os.Create(b.name + "_pool_" + suffix + ".json")
	require.NoError(b.t, err)
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	enc.Encode(stats)

	b.t.Logf("written %s", f.Name())
}

func (b *Benchmark) ResourcePool(capacity int) {
	factory := func(ctx context.Context) (pools.Resource, error) {
		conn := &BenchConn{latency: b.latency}

		b.mu.Lock()
		b.connstats = append(b.connstats, &conn.Stats)
		b.mu.Unlock()

		return conn, nil
	}
	pool := pools.NewResourcePool(factory, capacity, capacity, 0, 0, nil, nil, 0)

	perform := func(ctx context.Context, setting *smartconnpool.Setting, delay time.Duration) {
		conn, err := pool.Get(context.Background(), setting)
		if err != nil {
			panic(err)
		}

		conn.(*BenchConn).Stats.Requests++
		time.Sleep(delay)
		pool.Put(conn)
	}

	b.run(perform)
	b.serialize("before", &InternalStatistics{
		Capacity:   capacity,
		WaitCount:  pool.WaitCount(),
		WaitTime:   pool.WaitTime(),
		DiffCount:  pool.DiffSettingCount(),
		ResetCount: pool.ResetSettingCount(),
	})
}

func (b *Benchmark) SmartConnPool(capacity int) {
	connect := func(ctx context.Context) (*BenchConn, error) {
		conn := &BenchConn{latency: b.latency}

		b.mu.Lock()
		b.connstats = append(b.connstats, &conn.Stats)
		b.mu.Unlock()

		return conn, nil
	}

	pool := smartconnpool.NewPool(&smartconnpool.Config[*BenchConn]{
		Capacity: int64(capacity),
	}).Open(connect, nil)

	perform := func(ctx context.Context, setting *smartconnpool.Setting, delay time.Duration) {
		conn, err := pool.Get(context.Background(), setting)
		if err != nil {
			panic(err)
		}

		conn.Conn.Stats.Requests++
		time.Sleep(delay)
		conn.Recycle()
	}

	b.run(perform)
	b.serialize("after", &InternalStatistics{
		Capacity:   capacity,
		WaitCount:  pool.Metrics.WaitCount(),
		WaitTime:   pool.Metrics.WaitTime(),
		DiffCount:  pool.Metrics.DiffSettingCount(),
		ResetCount: pool.Metrics.ResetSettingCount(),
	})
}

type TraceOptions struct {
	RequestsPerSecond int
	DecayRate         float64
	Duration          time.Duration
	Latency           time.Duration
	Settings          []float64
}

func (opt *TraceOptions) arrivalTimes() (out []time.Duration) {
	var t time.Duration
	for t < opt.Duration {
		currentRate := float64(opt.RequestsPerSecond) * math.Exp(-opt.DecayRate*t.Seconds())
		interArrivalTime := time.Duration((rand.ExpFloat64() / currentRate) * float64(time.Second))
		if interArrivalTime >= opt.Duration {
			continue
		}

		out = append(out, interArrivalTime)
		t += interArrivalTime
	}
	return
}

func weightedDraw(p []float64, n int) []int {
	// Initialization: create the discrete CDF
	// We know that cdf is sorted in ascending order
	cdf := make([]float64, len(p))
	floats.CumSum(cdf, p)
	// Generation:
	// 1. Generate a uniformly-random value x in the range [0,1)
	// 2. Using a binary search, find the index of the smallest element in cdf larger than x
	var val float64
	indices := make([]int, n)
	for i := range indices {
		// multiply the sample with the largest CDF value; easier than normalizing to [0,1)
		val = distuv.UnitUniform.Rand() * cdf[len(cdf)-1]
		// Search returns the smallest index i such that cdf[i] > val
		indices[i] = sort.Search(len(cdf), func(i int) bool { return cdf[i] > val })
	}
	return indices
}

func (opt *TraceOptions) Generate() Trace {
	times := opt.arrivalTimes()

	var settings []int
	if len(opt.Settings) > 1 {
		settings = weightedDraw(opt.Settings, len(times))
	}

	durations := distuv.Pareto{
		Xm:    float64(opt.Latency),
		Alpha: 1,
	}

	var trace []Request
	for i := range times {
		req := Request{}
		req.Delay = times[i]
		req.Duration = time.Duration(durations.Rand())
		for req.Duration > opt.Duration/4 {
			req.Duration = time.Duration(durations.Rand())
		}
		if settings != nil {
			req.Setting = settings[i]
		}

		trace = append(trace, req)
	}
	return trace
}

func TestPoolPerformance(t *testing.T) {
	t.Skipf("skipping load tests...")

	t.Run("Contended", func(t *testing.T) {
		opt := TraceOptions{
			RequestsPerSecond: 100,
			DecayRate:         0.01,
			Duration:          30 * time.Second,
			Latency:           15 * time.Millisecond,
			Settings:          []float64{5, 1, 1, 1},
		}

		bench := NewBenchmark(t, "contended", &opt)
		bench.ResourcePool(8)
		bench.SmartConnPool(8)
	})

	t.Run("Uncontended", func(t *testing.T) {
		opt := TraceOptions{
			RequestsPerSecond: 20,
			DecayRate:         0.01,
			Duration:          30 * time.Second,
			Latency:           15 * time.Millisecond,
			Settings:          []float64{5, 1, 1, 1, 1, 1},
		}

		bench := NewBenchmark(t, "uncontended", &opt)
		bench.ResourcePool(16)
		bench.SmartConnPool(16)
	})

	t.Run("Uncontended Without Settings", func(t *testing.T) {
		opt := TraceOptions{
			RequestsPerSecond: 20,
			DecayRate:         0.01,
			Duration:          30 * time.Second,
			Latency:           15 * time.Millisecond,
			Settings:          []float64{5, 1},
		}

		bench := NewBenchmark(t, "uncontended_no_settings", &opt)
		bench.ResourcePool(16)
		bench.SmartConnPool(16)
	})

	t.Run("Points", func(t *testing.T) {
		opt := TraceOptions{
			RequestsPerSecond: 2000,
			DecayRate:         0.01,
			Duration:          30 * time.Second,
			Latency:           2 * time.Millisecond,
			Settings:          []float64{5, 2, 1, 1},
		}

		bench := NewBenchmark(t, "points", &opt)
		bench.ResourcePool(16)
		bench.SmartConnPool(16)
	})
}

func BenchmarkGetPut(b *testing.B) {
	connLegacy := func(context.Context) (pools.Resource, error) {
		return &BenchConn{}, nil
	}
	connSmart := func(ctx context.Context) (*BenchConn, error) {
		return &BenchConn{}, nil
	}

	for _, size := range []int{64, 128, 512} {
		for _, parallelism := range []int{1, 8, 32, 128} {
			rName := fmt.Sprintf("x%d-cap%d", parallelism, size)

			b.Run("Legacy/"+rName, func(b *testing.B) {
				pool := pools.NewResourcePool(connLegacy, size, size, 0, 0, nil, nil, 0)
				defer pool.Close()

				b.ReportAllocs()
				b.SetParallelism(parallelism)
				b.RunParallel(func(pb *testing.PB) {
					var ctx = context.Background()
					for pb.Next() {
						if conn, err := pool.Get(ctx, nil); err != nil {
							b.Error(err)
						} else {
							pool.Put(conn)
						}
					}
				})
			})

			b.Run("Smart/"+rName, func(b *testing.B) {
				pool := smartconnpool.NewPool[*BenchConn](&smartconnpool.Config[*BenchConn]{
					Capacity: int64(size),
				}).Open(connSmart, nil)

				defer pool.Close()

				b.ReportAllocs()
				b.SetParallelism(parallelism)
				b.RunParallel(func(pb *testing.PB) {
					var ctx = context.Background()
					for pb.Next() {
						if conn, err := pool.Get(ctx, nil); err != nil {
							b.Error(err)
						} else {
							conn.Recycle()
						}
					}
				})
			})
		}
	}
}
