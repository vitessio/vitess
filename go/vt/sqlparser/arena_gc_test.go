/*
Copyright 2024 The Vitess Authors.

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

package sqlparser

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

// gcStats captures GC-related metrics for a workload run.
type gcStats struct {
	NumGC       uint32
	PauseTotalNs uint64
	MaxPauseNs  uint64
	HeapInuse   uint64
	HeapObjects uint64
	Duration    time.Duration
}

func (s gcStats) String() string {
	return fmt.Sprintf("GCs=%d  PauseTotal=%s  MaxPause=%s  HeapInUse=%.1fMB  HeapObjects=%d",
		s.NumGC,
		time.Duration(s.PauseTotalNs),
		time.Duration(s.MaxPauseNs),
		float64(s.HeapInuse)/(1024*1024),
		s.HeapObjects,
	)
}

// measureGC runs fn and returns GC statistics collected during its execution.
// It forces a GC before starting to get a clean baseline.
func measureGC(fn func()) gcStats {
	// Force GC and get baseline stats.
	runtime.GC()
	debug.FreeOSMemory()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	start := time.Now()
	fn()
	elapsed := time.Since(start)

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	numGC := after.NumGC - before.NumGC

	// Find max pause during our run window.
	var maxPause uint64
	// PauseNs is a circular buffer of size 256.
	idx := int(after.NumGC % 256)
	for i := uint32(0); i < numGC && i < 256; i++ {
		pauseIdx := (idx - int(i) - 1 + 256) % 256
		p := after.PauseNs[pauseIdx]
		if p > maxPause {
			maxPause = p
		}
	}

	return gcStats{
		NumGC:        numGC,
		PauseTotalNs: after.PauseTotalNs - before.PauseTotalNs,
		MaxPauseNs:   maxPause,
		HeapInuse:    after.HeapInuse,
		HeapObjects:  after.HeapObjects,
		Duration:     elapsed,
	}
}

// TestArenaGCImpact measures the GC impact of parsing with and without arena
// allocation. It parses a large number of queries and compares GC pause times,
// cycle counts, and heap object counts.
//
// It tests under both default GOGC and aggressive GOGC=10 to highlight the
// impact under high GC pressure (e.g., memory-constrained environments).
func TestArenaGCImpact(t *testing.T) {
	parser := NewTestParser()
	queries := loadQueries(t, "lobsters.sql.gz")
	if len(queries) > 10000 {
		queries = queries[:10000]
	}

	const iterations = 50

	for _, gogc := range []int{100, 10} {
		t.Run(fmt.Sprintf("GOGC=%d", gogc), func(t *testing.T) {
			old := debug.SetGCPercent(gogc)
			defer debug.SetGCPercent(old)

			t.Run("heap", func(t *testing.T) {
				stats := measureGC(func() {
					for iter := 0; iter < iterations; iter++ {
						for _, q := range queries {
							_, _ = parser.Parse(q)
						}
					}
				})
				t.Logf("Heap:  %s  WallTime=%s", stats, stats.Duration)
			})

			t.Run("arena", func(t *testing.T) {
				stats := measureGC(func() {
					for iter := 0; iter < iterations; iter++ {
						for _, q := range queries {
							result, err := parser.Parse2WithArena(q)
							if err == nil {
								result.Release()
							}
						}
					}
				})
				t.Logf("Arena: %s  WallTime=%s", stats, stats.Duration)
			})
		})
	}
}

// BenchmarkGCPressure measures GC pressure by tracking the number of heap
// objects alive after parsing. Fewer heap objects means less GC scanning work.
func BenchmarkGCPressure(b *testing.B) {
	parser := NewTestParser()
	const sql = "select aaaa, bbb, ccc, ddd, eeee, ffff, gggg, hhhh, iiii from tttt, ttt1, ttt3 where aaaa = bbbb and bbbb = cccc and dddd+1 = eeee group by fff, gggg having hhhh = iiii and iiii = jjjj order by kkkk, llll limit 3, 4"

	b.Run("heap", func(b *testing.B) {
		// Hold onto parsed results to simulate real workloads where ASTs
		// survive beyond a single function scope (e.g., query plan caching).
		results := make([]Statement, 0, 1000)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			stmt, err := parser.Parse(sql)
			if err != nil {
				b.Fatal(err)
			}
			if len(results) < cap(results) {
				results = append(results, stmt)
			}
		}
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		b.ReportMetric(float64(m.HeapObjects), "heap-objects")
		b.ReportMetric(float64(m.NumGC), "total-gcs")
		_ = results
	})

	b.Run("arena", func(b *testing.B) {
		// Hold onto ParseResults to simulate real workloads.
		results := make([]*ParseResult, 0, 1000)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := parser.Parse2WithArena(sql)
			if err != nil {
				b.Fatal(err)
			}
			if len(results) < cap(results) {
				results = append(results, result)
			} else {
				result.Release()
			}
		}
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		b.ReportMetric(float64(m.HeapObjects), "heap-objects")
		b.ReportMetric(float64(m.NumGC), "total-gcs")
		for _, r := range results {
			r.Release()
		}
	})
}

// BenchmarkGCSteadyState measures parsing throughput and GC overhead in a
// steady-state workload where parsed ASTs are immediately released, simulating
// a query proxy that parses, inspects, and discards queries.
func BenchmarkGCSteadyState(b *testing.B) {
	parser := NewTestParser()
	queries := loadQueries(b, "lobsters.sql.gz")
	if len(queries) > 5000 {
		queries = queries[:5000]
	}

	b.Run("heap", func(b *testing.B) {
		runtime.GC()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q := queries[i%len(queries)]
			_, _ = parser.Parse(q)
		}
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		b.ReportMetric(float64(m.PauseTotalNs)/float64(b.N), "gc-pause-ns/op")
	})

	b.Run("arena", func(b *testing.B) {
		runtime.GC()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q := queries[i%len(queries)]
			result, err := parser.Parse2WithArena(q)
			if err == nil {
				result.Release()
			}
		}
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		b.ReportMetric(float64(m.PauseTotalNs)/float64(b.N), "gc-pause-ns/op")
	})
}
