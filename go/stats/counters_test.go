/*
Copyright 2019 The Vitess Authors.

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

package stats

import (
	"expvar"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCounters(t *testing.T) {
	clearStats()
	c := NewCountersWithSingleLabel("counter1", "help", "label")
	c.Add("c1", 1)
	c.Add("c2", 1)
	c.Add("c2", 1)
	want1 := `{"c1": 1, "c2": 2}`
	want2 := `{"c2": 2, "c1": 1}`
	s := c.String()
	assert.Truef(t, s == want1 || s == want2, "want %s or %s, got %s", want1, want2, s)
	counts := c.Counts()
	assert.Equalf(t, int64(1), counts["c1"], "counts[c1]")
	assert.Equalf(t, int64(2), counts["c2"], "counts[c2]")
}

func TestCountersTags(t *testing.T) {
	clearStats()
	c := NewCountersWithSingleLabel("counterTag1", "help", "label")
	want := map[string]int64{}
	assert.Equal(t, want, c.Counts())

	c = NewCountersWithSingleLabel("counterTag2", "help", "label", "tag1", "tag2")
	want = map[string]int64{"tag1": 0, "tag2": 0}
	assert.Equal(t, want, c.Counts())
}

func TestMultiCounters(t *testing.T) {
	clearStats()
	c := NewCountersWithMultiLabels("mapCounter1", "help", []string{"aaa", "bbb"})
	c.Add([]string{"c1a", "c1b"}, 1)
	c.Add([]string{"c2a", "c2b"}, 1)
	c.Add([]string{"c2a", "c2b"}, 1)
	want1 := `{"c1a.c1b": 1, "c2a.c2b": 2}`
	want2 := `{"c2a.c2b": 2, "c1a.c1b": 1}`
	s := c.String()
	assert.Truef(t, s == want1 || s == want2, "want %s or %s, got %s", want1, want2, s)
	counts := c.Counts()
	assert.Equalf(t, int64(1), counts["c1a.c1b"], "counts[c1a.c1b]")
	assert.Equalf(t, int64(2), counts["c2a.c2b"], "counts[c2a.c2b]")
	f := NewCountersFuncWithMultiLabels("", "help", []string{"aaa", "bbb"}, func() map[string]int64 {
		return map[string]int64{
			"c1a.c1b": 1,
			"c2a.c2b": 2,
		}
	})
	s = f.String()
	assert.Truef(t, s == want1 || s == want2, "want %s or %s, got %s", want1, want2, s)
}

func TestMultiCountersDot(t *testing.T) {
	clearStats()
	c := NewCountersWithMultiLabels("mapCounter2", "help", []string{"aaa", "bbb"})
	c.Add([]string{"c1.a", "c1b"}, 1)
	c.Add([]string{"c2a", "c2.b"}, 1)
	c.Add([]string{"c2a", "c2.b"}, 1)
	c1a := safeLabel("c1.a")
	c1aJSON := strings.ReplaceAll(c1a, "\\", "\\\\")
	c2b := safeLabel("c2.b")
	c2bJSON := strings.ReplaceAll(c2b, "\\", "\\\\")
	want1 := `{"` + c1aJSON + `.c1b": 1, "c2a.` + c2bJSON + `": 2}`
	want2 := `{"c2a.` + c2bJSON + `": 2, "` + c1aJSON + `.c1b": 1}`
	s := c.String()
	assert.Truef(t, s == want1 || s == want2, "want %s or %s, got %s", want1, want2, s)
	counts := c.Counts()
	assert.Equalf(t, int64(1), counts[c1a+".c1b"], "counts[c1a.c1b]")
	assert.Equalf(t, int64(2), counts["c2a."+c2b], "counts[c2a.c2b]")
}

func TestCountersHook(t *testing.T) {
	var gotname string
	var gotv *CountersWithSingleLabel
	clearStats()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*CountersWithSingleLabel)
	})

	v := NewCountersWithSingleLabel("counter2", "help", "label")
	assert.Equal(t, "counter2", gotname)
	assert.Same(t, v, gotv)
}

var benchCounter = NewCountersWithSingleLabel("bench", "help", "label")

func BenchmarkCounters(b *testing.B) {
	clearStats()
	benchCounter.Add("c1", 1)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchCounter.Add("c1", 1)
		}
	})
}

var benchMultiCounter = NewCountersWithMultiLabels("benchMulti", "help", []string{"call", "keyspace", "dbtype"})

func BenchmarkMultiCounters(b *testing.B) {
	clearStats()
	key := []string{"execute-key-ranges", "keyspacename", "replica"}
	benchMultiCounter.Add(key, 1)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchMultiCounter.Add(key, 1)
		}
	})
}

// benchmarkContention measures Add() latency under heavy contention.
// Each goroutine collects latencies locally to avoid measurement skew
// from shared channels or locks. Percentiles are computed after the
// benchmark completes.
//
// Run with high -cpu to increase goroutine count:
//
//	go test -bench=BenchmarkContention -benchtime=5s -cpu=10 -run='^$'
func benchmarkContention(b *testing.B, addFunc func(i int)) {
	b.Helper()

	b.SetParallelism(100) // 100 * GOMAXPROCS goroutines

	var mu sync.Mutex
	var allLatencies []time.Duration

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		local := make([]time.Duration, 0, 1024)
		i := 0
		for pb.Next() {
			start := time.Now()
			addFunc(i)
			local = append(local, time.Since(start))
			i++
		}
		mu.Lock()
		allLatencies = append(allLatencies, local...)
		mu.Unlock()
	})
	b.StopTimer()

	slices.Sort(allLatencies)
	n := len(allLatencies)
	if n > 0 {
		b.Logf("p50: %v  p99: %v  p999: %v  max: %v  (n=%d)",
			allLatencies[n*50/100],
			allLatencies[n*99/100],
			allLatencies[n*999/1000],
			allLatencies[n-1],
			n,
		)
	}
}

func BenchmarkCountersWithSingleLabelContention(b *testing.B) {
	clearStats()
	c := NewCountersWithSingleLabel("", "help", "key")
	c.Add("c1", 0) // pre-create

	benchmarkContention(b, func(_ int) {
		c.Add("c1", 1)
	})
}

func BenchmarkCountersWithMultiLabelsContention(b *testing.B) {
	clearStats()
	c := NewCountersWithMultiLabels("", "help", []string{"call", "keyspace", "dbtype"})

	// Pre-create 50 distinct key combos to simulate realistic workload.
	keys := make([][]string, 50)
	for i := range keys {
		keys[i] = []string{
			fmt.Sprintf("method-%d", i),
			fmt.Sprintf("ks-%d", i%5),
			fmt.Sprintf("type-%d", i%3),
		}
		c.Add(keys[i], 0)
	}

	benchmarkContention(b, func(i int) {
		c.Add(keys[i%len(keys)], 1)
	})
}

func TestCountersFuncWithMultiLabels(t *testing.T) {
	clearStats()
	f := NewCountersFuncWithMultiLabels("TestCountersFuncWithMultiLabels", "help", []string{"label1"}, func() map[string]int64 {
		return map[string]int64{
			"c1": 1,
			"c2": 2,
		}
	})

	want1 := `{"c1": 1, "c2": 2}`
	want2 := `{"c2": 2, "c1": 1}`
	s := f.String()
	assert.Truef(t, s == want1 || s == want2, "want %s or %s, got %s", want1, want2, s)
}

func TestCountersFuncWithMultiLabels_Hook(t *testing.T) {
	var gotname string
	var gotv *CountersFuncWithMultiLabels
	clearStats()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*CountersFuncWithMultiLabels)
	})

	v := NewCountersFuncWithMultiLabels("TestCountersFuncWithMultiLabels_Hook", "help", []string{"label1"}, func() map[string]int64 {
		return map[string]int64{}
	})
	assert.Equal(t, "TestCountersFuncWithMultiLabels_Hook", gotname)
	assert.Same(t, v, gotv)
}

func TestCountersCombineDimension(t *testing.T) {
	clearStats()
	// Empty labels shouldn't be combined.
	c0 := NewCountersWithSingleLabel("counter_combine_dim0", "help", "")
	c0.Add("c1", 1)
	assert.Equal(t, `{"c1": 1}`, c0.String())

	clearStats()
	combineDimensions = "a,c"

	c1 := NewCountersWithSingleLabel("counter_combine_dim1", "help", "label")
	c1.Add("c1", 1)
	assert.Equal(t, `{"c1": 1}`, c1.String())

	c2 := NewCountersWithSingleLabel("counter_combine_dim2", "help", "a")
	c2.Add("c1", 1)
	assert.Equal(t, `{"all": 1}`, c2.String())

	c3 := NewCountersWithSingleLabel("counter_combine_dim3", "help", "a")
	assert.Equal(t, `{"all": 0}`, c3.String())

	// Anything under "a" and "c" should get reported under a consolidated "all" value
	// instead of the specific supplied values.
	c4 := NewCountersWithMultiLabels("counter_combine_dim4", "help", []string{"a", "b", "c"})
	c4.Add([]string{"c1", "c2", "c3"}, 1)
	c4.Add([]string{"c4", "c2", "c5"}, 1)
	assert.Equal(t, `{"all.c2.all": 2}`, c4.String())
}
