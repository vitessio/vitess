// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"expvar"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestCounters(t *testing.T) {
	clear()
	c := NewCounters("counter1")
	c.Add("c1", 1)
	c.Add("c2", 1)
	c.Add("c2", 1)
	want1 := `{"c1": 1, "c2": 2}`
	want2 := `{"c2": 2, "c1": 1}`
	if s := c.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}
	counts := c.Counts()
	if counts["c1"] != 1 {
		t.Errorf("want 1, got %d", counts["c1"])
	}
	if counts["c2"] != 2 {
		t.Errorf("want 2, got %d", counts["c2"])
	}
	f := CountersFunc(func() map[string]int64 {
		return map[string]int64{
			"c1": 1,
			"c2": 2,
		}
	})
	if s := f.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}
}

func TestCountersTags(t *testing.T) {
	clear()
	c := NewCounters("counterTag1")
	want := map[string]int64{}
	got := c.Counts()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("want %v, got %v", want, got)
	}

	c = NewCounters("counterTag2", "tag1", "tag2")
	want = map[string]int64{"tag1": 0, "tag2": 0}
	got = c.Counts()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestMultiCounters(t *testing.T) {
	clear()
	c := NewMultiCounters("mapCounter1", []string{"aaa", "bbb"})
	c.Add([]string{"c1a", "c1b"}, 1)
	c.Add([]string{"c2a", "c2b"}, 1)
	c.Add([]string{"c2a", "c2b"}, 1)
	want1 := `{"c1a.c1b": 1, "c2a.c2b": 2}`
	want2 := `{"c2a.c2b": 2, "c1a.c1b": 1}`
	if s := c.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}
	counts := c.Counts()
	if counts["c1a.c1b"] != 1 {
		t.Errorf("want 1, got %d", counts["c1a.c1b"])
	}
	if counts["c2a.c2b"] != 2 {
		t.Errorf("want 2, got %d", counts["c2a.c2b"])
	}
	f := NewMultiCountersFunc("", []string{"aaa", "bbb"}, func() map[string]int64 {
		return map[string]int64{
			"c1a.c1b": 1,
			"c2a.c2b": 2,
		}
	})
	if s := f.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}
}

func TestCountersHook(t *testing.T) {
	var gotname string
	var gotv *Counters
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Counters)
	})

	v := NewCounters("counter2")
	if gotname != "counter2" {
		t.Errorf("want counter2, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
}

var benchCounter = NewCounters("bench")

func BenchmarkCounters(b *testing.B) {
	clear()
	benchCounter.Add("c1", 1)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchCounter.Add("c1", 1)
		}
	})
}

func BenchmarkCountersTailLatency(b *testing.B) {
	// For this one, ignore the time reported by 'go test'.
	// The 99th Percentile log line is all that matters.
	// (Cmd: go test -bench=BenchmarkCountersTailLatency -benchtime=30s -cpu=10)
	clear()
	benchCounter.Add("c1", 1)
	c := make(chan time.Duration, 100)
	done := make(chan struct{})
	go func() {
		all := make([]int, b.N)
		i := 0
		for dur := range c {
			all[i] = int(dur)
			i++
		}
		sort.Ints(all)
		p99 := time.Duration(all[b.N*99/100])
		b.Logf("99th Percentile (for N=%v): %v", b.N, p99)
		close(done)
	}()

	b.ResetTimer()
	b.SetParallelism(100) // The actual number of goroutines is 100*GOMAXPROCS
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		var start time.Time

		for pb.Next() {
			// sleep between 0~200ms to simulate 10 QPS per goroutine.
			time.Sleep(time.Duration(r.Int63n(200)) * time.Millisecond)
			start = time.Now()
			benchCounter.Add("c1", 1)
			c <- time.Since(start)
		}
	})
	b.StopTimer()

	close(c)
	<-done
}
