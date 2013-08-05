// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"encoding/json"
	"expvar"
	"fmt"
	"sync"
	"time"
)

// Timings is meant to tracks timing data
// by named categories as well as histograms.
type Timings struct {
	mu         sync.Mutex
	totalCount int64
	totalTime  int64
	histograms map[string]*Histogram
}

func NewTimings(name string) *Timings {
	t := &Timings{histograms: make(map[string]*Histogram)}
	if name != "" {
		expvar.Publish(name, t)
		callHook(name, t)
	}
	return t
}

func (t *Timings) Add(name string, elapsed time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	hist, ok := t.histograms[name]
	if !ok {
		hist = NewGenericHistogram("", bucketCutoffs, bucketLabels, "Count", "Time")
		t.histograms[name] = hist
	}
	elapsedNs := int64(elapsed)
	hist.Add(elapsedNs)
	t.totalCount++
	t.totalTime += elapsedNs
}

// Record is a convenience function that records completion
// timing data based on the provided start time of an event.
func (t *Timings) Record(name string, startTime time.Time) {
	t.Add(name, time.Now().Sub(startTime))
}

func (t *Timings) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	tm := struct {
		TotalCount int64
		totalTime  int64
		Histograms map[string]*Histogram
	}{
		t.totalCount,
		t.totalTime,
		t.histograms,
	}
	data, err := json.Marshal(tm)
	if err != nil {
		data, _ = json.Marshal(err.Error())
	}
	return string(data)
}

func (t *Timings) Histograms(h map[string]*Histogram) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h = make(map[string]*Histogram, len(t.histograms))
	for k, v := range t.histograms {
		h[k] = v
	}
	return
}

func (t *Timings) Count() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.totalCount
}

func (t *Timings) Time() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.totalTime
}

func (t *Timings) Counts() map[string]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	counts := make(map[string]int64, len(t.histograms)+1)
	for k, v := range t.histograms {
		counts[k] = v.Count()
	}
	counts["All"] = t.totalCount
	return counts
}

var bucketCutoffs = []int64{0.0005 * 1e9, 0.001 * 1e9, 0.005 * 1e9, 0.010 * 1e9, 0.050 * 1e9, 0.100 * 1e9, 0.500 * 1e9, 1.000 * 1e9, 5.000 * 1e9, 10.00 * 1e9}

var bucketLabels []string

func init() {
	bucketLabels = make([]string, len(bucketCutoffs)+1)
	for i, v := range bucketCutoffs {
		bucketLabels[i] = fmt.Sprintf("%.4f", float64(v)/1e9)
	}
	bucketLabels[len(bucketLabels)-1] = "Max"
}
