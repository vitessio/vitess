// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"encoding/json"
	"fmt"
	"strings"
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
	hook       func(string, time.Duration)
}

// NewTimings creates a new Timings object, and publishes it if name is set.
// categories is an optional list of categories to initialize to 0.
// Categories that aren't initialized will be missing from the map until the
// first time they are updated.
func NewTimings(name string, categories ...string) *Timings {
	t := &Timings{histograms: make(map[string]*Histogram)}
	for _, cat := range categories {
		t.histograms[cat] = NewGenericHistogram("", bucketCutoffs, bucketLabels, "Count", "Time")
	}
	if name != "" {
		Publish(name, t)
	}
	return t
}

// Add will add a new value to the named histogram.
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
	if t.hook != nil {
		t.hook(name, elapsed)
	}
}

// Record is a convenience function that records completion
// timing data based on the provided start time of an event.
func (t *Timings) Record(name string, startTime time.Time) {
	t.Add(name, time.Now().Sub(startTime))
}

// String is for expvar.
func (t *Timings) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	tm := struct {
		TotalCount int64
		TotalTime  int64
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

// Histograms returns a map pointing at the histograms.
func (t *Timings) Histograms() (h map[string]*Histogram) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h = make(map[string]*Histogram, len(t.histograms))
	for k, v := range t.histograms {
		h[k] = v
	}
	return
}

// Count returns the total count for all values.
func (t *Timings) Count() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.totalCount
}

// Time returns the total time elapsed for all values.
func (t *Timings) Time() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.totalTime
}

// Counts returns the total count for each value.
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

// Cutoffs returns the cutoffs used in the component histograms.
// Do not change the returned slice.
func (t *Timings) Cutoffs() []int64 {
	return bucketCutoffs
}

var bucketCutoffs = []int64{5e5, 1e6, 5e6, 1e7, 5e7, 1e8, 5e8, 1e9, 5e9, 1e10}

var bucketLabels []string

func init() {
	bucketLabels = make([]string, len(bucketCutoffs)+1)
	for i, v := range bucketCutoffs {
		bucketLabels[i] = fmt.Sprintf("%d", v)
	}
	bucketLabels[len(bucketLabels)-1] = "inf"
}

// MultiTimings is meant to tracks timing data by categories as well
// as histograms. The names of the categories are compound names made
// with joining multiple strings with '.'.
type MultiTimings struct {
	Timings
	labels []string
}

// NewMultiTimings creates a new MultiTimings object.
func NewMultiTimings(name string, labels []string) *MultiTimings {
	t := &MultiTimings{
		Timings: Timings{histograms: make(map[string]*Histogram)},
		labels:  labels,
	}
	if name != "" {
		Publish(name, t)
	}
	return t
}

// Labels returns descriptions of the parts of each compound category name.
func (mt *MultiTimings) Labels() []string {
	return mt.labels
}

// Add will add a new value to the named histogram.
func (mt *MultiTimings) Add(names []string, elapsed time.Duration) {
	if len(names) != len(mt.labels) {
		panic("MultiTimings: wrong number of values in Add")
	}
	mt.Timings.Add(strings.Join(names, "."), elapsed)
}

// Record is a convenience function that records completion
// timing data based on the provided start time of an event.
func (mt *MultiTimings) Record(names []string, startTime time.Time) {
	if len(names) != len(mt.labels) {
		panic("MultiTimings: wrong number of values in Record")
	}
	mt.Timings.Record(strings.Join(names, "."), startTime)
}

// Cutoffs returns the cutoffs used in the component histograms.
// Do not change the returned slice.
func (mt *MultiTimings) Cutoffs() []int64 {
	return bucketCutoffs
}
