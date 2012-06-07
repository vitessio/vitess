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

type Timings struct {
	mu         sync.Mutex
	TotalCount int64
	TotalTime  int64
	Histograms map[string]*Histogram
}

func NewTimings(name string) *Timings {
	t := &Timings{Histograms: make(map[string]*Histogram)}
	if name != "" {
		expvar.Publish(name, t)
	}
	return t
}

func (t *Timings) Add(name string, elapsed time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	hist, ok := t.Histograms[name]
	if !ok {
		hist = NewGenericHistogram("", bucketCutoffs, bucketLabels, "Count", "Time")
		t.Histograms[name] = hist
	}
	elapsedNs := int64(elapsed)
	hist.Add(elapsedNs)
	t.TotalCount++
	t.TotalTime += elapsedNs
}

func (t *Timings) Record(name string, startTime time.Time) {
	t.Add(name, time.Now().Sub(startTime))
}

func (t *Timings) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	data, err := json.MarshalIndent(t, "", " ")
	if err != nil {
		data, _ = json.Marshal(err.Error())
	}
	return string(data)
}

func (t *Timings) Counts() map[string]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	counts := make(map[string]int64, len(t.Histograms)+1)
	for k, v := range t.Histograms {
		counts[k] = v.Count()
	}
	counts["All"] = t.TotalCount
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
