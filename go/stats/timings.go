/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

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
