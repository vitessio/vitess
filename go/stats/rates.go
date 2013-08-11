// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"encoding/json"
	"sync"
	"time"
)

// CountTracker defines the interface that needs to
// be supported by a variable for being tracked by
// Rates.
type CountTracker interface {
	Counts() map[string]int64
}

// Rates is capable of reporting the rate (typically QPS)
// for any variable that satisfies the CountTracker interface.
type Rates struct {
	mu           sync.Mutex
	timeStamps   *RingInt64
	counts       map[string]*RingInt64
	countTracker CountTracker
	samples      int
	interval     time.Duration
}

// NewRates reports rolling rate information for countTracker. samples specifies
// the number of samples to report, and interval specifies the time interval
// between samples. The minimum interval is 1 second.
func NewRates(name string, countTracker CountTracker, samples int, interval time.Duration) *Rates {
	if interval < 1*time.Second {
		panic("interval too small")
	}
	rt := &Rates{
		timeStamps:   NewRingInt64(samples + 1),
		counts:       make(map[string]*RingInt64),
		countTracker: countTracker,
		samples:      samples + 1,
		interval:     interval,
	}
	if name != "" {
		Publish(name, rt)
	}
	go rt.track()
	return rt
}

func (rt *Rates) track() {
	for {
		rt.snapshot()
		<-time.After(rt.interval)
	}
}

func (rt *Rates) snapshot() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	currentCounts := rt.countTracker.Counts()
	rt.timeStamps.Add(time.Now().UnixNano())
	for k, v := range currentCounts {
		if values, ok := rt.counts[k]; ok {
			values.Add(v)
		} else {
			rt.counts[k] = NewRingInt64(rt.samples)
			rt.counts[k].Add(v)
		}
	}
}

func (rt *Rates) Get() (rateMap map[string][]float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rateMap = make(map[string][]float64)
	timeStamps := rt.timeStamps.Values()
	if len(timeStamps) <= 1 {
		return
	}
	for k, v := range rt.counts {
		rateMap[k] = make([]float64, len(timeStamps)-1)
		values := v.Values()
		valueIndex := len(values) - 1
		for i := len(timeStamps) - 1; i > 0; i-- {
			if valueIndex <= 0 {
				rateMap[k][i-1] = 0
				continue
			}
			elapsed := float64((timeStamps[i] - timeStamps[i-1]) / 1e9)
			rateMap[k][i-1] = float64(values[valueIndex]-values[valueIndex-1]) / elapsed
			valueIndex--
		}
	}
	return
}

func (rt *Rates) String() string {
	data, err := json.Marshal(rt.Get())
	if err != nil {
		data, _ = json.Marshal(err.Error())
	}
	return string(data)
}
