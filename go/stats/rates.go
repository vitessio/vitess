// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"encoding/json"
	"expvar"
	"sync"
	"time"
)

type CountTracker interface {
	Counts() map[string]int64
}

type Rates struct {
	mu           sync.Mutex
	timeStamps   *RingInt64
	counts       map[string]*RingInt64
	countTracker CountTracker
	samples      int
	interval     time.Duration
}

func NewRates(name string, countTracker CountTracker, samples int, interval time.Duration) *Rates {
	rt := &Rates{
		timeStamps:   NewRingInt64(samples),
		counts:       make(map[string]*RingInt64),
		countTracker: countTracker,
		samples:      samples,
		interval:     interval,
	}
	if name != "" {
		expvar.Publish(name, rt)
	}
	go rt.Track()
	return rt
}

func (rt *Rates) Track() {
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

func (rt *Rates) String() string {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	timeStamps := rt.timeStamps.Values()
	if len(timeStamps) <= 1 {
		return "{}"
	}
	rateMap := make(map[string][]float64)
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
	data, err := json.MarshalIndent(rateMap, "", "  ")
	if err != nil {
		data, _ = json.Marshal(err.Error())
	}
	return string(data)
}
