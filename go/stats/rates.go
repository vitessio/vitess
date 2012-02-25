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
	currentCounts := rt.countTracker.Counts()
	rt.mu.Lock()
	defer rt.mu.Unlock()
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
