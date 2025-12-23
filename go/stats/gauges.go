/*
Copyright 2025 The Vitess Authors.

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
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

// Gauges tracks gauge values over time by sampling them at regular intervals.
// Unlike Rates which tracks operation rates, Gauges tracks actual values.
type Gauges struct {
	mu         sync.Mutex
	timeStamps *RingInt64
	history    map[string]*RingInt64    // Historical sampled values
	current    map[string]*atomic.Int64 // Current gauge values
	samples    int
	interval   time.Duration
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewGauges creates a new Gauges object that samples gauge values at regular intervals.
// samples: number of historical samples to keep
// interval: how often to sample the current gauge values
func NewGauges(samples int, interval time.Duration) *Gauges {
	if interval < 1*time.Second && interval != -1*time.Second {
		panic("interval too small")
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := &Gauges{
		timeStamps: NewRingInt64(samples + 1),
		history:    make(map[string]*RingInt64),
		current:    make(map[string]*atomic.Int64),
		samples:    samples + 1,
		interval:   interval,
		ctx:        ctx,
		cancel:     cancel,
	}

	// Start background sampling goroutine
	if interval > 0 {
		go g.track()
	}

	return g
}

// Set updates the current value for the named gauge.
// This value will be sampled at the next interval.
func (g *Gauges) Set(name string, value int64) {
	g.mu.Lock()
	gauge, ok := g.current[name]
	if !ok {
		gauge = &atomic.Int64{}
		g.current[name] = gauge
	}
	g.mu.Unlock()

	gauge.Store(value)
}

// Stop terminates the background sampling goroutine.
func (g *Gauges) Stop() {
	g.cancel()
}

// track runs in a background goroutine and samples gauge values at regular intervals.
func (g *Gauges) track() {
	t := time.NewTicker(g.interval)
	defer t.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-t.C:
			g.snapshot()
		}
	}
}

// snapshot samples the current gauge values and stores them in the history.
func (g *Gauges) snapshot() {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := timeNow()
	g.timeStamps.Add(now.UnixNano())

	// Sample current value for each gauge.
	for name, gauge := range g.current {
		value := gauge.Load()

		if ring, ok := g.history[name]; ok {
			ring.Add(value)
		} else {
			g.history[name] = NewRingInt64(g.samples)
			g.history[name].Add(value)
		}
	}
}

// Get returns the historical sampled values for each gauge.
// Values are ordered from least recent (index 0) to most recent (end of slice).
func (g *Gauges) Get() map[string][]float64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	result := make(map[string][]float64)
	timeStamps := g.timeStamps.Values()
	if len(timeStamps) == 0 {
		return result
	}

	for name, ring := range g.history {
		values := ring.Values()
		if len(values) == 0 {
			continue
		}

		// Convert to float64 for consistency with Rates API.
		result[name] = make([]float64, len(values))
		for i, v := range values {
			result[name][i] = float64(v)
		}
	}

	return result
}

// GaugesFunc is a wrapper around a function that returns gauge values.
// It implements the expvar.Var interface for publishing.
type GaugesFunc struct {
	F    func() map[string][]float64
	help string
}

// NewGaugesFunc creates a GaugesFunc and publishes it if name is set.
// This is for time series gauges (multiple samples over time).
func NewGaugesFunc(name string, help string, f func() map[string][]float64) *GaugesFunc {
	c := &GaugesFunc{
		F:    f,
		help: help,
	}

	if name != "" {
		publish(name, c)
	}
	return c
}

// Help returns the help string.
func (gf *GaugesFunc) Help() string {
	return gf.help
}

// String implements the expvar.Var interface for publishing.
func (gf *GaugesFunc) String() string {
	data, err := json.Marshal(gf.F())
	if err != nil {
		data, _ = json.Marshal(err.Error())
	}
	return string(data)
}
