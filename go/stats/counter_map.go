/*
Copyright 2022 The Vitess Authors.

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
	"sync"
)

var (
	countersMap = map[string]interface{}{}
	countersMu  sync.RWMutex
)

// GetOrNewCounter returns a Counter with given name; the functiona either creates the counter
// if it does not exist, or returns a pre-existing one. The function is thread safe.
func GetOrNewCounter(name string, help string) *Counter {
	// first, attempt read lock only
	countersMu.RLock()
	c, ok := countersMap[name]
	countersMu.RUnlock()
	if ok {
		return c.(*Counter)
	}

	// escalate into write lock
	countersMu.Lock()
	defer countersMu.Unlock()
	// double check because we have released the lock in the interim
	if c, ok := countersMap[name]; ok {
		return c.(*Counter)
	}
	n := NewCounter(name, help)
	countersMap[name] = n
	return n
}

// GetOrNewGauge returns a Gauge with given name; the functiona either creates the gauge
// if it does not exist, or returns a pre-existing one. The function is thread safe.
func GetOrNewGauge(name string, help string) *Gauge {
	// first, attempt read lock only
	countersMu.RLock()
	c, ok := countersMap[name]
	countersMu.RUnlock()
	if ok {
		return c.(*Gauge)
	}

	// escalate into write lock
	countersMu.Lock()
	defer countersMu.Unlock()
	// double check because we have released the lock in the interim
	if c, ok := countersMap[name]; ok {
		return c.(*Gauge)
	}
	n := NewGauge(name, help)
	countersMap[name] = n
	return n
}

// GetOrNewGaugeFloat64 returns a Gauge (float64) with given name; the functiona either creates the gauge
// if it does not exist, or returns a pre-existing one. The function is thread safe.
func GetOrNewGaugeFloat64(name string, help string) *GaugeFloat64 {
	// first, attempt read lock only
	countersMu.RLock()
	c, ok := countersMap[name]
	countersMu.RUnlock()
	if ok {
		return c.(*GaugeFloat64)
	}

	// escalate into write lock
	countersMu.Lock()
	defer countersMu.Unlock()
	// double check because we have released the lock in the interim
	if c, ok := countersMap[name]; ok {
		return c.(*GaugeFloat64)
	}
	n := NewGaugeFloat64(name, help)
	countersMap[name] = n
	return n
}
