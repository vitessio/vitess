/*
Copyright 2017 Google Inc.

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
	"bytes"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// Counters is similar to expvar.Map, except that
// it doesn't allow floats. In addition, it provides
// a Counts method which can be used for tracking rates.
type Counters struct {
	// mu only protects adding and retrieving the value (*int64) from the map,
	// modification to the actual number (int64) should be done with atomic funcs.
	mu     sync.RWMutex
	counts map[string]*int64
	help   string
}

// NewCounters create a new Counters instance. If name is set, the variable
// gets published. The function also accepts an optional list of tags that
// pre-creates them initialized to 0.
func NewCounters(name string, help string, tags ...string) *Counters {
	c := &Counters{
		counts: make(map[string]*int64),
		help:   help,
	}

	for _, tag := range tags {
		c.counts[tag] = new(int64)
	}
	if name != "" {
		publish(name, c)
	}
	publishPullCounters(c, name)
	return c
}

func (c *Counters) String() string {
	b := bytes.NewBuffer(make([]byte, 0, 4096))

	c.mu.RLock()
	defer c.mu.RUnlock()

	fmt.Fprintf(b, "{")
	firstValue := true
	for k, a := range c.counts {
		if firstValue {
			firstValue = false
		} else {
			fmt.Fprintf(b, ", ")
		}
		fmt.Fprintf(b, "%q: %v", k, atomic.LoadInt64(a))
	}
	fmt.Fprintf(b, "}")
	return b.String()
}

func (c *Counters) getValueAddr(name string) *int64 {
	c.mu.RLock()
	a, ok := c.counts[name]
	c.mu.RUnlock()

	if ok {
		return a
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// we need to check the existence again
	// as it may be created by other goroutine.
	a, ok = c.counts[name]
	if ok {
		return a
	}
	a = new(int64)
	c.counts[name] = a
	return a
}

// Add adds a value to a named counter.
func (c *Counters) Add(name string, value int64) {
	a := c.getValueAddr(name)
	atomic.AddInt64(a, value)
}

// Reset resets all counter values
func (c *Counters) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts = make(map[string]*int64)
}

// ResetCounter resets a specific counter value to 0
func (c *Counters) ResetCounter(name string) {
	a := c.getValueAddr(name)
	atomic.StoreInt64(a, int64(0))
}

// Counts returns a copy of the Counters' map.
func (c *Counters) Counts() map[string]int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	counts := make(map[string]int64, len(c.counts))
	for k, a := range c.counts {
		counts[k] = atomic.LoadInt64(a)
	}
	return counts
}

// Help returns the help string.
func (c *Counters) Help() string {
	return c.help
}

// Gauges is similar to Counters, except its values can go up and down.
type Gauges struct {
	Counters
}

// NewGauges creates a new Gauge and publishes it if the name is set.
func NewGauges(name string, help string, tags ...string) *Gauges {
	g := &Gauges{Counters: Counters{
		counts: make(map[string]*int64),
		help:   help,
	}}

	for _, tag := range tags {
		g.Counters.counts[tag] = new(int64)
	}
	if name != "" {
		publish(name, g)
		publishPullGauges(g, name)
	}
	return g
}

// Set sets the value of a named counter.
func (g *Gauges) Set(name string, value int64) {
	a := g.Counters.getValueAddr(name)
	atomic.StoreInt64(a, value)
}

// CountersFunc converts a function that returns
// a map of int64 as an expvar.
type CountersFunc func() map[string]int64

// Counts returns a copy of the Counters' map.
func (f CountersFunc) Counts() map[string]int64 {
	return f()
}

// String is used by expvar.
func (f CountersFunc) String() string {
	m := f()
	if m == nil {
		return "{}"
	}
	b := bytes.NewBuffer(make([]byte, 0, 4096))
	fmt.Fprintf(b, "{")
	firstValue := true
	for k, v := range m {
		if firstValue {
			firstValue = false
		} else {
			fmt.Fprintf(b, ", ")
		}
		fmt.Fprintf(b, "%q: %v", k, v)
	}
	fmt.Fprintf(b, "}")
	return b.String()
}

// MultiCounters is a multidimensional Counters implementation where
// names of categories are compound names made with joining multiple
// strings with '.'.
type MultiCounters struct {
	Counters
	labels []string
}

// NewMultiCounters creates a new MultiCounters instance, and publishes it
// if name is set.
func NewMultiCounters(name string, help string, labels []string) *MultiCounters {
	t := &MultiCounters{
		Counters: Counters{
			counts: make(map[string]*int64),
			help:   help},
		labels: labels,
	}
	if name != "" {
		publish(name, t)
	}

	publishPullMultiCounters(t, name)
	return t
}

// Labels returns the list of labels.
func (mc *MultiCounters) Labels() []string {
	return mc.labels
}

// Add adds a value to a named counter. len(names) must be equal to
// len(Labels)
func (mc *MultiCounters) Add(names []string, value int64) {
	if len(names) != len(mc.labels) {
		panic("MultiCounters: wrong number of values in Add")
	}
	mc.Counters.Add(mapKey(names), value)
}

// ResetCounter resets the value of a named counter back to 0. len(names)
// must be equal to len(Labels)
func (mc *MultiCounters) ResetCounter(names []string) {
	if len(names) != len(mc.labels) {
		panic("MultiCounters: wrong number of values in Set")
	}

	mc.Counters.ResetCounter(mapKey(names))
}

// MultiGauges is a MultiCounters implementation where the values can go up and down
type MultiGauges struct {
	Gauges
	labels []string
}

// NewMultiGauges creates a new MultiGauges instance, and publishes it
// if name is set.
func NewMultiGauges(name string, help string, labels []string) *MultiGauges {
	t := &MultiGauges{
		Gauges: Gauges{Counters{
			counts: make(map[string]*int64),
			help:   help,
		}},
		labels: labels,
	}
	if name != "" {
		publish(name, t)
	}

	publishPullMultiGauges(t, name)
	return t
}

// Labels returns the list of labels.
func (mg *MultiGauges) Labels() []string {
	return mg.labels
}

// Add adds a value to a named counter. len(names) must be equal to
// len(Labels)
func (mg *MultiGauges) Add(names []string, value int64) {
	if len(names) != len(mg.labels) {
		panic("MultiGauges: wrong number of values in Add")
	}
	mg.Gauges.Counters.Add(mapKey(names), value)
}

// Set sets the value of a named counter. len(names) must be equal to
// len(Labels)
func (mg *MultiGauges) Set(names []string, value int64) {
	if len(names) != len(mg.labels) {
		panic("MultiGauges: wrong number of values in Set")
	}
	mg.Gauges.Set(mapKey(names), value)
}

// MultiCountersFunc is a multidimensional CountersFunc implementation
// where names of categories are compound names made with joining
// multiple strings with '.'.  Since the map is returned by the
// function, we assume it's in the right format (meaning each key is
// of the form 'aaa.bbb.ccc' with as many elements as there are in
// Labels).
type MultiCountersFunc struct {
	CountersFunc
	labels []string
	help   string
}

// Labels returns the list of labels.
func (mcf *MultiCountersFunc) Labels() []string {
	return mcf.labels
}

// Help returns the help string
func (mcf *MultiCountersFunc) Help() string {
	return mcf.help
}

// NewMultiCountersFunc creates a new MultiCountersFunc mapping to the provided
// function.
func NewMultiCountersFunc(name string, labels []string, help string, f CountersFunc) *MultiCountersFunc {
	t := &MultiCountersFunc{
		CountersFunc: f,
		labels:       labels,
		help:         help,
	}
	if name != "" {
		publish(name, t)
	}

	publishPullMultiCountersFunc(t, name)

	return t
}

var escaper = strings.NewReplacer(".", "\\.", "\\", "\\\\")

func mapKey(ss []string) string {
	esc := make([]string, len(ss))
	for i, f := range ss {
		esc[i] = escaper.Replace(f)
	}
	return strings.Join(esc, ".")
}
