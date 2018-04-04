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

// Counter is expvar.Int+Get+hook
type Counter struct {
	i    sync2.AtomicInt64
	help string
}

// NewCounter returns a new Counter
func NewCounter(name string, help string) *Counter {
	v := &Counter{help: help}
	if name != "" {
		publish(name, v)
	}
	return v
}

// Add adds the provided value to the Int
func (v *Counter) Add(delta int64) {
	v.i.Add(delta)
}

// Reset resets the value to 0
func (v *Counter) Reset() {
	v.i.Set(int64(0))
}

// Get returns the value
func (v *Counter) Get() int64 {
	return v.i.Get()
}

// String is the implementation of expvar.var
func (v *Counter) String() string {
	return strconv.FormatInt(v.i.Get(), 10)
}

// Help returns the help string
func (v *Counter) Help() string {
	return v.help
}

// Gauge is an unlabeled metric whose values can go up/down.
type Gauge struct {
	Counter
}

// NewGauge creates a new Gauge and publishes it if name is set
func NewGauge(name string, help string) *Gauge {
	v := &Gauge{Counter: Counter{help: help}}

	if name != "" {
		publish(name, v)
	}
	return v
}

// Set sets the value
func (v *Gauge) Set(value int64) {
	v.Counter.i.Set(value)
}

// Counters is similar to expvar.Map, except that
// it doesn't allow floats. It is used to build CountersWithLabels and GaugesWithLabels.
type Counters struct {
	// mu only protects adding and retrieving the value (*int64) from the map,
	// modification to the actual number (int64) should be done with atomic funcs.
	mu     sync.RWMutex
	counts map[string]*int64
	help   string
}

// CountersWithLabels provides a labelName for the tagged values in Counters
// It provides a Counts method which can be used for tracking rates.
type CountersWithLabels struct {
	Counters
	labelName string
}

// NewCountersWithLabels create a new Counters instance. If name is set, the variable
// gets published. The function also accepts an optional list of tags that
// pre-creates them initialized to 0.
// labelName is a category name used to organize the tags in Prometheus.
func NewCountersWithLabels(name string, help string, labelName string, tags ...string) *CountersWithLabels {
	c := &CountersWithLabels{
		Counters: Counters{
			counts: make(map[string]*int64),
			help:   help,
		},
		labelName: labelName,
	}

	for _, tag := range tags {
		c.counts[tag] = new(int64)
	}
	if name != "" {
		publish(name, c)
	}
	return c
}

// String implements expvar
func (c *CountersWithLabels) String() string {
	b := bytes.NewBuffer(make([]byte, 0, 4096))

	c.Counters.mu.RLock()
	defer c.Counters.mu.RUnlock()

	fmt.Fprintf(b, "{")
	firstValue := true
	for k, a := range c.Counters.counts {
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

func (c *CountersWithLabels) getValueAddr(name string) *int64 {
	c.Counters.mu.RLock()
	a, ok := c.Counters.counts[name]
	c.Counters.mu.RUnlock()

	if ok {
		return a
	}

	c.Counters.mu.Lock()
	defer c.Counters.mu.Unlock()
	// we need to check the existence again
	// as it may be created by other goroutine.
	a, ok = c.Counters.counts[name]
	if ok {
		return a
	}
	a = new(int64)
	c.Counters.counts[name] = a
	return a
}

// Add adds a value to a named counter.
func (c *CountersWithLabels) Add(name string, value int64) {
	a := c.Counters.getValueAddr(name)
	atomic.AddInt64(a, value)
}

// Reset resets all counter values
func (c *CountersWithLabels) Reset() {
	c.Counters.mu.Lock()
	defer c.Counters.mu.Unlock()
	c.Counters.counts = make(map[string]*int64)
}

// ResetCounter resets a specific counter value to 0
func (c *CountersWithLabels) ResetCounter(name string) {
	a := c.Counters.getValueAddr(name)
	atomic.StoreInt64(a, int64(0))
}

// Counts returns a copy of the Counters' map.
func (c *CountersWithLabels) Counts() map[string]int64 {
	c.Counters.mu.RLock()
	defer c.Counters.mu.RUnlock()

	counts := make(map[string]int64, len(c.Counters.counts))
	for k, a := range c.Counters.counts {
		counts[k] = atomic.LoadInt64(a)
	}
	return counts
}

// Help returns the help string.
func (c *CountersWithLabels) Help() string {
	return c.Counters.help
}

// GaugesWithLabels is similar to CountersWithLabels, except its values can go up and down.
type GaugesWithLabels struct {
	CountersWithLabels
}

// NewGaugesWithLabels creates a new GaugesWithLabels and publishes it if the name is set.
func NewGaugesWithLabels(name string, help string, labelName string, tags ...string) *GaugesWithLabels {
	g := &GaugesWithLabels{CountersWithLabels: CountersWithLabels{Counters: Counters{
		counts: make(map[string]*int64),
		help:   help,
	}}, labelName: labelName}

	for _, tag := range tags {
		g.CountersWithLabels.counts[tag] = new(int64)
	}
	if name != "" {
		publish(name, g)
	}
	return g
}

// Set sets the value of a named counter.
func (g *GaugesWithLabels) Set(name string, value int64) {
	a := g.CountersWithLabels.getValueAddr(name)
	atomic.StoreInt64(a, value)
}

// GaugeFunc converts a function that returns
// an int64 as an expvar.
type GaugeFunc struct {
	f    func() int64
	help string
}

// NewGaugeFunc creates a new GaugeFunc instance and publishes it if name is set
func NewGaugeFunc(name string, help string, f func() int64) *GaugeFunc {
	i := &GaugeFunc{
		f:    f,
		help: help,
	}

	if name != "" {
		publish(name, i)
	}
	return i
}

// String is the implementation of expvar.var
func (gf GaugeFunc) String() string {
	return strconv.FormatInt(gf.f(), 10)
}

// Help returns the help string
func (gf GaugeFunc) Help() string {
	return gf.help
}

// F calls and returns the result of the GaugeFunc function
func (gf GaugeFunc) F() int64 {
	return gf.f()
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

// CountersWithMultiLabels is a multidimensional Counters implementation where
// names of categories are compound names made with joining multiple
// strings with '.'.
type CountersWithMultiLabels struct {
	Counters
	labels []string
}

// NewCountersWithMultiLabels creates a new CountersWithMultiLabels instance, and publishes it
// if name is set.
func NewCountersWithMultiLabels(name string, help string, labels []string) *CountersWithMultiLabels {
	t := &CountersWithMultiLabels{
		Counters: Counters{
			counts: make(map[string]*int64),
			help:   help},
		labels: labels,
	}
	if name != "" {
		publish(name, t)
	}

	return t
}

// Labels returns the list of labels.
func (mc *CountersWithMultiLabels) Labels() []string {
	return mc.labels
}

// Add adds a value to a named counter. len(names) must be equal to
// len(Labels)
func (mc *CountersWithMultiLabels) Add(names []string, value int64) {
	if len(names) != len(mc.labels) {
		panic("CountersWithMultiLabels: wrong number of values in Add")
	}
	mc.Counters.Add(mapKey(names), value)
}

// ResetCounter resets the value of a named counter back to 0. len(names)
// must be equal to len(Labels)
func (mc *CountersWithMultiLabels) ResetCounter(names []string) {
	if len(names) != len(mc.labels) {
		panic("CountersWithMultiLabels: wrong number of values in ResetCounter")
	}

	mc.Counters.ResetCounter(mapKey(names))
}

// GaugesWithMultiLabels is a CountersWithMultiLabels implementation where the values can go up and down
type GaugesWithMultiLabels struct {
	CountersWithMultiLabels
}

// NewGaugesWithMultiLabels creates a new GaugesWithMultiLabels instance, and publishes it
// if name is set.
func NewGaugesWithMultiLabels(name string, help string, labels []string) *GaugesWithMultiLabels {
	t := &GaugesWithMultiLabels{
		CountersWithMultiLabels: CountersWithMultiLabels{Counters: Counters{
			counts: make(map[string]*int64),
			help:   help,
		},
			labels: labels,
		}}
	if name != "" {
		publish(name, t)
	}

	return t
}

// Set sets the value of a named counter. len(names) must be equal to
// len(Labels)
func (mg *GaugesWithMultiLabels) Set(names []string, value int64) {
	if len(names) != len(mg.CountersWithMultiLabels.labels) {
		panic("GaugesWithMultiLabels: wrong number of values in Set")
	}
	a := mg.CountersWithMultiLabels.Counters.getValueAddr(name)
	atomic.StoreInt64(a, value)
}

// CountersFuncWithMultiLabels is a multidimensional CountersFunc implementation
// where names of categories are compound names made with joining
// multiple strings with '.'.  Since the map is returned by the
// function, we assume it's in the right format (meaning each key is
// of the form 'aaa.bbb.ccc' with as many elements as there are in
// Labels).
type CountersFuncWithMultiLabels struct {
	CountersFunc
	labels []string
	help   string
}

// Labels returns the list of labels.
func (mcf *CountersFuncWithMultiLabels) Labels() []string {
	return mcf.labels
}

// Help returns the help string
func (mcf *CountersFuncWithMultiLabels) Help() string {
	return mcf.help
}

// NewCountersFuncWithMultiLabels creates a new CountersFuncWithMultiLabels mapping to the provided
// function.
func NewCountersFuncWithMultiLabels(name string, labels []string, help string, f CountersFunc) *CountersFuncWithMultiLabels {
	t := &CountersFuncWithMultiLabels{
		CountersFunc: f,
		labels:       labels,
		help:         help,
	}
	if name != "" {
		publish(name, t)
	}

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
