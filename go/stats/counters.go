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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/sync2"
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

// Add adds the provided value to the Counter
func (v *Counter) Add(delta int64) {
	v.i.Add(delta)
}

// Reset resets the counter value to 0
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

// String implements expvar
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

// ResetAll resets all counter values.
func (c *Counters) ResetAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts = make(map[string]*int64)
}

// Reset resets a specific counter value to 0
func (c *Counters) Reset(name string) {
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

// LabelName returns the label name.
func (c *CountersWithLabels) LabelName() string {
	return c.labelName
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
	}, labelName: labelName}}

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

// CounterFunc converts a function that returns
// an int64 as an expvar.
// For implementations that differentiate between Counters/Gauges,
// CounterFunc's values only go up (or are reset to 0)
type CounterFunc struct {
	Mf   MetricFunc
	help string
}

// NewCounterFunc creates a new CounterFunc instance and publishes it if name is set
func NewCounterFunc(name string, help string, Mf MetricFunc) *CounterFunc {
	c := &CounterFunc{
		Mf:   Mf,
		help: help,
	}

	if name != "" {
		publish(name, c)
	}
	return c
}

// Help returns the help string
func (cf *CounterFunc) Help() string {
	return cf.help
}

// String implements expvar.Var
func (cf *CounterFunc) String() string {
	return cf.Mf.String()
}

// MetricFunc defines an interface for things that can be exported with calls to stats.CounterFunc/stats.GaugeFunc
type MetricFunc interface {
	FloatVal() float64
	String() string
}

// IntFunc converst a function that returns an int64 as both an expvar and a MetricFunc
type IntFunc func() int64

// FloatVal is the implementation of MetricFunc
func (f IntFunc) FloatVal() float64 {
	return float64(f())
}

// String is the implementation of expvar.var
func (f IntFunc) String() string {
	return strconv.FormatInt(f(), 10)
}

// GaugeFunc converts a function that returns an int64 as an expvar.
// It's a wrapper around CounterFunc for values that go up/down
// for implementations (like Prometheus) that need to differ between Counters and Gauges.
type GaugeFunc struct {
	CounterFunc
}

// NewGaugeFunc creates a new GaugeFunc instance and publishes it if name is set
func NewGaugeFunc(name string, help string, Mf MetricFunc) *GaugeFunc {
	i := &GaugeFunc{
		CounterFunc: CounterFunc{
			Mf:   Mf,
			help: help,
		}}

	if name != "" {
		publish(name, i)
	}
	return i
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

// Reset resets the value of a named counter back to 0. len(names)
// must be equal to len(Labels)
func (mc *CountersWithMultiLabels) Reset(names []string) {
	if len(names) != len(mc.labels) {
		panic("CountersWithMultiLabels: wrong number of values in Reset")
	}

	mc.Counters.Reset(mapKey(names))
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
	a := mg.getValueAddr(mapKey(names))
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

// GaugesFuncWithMultiLabels is a wrapper around CountersFuncWithMultiLabels
// for values that go up/down for implementations (like Prometheus) that need to differ between Counters and Gauges.
type GaugesFuncWithMultiLabels struct {
	CountersFuncWithMultiLabels
}

// NewGaugesFuncWithMultiLabels creates a new GaugesFuncWithMultiLabels mapping to the provided
// function.
func NewGaugesFuncWithMultiLabels(name string, labels []string, help string, f CountersFunc) *GaugesFuncWithMultiLabels {
	t := &GaugesFuncWithMultiLabels{
		CountersFuncWithMultiLabels: CountersFuncWithMultiLabels{
			CountersFunc: f,
			labels:       labels,
			help:         help,
		}}

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
