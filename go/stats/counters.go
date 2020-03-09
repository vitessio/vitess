/*
Copyright 2019 The Vitess Authors.

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
)

// counters is similar to expvar.Map, except that it doesn't allow floats.
// It is used to build CountersWithSingleLabel and GaugesWithSingleLabel.
type counters struct {
	mu     sync.Mutex
	counts map[string]int64

	help string
}

func (c *counters) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	b := &strings.Builder{}
	fmt.Fprintf(b, "{")
	prefix := ""
	for k, v := range c.counts {
		fmt.Fprintf(b, "%s%q: %v", prefix, k, v)
		prefix = ", "
	}
	fmt.Fprintf(b, "}")
	return b.String()
}

func (c *counters) add(name string, value int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[name] = c.counts[name] + value
}

func (c *counters) set(name string, value int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[name] = value
}

func (c *counters) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts = make(map[string]int64)
}

// ZeroAll zeroes out all values
func (c *counters) ZeroAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k := range c.counts {
		c.counts[k] = 0
	}
}

// Counts returns a copy of the Counters' map.
func (c *counters) Counts() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	counts := make(map[string]int64, len(c.counts))
	for k, v := range c.counts {
		counts[k] = v
	}
	return counts
}

// Help returns the help string.
func (c *counters) Help() string {
	return c.help
}

// CountersWithSingleLabel tracks multiple counter values for a single
// dimension ("label").
// It provides a Counts method which can be used for tracking rates.
type CountersWithSingleLabel struct {
	counters
	label         string
	labelCombined bool
}

// NewCountersWithSingleLabel create a new Counters instance.
// If name is set, the variable gets published.
// The function also accepts an optional list of tags that pre-creates them
// initialized to 0.
// label is a category name used to organize the tags. It is currently only
// used by Prometheus, but not by the expvar package.
func NewCountersWithSingleLabel(name, help, label string, tags ...string) *CountersWithSingleLabel {
	c := &CountersWithSingleLabel{
		counters: counters{
			counts: make(map[string]int64),
			help:   help,
		},
		label:         label,
		labelCombined: IsDimensionCombined(label),
	}

	if c.labelCombined {
		c.counts[StatsAllStr] = 0
	} else {
		for _, tag := range tags {
			c.counts[tag] = 0
		}
	}
	if name != "" {
		publish(name, c)
	}
	return c
}

// Label returns the label name.
func (c *CountersWithSingleLabel) Label() string {
	return c.label
}

// Add adds a value to a named counter.
func (c *CountersWithSingleLabel) Add(name string, value int64) {
	if value < 0 {
		logCounterNegative.Warningf("Adding a negative value to a counter, %v should be a gauge instead", c)
	}
	if c.labelCombined {
		name = StatsAllStr
	}
	c.counters.add(name, value)
}

// Reset resets the value for the name.
func (c *CountersWithSingleLabel) Reset(name string) {
	if c.labelCombined {
		name = StatsAllStr
	}
	c.counters.set(name, 0)
}

// ResetAll clears the counters
func (c *CountersWithSingleLabel) ResetAll() {
	c.counters.reset()
}

// CountersWithMultiLabels is a multidimensional counters implementation.
// Internally, each tuple of dimensions ("labels") is stored as a single
// label value where all label values are joined with ".".
type CountersWithMultiLabels struct {
	counters
	labels         []string
	combinedLabels []bool
}

// NewCountersWithMultiLabels creates a new CountersWithMultiLabels
// instance, and publishes it if name is set.
func NewCountersWithMultiLabels(name, help string, labels []string) *CountersWithMultiLabels {
	t := &CountersWithMultiLabels{
		counters: counters{
			counts: make(map[string]int64),
			help:   help},
		labels:         labels,
		combinedLabels: make([]bool, len(labels)),
	}
	for i, label := range labels {
		t.combinedLabels[i] = IsDimensionCombined(label)
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

// Add adds a value to a named counter.
// len(names) must be equal to len(Labels)
func (mc *CountersWithMultiLabels) Add(names []string, value int64) {
	if len(names) != len(mc.labels) {
		panic("CountersWithMultiLabels: wrong number of values in Add")
	}
	if value < 0 {
		logCounterNegative.Warningf("Adding a negative value to a counter, %v should be a gauge instead", mc)
	}
	mc.counters.add(safeJoinLabels(names, mc.combinedLabels), value)
}

// Reset resets the value of a named counter back to 0.
// len(names) must be equal to len(Labels).
func (mc *CountersWithMultiLabels) Reset(names []string) {
	if len(names) != len(mc.labels) {
		panic("CountersWithMultiLabels: wrong number of values in Reset")
	}

	mc.counters.set(safeJoinLabels(names, mc.combinedLabels), 0)
}

// ResetAll clears the counters
func (mc *CountersWithMultiLabels) ResetAll() {
	mc.counters.reset()
}

// Counts returns a copy of the Counters' map.
// The key is a single string where all labels are joined by a "." e.g.
// "label1.label2".
func (mc *CountersWithMultiLabels) Counts() map[string]int64 {
	return mc.counters.Counts()
}

// CountersFuncWithMultiLabels is a multidimensional counters implementation
// where names of categories are compound names made with joining
// multiple strings with '.'.  Since the map is returned by the
// function, we assume it's in the right format (meaning each key is
// of the form 'aaa.bbb.ccc' with as many elements as there are in
// Labels).
//
// Note that there is no CountersFuncWithSingleLabel object. That this
// because such an object would be identical to this one because these
// function-based counters have no Add() or Set() method which are different
// for the single vs. multiple labels cases.
// If you have only a single label, pass an array with a single element.
type CountersFuncWithMultiLabels struct {
	f      func() map[string]int64
	help   string
	labels []string
}

// Labels returns the list of labels.
func (c CountersFuncWithMultiLabels) Labels() []string {
	return c.labels
}

// Help returns the help string.
func (c CountersFuncWithMultiLabels) Help() string {
	return c.help
}

// NewCountersFuncWithMultiLabels creates a new CountersFuncWithMultiLabels
// mapping to the provided function.
func NewCountersFuncWithMultiLabels(name, help string, labels []string, f func() map[string]int64) *CountersFuncWithMultiLabels {
	t := &CountersFuncWithMultiLabels{
		f:      f,
		help:   help,
		labels: labels,
	}
	if name != "" {
		publish(name, t)
	}

	return t
}

// Counts returns a copy of the counters' map.
func (c CountersFuncWithMultiLabels) Counts() map[string]int64 {
	return c.f()
}

// String implements the expvar.Var interface.
func (c CountersFuncWithMultiLabels) String() string {
	m := c.f()
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

// GaugesWithSingleLabel is similar to CountersWithSingleLabel, except its
// meant to track the current value and not a cumulative count.
type GaugesWithSingleLabel struct {
	CountersWithSingleLabel
}

// NewGaugesWithSingleLabel creates a new GaugesWithSingleLabel and
// publishes it if the name is set.
func NewGaugesWithSingleLabel(name, help, label string, tags ...string) *GaugesWithSingleLabel {
	g := &GaugesWithSingleLabel{
		CountersWithSingleLabel: CountersWithSingleLabel{
			counters: counters{
				counts: make(map[string]int64),
				help:   help,
			},
			label: label,
		},
	}

	for _, tag := range tags {
		g.counts[tag] = 0
	}
	if name != "" {
		publish(name, g)
	}
	return g
}

// Set sets the value of a named gauge.
func (g *GaugesWithSingleLabel) Set(name string, value int64) {
	g.counters.set(name, value)
}

// GaugesWithMultiLabels is a CountersWithMultiLabels implementation where
// the values can go up and down.
type GaugesWithMultiLabels struct {
	CountersWithMultiLabels
}

// NewGaugesWithMultiLabels creates a new GaugesWithMultiLabels instance,
// and publishes it if name is set.
func NewGaugesWithMultiLabels(name, help string, labels []string) *GaugesWithMultiLabels {
	t := &GaugesWithMultiLabels{
		CountersWithMultiLabels: CountersWithMultiLabels{
			counters: counters{
				counts: make(map[string]int64),
				help:   help,
			},
			labels: labels,
		}}
	if name != "" {
		publish(name, t)
	}

	return t
}

// Set sets the value of a named counter.
// len(names) must be equal to len(Labels).
func (mg *GaugesWithMultiLabels) Set(names []string, value int64) {
	if len(names) != len(mg.CountersWithMultiLabels.labels) {
		panic("GaugesWithMultiLabels: wrong number of values in Set")
	}
	mg.counters.set(safeJoinLabels(names, nil), value)
}

// GaugesFuncWithMultiLabels is a wrapper around CountersFuncWithMultiLabels
// for values that go up/down for implementations (like Prometheus) that
// need to differ between Counters and Gauges.
type GaugesFuncWithMultiLabels struct {
	CountersFuncWithMultiLabels
}

// NewGaugesFuncWithMultiLabels creates a new GaugesFuncWithMultiLabels
// mapping to the provided function.
func NewGaugesFuncWithMultiLabels(name, help string, labels []string, f func() map[string]int64) *GaugesFuncWithMultiLabels {
	t := &GaugesFuncWithMultiLabels{
		CountersFuncWithMultiLabels: CountersFuncWithMultiLabels{
			f:      f,
			help:   help,
			labels: labels,
		}}

	if name != "" {
		publish(name, t)
	}

	return t
}
