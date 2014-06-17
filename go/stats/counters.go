// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
)

// Counters is similar to expvar.Map, except that
// it doesn't allow floats. In addition, it provides
// a Counts method which can be used for tracking rates.
type Counters struct {
	mu     sync.Mutex
	counts map[string]int64
}

// NewCounters create a new Counters instance. If name is set, all publishes it.
func NewCounters(name string) *Counters {
	c := &Counters{counts: make(map[string]int64)}
	if name != "" {
		Publish(name, c)
	}
	return c
}

// String is used by expvar.
func (c *Counters) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return counterToString(c.counts)
}

// Add adds a value to a named counter.
func (c *Counters) Add(name string, value int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[name] += value
}

// Set sets the value of a named counter.
func (c *Counters) Set(name string, value int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[name] = value
}

// Counts returns a copy of the Counters' map.
func (c *Counters) Counts() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	counts := make(map[string]int64, len(c.counts))
	for k, v := range c.counts {
		counts[k] = v
	}
	return counts
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
	return counterToString(m)
}

func counterToString(m map[string]int64) string {
	b := bytes.NewBuffer(make([]byte, 0, 4096))
	fmt.Fprintf(b, "{")
	firstValue := true
	for k, v := range m {
		if firstValue {
			firstValue = false
		} else {
			fmt.Fprintf(b, ", ")
		}
		fmt.Fprintf(b, "\"%v\": %v", k, v)
	}
	fmt.Fprintf(b, "}")
	return b.String()
}

// MapCounters is a Counters implementation where names of categories
// are compound names made with joining multiple strings with '.'.
type MapCounters struct {
	Counters
	Labels []string
}

// NewMapCounters creates a new MapCounters instance, and publishes it
// if name is set.
func NewMapCounters(name string, labels []string) *MapCounters {
	t := &MapCounters{
		Counters: Counters{counts: make(map[string]int64)},
		Labels:   labels,
	}
	if name != "" {
		Publish(name, t)
	}
	return t
}

// Add adds a value to a named counter. len(names) must be equal to
// len(Labels)
func (mc *MapCounters) Add(names []string, value int64) {
	if len(names) != len(mc.Labels) {
		panic("MapCounters: wrong number of values in Add")
	}
	mc.Counters.Add(strings.Join(names, "."), value)
}

// Set sets the value of a named counter. len(names) must be equal to
// len(Labels)
func (mc *MapCounters) Set(names []string, value int64) {
	if len(names) != len(mc.Labels) {
		panic("MapCounters: wrong number of values in Set")
	}
	mc.Counters.Set(strings.Join(names, "."), value)
}

// MapCountersFunc is a CountersFunc implementation where names of categories
// are compound names made with joining multiple strings with '.'.
// Since the map is returned by the function, we assume it's in the rigth
// format (meaning each key is of the form 'aaa.bbb.ccc' with as many elements
// as there are in Labels).
type MapCountersFunc struct {
	CountersFunc
	Labels []string
}

// NewMapCountersFunc creates a new MapCountersFunc mapping to the provided
// function.
func NewMapCountersFunc(name string, labels []string, f CountersFunc) *MapCountersFunc {
	t := &MapCountersFunc{
		CountersFunc: f,
		Labels:       labels,
	}
	if name != "" {
		Publish(name, t)
	}
	return t
}
