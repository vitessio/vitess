// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
}

// NewCounters create a new Counters instance. If name is set, the variable
// gets published. The functional also accepts an optional list of tags that
// pre-creates them initialized to 0.
func NewCounters(name string, tags ...string) *Counters {
	c := &Counters{counts: make(map[string]*int64)}
	for _, tag := range tags {
		c.counts[tag] = new(int64)
	}
	if name != "" {
		publish(name, c)
	}
	return c
}

// String is used by expvar.
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
		fmt.Fprintf(b, "\"%v\": %v", k, atomic.LoadInt64(a))
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

// Set sets the value of a named counter.
func (c *Counters) Set(name string, value int64) {
	a := c.getValueAddr(name)
	atomic.StoreInt64(a, value)
}

// Reset resets all counter values
func (c *Counters) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts = make(map[string]*int64)
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
		fmt.Fprintf(b, "\"%v\": %v", k, v)
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
func NewMultiCounters(name string, labels []string) *MultiCounters {
	t := &MultiCounters{
		Counters: Counters{counts: make(map[string]*int64)},
		labels:   labels,
	}
	if name != "" {
		publish(name, t)
	}
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
	mc.Counters.Add(strings.Join(names, "."), value)
}

// Set sets the value of a named counter. len(names) must be equal to
// len(Labels)
func (mc *MultiCounters) Set(names []string, value int64) {
	if len(names) != len(mc.labels) {
		panic("MultiCounters: wrong number of values in Set")
	}
	mc.Counters.Set(strings.Join(names, "."), value)
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
}

// Labels returns the list of labels.
func (mcf *MultiCountersFunc) Labels() []string {
	return mcf.labels
}

// NewMultiCountersFunc creates a new MultiCountersFunc mapping to the provided
// function.
func NewMultiCountersFunc(name string, labels []string, f CountersFunc) *MultiCountersFunc {
	t := &MultiCountersFunc{
		CountersFunc: f,
		labels:       labels,
	}
	if name != "" {
		publish(name, t)
	}
	return t
}
