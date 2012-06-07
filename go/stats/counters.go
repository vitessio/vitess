// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"bytes"
	"expvar"
	"fmt"
	"sync"
)

type Counters struct {
	mu     sync.Mutex
	counts map[string]int64
}

func NewCounters(name string) *Counters {
	c := &Counters{counts: make(map[string]int64)}
	if name != "" {
		expvar.Publish(name, c)
	}
	return c
}

func (c *Counters) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	b := bytes.NewBuffer(make([]byte, 0, 4096))
	fmt.Fprintf(b, "{")
	firstValue := true
	for k, v := range c.counts {
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

func (c *Counters) Add(name string, value int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[name] += value
}

func (c *Counters) Counts() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	counts := make(map[string]int64, len(c.counts))
	for k, v := range c.counts {
		counts[k] = v
	}
	return counts
}
