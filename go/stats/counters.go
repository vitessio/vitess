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
	"bytes"
	"expvar"
	"fmt"
	"sync"
	"sync/atomic"
)

type Counters struct {
	mu     sync.RWMutex
	counts map[string][]int64 // slice lets us use pointers
}

func NewCounters(name string) *Counters {
	c := &Counters{counts: make(map[string][]int64)}
	if name != "" {
		expvar.Publish(name, c)
	}
	return c
}

func (c *Counters) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	b := bytes.NewBuffer(make([]byte, 0, 4096))
	fmt.Fprintf(b, "{")
	firstValue := true
	for k, v := range c.counts {
		if firstValue {
			firstValue = false
		} else {
			fmt.Fprintf(b, ", ")
		}
		fmt.Fprintf(b, "\"%v\": %v", k, v[0])
	}
	fmt.Fprintf(b, "}")
	return b.String()
}

func (c *Counters) Add(name string, value int64) {
	c.mu.RLock()
	v, ok := c.counts[name]
	c.mu.RUnlock()

	if ok {
		atomic.AddInt64(&v[0], value)
	} else {
		c.create(name, value)
	}
}

func (c *Counters) create(name string, value int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.counts[name]; ok {
		atomic.AddInt64(&v[0], value)
	} else {
		v = make([]int64, 1)
		v[0] = value
		c.counts[name] = v
	}
}

func (c *Counters) Counts() map[string]int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	counts := make(map[string]int64, len(c.counts))
	for k, v := range c.counts {
		counts[k] = v[0]
	}
	return counts
}
