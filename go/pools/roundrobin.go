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

/*
	Package pools provides functionality to manage and reuse resources
	like connections.
*/
package pools

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Factory is a function that can be used to create a resource.
type Factory func() (Resource, error)

// Every resource needs to suport the Resource interface.
type Resource interface {
	Close()
	IsClosed() bool
}

// RoundRobin allows you to use a pool of resources in a round robin fashion.
type RoundRobin struct {
	// mu controls resources & factory
	// Use Lock to modify, RLock otherwise
	mu        sync.RWMutex
	resources chan fifoWrapper
	factory   Factory

	// Use sync/atomic to access the following vars
	size        int64
	waitCount   int64
	waitTime    int64
	idleTimeout int64
}

type fifoWrapper struct {
	resource Resource
	timeUsed time.Time
}

// NewRoundRobin creates a new RoundRobin pool.
// capacity is the maximum number of resources RoundRobin will create.
// factory will be the function used to create resources.
// If a resource is unused beyond idleTimeout, it's discarded.
func NewRoundRobin(capacity int, idleTimeout time.Duration) *RoundRobin {
	return &RoundRobin{
		resources:   make(chan fifoWrapper, capacity),
		size:        0,
		idleTimeout: int64(idleTimeout),
	}
}

// Open starts allowing the creation of resources
func (self *RoundRobin) Open(factory Factory) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.factory = factory
}

// Close empties the pool calling Close on all its resources.
// It waits for all resources to be returned (Put).
func (self *RoundRobin) Close() {
	self.mu.RLock()
	defer self.mu.RUnlock()
	for atomic.LoadInt64(&self.size) > 0 {
		fw := <-self.resources
		fw.resource.Close()
		atomic.AddInt64(&self.size, -1)
	}
	self.factory = nil
}

func (self *RoundRobin) IsClosed() bool {
	return self.factory == nil
}

// Get will return the next available resource. If none is available, and capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will indefinitely wait till the next resource becomes available.
func (self *RoundRobin) Get() (resource Resource, err error) {
	if resource, err = self.TryGet(); err != nil {
		return nil, err
	}
	if resource == nil {
		defer self.recordWait(time.Now())
		self.mu.RLock()
		defer self.mu.RUnlock()
		resource = (<-self.resources).resource
	}
	return resource, nil
}

// Get will return the next available resource. If none is available, and capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will return nil with no error.
func (self *RoundRobin) TryGet() (resource Resource, err error) {
	self.mu.RLock()
	defer self.mu.RUnlock()
	idleTimeout := time.Duration(atomic.LoadInt64(&self.idleTimeout))
	for {
		select {
		case fw := <-self.resources:
			if idleTimeout > 0 && fw.timeUsed.Add(idleTimeout).Sub(time.Now()) < 0 {
				fw.resource.Close()
				atomic.AddInt64(&self.size, -1)
				continue
			}
			return fw.resource, nil
		default:
			if atomic.LoadInt64(&self.size) >= int64(cap(self.resources)) {
				return nil, nil
			}
			// Prevent thundering herd: optimistically increment
			// size before creating resource
			atomic.AddInt64(&self.size, 1)
			if resource, err = self.factory(); err != nil {
				atomic.AddInt64(&self.size, -1)
			}
			return resource, err
		}
	}
	panic("unreachable")
}

// Put will return a resource to the pool. You MUST return every resource to the pool,
// even if it's closed. If a resource is closed, Put will discard it.
func (self *RoundRobin) Put(resource Resource) {
	self.mu.RLock()
	defer self.mu.RUnlock()
	if atomic.LoadInt64(&self.size) > int64(cap(self.resources)) {
		resource.Close()
		// Better not trust the resource to return true for IsClosed.
		atomic.AddInt64(&self.size, -1)
		return
	}
	if resource.IsClosed() {
		atomic.AddInt64(&self.size, -1)
	} else {
		self.resources <- fifoWrapper{resource, time.Now()}
	}
}

// Set capacity changes the capacity of the pool.
// You can use it to expand or shrink.
func (self *RoundRobin) SetCapacity(capacity int) {
	self.mu.Lock()
	defer self.mu.Unlock()
	nr := make(chan fifoWrapper, capacity)
	for {
		select {
		case fw := <-self.resources:
			if len(nr) < cap(nr) {
				nr <- fw
			} else {
				go fw.resource.Close()
				atomic.AddInt64(&self.size, -1)
			}
			continue
		default:
		}
		break
	}
	self.resources = nr
}

func (self *RoundRobin) SetIdleTimeout(idleTimeout time.Duration) {
	atomic.StoreInt64(&self.idleTimeout, int64(idleTimeout))
}

func (self *RoundRobin) StatsJSON() string {
	s, c, a, wc, wt, it := self.Stats()
	return fmt.Sprintf("{\"Size\": %v, \"Capacity\": %v, \"Available\": %v, \"WaitCount\": %v, \"WaitTime\": %v, \"IdleTimeout\": %v}", s, c, a, wc, float64(wt)/1e9, float64(it)/1e9)
}

func (self *RoundRobin) Stats() (size, capacity, available, waitCount int64, waitTime, idleTimeout time.Duration) {
	self.mu.RLock()
	defer self.mu.RUnlock()
	return atomic.LoadInt64(&self.size), int64(cap(self.resources)), int64(len(self.resources)), atomic.LoadInt64(&self.waitCount), time.Duration(atomic.LoadInt64(&self.waitTime)), time.Duration(atomic.LoadInt64(&self.idleTimeout))
}

func (self *RoundRobin) recordWait(start time.Time) {
	diff := int64(time.Now().Sub(start))
	atomic.AddInt64(&self.waitCount, 1)
	atomic.AddInt64(&self.waitTime, diff)
}
