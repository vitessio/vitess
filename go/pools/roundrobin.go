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
	"time"
)

// Factory is a function that can be used to create a resource.
type Factory func() (Resource, error)

// Every resource needs to suport the Resource interface.
// Thread synchronization between Close() and IsClosed()
// is the responsibility the caller.
type Resource interface {
	Close()
	IsClosed() bool
}

// RoundRobin allows you to use a pool of resources in a round robin fashion.
type RoundRobin struct {
	mu          sync.Mutex
	available   *sync.Cond
	resources   chan fifoWrapper
	size        int64
	factory     Factory
	idleTimeout time.Duration

	// stats
	waitCount int64
	waitTime  time.Duration
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
	r := &RoundRobin{
		resources:   make(chan fifoWrapper, capacity),
		size:        0,
		idleTimeout: idleTimeout,
	}
	r.available = sync.NewCond(&r.mu)
	return r
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
	self.mu.Lock()
	defer self.mu.Unlock()
	for self.size > 0 {
		select {
		case fw := <-self.resources:
			go fw.resource.Close()
			self.size--
		default:
			self.available.Wait()
		}
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
	return self.get(true)
}

// TryGet will return the next available resource. If none is available, and capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will return nil with no error.
func (self *RoundRobin) TryGet() (resource Resource, err error) {
	return self.get(false)
}

func (self *RoundRobin) get(wait bool) (resource Resource, err error) {
	self.mu.Lock()
	defer self.mu.Unlock()
	// Any waits in this loop will release the lock, and it will be
	// reacquired before the waits return.
	for {
		select {
		case fw := <-self.resources:
			// Found a free resource in the channel
			if self.idleTimeout > 0 && fw.timeUsed.Add(self.idleTimeout).Sub(time.Now()) < 0 {
				// resource has been idle for too long. Discard & go for next.
				go fw.resource.Close()
				self.size--
				continue
			}
			return fw.resource, nil
		default:
			// resource channel is empty
			if self.size >= int64(cap(self.resources)) {
				// The pool is full
				if wait {
					start := time.Now()
					self.available.Wait()
					self.recordWait(start)
					continue
				}
				return nil, nil
			}
			// Pool is not full. Create a resource.
			if resource, err = self.waitForCreate(); err == nil {
				// Creation successful. Account for this by incrementing size.
				self.size++
			}
			return resource, err
		}
	}
	panic("unreachable")
}

func (self *RoundRobin) recordWait(start time.Time) {
	self.waitCount++
	self.waitTime += time.Now().Sub(start)
}

func (self *RoundRobin) waitForCreate() (resource Resource, err error) {
	// Prevent thundering herd: increment size before creating resource, and decrement after.
	self.size++
	self.mu.Unlock()
	defer func() {
		self.mu.Lock()
		self.size--
	}()
	return self.factory()
}

// Put will return a resource to the pool. You MUST return every resource to the pool,
// even if it's closed. If a resource is closed, Put will discard it. Thread synchronization
// between Close() and IsClosed() is the caller's responsibility.
func (self *RoundRobin) Put(resource Resource) {
	self.mu.Lock()
	defer self.mu.Unlock()
	defer self.available.Signal()

	if self.size > int64(cap(self.resources)) {
		go resource.Close()
		self.size--
	} else if resource.IsClosed() {
		self.size--
	} else {
		self.resources <- fifoWrapper{resource, time.Now()}
	}
}

// Set capacity changes the capacity of the pool.
// You can use it to expand or shrink.
func (self *RoundRobin) SetCapacity(capacity int) {
	self.mu.Lock()
	defer self.mu.Unlock()
	defer self.available.Broadcast()

	nr := make(chan fifoWrapper, capacity)
	// This loop transfers resources from the old channel
	// to the new one, until it fills up or runs out.
	// It discards extras, if any.
	for {
		select {
		case fw := <-self.resources:
			if len(nr) < cap(nr) {
				nr <- fw
			} else {
				go fw.resource.Close()
				self.size--
			}
			continue
		default:
		}
		break
	}
	self.resources = nr
}

func (self *RoundRobin) SetIdleTimeout(idleTimeout time.Duration) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.idleTimeout = idleTimeout
}

func (self *RoundRobin) StatsJSON() string {
	s, c, a, wc, wt, it := self.Stats()
	return fmt.Sprintf("{\"Size\": %v, \"Capacity\": %v, \"Available\": %v, \"WaitCount\": %v, \"WaitTime\": %v, \"IdleTimeout\": %v}", s, c, a, wc, float64(wt)/1e9, float64(it)/1e9)
}

func (self *RoundRobin) Stats() (size, capacity, available, waitCount int64, waitTime, idleTimeout time.Duration) {
	self.mu.Lock()
	defer self.mu.Unlock()
	return self.size, int64(cap(self.resources)), int64(len(self.resources)), self.waitCount, self.waitTime, self.idleTimeout
}
