// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pools provides functionality to manage and reuse resources
// like connections.
package pools

import (
	"fmt"
	"sync"
	"time"

	"code.google.com/p/vitess/go/sync2"
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

// ResourcePool allows you to use a pool of resources.
type ResourcePool struct {
	mu          sync.RWMutex
	resources   chan resourceWrapper
	factory     Factory
	idleTimeout sync2.AtomicDuration

	// housekeeping is a lock used by Open, Close and SetCapacity to prevent
	// race conditions between themselves.
	// mu cannot relied upon because some of the house keeping work requires
	// mu to be released to allow some normal operations.
	housekeeping sync.Mutex

	// stats
	size      sync2.AtomicInt64
	waitCount sync2.AtomicInt64
	waitTime  sync2.AtomicDuration
}

type resourceWrapper struct {
	resource Resource
	timeUsed time.Time
}

// NewResourcePool creates a new ResourcePool pool.
// capacity is the maximum number of resources ResourcePool will maintain.
// factory will be the function used to create resources.
// If a resource is unused beyond idleTimeout, it's discarded.
// An idleTimeout of 0 means that there is no timeout.
func NewResourcePool(capacity int, idleTimeout time.Duration) *ResourcePool {
	rp := &ResourcePool{
		resources:   make(chan resourceWrapper, capacity),
		idleTimeout: sync2.AtomicDuration(idleTimeout),
	}
	return rp
}

// Open starts allowing the creation of resources
func (rp *ResourcePool) Open(factory Factory) {
	rp.housekeeping.Lock()
	defer rp.housekeeping.Unlock()
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if rp.factory != nil {
		panic(fmt.Errorf("ResourcePool is already open"))
	}
	rp.factory = factory
	for i := 0; i < cap(rp.resources); i++ {
		rp.resources <- resourceWrapper{}
	}
}

// Close empties the pool calling Close on all its resources.
// You can call Close while there are outstanding resources.
// It waits for all resources to be returned (Put).
// During a Close, Get and TryGet are not allowed.
func (rp *ResourcePool) Close() {
	rp.housekeeping.Lock()
	defer rp.housekeeping.Unlock()
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if rp.factory == nil {
		return
	}
	// Setting factory to nil will disallow Get & TryGet
	rp.factory = nil
	func() {
		// Drain & close all resources.
		// Can't hold a lock because we want to allow Puts.
		rp.mu.Unlock()
		defer rp.mu.Lock()
		for i := 0; i < cap(rp.resources); i++ {
			rw := <-rp.resources
			rp.closeResource(rw.resource)
		}
	}()
}

func (rp *ResourcePool) IsClosed() bool {
	rp.mu.RLock()
	defer rp.mu.RUnlock()
	return rp.factory == nil
}

// Get will return the next available resource. If none is available, and capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will indefinitely wait till the next resource becomes available.
func (rp *ResourcePool) Get() (resource Resource, err error) {
	rp.mu.RLock()
	defer rp.mu.RUnlock()
	if rp.factory == nil {
		return nil, fmt.Errorf("ResourcePool is closed or closing")
	}
	var rw resourceWrapper
	select {
	case rw = <-rp.resources:
	default:
		// Record stats only if we have to wait
		start := time.Now()
		rw = <-rp.resources
		rp.recordWait(start)
	}
	return rp.process(rw)
}

// TryGet will return the next available resource. If none is available, and capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will return nil with no error.
func (rp *ResourcePool) TryGet() (resource Resource, err error) {
	rp.mu.RLock()
	defer rp.mu.RUnlock()
	if rp.factory == nil {
		return nil, fmt.Errorf("ResourcePool is closed or closing")
	}
	var rw resourceWrapper
	select {
	case rw = <-rp.resources:
	default:
		return nil, nil
	}
	return rp.process(rw)
}

// Put will return a resource to the pool. You MUST return every resource to the pool,
// even if it's closed. If a resource is closed, Put will discard it. Thread synchronization
// between Close() and IsClosed() is the caller's responsibility.
func (rp *ResourcePool) Put(resource Resource) {
	rp.mu.RLock()
	defer rp.mu.RUnlock()
	var wrapper resourceWrapper
	if resource.IsClosed() {
		rp.size.Add(-1)
	} else {
		wrapper = resourceWrapper{resource, time.Now()}
	}
	select {
	case rp.resources <- wrapper:
	default:
		panic("Attempt to Put into a full ResourcePool")
	}
}

// SetCapacity changes the capacity of the pool.
// You can use it to expand or shrink. If the change
// requires the pool to be shrunk, SetCapacity waits till
// the necessary number of resources are returned
// to the pool.
func (rp *ResourcePool) SetCapacity(capacity int) {
	rp.housekeeping.Lock()
	defer rp.housekeeping.Unlock()
	rp.mu.Lock()
	defer rp.mu.Unlock()
	// If the pool is closed, just make a new channel.
	if rp.factory == nil {
		rp.resources = make(chan resourceWrapper, capacity)
		return
	}
	newpool := make(chan resourceWrapper, capacity)
	if capacity < cap(rp.resources) {
		// We need to shrink
		func() {
			// Unlock needed to allow Puts to go through
			rp.mu.Unlock()
			defer rp.mu.Lock()
			for i := 0; i < cap(rp.resources)-capacity; i++ {
				rw := <-rp.resources
				rp.closeResource(rw.resource)
			}
		}()
	} else {
		// Add more slots to new channel
		for i := 0; i < capacity-cap(rp.resources); i++ {
			newpool <- resourceWrapper{}
		}
	}
	// Transfer left-over slots if any
	for {
		select {
		case rw := <-rp.resources:
			newpool <- rw
			continue
		default:
		}
		break
	}
	rp.resources = newpool
}

// process validates the resource in the wrapper before returning the resource.
// If the wrapper is empty, it tries to create a resource.
// If the resource was idle for too long, it creates a new resource instead.
func (rp *ResourcePool) process(rw resourceWrapper) (resource Resource, err error) {
	if rw.resource == nil {
		return rp.createResource()
	}
	timeout := rp.idleTimeout.Get()
	if timeout > 0 && rw.timeUsed.Add(timeout).Sub(time.Now()) < 0 {
		rp.closeResource(rw.resource)
		return rp.createResource()
	}
	return rw.resource, nil
}

// createResource creates a resource using the factory and
// returns it. I can only be called after a resource wrapper
// has been fetched from resources. On failure, it puts an
// empty wrapper into the pool to keep the channel full.
func (rp *ResourcePool) createResource() (Resource, error) {
	r, err := rp.factory()
	if err != nil {
		rp.resources <- resourceWrapper{}
		return nil, err
	}
	rp.size.Add(1)
	return r, nil
}

func (rp *ResourcePool) closeResource(resource Resource) {
	if resource == nil {
		return
	}
	rp.size.Add(-1)
	if !resource.IsClosed() {
		resource.Close()
	}
}

func (rp *ResourcePool) recordWait(start time.Time) {
	rp.waitCount.Add(1)
	rp.waitTime.Add(time.Now().Sub(start))
}

func (rp *ResourcePool) SetIdleTimeout(idleTimeout time.Duration) {
	rp.idleTimeout.Set(idleTimeout)
}

func (rp *ResourcePool) StatsJSON() string {
	s, c, a, wc, wt, it := rp.Stats()
	return fmt.Sprintf(`{"Size": %v, "Capacity": %v, "Available": %v, "WaitCount": %v, "WaitTime": %v, "IdleTimeout": %v}`, s, c, a, wc, int64(wt), int64(it))
}

func (rp *ResourcePool) Stats() (size, capacity, available, waitCount int64, waitTime, idleTimeout time.Duration) {
	return rp.size.Get(), int64(cap(rp.resources)), int64(len(rp.resources)), rp.waitCount.Get(), rp.waitTime.Get(), rp.idleTimeout.Get()
}
