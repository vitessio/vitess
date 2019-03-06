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

// Package pools provides functionality to manage and reuse resources
// like connections.
package pools

import (
	"errors"
	"fmt"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/timer"
)

var (
	// ErrClosed is returned if ResourcePool is used when it's closed.
	ErrClosed = errors.New("resource pool is closed")

	// ErrTimeout is returned if a resource get times out.
	ErrTimeout = errors.New("resource pool timed out")
)

// Factory is a function that can be used to create a resource.
type Factory func() (Resource, error)

// Resource defines the interface that every resource must provide.
// Thread synchronization between Close() and IsClosed()
// is the responsibility of the caller.
type Resource interface {
	Close()
}

// ResourcePool allows you to use a pool of resources.
type ResourcePool struct {
	resources   chan resourceWrapper
	factory     Factory
	capacity    sync2.AtomicInt64
	idleTimeout sync2.AtomicDuration
	idleTimer   *timer.Timer

	// stats
	available  sync2.AtomicInt64
	active     sync2.AtomicInt64
	inUse      sync2.AtomicInt64
	waitCount  sync2.AtomicInt64
	waitTime   sync2.AtomicDuration
	idleClosed sync2.AtomicInt64
}

type resourceWrapper struct {
	resource Resource
	timeUsed time.Time
}

// NewResourcePool creates a new ResourcePool pool.
// capacity is the number of possible resources in the pool:
// there can be up to 'capacity' of these at a given time.
// maxCap specifies the extent to which the pool can be resized
// in the future through the SetCapacity function.
// You cannot resize the pool beyond maxCap.
// If a resource is unused beyond idleTimeout, it's discarded.
// An idleTimeout of 0 means that there is no timeout.
func NewResourcePool(factory Factory, capacity, maxCap int, idleTimeout time.Duration) *ResourcePool {
	if capacity <= 0 || maxCap <= 0 || capacity > maxCap {
		panic(errors.New("invalid/out of range capacity"))
	}
	rp := &ResourcePool{
		resources:   make(chan resourceWrapper, maxCap),
		factory:     factory,
		available:   sync2.NewAtomicInt64(int64(capacity)),
		capacity:    sync2.NewAtomicInt64(int64(capacity)),
		idleTimeout: sync2.NewAtomicDuration(idleTimeout),
	}
	for i := 0; i < capacity; i++ {
		rp.resources <- resourceWrapper{}
	}

	if idleTimeout != 0 {
		rp.idleTimer = timer.NewTimer(idleTimeout / 10)
		rp.idleTimer.Start(rp.closeIdleResources)
	}
	return rp
}

// Close empties the pool calling Close on all its resources.
// You can call Close while there are outstanding resources.
// It waits for all resources to be returned (Put).
// After a Close, Get is not allowed.
func (rp *ResourcePool) Close() {
	if rp.idleTimer != nil {
		rp.idleTimer.Stop()
	}
	_ = rp.SetCapacity(0)
}

// IsClosed returns true if the resource pool is closed.
func (rp *ResourcePool) IsClosed() (closed bool) {
	return rp.capacity.Get() == 0
}

// closeIdleResources scans the pool for idle resources
func (rp *ResourcePool) closeIdleResources() {
	available := int(rp.Available())
	idleTimeout := rp.IdleTimeout()

	for i := 0; i < available; i++ {
		var wrapper resourceWrapper
		select {
		case wrapper = <-rp.resources:
		default:
			// stop early if we don't get anything new from the pool
			return
		}

		if wrapper.resource != nil && idleTimeout > 0 && time.Until(wrapper.timeUsed.Add(idleTimeout)) < 0 {
			wrapper.resource.Close()
			wrapper.resource = nil
			rp.idleClosed.Add(1)
			rp.active.Add(-1)
		}

		rp.resources <- wrapper
	}
}

// Get will return the next available resource. If capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will wait till the next resource becomes available or a timeout.
// A timeout of 0 is an indefinite wait.
func (rp *ResourcePool) Get(ctx context.Context) (resource Resource, err error) {
	return rp.get(ctx, true)
}

func (rp *ResourcePool) get(ctx context.Context, wait bool) (resource Resource, err error) {
	// If ctx has already expired, avoid racing with rp's resource channel.
	select {
	case <-ctx.Done():
		return nil, ErrTimeout
	default:
	}

	// Fetch
	var wrapper resourceWrapper
	var ok bool
	select {
	case wrapper, ok = <-rp.resources:
	default:
		if !wait {
			return nil, nil
		}
		startTime := time.Now()
		select {
		case wrapper, ok = <-rp.resources:
		case <-ctx.Done():
			return nil, ErrTimeout
		}
		rp.recordWait(startTime)
	}
	if !ok {
		return nil, ErrClosed
	}

	// Unwrap
	if wrapper.resource == nil {
		wrapper.resource, err = rp.factory()
		if err != nil {
			rp.resources <- resourceWrapper{}
			return nil, err
		}
		rp.active.Add(1)
	}
	rp.available.Add(-1)
	rp.inUse.Add(1)
	return wrapper.resource, err
}

// Put will return a resource to the pool. For every successful Get,
// a corresponding Put is required. If you no longer need a resource,
// you will need to call Put(nil) instead of returning the closed resource.
// The will eventually cause a new resource to be created in its place.
func (rp *ResourcePool) Put(resource Resource) {
	var wrapper resourceWrapper
	if resource != nil {
		wrapper = resourceWrapper{resource, time.Now()}
	} else {
		rp.active.Add(-1)
	}
	select {
	case rp.resources <- wrapper:
	default:
		panic(errors.New("attempt to Put into a full ResourcePool"))
	}
	rp.inUse.Add(-1)
	rp.available.Add(1)
}

// SetCapacity changes the capacity of the pool.
// You can use it to shrink or expand, but not beyond
// the max capacity. If the change requires the pool
// to be shrunk, SetCapacity waits till the necessary
// number of resources are returned to the pool.
// A SetCapacity of 0 is equivalent to closing the ResourcePool.
func (rp *ResourcePool) SetCapacity(capacity int) error {
	if capacity < 0 || capacity > cap(rp.resources) {
		return fmt.Errorf("capacity %d is out of range", capacity)
	}

	// Atomically swap new capacity with old, but only
	// if old capacity is non-zero.
	var oldcap int
	for {
		oldcap = int(rp.capacity.Get())
		if oldcap == 0 {
			return ErrClosed
		}
		if oldcap == capacity {
			return nil
		}
		if rp.capacity.CompareAndSwap(int64(oldcap), int64(capacity)) {
			break
		}
	}

	if capacity < oldcap {
		for i := 0; i < oldcap-capacity; i++ {
			wrapper := <-rp.resources
			if wrapper.resource != nil {
				wrapper.resource.Close()
				rp.active.Add(-1)
			}
			rp.available.Add(-1)
		}
	} else {
		for i := 0; i < capacity-oldcap; i++ {
			rp.resources <- resourceWrapper{}
			rp.available.Add(1)
		}
	}
	if capacity == 0 {
		close(rp.resources)
	}
	return nil
}

func (rp *ResourcePool) recordWait(start time.Time) {
	rp.waitCount.Add(1)
	rp.waitTime.Add(time.Since(start))
}

// SetIdleTimeout sets the idle timeout. It can only be used if there was an
// idle timeout set when the pool was created.
func (rp *ResourcePool) SetIdleTimeout(idleTimeout time.Duration) {
	if rp.idleTimer == nil {
		panic("SetIdleTimeout called when timer not initialized")
	}

	rp.idleTimeout.Set(idleTimeout)
	rp.idleTimer.SetInterval(idleTimeout / 10)
}

// StatsJSON returns the stats in JSON format.
func (rp *ResourcePool) StatsJSON() string {
	return fmt.Sprintf(`{"Capacity": %v, "Available": %v, "Active": %v, "InUse": %v, "MaxCapacity": %v, "WaitCount": %v, "WaitTime": %v, "IdleTimeout": %v, "IdleClosed": %v}`,
		rp.Capacity(),
		rp.Available(),
		rp.Active(),
		rp.InUse(),
		rp.MaxCap(),
		rp.WaitCount(),
		rp.WaitTime().Nanoseconds(),
		rp.IdleTimeout().Nanoseconds(),
		rp.IdleClosed(),
	)
}

// Capacity returns the capacity.
func (rp *ResourcePool) Capacity() int64 {
	return rp.capacity.Get()
}

// Available returns the number of currently unused and available resources.
func (rp *ResourcePool) Available() int64 {
	return rp.available.Get()
}

// Active returns the number of active (i.e. non-nil) resources either in the
// pool or claimed for use
func (rp *ResourcePool) Active() int64 {
	return rp.active.Get()
}

// InUse returns the number of claimed resources from the pool
func (rp *ResourcePool) InUse() int64 {
	return rp.inUse.Get()
}

// MaxCap returns the max capacity.
func (rp *ResourcePool) MaxCap() int64 {
	return int64(cap(rp.resources))
}

// WaitCount returns the total number of waits.
func (rp *ResourcePool) WaitCount() int64 {
	return rp.waitCount.Get()
}

// WaitTime returns the total wait time.
func (rp *ResourcePool) WaitTime() time.Duration {
	return rp.waitTime.Get()
}

// IdleTimeout returns the idle timeout.
func (rp *ResourcePool) IdleTimeout() time.Duration {
	return rp.idleTimeout.Get()
}

// IdleClosed returns the count of resources closed due to idle timeout.
func (rp *ResourcePool) IdleClosed() int64 {
	return rp.idleClosed.Get()
}
