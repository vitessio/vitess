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
	"golang.org/x/net/context"
	"sync"
	"time"
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
	sync.Mutex

	factory Factory

	// state contains settings, inventory counts, and statistics of the pool.
	state State

	// pool contains active resources.
	pool chan resourceWrapper

	// idleTimer is used to terminate idle resources that are in the pool.
	idleTimer *timer.Timer

	// waitTimers tracks when each waiter started to wait.
	waitTimers []time.Time
}

type State struct {
	Waiters     int
	InPool      int
	InUse       int
	Capacity    int
	MinActive   int
	Closed      bool
	Draining    bool
	IdleTimeout time.Duration

	// IdleClosed tracks the number of resources closed due to being idle.
	IdleClosed int64

	// WaitCount contains the number of times Get() had to block and wait
	// for a resource.
	WaitCount int64

	// WaitCount tracks the total time waiting for a resource.
	WaitTime time.Duration
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
//
// minActive is used to prepare and maintain a minimum amount
// of active resources. Any errors when instantiating the factory
// will cause the active resource count to be lower than requested.
func NewResourcePool(factory Factory, capacity, maxCap int, idleTimeout time.Duration, minActive int) *ResourcePool {
	if capacity <= 0 || maxCap <= 0 || capacity > maxCap {
		panic(errors.New("invalid/out of range capacity"))
	}
	if minActive > capacity {
		panic(fmt.Errorf("minActive %v higher than capacity %v", minActive, capacity))
	}

	rp := &ResourcePool{
		factory: factory,
		pool:    make(chan resourceWrapper, maxCap),
	}

	rp.state = State{
		Capacity:  capacity,
		MinActive: minActive,
	}

	rp.SetIdleTimeout(idleTimeout)
	rp.ensureMinimumActive()

	return rp
}

func (rp *ResourcePool) create() (resourceWrapper, error) {
	r, err := rp.factory()
	if err != nil {
		return resourceWrapper{}, err
	}

	return resourceWrapper{
		resource: r,
		timeUsed: time.Now(),
	}, nil
}

func (rp *ResourcePool) createEmpty() resourceWrapper {
	return resourceWrapper{}
}

func (rp *ResourcePool) ensureMinimumActive() {
	rp.Lock()
	defer rp.Unlock()
	if rp.state.MinActive == 0 || rp.state.Closed {
		return
	}

	required := rp.state.MinActive - rp.active()
	for i := 0; i < required; i++ {
		// TODO(gak): This might be better as an empty, so it doesn't
		// block the main goroutine.
		r, err := rp.create()
		if err != nil {
			fmt.Println("error creating factory", err)
			// TODO(gak): How to handle factory error?
			break
		}
		rp.pool <- r
		rp.state.InPool++
	}
}

// Close empties the pool calling Close on all its resources.
// You can call Close while there are outstanding resources.
// It waits for all resources to be returned (Put).
// After a Close, Get is not allowed.
func (rp *ResourcePool) Close() {
	rp.SetIdleTimeout(0)
	_ = rp.SetCapacity(0, true)
}

// IsClosed returns true if the resource pool is closed.
func (rp *ResourcePool) IsClosed() bool {
	return rp.State().Closed
}

// Get will return the next available resource. If capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will wait till the next resource becomes available or a timeout.
func (rp *ResourcePool) Get(ctx context.Context) (resource Resource, err error) {
	select {
	case wrapper, ok := <-rp.pool:
		if !ok {
			return nil, ErrClosed
		}

		rp.Lock()
		rp.state.InPool--
		rp.state.InUse++
		rp.Unlock()

		return wrapper.resource, nil

	case <-ctx.Done():
		return nil, ErrTimeout

	default:
		return rp.getWaiting(ctx)

	}
}

func (rp *ResourcePool) getWaiting(ctx context.Context) (Resource, error) {
	startTime := time.Now()

	rp.Lock()
	if rp.active() >= rp.state.Capacity {
		// We don't have capacity, so now we block on pool.
		rp.state.Waiters++
		rp.Unlock()

		select {
		case wrapper, ok := <-rp.pool:
			if !ok {
				return nil, ErrClosed
			}

			rp.Lock()
			rp.state.InPool--
			rp.state.InUse++
			rp.state.Waiters--
			rp.state.WaitCount++
			rp.state.WaitTime += time.Now().Sub(startTime)
			rp.Unlock()

			return wrapper.resource, nil

		case <-ctx.Done():
			return nil, ErrTimeout
		}
	}
	rp.Unlock()

	wrapper, err := rp.create()
	if err != nil {
		return nil, err
	}

	rp.Lock()
	rp.state.InUse++
	rp.Unlock()

	return wrapper.resource, nil

}

// Put will return a resource to the pool. For every successful Get,
// a corresponding Put is required. If you no longer need a resource,
// you will need to call Put(nil) instead of returning the closed resource.
// The will eventually cause a new resource to be created in its place.
func (rp *ResourcePool) Put(resource Resource) {
	rp.Lock()
	defer rp.Unlock()

	rp.state.InUse--

	if rp.state.Closed || rp.active() > rp.state.Capacity {
		if resource != nil {
			resource.Close()
		}
		return
	}

	if resource == nil {

	} else {
		rp.state.InPool++
		rp.pool <- resourceWrapper{
			resource: resource,
			timeUsed: time.Now(),
		}

		// TODO: functionality to error when putting into a full pool.
		//select {
		//case rp.resources <- wrapper:
		//default:
		//	panic(errors.New("attempt to Put into a full ResourcePool (2)"))
		//}
	}
}

// SetCapacity changes the capacity of the pool.
// You can use it to shrink or expand, but not beyond
// the max capacity. If the change requires the pool
// to be shrunk and `block` is true, SetCapacity waits
// till the necessary number of resources are returned
// to the pool.
// A SetCapacity of 0 is equivalent to closing the ResourcePool.
func (rp *ResourcePool) SetCapacity(capacity int, block bool) error {
	rp.Lock()

	if rp.state.Closed {
		rp.Unlock()
		return ErrClosed
	}

	if capacity < 0 || capacity > cap(rp.pool) {
		rp.Unlock()
		return fmt.Errorf("capacity %d is out of range", capacity)
	}

	if capacity != 0 {
		minActive := rp.state.MinActive
		if capacity < minActive {
			rp.Unlock()
			return fmt.Errorf("minActive %v would now be higher than capacity %v", minActive, capacity)
		}
	}

	isGrowing := capacity > rp.state.Capacity
	rp.state.Capacity = capacity

	if isGrowing {
		rp.state.Draining = false
		rp.Unlock()
		return nil
	}

	rp.state.Draining = true
	rp.Unlock()

	var done chan bool
	if block {
		done = make(chan bool)
	}
	go func() {
		// Shrinking capacity. Loop until enough pool resources are closed.
		for {
			rp.Lock()
			remaining := rp.active() - rp.state.Capacity
			fmt.Println("drain: remaining", remaining)
			if remaining <= 0 {
				rp.state.Draining = false
				if rp.state.Capacity == 0 {
					close(rp.pool)
					rp.state.Closed = true
				}
				rp.Unlock()
				done <- true
				return
			}
			rp.Unlock()

			// We can't remove InUse resources, so only target the pool.
			// Collect the other resources lazily when they're returned.
			fmt.Println("drain: waiting to close a resource")
			r := <-rp.pool
			r.resource.Close()
			fmt.Println("drain: closing resource")

			rp.Lock()
			rp.state.InPool--
			rp.Unlock()
		}
	}()

	if block {
		<-done
	}

	return nil
}

// SetIdleTimeout sets the idle timeout for resources. The timeout is
// checked at the 10th of the period of the timeout.
func (rp *ResourcePool) SetIdleTimeout(idleTimeout time.Duration) {
	rp.Lock()
	defer rp.Unlock()

	rp.state.IdleTimeout = idleTimeout
	fastInterval := rp.state.IdleTimeout / 10

	if rp.idleTimer == nil {
		rp.idleTimer = timer.NewTimer(fastInterval)
	}

	if rp.state.IdleTimeout == 0 {
		rp.idleTimer.Stop()
		return
	}

	rp.idleTimer.SetInterval(fastInterval)
	rp.idleTimer.Start(rp.closeIdleResources)
}

// closeIdleResources scans the pool for idle resources
// and closes them.
func (rp *ResourcePool) closeIdleResources() {
	rp.Lock()

	// Shouldn't be zero, but checking in case.
	if rp.state.IdleTimeout == 0 {
		rp.Unlock()
		return
	}
	rp.Unlock()

	for i := 0; i < rp.state.InPool; i++ {
		select {
		case wrapper, ok := <-rp.pool:
			if !ok {
				return
			}

			rp.Lock()
			if wrapper.timeUsed.Add(rp.state.IdleTimeout).Sub(time.Now()) < 0 {
				fmt.Println("closing resource due to timeout")
				rp.state.IdleClosed++
				rp.state.InPool--
				rp.Unlock()
				wrapper.resource.Close()
				rp.Lock()
				wrapper.resource = nil
				continue
			}
			rp.Unlock()

			// Not expired--back into the pool we go.
			rp.pool <- wrapper

		default:
			// The pool might have been used while we were iterating.
			// Maybe next time!
			return
		}
	}
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

func (rp *ResourcePool) State() State {
	rp.Lock()
	state := rp.state
	rp.Unlock()

	return state
}

// Capacity returns the capacity.
func (rp *ResourcePool) Capacity() int {
	return rp.State().Capacity
}

// Available returns the number of currently unused and available resources.
func (rp *ResourcePool) Available() int {
	s := rp.State()
	available := s.Capacity - s.InUse
	// Sometimes we can be over capacity temporarily while the capacity shrinks.
	if available < 0 {
		return 0
	}
	return available
}

// Active returns the number of active (i.e. non-nil) resources either in the
// pool or claimed for use
func (rp *ResourcePool) Active() int {
	rp.Lock()
	v := rp.active()
	rp.Unlock()
	return v
}

func (rp *ResourcePool) active() int {
	return rp.state.InUse + rp.state.InPool
}

// MinActive returns the minimum amount of resources keep active.
func (rp *ResourcePool) MinActive() int {
	return rp.State().MinActive
}

// InUse returns the number of claimed resources from the pool
func (rp *ResourcePool) InUse() int {
	return rp.State().InUse
}

// MaxCap returns the max capacity.
func (rp *ResourcePool) MaxCap() int {
	return cap(rp.pool)
}

// WaitCount returns the total number of waits.
func (rp *ResourcePool) WaitCount() int64 {
	return rp.State().WaitCount
}

// WaitTime returns the total wait time.
func (rp *ResourcePool) WaitTime() time.Duration {
	return rp.State().WaitTime
}

// IdleTimeout returns the idle timeout.
func (rp *ResourcePool) IdleTimeout() time.Duration {
	return rp.State().IdleTimeout
}

// IdleClosed returns the count of resources closed due to idle timeout.
func (rp *ResourcePool) IdleClosed() int64 {
	return rp.State().IdleClosed
}
