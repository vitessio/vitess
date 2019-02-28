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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/timer"
)

var _ Pool = &FastPool{}

// FastPool allows you to use a pool of resources.
type FastPool struct {
	sync.Mutex

	factory CreateFactory

	// state contains settings, inventory counts, and statistics of the pool.
	state State

	// pool contains active resources.
	pool chan resourceWrapper

	// idleTimer is used to terminate idle resources that are in the pool.
	idleTimer    *timer.Timer
}

type State struct {
	// Capacity is the maximum number of resources in and out of the pool.
	Capacity int

	// InPool is the number of resources in the pool.
	InPool int

	// InUse is the number of resources allocated outside of the pool.
	InUse int

	// Spawning is the number of resources currently being created.
	Spawning int

	// Waiters is the number of Put() callers waiting for a resource when the pool is empty.
	Waiters int

	// MinActive maintains a minimum number of active resources.
	MinActive int

	// Closed is when the pool is shutting down or already shut down.
	Closed bool

	// Draining is set when the new capacity is lower than the number of active resources.
	Draining bool

	// IdleTimeout specifies how long to leave a resource in existence within the pool.
	IdleTimeout time.Duration

	// IdleClosed tracks the number of resources closed due to being idle.
	IdleClosed int64

	// WaitCount contains the number of times Get() had to block and wait
	// for a resource.
	WaitCount int64

	// WaitCount tracks the total time waiting for a resource.
	WaitTime time.Duration
}

// NewFastPool creates a new pool for generic resources.
//
// capacity is the number of possible active resources allocated.
//
// maxCap specifies the extent to which the pool can be resized
// in the future through the SetCapacity function.
//
// If a resource is unused beyond idleTimeout, it's discarded.
// An idleTimeout of 0 means that there is no timeout.
//
// minActive is used to prepare and maintain a minimum amount
// of active resources. Any errors when instantiating the factory
// will cause the active resource count to be lower than requested.
func NewFastPool(factory CreateFactory, capacity, maxCap int, idleTimeout time.Duration, minActive int) *FastPool {
	if capacity <= 0 || maxCap <= 0 || capacity > maxCap {
		panic(errors.New("invalid/out of range capacity"))
	}
	if minActive > capacity {
		panic(fmt.Errorf("minActive %v higher than capacity %v", minActive, capacity))
	}

	p := &FastPool{
		factory: factory,
		pool:    make(chan resourceWrapper, maxCap),
	}

	p.state = State{
		Capacity:  capacity,
		MinActive: minActive,
	}

	p.ensureMinimumActive()
	p.SetIdleTimeout(idleTimeout)

	return p
}

func (p *FastPool) create() (resourceWrapper, error) {
	r, err := p.factory()
	if err != nil {
		return resourceWrapper{}, err
	}

	return resourceWrapper{
		resource: r,
		timeUsed: time.Now(),
	}, nil
}

func (p *FastPool) safeCreate() (resourceWrapper, error) {
	p.Lock()
	capacity := p.hasFreeCapacity()
	p.state.Spawning++
	p.Unlock()

	if !capacity {
		p.Lock()
		p.state.Spawning--
		p.Unlock()
		return resourceWrapper{}, errNeedToQueue
	}

	wrapper, err := p.create()
	if err != nil {
		p.Lock()
		p.state.Spawning--
		p.Unlock()
		return resourceWrapper{}, err
	}

	p.Lock()
	p.state.InUse++
	p.state.Spawning--
	p.Unlock()

	return wrapper, nil
}

func (p *FastPool) ensureMinimumActive() {
	p.Lock()
	if p.state.MinActive == 0 || p.state.Closed {
		p.Unlock()
		return
	}
	required := p.state.MinActive - p.active()
	p.Unlock()

	for i := 0; i < required; i++ {
		r, err := p.create()
		if err != nil {
			// TODO(gak): How to handle factory error?
			break
		}
		p.Lock()
		p.state.InPool++
		p.Unlock()
		p.pool <- r
	}
}

// Close empties the pool calling Close on all its resources.
// You can call Close while there are outstanding resources.
// It waits for all resources to be returned (Put).
// After a Close, Get is not allowed.
func (p *FastPool) Close() {
	p.SetIdleTimeout(0)
	_ = p.SetCapacity(0, true)
}

// IsClosed returns true if the resource pool is closed.
func (p *FastPool) IsClosed() bool {
	return p.State().Closed
}

// Get will return the next available resource. If capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will wait till the next resource becomes available or a timeout.
func (p *FastPool) Get(ctx context.Context) (resource Resource, err error) {
	if p.State().Closed {
		return nil, ErrClosed
	}

	select {
	case wrapper, ok := <-p.pool:
		if !ok {
			return nil, ErrClosed
		}

		p.Lock()
		p.state.InPool--
		p.state.InUse++
		p.Unlock()

		return wrapper.resource, nil

	case <-ctx.Done():
		return nil, ErrTimeout

	default:
		wrapper, err := p.safeCreate()
		if err == errNeedToQueue {
			return p.getQueued(ctx)
		} else if err != nil {
			return nil, err
		}

		return wrapper.resource, nil
	}
}

func (p *FastPool) getQueued(ctx context.Context) (Resource, error) {
	startTime := time.Now()

	p.Lock()
	p.state.Waiters++
	p.Unlock()

	// We don't have capacity, so now we block on pool.
	for {
		select {
		case wrapper, ok := <-p.pool:
			if !ok {
				p.Lock()
				p.state.Waiters--
				p.Unlock()

				return nil, ErrClosed
			}

			p.Lock()
			p.state.InPool--
			p.state.InUse++
			p.state.Waiters--
			p.state.WaitCount++
			p.state.WaitTime += time.Now().Sub(startTime)
			p.Unlock()

			return wrapper.resource, nil

		case <-ctx.Done():
			p.Lock()
			p.state.Waiters--
			p.Unlock()

			return nil, ErrTimeout

		case <-time.After(100 * time.Millisecond):
			// There could be a condition where this caller has been
			// put into a queue, but another caller has failed in creating
			// a resource, causing a deadlock. We'll check occasionally to see
			// if there is now capacity to create.
			if p.State().Closed {
				return nil, ErrClosed
			}
			wrapper, err := p.safeCreate()
			if err == errNeedToQueue {
				continue
			} else if err != nil {
				return nil, err
			} else {
				return wrapper.resource, nil
			}
		}
	}
}

// Put will return a resource to the pool. For every successful Get,
// a corresponding Put is required. If you no longer need a resource,
// you will need to call Put(nil) instead of returning the closed resource.
// The will eventually cause a new resource to be created in its place.
func (p *FastPool) Put(resource Resource) {
	p.Lock()

	p.state.InUse--

	if p.state.Closed || p.active() > p.state.Capacity {
		p.Unlock()
		if resource != nil {
			resource.Close()
		}
		return
	}

	if resource == nil {
		p.Unlock()
		p.ensureMinimumActive()
		return
	}

	if p.state.InUse < 0 {
		p.state.InUse++
		p.Unlock()
		panic(ErrPutBeforeGet)
	}

	w := resourceWrapper{resource: resource, timeUsed: time.Now()}
	select {
	case p.pool <- w:
		p.state.InPool++
	default:
		// We don't have room.
		p.state.InUse++
		p.Unlock()
		panic(ErrFull)
	}

	p.Unlock()
}

// SetCapacity changes the capacity of the pool.
// You can use it to shrink or expand, but not beyond
// the max capacity. If the change requires the pool
// to be shrunk and `block` is true, SetCapacity waits
// till the necessary number of resources are returned
// to the pool.
// A SetCapacity of 0 is equivalent to closing the FastPool.
func (p *FastPool) SetCapacity(capacity int, block bool) error {
	p.Lock()

	if p.state.Closed {
		p.Unlock()
		return ErrClosed
	}

	if capacity < 0 || capacity > cap(p.pool) {
		p.Unlock()
		return fmt.Errorf("capacity %d is out of range", capacity)
	}

	if capacity != 0 {
		minActive := p.state.MinActive
		if capacity < minActive {
			p.Unlock()
			return fmt.Errorf("minActive %v would now be higher than capacity %v", minActive, capacity)
		}
	}

	if capacity == 0 {
		p.state.Closed = true
	}

	isGrowing := capacity > p.state.Capacity
	p.state.Capacity = capacity

	if isGrowing {
		p.state.Draining = false
		p.Unlock()
		return nil
	}

	p.state.Draining = true
	p.Unlock()

	var done chan bool
	if block {
		done = make(chan bool)
	}
	go p.shrink(done)
	if block {
		<-done
	}

	return nil
}

// shrink active resources until capacity it is not above the set capacity.
func (p *FastPool) shrink(done chan<- bool) {
	for {
		p.Lock()
		remaining := p.active() - p.state.Capacity
		if remaining <= 0 {
			p.state.Draining = false
			if p.state.Capacity == 0 {
				close(p.pool)
				p.state.Closed = true
			}
			p.Unlock()
			done <- true
			return
		}
		p.Unlock()

		// We can't remove InUse resources, so only target the pool.
		// Collect the InUse resources lazily when they're returned.
		select {
		case wrapper := <-p.pool:
			wrapper.resource.Close()
			wrapper.resource = nil

			p.Lock()
			p.state.InPool--
			p.Unlock()

		case <-time.After(time.Second):
			// Someone could have pulled from the pool just before
			// we started waiting. Let's check the pool status again.
		}
	}
}

// SetIdleTimeout sets the idle timeout for resources. The timeout is
// checked at the 10th of the period of the timeout.
func (p *FastPool) SetIdleTimeout(idleTimeout time.Duration) {
	p.Lock()
	p.state.IdleTimeout = idleTimeout
	fastInterval := p.state.IdleTimeout / 10

	if p.idleTimer == nil {
		p.idleTimer = timer.NewTimer(fastInterval)
	} else {
		p.Unlock()
		p.idleTimer.Stop()
		p.Lock()
	}

	if p.state.IdleTimeout == 0 {
		p.Unlock()
		return
	}

	p.idleTimer.SetInterval(fastInterval)
	p.idleTimer.Start(p.closeIdleResources)
	p.Unlock()
}

// closeIdleResources scans the pool for idle resources
// and closes them.
func (p *FastPool) closeIdleResources() {
	// Shouldn't be zero, but checking in case.
	if p.State().IdleTimeout == 0 {
		return
	}

	for {
		p.Lock()
		inPool := p.state.InPool
		minActive := p.state.MinActive
		active := p.active()
		p.Unlock()

		if active <= minActive {
			return
		}

		if inPool == 0 {
			return
		}

		select {
		case wrapper, ok := <-p.pool:
			if !ok {
				return
			}

			p.Lock()
			deadline := wrapper.timeUsed.Add(p.state.IdleTimeout)
			if time.Now().After(deadline) {
				p.state.IdleClosed++
				p.state.InPool--
				p.Unlock()

				wrapper.resource.Close()
				wrapper.resource = nil
				break
			}
			p.Unlock()

			// Not expired--back into the pool we go.
			p.pool <- wrapper

		default:
			// The pool might have been used while we were iterating.
			// Maybe next time!
			return
		}
	}
}

// StatsJSON returns the stats in JSON format.
func (p *FastPool) StatsJSON() string {
	state := p.State()
	d, err := json.Marshal(&state)
	if err != nil {
		return ""
	}
	return string(d)
}

func (p *FastPool) State() State {
	p.Lock()
	state := p.state
	p.Unlock()

	return state
}

// Capacity returns the capacity.
func (p *FastPool) Capacity() int {
	return p.State().Capacity
}

// Available returns the number of currently unused and available resources.
func (p *FastPool) Available() int {
	s := p.State()
	available := s.Capacity - s.InUse
	// Sometimes we can be over capacity temporarily while the capacity shrinks.
	if available < 0 {
		return 0
	}
	return available
}

// Active returns the number of active (i.e. non-nil) resources either in the
// pool or claimed for use
func (p *FastPool) Active() int {
	p.Lock()
	v := p.active()
	p.Unlock()
	return v
}

func (p *FastPool) active() int {
	return p.state.InUse + p.state.InPool + p.state.Spawning
}

func (p *FastPool) freeCapacity() int {
	return p.state.Capacity - p.active()
}

func (p *FastPool) hasFreeCapacity() bool {
	return p.freeCapacity() > 0
}

// MinActive returns the minimum amount of resources keep active.
func (p *FastPool) MinActive() int {
	return p.State().MinActive
}

// InUse returns the number of claimed resources from the pool
func (p *FastPool) InUse() int {
	return p.State().InUse
}

// MaxCap returns the max capacity.
func (p *FastPool) MaxCap() int {
	return cap(p.pool)
}

// WaitCount returns the total number of waits.
func (p *FastPool) WaitCount() int64 {
	return p.State().WaitCount
}

// WaitTime returns the total wait time.
func (p *FastPool) WaitTime() time.Duration {
	return p.State().WaitTime
}

// IdleTimeout returns the idle timeout.
func (p *FastPool) IdleTimeout() time.Duration {
	return p.State().IdleTimeout
}

// IdleClosed returns the count of resources closed due to idle timeout.
func (p *FastPool) IdleClosed() int64 {
	return p.State().IdleClosed
}
