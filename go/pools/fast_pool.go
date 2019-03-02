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

	p.ensureMinActive()
	p.SetIdleTimeout(idleTimeout)

	go p.closeIdleResources()
	go p.maintainMinActive()

	return p
}

// create a resource wrapper from the factory.
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

// createType is used by safeCreate to know which counter to increment.
type createType int

const (
	forUse createType = iota
	forPool
)

// safeCreate will prevent allocating a resource past the capacity of the pool.
func (p *FastPool) safeCreate(ct createType) (resourceWrapper, error) {
	err := p.safeCreateBegin()
	if err != nil {
		return resourceWrapper{}, err
	}
	defer func() {
		r := recover()
		p.safeCreateEnd(ct, err != nil || r != nil)
		if r != nil {
			panic(r)
		}
	}()

	wrapper, err := p.create()
	return wrapper, err
}

// safeCreateBegin sanity checks and increments `state.Spawning`.
func (p *FastPool) safeCreateBegin() error {
	p.Lock()
	defer p.Unlock()

	if !p.hasFreeCapacity() {
		return errNeedToQueue
	}

	p.state.Spawning++
	return nil
}

// safeCreateBegin sanity checks and decrements `Spawning` as well as
// incrementing either `InUse` or `InPool`.
func (p *FastPool) safeCreateEnd(ct createType, didError bool) {
	p.Lock()
	defer p.Unlock()

	p.state.Spawning--

	if didError {
		return
	}

	switch ct {
	case forUse:
		p.state.InUse++
	case forPool:
		p.state.InPool++
	}
}

// maintainMinActive will keep calling ensureMinActive until quit.
// This function is intended to be used as a goroutine.
func (p *FastPool) maintainMinActive() {
	for !p.IsClosed() {
		p.ensureMinActive()
		time.Sleep(100 * time.Millisecond)
	}
}

// ensureMinActive keeps at least a certain amount of resources instantiated.
func (p *FastPool) ensureMinActive() {
	required := p.requiredResources()
	for i := 0; i < required; i++ {
		r, err := p.safeCreate(forPool)
		if err == errNeedToQueue {
			// Not enough room in the pool. Aborting.
			return
		} else if err != nil {
			// TODO(gak): How to handle factory error not initiated by the user?
			return
		}

		select {
		case p.pool <- r:
		default:
			return
		}
	}
}

// requiredResources returns the number of new resources to create based on MinActive.
func (p *FastPool) requiredResources() int {
	p.Lock()
	defer p.Unlock()

	if p.state.MinActive == 0 || p.state.Closed {
		return 0
	}

	return p.state.MinActive - p.active()
}

// Close empties the pool calling Close on all its resources.
//
// You can call Close while there are outstanding resources.
// It waits for all resources to be returned (Put).
// After a Close, Get is not allowed.
func (p *FastPool) Close() {
	_ = p.SetCapacity(0, true)
}

// IsClosed returns true if the resource pool is closed.
func (p *FastPool) IsClosed() bool {
	return p.State().Closed
}

// Get will return the next available resource. If capacity
// has not been reached, it will create a new one using the factory.
// Otherwise, it will wait till the next resource becomes available or a timeout.
func (p *FastPool) Get(ctx context.Context) (resource Resource, err error) {
	if p.State().Closed {
		return nil, ErrClosed
	}

	select {
	case wrapper, ok := <-p.pool:
		if !ok {
			return nil, ErrClosed
		}
		p.usedFromPool()
		return wrapper.resource, nil

	case <-ctx.Done():
		return nil, ErrTimeout

	default:
		wrapper, err := p.safeCreate(forUse)
		if err == errNeedToQueue {
			return p.getQueued(ctx)
		} else if err != nil {
			return nil, err
		}

		return wrapper.resource, nil
	}
}

// getQueued will wait for a resource to become available in the pool.
// This is called when there is no capacity to create a resource and
// there is nothing in the pool.
func (p *FastPool) getQueued(ctx context.Context) (Resource, error) {
	p.withLock(func() {
		p.state.Waiters++
	})
	defer p.withLock(func() {
		p.state.Waiters--
	})

	startTime := time.Now()
	for {
		select {
		case wrapper, ok := <-p.pool:
			if !ok {
				return nil, ErrClosed
			}
			p.usedFromPool()
			p.recordWait(startTime)
			return wrapper.resource, nil

		case <-ctx.Done():
			return nil, ErrTimeout

		case <-time.After(100 * time.Millisecond):
			// There could be a condition where this caller has been
			// put into a queue, but another caller has failed in creating
			// a resource, causing a deadlock. We'll check occasionally to see
			// if there is now capacity to create.
			if p.State().Closed {
				return nil, ErrClosed
			}
			wrapper, err := p.safeCreate(forUse)
			if err == errNeedToQueue {
				continue
			} else if err != nil {
				return nil, err
			}

			p.recordWait(startTime)
			return wrapper.resource, nil
		}
	}
}

// recordWait records how long a caller waited for a resource to be available in the pool.
func (p *FastPool) recordWait(startTime time.Time) {
	p.Lock()
	defer p.Unlock()

	p.state.WaitCount++
	p.state.WaitTime += time.Now().Sub(startTime)
}

// usedFromPool moves a resource from `InPool` to `InUse`.
func (p *FastPool) usedFromPool() {
	defer p.Unlock()
	p.Lock()
	p.state.InPool--
	p.state.InUse++
}

// Put back a resource into the pool. For every successful Get,
// a corresponding Put is required. If you no longer need a resource,
// you will need to call Put(nil) instead of returning the closed resource.
// The will eventually cause a new resource to be created in its place.
func (p *FastPool) Put(resource Resource) {
	p.Lock()

	p.state.InUse--

	if p.state.Closed || p.active() > p.state.Capacity {
		// We're ether closed or shrinking.
		p.Unlock()
		if resource != nil {
			resource.Close()
		}
		return
	}

	defer p.Unlock()

	if resource == nil {
		return
	}

	if p.state.InUse < 0 {
		p.state.InUse++
		panic(ErrPutBeforeGet)
	}

	w := resourceWrapper{resource: resource, timeUsed: time.Now()}
	select {
	case p.pool <- w:
		p.state.InPool++
	default:
		// We don't have room in the pool.
		p.state.InUse++
		panic(ErrFull)
	}
}

// SetCapacity changes the capacity of the pool.
// You can use it to shrink or expand, but not beyond
// the max capacity. If the change requires the pool
// to be shrunk and `block` is true, SetCapacity waits
// till the necessary number of resources are returned
// to the pool.
//
// A SetCapacity of 0 is equivalent to closing the pool.
func (p *FastPool) SetCapacity(capacity int, block bool) error {
	err := p.setCapacityHandle(capacity)
	if err != nil {
		return err
	}
	if !p.State().Draining {
		return nil
	}

	if block {
		p.shrink()
	} else {
		go p.shrink()
	}

	return nil
}

// setCapacityHandle is used by SetCapacity to do sanity checks and change
// state appropriately, while locking the state. It will also let the caller know
// if the pool needs to shrink.
func (p *FastPool) setCapacityHandle(capacity int) error {
	p.Lock()
	defer p.Unlock()

	if p.state.Closed {
		return ErrClosed
	}
	if capacity < 0 || capacity > cap(p.pool) {
		return fmt.Errorf("capacity %d is out of range", capacity)
	}
	if capacity > 0 {
		minActive := p.state.MinActive
		if capacity < minActive {
			return fmt.Errorf("minActive %v would now be higher than capacity %v", minActive, capacity)
		}
	}
	if capacity == 0 {
		p.state.Closed = true
	}

	p.state.Draining = capacity < p.state.Capacity
	p.state.Capacity = capacity
	return nil
}

// shrink active resources until the active count is not above capacity.
func (p *FastPool) shrink() {
	for p.continueShrink() {
		// We can't remove InUse resources, so only target the pool.
		// Collect the InUse resources lazily when they're returned.
		select {
		case wrapper := <-p.pool:
			wrapper.resource.Close()
			wrapper.resource = nil

			p.withLock(func() {
				p.state.InPool--
			})

		case <-time.After(100 * time.Millisecond):
			// Someone could have pulled from the pool just before
			// we started waiting. Let's check the pool status again.
		}
	}
}

// continueShrink lets `shrink()` know to keep shrinking. It will also
// set the pool state to closed when drained the capacity is 0.
func (p *FastPool) continueShrink() bool {
	p.Lock()
	defer p.Unlock()

	remaining := p.active() - p.state.Capacity
	if remaining > 0 {
		return true
	}

	p.state.Draining = false
	if p.state.Capacity == 0 {
		close(p.pool)
		p.state.Closed = true
	}
	return false
}

// SetIdleTimeout sets the idle timeout for resources. The timeout is
// checked at the 10th of the period of the timeout.
func (p *FastPool) SetIdleTimeout(idleTimeout time.Duration) {
	p.withLock(func() {
		p.state.IdleTimeout = idleTimeout
	})
}

// closeIdleResources scans the pool for idle resources
// and closes them. It is meant to run as a goroutine and will
// return when the pool is closed.
func (p *FastPool) closeIdleResources() {
	for !p.IsClosed() {

		timeout := p.State().IdleTimeout

		if timeout == 0 {
			// Wait for an updated idleTimeout.
			time.Sleep(100 * time.Millisecond)
			continue
		}

		time.Sleep(timeout / 10)

		var active, minActive int
		var closed bool
		for scanning, remaining := true, p.State().InPool; scanning && remaining > 0; remaining-- {
			p.withLock(func() {
				active = p.active()
				minActive = p.state.MinActive
				closed = p.state.Closed
			})

			if closed {
				return
			}

			if active <= minActive {
				break
			}

			select {
			case wrapper, ok := <-p.pool:
				if !ok {
					return
				}

				deadline := wrapper.timeUsed.Add(timeout)
				if time.Now().After(deadline) {
					p.withLock(func() {
						p.state.IdleClosed++
						p.state.InPool--
					})

					wrapper.resource.Close()
					wrapper.resource = nil
					break
				}

				// Not expired--back into the pool we go.
				select {
				case p.pool <- wrapper:
				default:
					// Can't put back into pool. Might be full.
					p.withLock(func() {
						p.state.InPool--
					})

					wrapper.resource.Close()
					wrapper.resource = nil

					scanning = false
				}

			default:
				// The pool might have been used while we were iterating.
				// Maybe next time!
				scanning = false
			}
		}
	}
}

func (p *FastPool) withLock(f func()) {
	p.Lock()
	defer p.Unlock()

	f()
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

// State returns the state struct with state, statistics, etc.
func (p *FastPool) State() State {
	p.Lock()
	defer p.Unlock()

	return p.state
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
	defer p.Unlock()

	return p.active()
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
