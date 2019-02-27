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
	idleTimer *timer.Timer
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

// NewFastPool creates a new FastPool pool.
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
func NewFastPool(factory CreateFactory, capacity, maxCap int, idleTimeout time.Duration, minActive int) *FastPool {
	//fmt.Println("NewFastPool", capacity, maxCap	, idleTimeout)
	if capacity <= 0 || maxCap <= 0 || capacity > maxCap {
		panic(errors.New("invalid/out of range capacity"))
	}
	if minActive > capacity {
		panic(fmt.Errorf("minActive %v higher than capacity %v", minActive, capacity))
	}

	rp := &FastPool{
		factory: factory,
		pool:    make(chan resourceWrapper, maxCap),
	}

	rp.state = State{
		Capacity:  capacity,
		MinActive: minActive,
	}

	rp.ensureMinimumActive()
	rp.SetIdleTimeout(idleTimeout)

	return rp
}

func (rp *FastPool) create() (resourceWrapper, error) {
	//fmt.Println("RP create")
	r, err := rp.factory()
	if err != nil {
		return resourceWrapper{}, err
	}

	return resourceWrapper{
		resource: r,
		timeUsed: time.Now(),
	}, nil
}

func (rp *FastPool) safeCreate() (resourceWrapper, error) {
	//fmt.Println("RP safeCreate")
	rp.Lock()
	capacity := rp.hasFreeCapacity()
	rp.state.Spawning++
	rp.Unlock()

	if !capacity {
		rp.Lock()
		rp.state.Spawning--
		rp.Unlock()
		return resourceWrapper{}, errNeedToQueue
	}

	wrapper, err := rp.create()
	if err != nil {
		rp.Lock()
		rp.state.Spawning--
		rp.Unlock()
		return resourceWrapper{}, err
	}

	rp.Lock()
	rp.state.InUse++
	rp.state.Spawning--
	rp.Unlock()

	return wrapper, nil
}

func (rp *FastPool) ensureMinimumActive() {
	rp.Lock()
	if rp.state.MinActive == 0 || rp.state.Closed {
		rp.Unlock()
		return
	}
	required := rp.state.MinActive - rp.active()
	rp.Unlock()

	for i := 0; i < required; i++ {
		r, err := rp.create()
		if err != nil {
			//fmt.Println("error creating factory", err)
			// TODO(gak): How to handle factory error?
			break
		}
		rp.Lock()
		rp.state.InPool++
		rp.Unlock()
		rp.pool <- r
	}
}

// Close empties the pool calling Close on all its resources.
// You can call Close while there are outstanding resources.
// It waits for all resources to be returned (Put).
// After a Close, Get is not allowed.
func (rp *FastPool) Close() {
	//fmt.Println("RP Close")
	rp.SetIdleTimeout(0)
	_ = rp.SetCapacity(0, true)
}

// IsClosed returns true if the resource pool is closed.
func (rp *FastPool) IsClosed() bool {
	return rp.State().Closed
}

// Get will return the next available resource. If capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will wait till the next resource becomes available or a timeout.
func (rp *FastPool) Get(ctx context.Context) (resource Resource, err error) {
	//fmt.Printf("RP Get %+v\n", rp.State())
	select {
	case wrapper, ok := <-rp.pool:
		if !ok {
			//fmt.Printf("RP Get ErrClosed %+v\n", rp.State())
			return nil, ErrClosed
		}

		rp.Lock()
		rp.state.InPool--
		rp.state.InUse++
		rp.Unlock()

		//fmt.Printf("RP Get found in pool %v %+v\n", wrapper.resource, rp.State())
		return wrapper.resource, nil

	case <-ctx.Done():
		//fmt.Printf("RP Get ErrTimeout1 %+v\n", rp.State())
		return nil, ErrTimeout

	default:
		//fmt.Printf("RP Get Nothing in pool %+v\n", rp.State())
		wrapper, err := rp.safeCreate()
		if err == errNeedToQueue {
			return rp.getQueued(ctx)
		} else if err != nil {
			//fmt.Printf("RP Error creating1 %v %+v\n", err, rp.State())
			return nil, err
		}

		//fmt.Printf("RP Get created and returned %v %+v\n", wrapper.resource, rp.State())
		return wrapper.resource, nil
	}
}

func (rp *FastPool) getQueued(ctx context.Context) (Resource, error) {
	//fmt.Printf("RP GetQueued %+v\n", rp.State())
	startTime := time.Now()

	rp.Lock()
	rp.state.Waiters++
	rp.Unlock()

	// We don't have capacity, so now we block on pool.
	for {
		select {
		case wrapper, ok := <-rp.pool:
			if !ok {
				rp.Lock()
				rp.state.Waiters--
				rp.Unlock()

				//fmt.Printf("RP GetQueued ErrClosed %+v\n", rp.State())
				return nil, ErrClosed
			}

			rp.Lock()
			rp.state.InPool--
			rp.state.InUse++
			rp.state.Waiters--
			rp.state.WaitCount++
			rp.state.WaitTime += time.Now().Sub(startTime)
			rp.Unlock()

			//fmt.Printf("RP GetQueued got from pool %v %+v\n", wrapper.resource, rp.State())
			return wrapper.resource, nil

		case <-ctx.Done():
			rp.Lock()
			rp.state.Waiters--
			rp.Unlock()

			//fmt.Printf("RP GetQueued ErrTimeout %+v\n", rp.State())
			return nil, ErrTimeout

		case <-time.After(100 * time.Millisecond):
			// There could be a condition where this caller has been
			// put into a queue, but another caller has failed in creating
			// a resource, causing a deadlock. We'll check occasionally to see
			// if there is now capacity to create.
			//fmt.Printf("RP GetQueued After 100ms %+v\n", rp.State())

			wrapper, err := rp.safeCreate()
			if err == errNeedToQueue {
				continue
			} else if err != nil {
				//fmt.Printf("RP GetQueued err on create %v %+v\n", err, rp.State())
				return nil, err
			} else {
				//fmt.Printf("RP GetQueued create after race %v %+v\n", wrapper.resource, rp.State())
				return wrapper.resource, nil
			}
		}
	}
}

// Put will return a resource to the pool. For every successful Get,
// a corresponding Put is required. If you no longer need a resource,
// you will need to call Put(nil) instead of returning the closed resource.
// The will eventually cause a new resource to be created in its place.
func (rp *FastPool) Put(resource Resource) {
	//fmt.Printf("RP Put %v %+v\n", resource, rp.State())
	rp.Lock()

	rp.state.InUse--

	if rp.state.Closed || rp.active() > rp.state.Capacity {
		if resource != nil {
			//fmt.Println("RP Put closing resource due to closing or capacity", rp.state.Closed, rp.active())
			resource.Close()
		}
		rp.Unlock()
		return
	}

	if resource == nil {
		rp.Unlock()
		//fmt.Printf("RP Put got nil %v %+v\n", resource, rp.State())
		rp.ensureMinimumActive()
		return
	}

	if rp.state.InUse < 0 {
		rp.state.InUse++
		rp.Unlock()
		//fmt.Printf("RP Put ErRPutBeforeGet %+v\n", rp.State())
		panic(ErrPutBeforeGet)
	}

	w := resourceWrapper{resource: resource, timeUsed: time.Now()}
	select {
	case rp.pool <- w:
		//fmt.Printf("RP Put back into pool %+v\n", rp.state)
		rp.state.InPool++
	default:
		// We don't have room.
		rp.state.InUse++
		rp.Unlock()
		//fmt.Printf("RP Put full pool %+v\n", rp.State())
		panic(ErrFull)
	}

	rp.Unlock()
}

// SetCapacity changes the capacity of the pool.
// You can use it to shrink or expand, but not beyond
// the max capacity. If the change requires the pool
// to be shrunk and `block` is true, SetCapacity waits
// till the necessary number of resources are returned
// to the pool.
// A SetCapacity of 0 is equivalent to closing the FastPool.
func (rp *FastPool) SetCapacity(capacity int, block bool) error {
	//fmt.Printf("RP SetCapacity %d %+v\n", capacity, rp.State())
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
		//fmt.Printf("SetCapacity starting drain... %+v\n", rp.State())
		for {
			rp.Lock()
			remaining := rp.active() - rp.state.Capacity
			if remaining <= 0 {
				//fmt.Printf("SetCapacity inpool fully drained %+v\n", rp.state)
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
			// Collect the InUse resources lazily when they're returned.
			select {
			case wrapper := <-rp.pool:
				//fmt.Printf("SetCapacity closing resource due to shrinking %v %+v\n", wrapper.resource, rp.State())
				wrapper.resource.Close()
				wrapper.resource = nil

				rp.Lock()
				rp.state.InPool--
				rp.Unlock()
				//fmt.Printf("SetCapacity closed resource due to shrinking %v %+v\n", wrapper.resource, rp.State())

			case <-time.After(time.Second):
				// Someone could have pulled from the pool just before
				// we started waiting. Let's check the pool status again.
			}
		}
	}()

	if block {
		<-done
	}

	return nil
}

// SetIdleTimeout sets the idle timeout for resources. The timeout is
// checked at the 10th of the period of the timeout.
func (rp *FastPool) SetIdleTimeout(idleTimeout time.Duration) {
	rp.Lock()
	rp.state.IdleTimeout = idleTimeout
	fastInterval := rp.state.IdleTimeout / 10

	if rp.idleTimer == nil {
		//fmt.Println("RP creating new timer", fastInterval)
		rp.idleTimer = timer.NewTimer(fastInterval)
	} else {
		//fmt.Println("RP stopping timer")
		rp.Unlock()
		rp.idleTimer.Stop()
		rp.Lock()
	}

	if rp.state.IdleTimeout == 0 {
		//fmt.Println("RP disabling timer")
		rp.Unlock()
		return
	}

	//fmt.Println("RP activating timer", fastInterval)
	rp.idleTimer.SetInterval(fastInterval)
	rp.idleTimer.Start(rp.closeIdleResources)
	rp.Unlock()
}

// closeIdleResources scans the pool for idle resources
// and closes them.
func (rp *FastPool) closeIdleResources() {
	// Shouldn't be zero, but checking in case.
	if rp.State().IdleTimeout == 0 {
		return
	}

	for {
		rp.Lock()
		inPool := rp.state.InPool
		minActive := rp.state.MinActive
		active := rp.active()
		rp.Unlock()

		if active <= minActive {
			return
		}

		if inPool == 0 {
			return
		}

		//fmt.Printf("RP CIR Waiting for pool %+v\n", rp.State())
		select {
		case wrapper, ok := <-rp.pool:
			if !ok {
				return
			}
			//fmt.Printf("RP CIR popped from pool %v %+v\n", wrapper.resource, rp.State())

			rp.Lock()
			deadline := wrapper.timeUsed.Add(rp.state.IdleTimeout)
			if time.Now().After(deadline) {
				rp.state.IdleClosed++
				rp.state.InPool--
				rp.Unlock()
				//fmt.Printf("RP CIR closing resource due to timeout %v %+v\n", wrapper.resource, rp.State())
				wrapper.resource.Close()
				wrapper.resource = nil
				continue
			}
			rp.Unlock()

			// Not expired--back into the pool we go.
			//fmt.Printf("RP CIR back to pool %v %+v\n", wrapper.resource, rp.State())
			rp.pool <- wrapper

		default:
			//fmt.Println("CIR: nothing in pool")
			// The pool might have been used while we were iterating.
			// Maybe next time!
			return
		}
	}
}

// StatsJSON returns the stats in JSON format.
func (rp *FastPool) StatsJSON() string {
	state := rp.State()
	d, err := json.Marshal(&state)
	if err != nil {
		return ""
	}
	return string(d)
}

func (rp *FastPool) State() State {
	rp.Lock()
	state := rp.state
	rp.Unlock()

	return state
}

// Capacity returns the capacity.
func (rp *FastPool) Capacity() int {
	return rp.State().Capacity
}

// Available returns the number of currently unused and available resources.
func (rp *FastPool) Available() int {
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
func (rp *FastPool) Active() int {
	rp.Lock()
	v := rp.active()
	rp.Unlock()
	return v
}

func (rp *FastPool) active() int {
	return rp.state.InUse + rp.state.InPool + rp.state.Spawning
}

func (rp *FastPool) freeCapacity() int {
	return rp.state.Capacity - rp.active()
}

func (rp *FastPool) hasFreeCapacity() bool {
	return rp.freeCapacity() > 0
}

// MinActive returns the minimum amount of resources keep active.
func (rp *FastPool) MinActive() int {
	return rp.State().MinActive
}

// InUse returns the number of claimed resources from the pool
func (rp *FastPool) InUse() int {
	return rp.State().InUse
}

// MaxCap returns the max capacity.
func (rp *FastPool) MaxCap() int {
	return cap(rp.pool)
}

// WaitCount returns the total number of waits.
func (rp *FastPool) WaitCount() int64 {
	return rp.State().WaitCount
}

// WaitTime returns the total wait time.
func (rp *FastPool) WaitTime() time.Duration {
	return rp.State().WaitTime
}

// IdleTimeout returns the idle timeout.
func (rp *FastPool) IdleTimeout() time.Duration {
	return rp.State().IdleTimeout
}

// IdleClosed returns the count of resources closed due to idle timeout.
func (rp *FastPool) IdleClosed() int64 {
	return rp.State().IdleClosed
}
