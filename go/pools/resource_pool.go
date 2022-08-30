/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type (
	IResourcePool interface {
		Close()
		Name() string
		Get(ctx context.Context, settings []string) (resource Resource, err error)
		Put(resource Resource)
		SetCapacity(capacity int) error
		SetIdleTimeout(idleTimeout time.Duration)
		StatsJSON() string
		Capacity() int64
		Available() int64
		Active() int64
		InUse() int64
		MaxCap() int64
		WaitCount() int64
		WaitTime() time.Duration
		IdleTimeout() time.Duration
		IdleClosed() int64
		Exhausted() int64
	}

	// Resource defines the interface that every resource must provide.
	// Thread synchronization between Close() and IsClosed()
	// is the responsibility of the caller.
	Resource interface {
		Close()
		ApplySettings(ctx context.Context, settings []string) error
		IsSettingsApplied() bool
		IsSameSetting(settings []string) bool
	}

	// Factory is a function that can be used to create a resource.
	Factory func(context.Context) (Resource, error)

	resourceWrapper struct {
		resource Resource
		timeUsed time.Time
	}

	// ResourcePool allows you to use a pool of resources.
	ResourcePool struct {
		// stats. Atomic fields must remain at the top in order to prevent panics on certain architectures.
		available  sync2.AtomicInt64
		active     sync2.AtomicInt64
		inUse      sync2.AtomicInt64
		waitCount  sync2.AtomicInt64
		waitTime   sync2.AtomicDuration
		idleClosed sync2.AtomicInt64
		exhausted  sync2.AtomicInt64

		capacity    sync2.AtomicInt64
		idleTimeout sync2.AtomicDuration

		resources chan resourceWrapper
		factory   Factory
		idleTimer *timer.Timer
		logWait   func(time.Time)

		settingResources chan resourceWrapper

		reopenMutex sync.Mutex
		refresh     *poolRefresh
	}
)

var (
	// ErrClosed is returned if ResourcePool is used when it's closed.
	ErrClosed = errors.New("resource pool is closed")

	// ErrTimeout is returned if a resource get times out.
	ErrTimeout = vterrors.New(vtrpcpb.Code_RESOURCE_EXHAUSTED, "resource pool timed out")

	// ErrCtxTimeout is returned if a ctx is already expired by the time the resource pool is used
	ErrCtxTimeout = vterrors.New(vtrpcpb.Code_DEADLINE_EXCEEDED, "resource pool context already expired")
)

// NewResourcePool creates a new ResourcePool pool.
// capacity is the number of possible resources in the pool:
// there can be up to 'capacity' of these at a given time.
// maxCap specifies the extent to which the pool can be resized
// in the future through the SetCapacity function.
// You cannot resize the pool beyond maxCap.
// If a resource is unused beyond idleTimeout, it's replaced
// with a new one.
// An idleTimeout of 0 means that there is no timeout.
// A non-zero value of prefillParallelism causes the pool to be pre-filled.
// The value specifies how many resources can be opened in parallel.
// refreshCheck is a function we consult at refreshInterval
// intervals to determine if the pool should be drained and reopened
func NewResourcePool(factory Factory, capacity, maxCap int, idleTimeout time.Duration, logWait func(time.Time), refreshCheck RefreshCheck, refreshInterval time.Duration) *ResourcePool {
	if capacity <= 0 || maxCap <= 0 || capacity > maxCap {
		panic(errors.New("invalid/out of range capacity"))
	}
	rp := &ResourcePool{
		resources:        make(chan resourceWrapper, maxCap),
		settingResources: make(chan resourceWrapper, maxCap),
		factory:          factory,
		available:        sync2.NewAtomicInt64(int64(capacity)),
		capacity:         sync2.NewAtomicInt64(int64(capacity)),
		idleTimeout:      sync2.NewAtomicDuration(idleTimeout),
		logWait:          logWait,
	}
	for i := 0; i < capacity; i++ {
		rp.resources <- resourceWrapper{}
	}

	if idleTimeout != 0 {
		rp.idleTimer = timer.NewTimer(idleTimeout / 10)
		rp.idleTimer.Start(rp.closeIdleResources)
	}

	rp.refresh = newPoolRefresh(rp, refreshCheck, refreshInterval)
	rp.refresh.startRefreshTicker()

	return rp
}

func (rp *ResourcePool) Name() string {
	return "ResourcePool"
}

// Close empties the pool calling Close on all its resources.
// You can call Close while there are outstanding resources.
// It waits for all resources to be returned (Put).
// After a Close, Get is not allowed.
func (rp *ResourcePool) Close() {
	if rp.idleTimer != nil {
		rp.idleTimer.Stop()
	}
	rp.refresh.stop()
	_ = rp.SetCapacity(0)
}

// closeIdleResources scans the pool for idle resources
func (rp *ResourcePool) closeIdleResources() {
	available := int(rp.Available())
	idleTimeout := rp.IdleTimeout()

	for i := 0; i < available; i++ {
		var wrapper resourceWrapper
		var origPool bool
		select {
		case wrapper = <-rp.resources:
			origPool = true
		case wrapper = <-rp.settingResources:
			origPool = false
		default:
			// stop early if we don't get anything new from the pool
			return
		}

		var reopened bool
		if wrapper.resource != nil && idleTimeout > 0 && time.Until(wrapper.timeUsed.Add(idleTimeout)) < 0 {
			wrapper.resource.Close()
			rp.idleClosed.Add(1)
			rp.reopenResource(&wrapper)
			reopened = true
		}
		rp.returnResource(&wrapper, origPool, reopened)
	}
}

func (rp *ResourcePool) returnResource(wrapper *resourceWrapper, origPool bool, reopened bool) {
	if origPool || reopened {
		rp.resources <- *wrapper
	} else {
		rp.settingResources <- *wrapper
	}
}

// reopen drains and reopens the connection pool
func (rp *ResourcePool) reopen() {
	rp.reopenMutex.Lock() // Avoid race, since we can refresh asynchronously
	defer rp.reopenMutex.Unlock()
	capacity := int(rp.capacity.Get())
	log.Infof("Draining and reopening resource pool with capacity %d by request", capacity)
	rp.Close()
	_ = rp.SetCapacity(capacity)
	if rp.idleTimer != nil {
		rp.idleTimer.Start(rp.closeIdleResources)
	}
	rp.refresh.startRefreshTicker()
}

// Get will return the next available resource. If capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will wait till the next resource becomes available or a timeout.
// A timeout of 0 is an indefinite wait.
func (rp *ResourcePool) Get(ctx context.Context, settings []string) (resource Resource, err error) {
	// If ctx has already expired, avoid racing with rp's resource channel.
	if ctx.Err() != nil {
		return nil, ErrCtxTimeout
	}
	if len(settings) == 0 {
		return rp.get(ctx)
	}
	return rp.getWithSettings(ctx, settings)
}

func (rp *ResourcePool) get(ctx context.Context) (resource Resource, err error) {
	// Fetch
	var wrapper resourceWrapper
	var ok bool
	// If we put both the channel together, then, go select can read from any channel
	// this way we guarantee it will try to read from the channel we intended to read it from first
	// and then try to read from next best available resource.
	select {
	// check normal resources first
	case wrapper, ok = <-rp.resources:
	default:
		select {
		// then checking setting resources
		case wrapper, ok = <-rp.settingResources:
		default:
			// now waiting
			startTime := time.Now()
			select {
			case wrapper, ok = <-rp.resources:
			case wrapper, ok = <-rp.settingResources:
			case <-ctx.Done():
				return nil, ErrTimeout
			}
			rp.recordWait(startTime)
		}
	}
	if !ok {
		return nil, ErrClosed
	}

	// if the resource has settings applied, we will close it and return a new one
	if wrapper.resource != nil && wrapper.resource.IsSettingsApplied() {
		wrapper.resource.Close()
		wrapper.resource = nil
		rp.active.Add(-1)
	}

	// Unwrap
	if wrapper.resource == nil {
		wrapper.resource, err = rp.factory(ctx)
		if err != nil {
			rp.resources <- resourceWrapper{}
			return nil, err
		}
		rp.active.Add(1)
	}
	if rp.available.Add(-1) <= 0 {
		rp.exhausted.Add(1)
	}
	rp.inUse.Add(1)
	return wrapper.resource, err
}

func (rp *ResourcePool) getWithSettings(ctx context.Context, settings []string) (Resource, error) {
	var wrapper resourceWrapper
	var ok bool
	var err error

	// Fetch
	select {
	// check setting resources first
	case wrapper, ok = <-rp.settingResources:
	default:
		select {
		// then, check normal resources
		case wrapper, ok = <-rp.resources:
		default:
			// now waiting
			startTime := time.Now()
			select {
			case wrapper, ok = <-rp.settingResources:
			case wrapper, ok = <-rp.resources:
			case <-ctx.Done():
				return nil, ErrTimeout
			}
			rp.recordWait(startTime)
		}
	}
	if !ok {
		return nil, ErrClosed
	}

	// Checking setting hash id, if it is different, we will close the resource and return a new one later in unwrap
	if wrapper.resource != nil && wrapper.resource.IsSettingsApplied() && !wrapper.resource.IsSameSetting(settings) {
		wrapper.resource.Close()
		wrapper.resource = nil
		rp.active.Add(-1)
	}

	// Unwrap
	if wrapper.resource == nil {
		wrapper.resource, err = rp.factory(ctx)
		if err != nil {
			rp.resources <- resourceWrapper{}
			return nil, err
		}
		rp.active.Add(1)
	}

	if !wrapper.resource.IsSettingsApplied() {
		if err = wrapper.resource.ApplySettings(ctx, settings); err != nil {
			// as we are not able to apply settings, we can return this connection to non-settings channel.
			// TODO: may check the error code to see if it is recoverable or not.
			rp.resources <- wrapper
			return nil, err
		}
	}

	if rp.available.Add(-1) <= 0 {
		rp.exhausted.Add(1)
	}
	rp.inUse.Add(1)
	return wrapper.resource, err
}

// Put will return a resource to the pool. For every successful Get,
// a corresponding Put is required. If you no longer need a resource,
// you will need to call Put(nil) instead of returning the closed resource.
// This will cause a new resource to be created in its place.
func (rp *ResourcePool) Put(resource Resource) {
	var wrapper resourceWrapper
	var recreated bool
	var hasSettings bool
	if resource != nil {
		wrapper = resourceWrapper{
			resource: resource,
			timeUsed: time.Now(),
		}
		hasSettings = resource.IsSettingsApplied()
	} else {
		rp.reopenResource(&wrapper)
		recreated = true
	}
	if !hasSettings || recreated {
		select {
		case rp.resources <- wrapper:
		default:
			panic(errors.New("attempt to Put into a full ResourcePool"))
		}
	} else {
		select {
		case rp.settingResources <- wrapper:
		default:
			panic(errors.New("attempt to Put into a full ResourcePool"))
		}
	}
	rp.inUse.Add(-1)
	rp.available.Add(1)
}

func (rp *ResourcePool) reopenResource(wrapper *resourceWrapper) {
	if r, err := rp.factory(context.TODO()); err == nil {
		wrapper.resource = r
		wrapper.timeUsed = time.Now()
	} else {
		wrapper.resource = nil
		rp.active.Add(-1)
	}
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

	// Atomically swap new capacity with old
	var oldcap int
	for {
		oldcap = int(rp.capacity.Get())
		if oldcap == 0 && capacity > 0 {
			// Closed this before, re-open the channel
			rp.resources = make(chan resourceWrapper, cap(rp.resources))
			rp.settingResources = make(chan resourceWrapper, cap(rp.settingResources))
		}
		if oldcap == capacity {
			return nil
		}
		if rp.capacity.CompareAndSwap(int64(oldcap), int64(capacity)) {
			break
		}
	}

	// If the required capacity is less than the current capacity,
	// then we need to wait till the current resources are returned
	// to the pool and close them from any of the channel.
	// Otherwise, if the required capacity is more than the current capacity,
	// then we just add empty resource to the channel.
	if capacity < oldcap {
		for i := 0; i < oldcap-capacity; i++ {
			var wrapper resourceWrapper
			select {
			case wrapper = <-rp.resources:
			case wrapper = <-rp.settingResources:
			}
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
		close(rp.settingResources)
	}
	return nil
}

func (rp *ResourcePool) recordWait(start time.Time) {
	rp.waitCount.Add(1)
	rp.waitTime.Add(time.Since(start))
	if rp.logWait != nil {
		rp.logWait(start)
	}
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
	return fmt.Sprintf(`{"Capacity": %v, "Available": %v, "Active": %v, "InUse": %v, "MaxCapacity": %v, "WaitCount": %v, "WaitTime": %v, "IdleTimeout": %v, "IdleClosed": %v, "Exhausted": %v}`,
		rp.Capacity(),
		rp.Available(),
		rp.Active(),
		rp.InUse(),
		rp.MaxCap(),
		rp.WaitCount(),
		rp.WaitTime().Nanoseconds(),
		rp.IdleTimeout().Nanoseconds(),
		rp.IdleClosed(),
		rp.Exhausted(),
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

// Exhausted returns the number of times Available dropped below 1
func (rp *ResourcePool) Exhausted() int64 {
	return rp.exhausted.Get()
}
