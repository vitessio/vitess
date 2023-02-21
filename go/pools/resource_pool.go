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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type (
	IResourcePool interface {
		Close()
		Name() string
		Get(ctx context.Context, setting *Setting) (resource Resource, err error)
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
		MaxLifetimeClosed() int64
		Exhausted() int64
		GetCount() int64
		GetSettingCount() int64
		DiffSettingCount() int64
		ResetSettingCount() int64
	}

	// Resource defines the interface that every resource must provide.
	// Thread synchronization between Close() and IsClosed()
	// is the responsibility of the caller.
	Resource interface {
		Close()
		Expired(time.Duration) bool
		ApplySetting(ctx context.Context, setting *Setting) error
		IsSettingApplied() bool
		IsSameSetting(setting string) bool
		ResetSetting(ctx context.Context) error
	}

	// Factory is a function that can be used to create a resource.
	Factory func(context.Context) (Resource, error)

	resourceWrapper struct {
		resource Resource
		timeUsed time.Time
	}

	// Setting represents a set query and reset query for system settings.
	Setting struct {
		query      string
		resetQuery string
	}

	// ResourcePool allows you to use a pool of resources.
	ResourcePool struct {
		available         atomic.Int64
		active            atomic.Int64
		inUse             atomic.Int64
		waitCount         atomic.Int64
		waitTime          atomic.Int64
		idleClosed        atomic.Int64
		maxLifetimeClosed atomic.Int64
		exhausted         atomic.Int64

		capacity    atomic.Int64
		idleTimeout atomic.Int64
		maxLifetime atomic.Int64

		resources chan resourceWrapper
		factory   Factory
		idleTimer *timer.Timer
		logWait   func(time.Time)

		settingResources  chan resourceWrapper
		getCount          atomic.Int64
		getSettingCount   atomic.Int64
		diffSettingCount  atomic.Int64
		resetSettingCount atomic.Int64

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

func NewSetting(query, resetQuery string) *Setting {
	return &Setting{
		query:      query,
		resetQuery: resetQuery,
	}
}

func (s *Setting) GetQuery() string {
	return s.query
}

func (s *Setting) GetResetQuery() string {
	return s.resetQuery
}

// NewResourcePool creates a new ResourcePool pool.
// capacity is the number of possible resources in the pool:
// there can be up to 'capacity' of these at a given time.
// maxCap specifies the extent to which the pool can be resized
// in the future through the SetCapacity function.
// You cannot resize the pool beyond maxCap.
// If a resource is unused beyond idleTimeout, it's replaced
// with a new one.
// An idleTimeout of 0 means that there is no timeout.
// An maxLifetime of 0 means that there is no timeout.
// A non-zero value of prefillParallelism causes the pool to be pre-filled.
// The value specifies how many resources can be opened in parallel.
// refreshCheck is a function we consult at refreshInterval
// intervals to determine if the pool should be drained and reopened
func NewResourcePool(factory Factory, capacity, maxCap int, idleTimeout time.Duration, maxLifetime time.Duration, logWait func(time.Time), refreshCheck RefreshCheck, refreshInterval time.Duration) *ResourcePool {
	if capacity <= 0 || maxCap <= 0 || capacity > maxCap {
		panic(errors.New("invalid/out of range capacity"))
	}
	rp := &ResourcePool{
		resources:        make(chan resourceWrapper, maxCap),
		settingResources: make(chan resourceWrapper, maxCap),
		factory:          factory,
		logWait:          logWait,
	}
	rp.available.Store(int64(capacity))
	rp.capacity.Store(int64(capacity))
	rp.idleTimeout.Store(idleTimeout.Nanoseconds())
	rp.maxLifetime.Store(maxLifetime.Nanoseconds())

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
	capacity := int(rp.capacity.Load())
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
func (rp *ResourcePool) Get(ctx context.Context, setting *Setting) (resource Resource, err error) {
	// If ctx has already expired, avoid racing with rp's resource channel.
	if ctx.Err() != nil {
		return nil, ErrCtxTimeout
	}
	if setting == nil {
		return rp.get(ctx)
	}
	return rp.getWithSettings(ctx, setting)
}

func (rp *ResourcePool) get(ctx context.Context) (resource Resource, err error) {
	rp.getCount.Add(1)
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

	// if the resource has setting applied, we will close it and return a new one
	if wrapper.resource != nil && wrapper.resource.IsSettingApplied() {
		rp.resetSettingCount.Add(1)
		err = wrapper.resource.ResetSetting(ctx)
		if err != nil {
			// as reset is unsuccessful, we will close this resource
			wrapper.resource.Close()
			wrapper.resource = nil
			rp.active.Add(-1)
		}
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

func (rp *ResourcePool) getWithSettings(ctx context.Context, setting *Setting) (Resource, error) {
	rp.getSettingCount.Add(1)
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
	if wrapper.resource != nil && wrapper.resource.IsSettingApplied() && !wrapper.resource.IsSameSetting(setting.query) {
		rp.diffSettingCount.Add(1)
		err = wrapper.resource.ResetSetting(ctx)
		if err != nil {
			// as reset is unsuccessful, we will close this resource
			wrapper.resource.Close()
			wrapper.resource = nil
			rp.active.Add(-1)
		}
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

	if !wrapper.resource.IsSettingApplied() {
		if err = wrapper.resource.ApplySetting(ctx, setting); err != nil {
			// as we are not able to apply setting, we can return this connection to non-setting channel.
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
		hasSettings = resource.IsSettingApplied()
		if resource.Expired(rp.extendedMaxLifetime()) {
			rp.maxLifetimeClosed.Add(1)
			resource.Close()
			resource = nil
		}
	}
	if resource == nil {
		// Create new resource
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
		oldcap = int(rp.capacity.Load())
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
	rp.waitTime.Add(time.Since(start).Nanoseconds())
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

	rp.idleTimeout.Store(idleTimeout.Nanoseconds())
	rp.idleTimer.SetInterval(idleTimeout / 10)
}

// StatsJSON returns the stats in JSON format.
func (rp *ResourcePool) StatsJSON() string {
	return fmt.Sprintf(`{"Capacity": %v, "Available": %v, "Active": %v, "InUse": %v, "MaxCapacity": %v, "WaitCount": %v, "WaitTime": %v, "IdleTimeout": %v, "IdleClosed": %v, "MaxLifetimeClosed": %v, "Exhausted": %v}`,
		rp.Capacity(),
		rp.Available(),
		rp.Active(),
		rp.InUse(),
		rp.MaxCap(),
		rp.WaitCount(),
		rp.WaitTime().Nanoseconds(),
		rp.IdleTimeout().Nanoseconds(),
		rp.IdleClosed(),
		rp.MaxLifetimeClosed(),
		rp.Exhausted(),
	)
}

// Capacity returns the capacity.
func (rp *ResourcePool) Capacity() int64 {
	return rp.capacity.Load()
}

// Available returns the number of currently unused and available resources.
func (rp *ResourcePool) Available() int64 {
	return rp.available.Load()
}

// Active returns the number of active (i.e. non-nil) resources either in the
// pool or claimed for use
func (rp *ResourcePool) Active() int64 {
	return rp.active.Load()
}

// InUse returns the number of claimed resources from the pool
func (rp *ResourcePool) InUse() int64 {
	return rp.inUse.Load()
}

// MaxCap returns the max capacity.
func (rp *ResourcePool) MaxCap() int64 {
	return int64(cap(rp.resources))
}

// WaitCount returns the total number of waits.
func (rp *ResourcePool) WaitCount() int64 {
	return rp.waitCount.Load()
}

// WaitTime returns the total wait time.
func (rp *ResourcePool) WaitTime() time.Duration {
	return time.Duration(rp.waitTime.Load())
}

// IdleTimeout returns the resource idle timeout.
func (rp *ResourcePool) IdleTimeout() time.Duration {
	return time.Duration(rp.idleTimeout.Load())
}

// IdleClosed returns the count of resources closed due to idle timeout.
func (rp *ResourcePool) IdleClosed() int64 {
	return rp.idleClosed.Load()
}

// extendedLifetimeTimeout returns random duration within range [maxLifetime, 2*maxLifetime)
func (rp *ResourcePool) extendedMaxLifetime() time.Duration {
	maxLifetime := rp.maxLifetime.Load()
	if maxLifetime == 0 {
		return 0
	}
	return time.Duration(maxLifetime + rand.Int63n(maxLifetime))
}

// MaxLifetimeClosed returns the count of resources closed due to refresh timeout.
func (rp *ResourcePool) MaxLifetimeClosed() int64 {
	return rp.maxLifetimeClosed.Load()
}

// Exhausted returns the number of times Available dropped below 1
func (rp *ResourcePool) Exhausted() int64 {
	return rp.exhausted.Load()
}

// GetCount returns the number of times get was called
func (rp *ResourcePool) GetCount() int64 {
	return rp.getCount.Load()
}

// GetSettingCount returns the number of times getWithSettings was called
func (rp *ResourcePool) GetSettingCount() int64 {
	return rp.getSettingCount.Load()
}

// DiffSettingCount returns the number of times different setting were applied on the resource.
func (rp *ResourcePool) DiffSettingCount() int64 {
	return rp.diffSettingCount.Load()
}

// ResetSettingCount returns the number of times setting were reset on the resource.
func (rp *ResourcePool) ResetSettingCount() int64 {
	return rp.resetSettingCount.Load()
}
