/*
Copyright 2022 The Vitess Authors.

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
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/timer"
)

var _ IResourcePool = (*DynamicResourcePool)(nil)
var _ refreshPool = (*DynamicResourcePool)(nil)

// DynamicResourcePool the design is inspired from the golang database/sql package
// whose source code is governed by a BSD-style license.
type DynamicResourcePool struct {
	// Atomic access only (stats). At top of in order to prevent panics on certain architectures.
	waitTime sync2.AtomicDuration // Total time, the connections waited for.

	f Factory // function that creates a new resource when needed.

	maxCapacity int // maximum number of resources in the pool

	mu            sync.Mutex // protects following
	closed        bool
	rawConnection []resourceWrapper
	connReceiver  chan resourceWrapper
	connRequests  map[uint64]any
	nextRequest   uint64
	numOpen       int // number of open resources
	// signals the need for new resource to a goroutine running resourceOpener().
	// mayCreateResourceLocked sends on this chan (one send per needed connection)
	// It is closed during dp.Close(). The close tells the resourceOpener
	// goroutine to exit.
	openerCh    chan struct{}
	maxIdleTime time.Duration // maximum amount of time a resource may remain idle before being closed
	idleTimer   *timer.Timer  // idleTimer periodically closes idle resources.

	waitCount         int64 // Total number of connections waited for.
	maxIdleTimeClosed int64 // Total number of connections closed due to idle time.

	cancel func() // cancel stops the connection opener.
}

func NewDynamicResourcePool(factory Factory, maxCap int, idleTimeout time.Duration) *DynamicResourcePool {
	if maxCap <= 0 {
		// TODO: we can look at it if we want to support unlimited capacity.
		panic("pool capacity must be greater than 0")
	}
	ctx, cancel := context.WithCancel(context.Background())
	pool := &DynamicResourcePool{
		f:            factory,
		maxCapacity:  maxCap,
		maxIdleTime:  idleTimeout,
		openerCh:     make(chan struct{}, 4*maxCap),
		connReceiver: make(chan resourceWrapper, 4*maxCap),
		connRequests: make(map[uint64]any),
		cancel:       cancel,
	}

	go pool.resourceOpener(ctx)

	if idleTimeout > 0 {
		// check little aggressively for idle resources, every 1/10th of the idle timeout
		pool.idleTimer = timer.NewTimer(idleTimeout / 10)
		pool.idleTimer.Start(pool.closeIdleResources)
	}

	return pool
}

func (dp *DynamicResourcePool) Name() string {
	return "DynamicResourcePool"
}

// Runs in a separate goroutine, opens new connections when requested.
func (dp *DynamicResourcePool) resourceOpener(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-dp.openerCh:
			dp.openNewResource(ctx)
		}
	}
}

// Open one new resource
func (dp *DynamicResourcePool) openNewResource(ctx context.Context) {
	// mayCreateResourceLocked has already executed dp.numOpen++ before it sent
	// on dp.openerCh. This function must execute dp.numOpen-- if the
	// connection fails or is closed before returning.
	resource, err := dp.f(ctx)
	dp.mu.Lock()
	defer dp.mu.Unlock()
	if dp.closed {
		if err == nil {
			resource.Close()
		}
		dp.numOpen--
		return
	}
	if err != nil {
		dp.numOpen--
		dp.putLocked(nil, err)
		dp.mayCreateResourceLocked()
		return
	}

	added := dp.putLocked(resource, err)
	if !added {
		dp.numOpen--
		resource.Close()
	}
}

func (dp *DynamicResourcePool) Get(ctx context.Context) (resource Resource, err error) {
	dp.mu.Lock()
	if dp.closed {
		// pool is closed, return error
		dp.mu.Unlock()
		return nil, ErrClosed
	}
	// 1: Check if the context is expired.
	select {
	default:
	case <-ctx.Done():
		dp.mu.Unlock()
		return nil, ctx.Err()
	}
	// 2: Check if there are resources available.
	last := len(dp.rawConnection) - 1
	if last >= 0 {
		conn := dp.rawConnection[last]
		dp.rawConnection = dp.rawConnection[:last]
		dp.mu.Unlock()
		return conn.resource, nil
	}
	// 3: Check if the pool is at maxCapacity.
	if dp.numOpen >= dp.maxCapacity {
		dp.nextRequest++
		reqKey := dp.nextRequest
		dp.connRequests[reqKey] = nil
		dp.waitCount++
		dp.mu.Unlock()

		startTime := time.Now()

		select {
		case <-ctx.Done():
			// Timeout happened.
			// Removing the connection request and ensure no value has been sent on the channel.
			connSentForThisReq := false
			dp.mu.Lock()
			// If the key is still in the map, it means the connection request has not been fulfilled.
			// And if the key is not in the map, that means the connection request is fulfilled.
			if _, exists := dp.connRequests[reqKey]; !exists {
				connSentForThisReq = true
			} else {
				delete(dp.connRequests, reqKey)
			}
			dp.mu.Unlock()

			dp.waitTime.Add(time.Since(startTime))

			if connSentForThisReq {
				select {
				default:
				case ret, ok := <-dp.connReceiver:
					if ok && ret.resource != nil {
						dp.Put(ret.resource)
					}
				}
			}
			return nil, ctx.Err()
		case ret, ok := <-dp.connReceiver:
			dp.waitTime.Add(time.Since(startTime))

			if !ok {
				return nil, ErrClosed
			}
			if ret.resource == nil {
				return nil, ret.err
			}
			return ret.resource, nil
		}
	}
	// 4: Create a new resource.
	dp.numOpen++ // optimistically increase the number of open resources.
	dp.mu.Unlock()
	resource, err = dp.f(ctx)
	if err != nil {
		dp.mu.Lock()
		dp.numOpen-- // undo optimistic increase.
		dp.mayCreateResourceLocked()
		dp.mu.Unlock()
		return nil, err
	}
	return resource, nil
}

// Put puts the resource back into the pool.
func (dp *DynamicResourcePool) Put(resource Resource) {
	dp.mu.Lock()
	if resource == nil {
		// the resource was probably closed outside.
		// so, decrement numOpen and create the difference
		dp.numOpen--
		dp.mayCreateResourceLocked()
		dp.mu.Unlock()
		return
	}
	added := dp.putLocked(resource, nil)
	dp.mu.Unlock()

	if !added {
		dp.resourceClose(resource)
		return
	}

}

func (dp *DynamicResourcePool) putLocked(resource Resource, err error) bool {
	if dp.closed {
		return false
	}
	if dp.numOpen > dp.maxCapacity {
		return false
	}
	if reqC := len(dp.connRequests); reqC > 0 {
		var reqKey uint64
		for reqKey = range dp.connRequests {
			break
		}
		delete(dp.connRequests, reqKey) // Remove from pending request.
		dp.connReceiver <- resourceWrapper{
			resource: resource,
			err:      err,
		}
		return true
	}
	if err == nil {
		dp.rawConnection = append(dp.rawConnection, resourceWrapper{
			resource: resource,
			timeUsed: time.Now(),
		})
		return true
	}
	return false
}

func (dp *DynamicResourcePool) resourceClose(resource Resource) {
	resource.Close()
	dp.mu.Lock()
	dp.numOpen--
	dp.mayCreateResourceLocked()
	dp.mu.Unlock()
}

// Assumes dp.mu is locked.
// If there are connRequests and the resource limit hasn't been reached,
// then open new resources.
func (dp *DynamicResourcePool) mayCreateResourceLocked() {
	if dp.closed {
		return
	}
	numRequests := len(dp.connRequests)
	numCanOpen := dp.maxCapacity - dp.numOpen
	if numRequests > numCanOpen {
		numRequests = numCanOpen
	}
	for numRequests > 0 {
		dp.numOpen++ // optimistically
		numRequests--
		dp.openerCh <- struct{}{}
	}
}

func (dp *DynamicResourcePool) Close() {
	dp.mu.Lock()
	if dp.closed { // idempotent
		dp.mu.Unlock()
		return
	}

	if dp.idleTimer != nil {
		dp.idleTimer.Stop()
	}

	copyResources := make([]resourceWrapper, 0, len(dp.rawConnection))
	copyResources = append(copyResources, dp.rawConnection...)

	dp.rawConnection = nil
	dp.closed = true
	// TODO: verify if this works as intended.
	// Close the receiver conn channel.
	close(dp.connReceiver)
	dp.mu.Unlock()
	for _, r := range copyResources {
		dp.resourceClose(r.resource)
	}
	dp.cancel() // stops the resource opener.
}

func (dp *DynamicResourcePool) reopen() {
	//TODO implement me
	panic("implement me")
}

func (dp *DynamicResourcePool) closeIdleResources() {
	dp.mu.Lock()
	if dp.closed || dp.numOpen == 0 {
		dp.mu.Unlock()
		return
	}
	closing := dp.resourceCleanerLocked()
	dp.mu.Unlock()
	for _, c := range closing {
		dp.resourceClose(c.resource)
	}
}

// resourceCleanerLocked removes resources that should be closed.
func (dp *DynamicResourcePool) resourceCleanerLocked() []resourceWrapper {
	var closing []resourceWrapper

	// As rawConnection is ordered by timeUsed process
	// in reverse order to minimise the work needed.
	idleSince := time.Now().Add(-dp.maxIdleTime)
	last := len(dp.rawConnection) - 1
	for i := last; i >= 0; i-- {
		c := dp.rawConnection[i]
		if c.timeUsed.Before(idleSince) {
			i++
			closing = dp.rawConnection[:i:i]
			dp.rawConnection = dp.rawConnection[i:]
			idleClosing := int64(len(closing))
			dp.maxIdleTimeClosed += idleClosing
			break
		}
	}
	return closing
}

func (dp *DynamicResourcePool) SetCapacity(capacity int) error {
	// This pool does resize based on the lazy resource creation and idle time.
	// So, this is a no-op.
	return nil
}

func (dp *DynamicResourcePool) SetIdleTimeout(idleTimeout time.Duration) {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	dp.maxIdleTime = idleTimeout
	if idleTimeout <= 0 {
		if dp.idleTimer != nil {
			dp.idleTimer.Stop()
			dp.idleTimer = nil
		}
		return
	}
	if dp.idleTimer == nil {
		dp.idleTimer = timer.NewTimer(idleTimeout / 10)
		dp.idleTimer.Start(dp.closeIdleResources)
	} else {
		dp.idleTimer.SetInterval(idleTimeout / 10)
	}
}

func (dp *DynamicResourcePool) StatsJSON() string {
	return fmt.Sprintf(`{"Capacity": %v, "Available": %v, "Active": %v, "InUse": %v, "MaxCapacity": %v, "WaitCount": %v, "WaitTime": %v, "IdleTimeout": %v, "IdleClosed": %v, "Exhausted": %v}`,
		dp.Capacity(),
		dp.Available(),
		dp.Active(),
		dp.InUse(),
		dp.MaxCap(),
		dp.WaitCount(),
		dp.WaitTime().Nanoseconds(),    // nolint: staticcheck
		dp.IdleTimeout().Nanoseconds(), // nolint: staticcheck
		dp.IdleClosed(),
		dp.Exhausted(),
	)
}

func (dp *DynamicResourcePool) Capacity() int64 {
	return int64(dp.maxCapacity)
}

func (dp *DynamicResourcePool) Available() int64 {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	return int64(dp.numOpen)
}

func (dp *DynamicResourcePool) Active() int64 {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	return int64(dp.numOpen)
}

func (dp *DynamicResourcePool) InUse() int64 {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	inUse := dp.numOpen - len(dp.rawConnection)
	return int64(inUse)
}

func (dp *DynamicResourcePool) MaxCap() int64 {
	return int64(dp.maxCapacity)
}

func (dp *DynamicResourcePool) WaitCount() int64 {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	waitCount := dp.waitCount
	return waitCount
}

func (dp *DynamicResourcePool) WaitTime() time.Duration {
	return dp.waitTime.Get()
}

func (dp *DynamicResourcePool) IdleTimeout() time.Duration {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	idleTimeout := dp.maxIdleTime
	return idleTimeout
}

func (dp *DynamicResourcePool) IdleClosed() int64 {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	idleClosed := dp.maxIdleTimeClosed
	return idleClosed
}

func (dp *DynamicResourcePool) Exhausted() int64 {
	//TODO implement me
	panic("implement me")
}
