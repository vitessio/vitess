package pools

import (
	"context"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
)

// DynamicResourcePool is a ResourcePool that adds resources on demand, and removes them completely after the idle timeout.
// Unlike StaticResourcePool, this effectively scales the pool down to zero.
type DynamicResourcePool struct {
	// number ever created. monotonic counter.
	countCreated sync2.AtomicInt64
	// number ever expired. monotonic counter.
	countExpired sync2.AtomicInt64
	// number of Get calls that had to enter a blocking wait.
	waitCount sync2.AtomicInt64
	// total time waited in the above Get calls.
	waitTime sync2.AtomicDuration
	// how long before unused resources are closed.
	idleTimeout sync2.AtomicDuration
	// how many resources are scheduled to be closed when returned to the pool. used when SetCapacity is called to reduce capacity,
	// but there are not enough unborrowed resources or seeds in the pool.
	pendingRemovals sync2.AtomicInt64

	// current capacity.
	currentCapacity int64
	// only SetCapacity needs a mutex.
	setCapacityMutex *sync.Mutex

	// every item in this channel represents a resource that can be created. it is filled on instantiation, and refilled as items expire.
	seeds chan struct{}
	// every item in this channel is a resource that can be borrowed, added when returned to the pool by a borrower.
	resources chan resourceWrapper
	// standard stop channel, gets closed when stopping
	stop chan struct{}

	// function that creates a new resource when needed.
	factory Factory
	// function for logging waits:
	logWait func(time.Time)
	ticker  *time.Ticker
}

func NewDynamicResourcePool(factory Factory, capacity, maxCap int, idleTimeout, expireInterval time.Duration, logWait func(time.Time)) ResourcePool {
	res := &DynamicResourcePool{
		idleTimeout:      sync2.NewAtomicDuration(idleTimeout),
		currentCapacity:  int64(capacity),
		setCapacityMutex: &sync.Mutex{},
		seeds:            make(chan struct{}, maxCap),
		resources:        make(chan resourceWrapper, maxCap),
		stop:             make(chan struct{}),
		factory:          factory,
		logWait:          logWait,
	}
	if expireInterval > 0 {
		res.ticker = time.NewTicker(expireInterval)
		go res.runExpireTicker()
	}
	for i := 0; i < capacity; i++ {
		// add the initial seeds, so connections can be created!
		res.seeds <- struct{}{}
	}
	return res
}

func (d *DynamicResourcePool) Close() {
	if d.ticker != nil {
		d.ticker.Stop()
	}
	close(d.stop)
	close(d.seeds)
	close(d.resources)
	_ = d.SetCapacity(0)
}

func (d *DynamicResourcePool) runExpireTicker() {
	for {
		select {
		case <-d.stop:
			return
		case <-d.ticker.C:
			// takes all the resources out and puts the unexpired ones back
			for _, rw := range d.takeUnexpiredResources() {
				d.resources <- rw
			}
		}
	}
}

func (d *DynamicResourcePool) takeUnexpiredResources() []resourceWrapper {
	rws := make([]resourceWrapper, 0, d.MaxCap())
	idleTimeout := d.idleTimeout.Get()
	for {
		select {
		case rw := <-d.resources:
			if rw.IsExpired(idleTimeout) {
				if rw.resource != nil {
					rw.resource.Close()
				}
				d.countExpired.Add(1)
				d.seeds <- struct{}{}
			} else {
				rws = append(rws, rw)
			}
		default:
			// no more resources
			return rws
		}
	}
}

func (d *DynamicResourcePool) maybeUnwrapResource(rw resourceWrapper) Resource {
	if !rw.IsExpired(d.idleTimeout.Get()) {
		return rw.resource
	}
	if rw.resource != nil {
		rw.resource.Close()
	}
	d.countExpired.Add(1)
	d.seeds <- struct{}{}
	return nil
}

func (d *DynamicResourcePool) getResourceNonblocking() (Resource, error) {
	for {
		select {
		case rw, ok := <-d.resources:
			if !ok {
				return nil, ErrClosed
			} else if r := d.maybeUnwrapResource(rw); r != nil {
				return r, nil
			}
			// NOTE: here, if r == nil was true, that means an expired resource was at the head of the pool.
			// Because the resource channel is ordered (fifo), the oldest resources will be the ones dequeued first.
		default:
			return nil, nil
		}
	}
}

func (d *DynamicResourcePool) getOrCreateResourceBlocking(ctx context.Context) (Resource, error) {
	t0 := time.Now()
	defer d.recordWait(t0)
	for {
		select {
		case <-ctx.Done():
			// taking too long, bye!
			return nil, ErrTimeout
		case <-d.stop:
			// pool is closing
			return nil, ErrClosed
		case _, ok := <-d.seeds:
			// a resource can be created:
			if !ok {
				return nil, ErrClosed
			}
			newResource, err := d.createResource(ctx)
			if err != nil {
				go (func() {
					select {
					case <-d.stop:
					case <-time.After(500 * time.Millisecond):
						// re-add the seed after 500ms:
						select {
						case d.seeds <- struct{}{}:
							log.Infof("Re-added resource seed after delay: err=%v", err)
						default:
							log.Infof("Could not re-added resource seed after delay: err=%v", err)
						}
					}
				})()
			} else {
				return newResource, nil
			}
		case rw, ok := <-d.resources:
			// a pooled resource has been returned:
			if !ok {
				return nil, ErrClosed
			} else if r := d.maybeUnwrapResource(rw); r != nil {
				return r, nil
			}
		}
	}
}

func (d *DynamicResourcePool) recordWait(start time.Time) {
	d.waitCount.Add(1)
	d.waitTime.Add(time.Since(start))
	if d.logWait != nil {
		d.logWait(start)
	}
}

func (d *DynamicResourcePool) createResource(ctx context.Context) (Resource, error) {
	resource, err := d.factory(ctx)
	if err != nil {
		log.Errorf("Failed to create resource: resource=%v, err=%v", resource, err)
		if resource != nil {
			// misbehaving factory: returns a non-nil resource with a non-nil error.
			// prevent resource leaks by closing explicitly.
			resource.Close()
		}
		return nil, err
	}
	if resource != nil {
		d.countCreated.Add(1)
	}
	return resource, nil
}

func (d *DynamicResourcePool) Get(ctx context.Context) (Resource, error) {
	if err := ctx.Err(); err != nil {
		// ctx already canceled or timed out:
		return nil, ErrCtxTimeout
	}

	// first, we try to get a resource from the pool without waiting.
	resource, err := d.getResourceNonblocking()
	if err != nil || resource != nil {
		return resource, err
	}

	// hmm, ok, so there was no non-expired resource in the pool for us.
	// now we either wait until we are allowed to make one, or one is
	// returned to the pool:
	return d.getOrCreateResourceBlocking(ctx)
}

func (d *DynamicResourcePool) Put(resource Resource) {
	if d.hasPendingRemoval() {
		// dont add a seed or a resource, there is a removal pending. discard this resource.
		if resource != nil {
			resource.Close()
		}
		return
	}
	if resource == nil {
		// returning a "closed" resource by passing nil...
		// we need to inject a seed to let a new resource get created.
		d.seeds <- struct{}{}
		return
	}
	d.resources <- resourceWrapper{
		resource: resource,
		timeUsed: time.Now(),
	}
}

func (d *DynamicResourcePool) Capacity() int64 {
	d.setCapacityMutex.Lock()
	defer d.setCapacityMutex.Unlock()
	return d.currentCapacity
}

func (d *DynamicResourcePool) zeroPendingRemovals() int64 {
	// lockless (cas) get-and-set(0)
	for {
		pr := d.pendingRemovals.Get()
		if d.pendingRemovals.CompareAndSwap(pr, 0) {
			return pr
		}
	}
}

func (d *DynamicResourcePool) hasPendingRemoval() bool {
	// lockless (cas) get-and-decr(if > 0)
	for {
		pr := d.pendingRemovals.Get()
		if pr < 1 {
			return false
		}
		if d.pendingRemovals.CompareAndSwap(pr, pr-1) {
			return true
		}
	}
}

func (d *DynamicResourcePool) SetCapacity(newCapacity int) error {
	// only one thread can change capacity at a time.
	if newCapacity < 0 || int64(newCapacity) > d.MaxCap() {
		return fmt.Errorf("capacity %d is out of range", newCapacity)
	}
	d.setCapacityMutex.Lock()
	defer d.setCapacityMutex.Unlock()
	oldCapacity := d.currentCapacity
	d.currentCapacity = int64(newCapacity)
	toAdd := (int64(newCapacity) - oldCapacity) - d.zeroPendingRemovals()
	// if we are going from 3 to 5 (+2) but there is one pending removal, then it is +1 only because one removal from
	// the last call to SetCapacity
	for toAdd > 0 {
		// adding seeds
		d.seeds <- struct{}{}
		toAdd--
	}

	done := false
	for (!done) && toAdd < 0 {
		// try to remove seeds first
		select {
		case <-d.seeds:
			toAdd++
		default:
			done = true
		}
	}

	done = false
	for (!done) && toAdd < 0 {
		select {
		case rw := <-d.resources:
			if rw.resource != nil {
				rw.resource.Close()
			}
			toAdd++
		default:
			done = true
		}
	}

	if toAdd < 0 {
		d.pendingRemovals.Add(-toAdd)
	}

	return nil
}

func (d *DynamicResourcePool) MaxCap() int64 {
	return int64(cap(d.resources))
}

func (d *DynamicResourcePool) Available() int64 {
	return int64(len(d.seeds) + len(d.resources))
}

func (d *DynamicResourcePool) Active() int64 {
	return d.Capacity() - int64(len(d.seeds))
}

func (d *DynamicResourcePool) InUse() int64 {
	return d.Active() - int64(len(d.resources))
}

func (d *DynamicResourcePool) IdleClosed() int64 {
	return d.countExpired.Get()
}

func (d *DynamicResourcePool) Exhausted() int64 {
	return d.WaitCount()
}

func (d *DynamicResourcePool) WaitCount() int64 {
	return d.waitCount.Get()
}

func (d *DynamicResourcePool) WaitTime() time.Duration {
	return d.waitTime.Get()
}

func (d *DynamicResourcePool) StatsJSON() string {
	return fmt.Sprintf(`{"Capacity": %v, "Available": %v, "Active": %v, "InUse": %v, "MaxCapacity": %v, "WaitCount": %v, "WaitTime": %v, "IdleTimeout": %v, "IdleClosed": %v, "Exhausted": %v}`,
		d.Capacity(),
		d.Available(),
		d.Active(),
		d.InUse(),
		d.MaxCap(),
		d.WaitCount(),
		d.WaitTime().Nanoseconds(),
		d.IdleTimeout().Nanoseconds(),
		d.IdleClosed(),
		d.Exhausted(),
	)
}

func (d *DynamicResourcePool) SetIdleTimeout(idleTimeout time.Duration) {
	d.idleTimeout.Set(idleTimeout)
}

func (d *DynamicResourcePool) IdleTimeout() time.Duration {
	return d.idleTimeout.Get()
}
