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

/*
Package dbconnpool exposes a single DBConnection object
with wrapped access to a single DB connection, and a ConnectionPool
object to pool these DBConnections.
*/
package dbconnpool

import (
	"errors"
	"net"
	"sync"
	"time"

	"vitess.io/vitess/go/netutil"

	"context"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/dbconfigs"
)

var (
	// ErrConnPoolClosed is returned if the connection pool is closed.
	ErrConnPoolClosed = errors.New("connection pool is closed")
	// usedNames is for preventing expvar from panicking. Tests
	// create pool objects multiple time. If a name was previously
	// used, expvar initialization is skipped.
	// TODO(sougou): Find a way to still crash if this happened
	// through non-test code.
	usedNames = make(map[string]bool)
)

// ConnectionPool re-exposes ResourcePool as a pool of
// PooledDBConnection objects.
type ConnectionPool struct {
	mu                  sync.Mutex
	connections         pools.IResourcePool
	capacity            int
	idleTimeout         time.Duration
	maxLifetime         time.Duration
	resolutionFrequency time.Duration

	// info is set at Open() time
	info dbconfigs.Connector
	name string
}

// NewConnectionPool creates a new ConnectionPool. The name is used
// to publish stats only.
func NewConnectionPool(name string, capacity int, idleTimeout time.Duration, maxLifetime time.Duration, dnsResolutionFrequency time.Duration) *ConnectionPool {
	cp := &ConnectionPool{name: name, capacity: capacity, idleTimeout: idleTimeout, maxLifetime: maxLifetime, resolutionFrequency: dnsResolutionFrequency}
	if name == "" || usedNames[name] {
		return cp
	}
	usedNames[name] = true
	stats.NewGaugeFunc(name+"Capacity", "Connection pool capacity", cp.Capacity)
	stats.NewGaugeFunc(name+"Available", "Connection pool available", cp.Available)
	stats.NewGaugeFunc(name+"Active", "Connection pool active", cp.Active)
	stats.NewGaugeFunc(name+"InUse", "Connection pool in-use", cp.InUse)
	stats.NewGaugeFunc(name+"MaxCap", "Connection pool max cap", cp.MaxCap)
	stats.NewCounterFunc(name+"WaitCount", "Connection pool wait count", cp.WaitCount)
	stats.NewCounterDurationFunc(name+"WaitTime", "Connection pool wait time", cp.WaitTime)
	stats.NewGaugeDurationFunc(name+"IdleTimeout", "Connection pool idle timeout", cp.IdleTimeout)
	stats.NewGaugeFunc(name+"IdleClosed", "Connection pool idle closed", cp.IdleClosed)
	stats.NewGaugeFunc(name+"MaxLifetimeClosed", "Connection pool refresh closed", cp.MaxLifetimeClosed)
	stats.NewCounterFunc(name+"Exhausted", "Number of times pool had zero available slots", cp.Exhausted)
	return cp
}

func (cp *ConnectionPool) pool() (p pools.IResourcePool) {
	cp.mu.Lock()
	p = cp.connections
	cp.mu.Unlock()
	return p
}

// Open must be called before starting to use the pool.
//
// For instance:
// pool := dbconnpool.NewConnectionPool("name", 10, 30*time.Second)
// pool.Open(info)
// ...
// conn, err := pool.Get()
// ...
func (cp *ConnectionPool) Open(info dbconfigs.Connector) {
	var refreshCheck pools.RefreshCheck
	if net.ParseIP(info.Host()) == nil {
		refreshCheck = netutil.DNSTracker(info.Host())
	} else {
		refreshCheck = nil
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.info = info
	cp.connections = pools.NewResourcePool(cp.connect, cp.capacity, cp.capacity, cp.idleTimeout, cp.maxLifetime, nil, refreshCheck, cp.resolutionFrequency)
}

// connect is used by the resource pool to create a new Resource.
func (cp *ConnectionPool) connect(ctx context.Context) (pools.Resource, error) {
	c, err := NewDBConnection(ctx, cp.info)
	if err != nil {
		return nil, err
	}
	return &PooledDBConnection{
		DBConnection: c,
		timeCreated:  time.Now(),
		pool:         cp,
	}, nil
}

// Close will close the pool and wait for connections to be returned before
// exiting.
func (cp *ConnectionPool) Close() {
	p := cp.pool()
	if p == nil {
		return
	}
	// We should not hold the lock while calling Close
	// because it waits for connections to be returned.
	p.Close()
	cp.mu.Lock()
	cp.connections = nil
	cp.mu.Unlock()
}

// Get returns a connection.
// You must call Recycle on the PooledDBConnection once done.
func (cp *ConnectionPool) Get(ctx context.Context) (*PooledDBConnection, error) {
	p := cp.pool()
	if p == nil {
		return nil, ErrConnPoolClosed
	}
	r, err := p.Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return r.(*PooledDBConnection), nil
}

// Put puts a connection into the pool.
func (cp *ConnectionPool) Put(conn *PooledDBConnection) {
	p := cp.pool()
	if p == nil {
		panic(ErrConnPoolClosed)
	}
	if conn == nil {
		// conn has a type, if we just Put(conn), we end up
		// putting an interface with a nil value, that is not
		// equal to a nil value. So just put a plain nil.
		p.Put(nil)
		return
	}
	p.Put(conn)
}

// SetCapacity alters the size of the pool at runtime.
func (cp *ConnectionPool) SetCapacity(capacity int) (err error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.connections != nil {
		err = cp.connections.SetCapacity(capacity)
		if err != nil {
			return err
		}
	}
	cp.capacity = capacity
	return nil
}

// SetIdleTimeout sets the idleTimeout on the pool.
func (cp *ConnectionPool) SetIdleTimeout(idleTimeout time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.connections != nil {
		cp.connections.SetIdleTimeout(idleTimeout)
	}
	cp.idleTimeout = idleTimeout
}

// StatsJSON returns the pool stats as a JSOn object.
func (cp *ConnectionPool) StatsJSON() string {
	p := cp.pool()
	if p == nil {
		return "{}"
	}
	return p.StatsJSON()
}

// Capacity returns the pool capacity.
func (cp *ConnectionPool) Capacity() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Capacity()
}

// Available returns the number of available connections in the pool
func (cp *ConnectionPool) Available() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Available()
}

// Active returns the number of active connections in the pool
func (cp *ConnectionPool) Active() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Active()
}

// InUse returns the number of in-use connections in the pool
func (cp *ConnectionPool) InUse() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.InUse()
}

// MaxCap returns the maximum size of the pool
func (cp *ConnectionPool) MaxCap() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.MaxCap()
}

// WaitCount returns how many clients are waiting for a connection
func (cp *ConnectionPool) WaitCount() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.WaitCount()
}

// WaitTime return the pool WaitTime.
func (cp *ConnectionPool) WaitTime() time.Duration {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.WaitTime()
}

// IdleTimeout returns the idle timeout for the pool.
func (cp *ConnectionPool) IdleTimeout() time.Duration {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.IdleTimeout()
}

// IdleClosed returns the number of connections closed due to idle timeout for the pool.
func (cp *ConnectionPool) IdleClosed() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.IdleClosed()
}

// MaxLifetimeClosed returns the number of connections closed due to refresh timeout for the pool.
func (cp *ConnectionPool) MaxLifetimeClosed() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.MaxLifetimeClosed()
}

// Exhausted returns the number of times available went to zero for the pool.
func (cp *ConnectionPool) Exhausted() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Exhausted()
}
