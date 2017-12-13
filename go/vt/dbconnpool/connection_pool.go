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

/*
Package dbconnpool exposes a single DBConnection object
with wrapped access to a single DB connection, and a ConnectionPool
object to pool these DBConnections.
*/
package dbconnpool

import (
	"errors"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/stats"
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
	mu          sync.Mutex
	connections *pools.ResourcePool
	capacity    int
	idleTimeout time.Duration

	// info and mysqlStats are set at Open() time
	info       *mysql.ConnParams
	mysqlStats *stats.Timings
}

// NewConnectionPool creates a new ConnectionPool. The name is used
// to publish stats only.
func NewConnectionPool(name string, capacity int, idleTimeout time.Duration) *ConnectionPool {
	cp := &ConnectionPool{capacity: capacity, idleTimeout: idleTimeout}
	if name == "" || usedNames[name] {
		return cp
	}
	usedNames[name] = true
	stats.Publish(name+"Capacity", stats.IntFunc(cp.Capacity))
	stats.Publish(name+"Available", stats.IntFunc(cp.Available))
	stats.Publish(name+"Active", stats.IntFunc(cp.Active))
	stats.Publish(name+"InUse", stats.IntFunc(cp.InUse))
	stats.Publish(name+"MaxCap", stats.IntFunc(cp.MaxCap))
	stats.Publish(name+"WaitCount", stats.IntFunc(cp.WaitCount))
	stats.Publish(name+"WaitTime", stats.DurationFunc(cp.WaitTime))
	stats.Publish(name+"IdleTimeout", stats.DurationFunc(cp.IdleTimeout))
	stats.Publish(name+"IdleClosed", stats.IntFunc(cp.IdleClosed))
	return cp
}

func (cp *ConnectionPool) pool() (p *pools.ResourcePool) {
	cp.mu.Lock()
	p = cp.connections
	cp.mu.Unlock()
	return p
}

// Open must be call before starting to use the pool.
//
// For instance:
// mysqlStats := stats.NewTimings("Mysql")
// pool := dbconnpool.NewConnectionPool("name", 10, 30*time.Second)
// pool.Open(info, mysqlStats)
// ...
// conn, err := pool.Get()
// ...
func (cp *ConnectionPool) Open(info *mysql.ConnParams, mysqlStats *stats.Timings) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.info = info
	cp.mysqlStats = mysqlStats
	cp.connections = pools.NewResourcePool(cp.connect, cp.capacity, cp.capacity, cp.idleTimeout)
}

// connect is used by the resource pool to create a new Resource.
func (cp *ConnectionPool) connect() (pools.Resource, error) {
	c, err := NewDBConnection(cp.info, cp.mysqlStats)
	if err != nil {
		return nil, err
	}
	return &PooledDBConnection{
		DBConnection: c,
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
	r, err := p.Get(ctx)
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

// IdleClosed returns the number of closed connections for the pool.
func (cp *ConnectionPool) IdleClosed() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.IdleClosed()
}
