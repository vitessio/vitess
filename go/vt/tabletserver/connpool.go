// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"sync"
	"time"

	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"golang.org/x/net/context"
)

// ConnPool implements a custom connection pool for tabletserver.
// It's similar to dbconnpool.ConnPool, but the connections it creates
// come with built-in ability to kill in-flight queries. These connections
// also trigger a CheckMySQL call if we fail to connect to MySQL.
// Other than the connection type, ConnPool maintains an additional
// pool of dba connections that are used to kill connections.
type ConnPool struct {
	mu                sync.Mutex
	connections       *pools.ResourcePool
	capacity          int
	idleTimeout       time.Duration
	dbaPool           *dbconnpool.ConnectionPool
	queryServiceStats *QueryServiceStats
	checker           MySQLChecker
}

// NewConnPool creates a new ConnPool. The name is used
// to publish stats only.
func NewConnPool(
	name string,
	capacity int,
	idleTimeout time.Duration,
	enablePublishStats bool,
	queryServiceStats *QueryServiceStats,
	checker MySQLChecker) *ConnPool {
	cp := &ConnPool{
		capacity:          capacity,
		idleTimeout:       idleTimeout,
		dbaPool:           dbconnpool.NewConnectionPool("", 1, idleTimeout),
		queryServiceStats: queryServiceStats,
		checker:           checker,
	}
	if name == "" {
		return cp
	}
	if enablePublishStats {
		stats.Publish(name+"Capacity", stats.IntFunc(cp.Capacity))
		stats.Publish(name+"Available", stats.IntFunc(cp.Available))
		stats.Publish(name+"MaxCap", stats.IntFunc(cp.MaxCap))
		stats.Publish(name+"WaitCount", stats.IntFunc(cp.WaitCount))
		stats.Publish(name+"WaitTime", stats.DurationFunc(cp.WaitTime))
		stats.Publish(name+"IdleTimeout", stats.DurationFunc(cp.IdleTimeout))
	}
	return cp
}

func (cp *ConnPool) pool() (p *pools.ResourcePool) {
	cp.mu.Lock()
	p = cp.connections
	cp.mu.Unlock()
	return p
}

// Open must be called before starting to use the pool.
func (cp *ConnPool) Open(appParams, dbaParams *sqldb.ConnParams) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	f := func() (pools.Resource, error) {
		return NewDBConn(cp, appParams, dbaParams, cp.queryServiceStats)
	}
	cp.connections = pools.NewResourcePool(f, cp.capacity, cp.capacity, cp.idleTimeout)
	cp.dbaPool.Open(dbconnpool.DBConnectionCreator(dbaParams, cp.queryServiceStats.MySQLStats))
}

// Close will close the pool and wait for connections to be returned before
// exiting.
func (cp *ConnPool) Close() {
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
	cp.dbaPool.Close()
}

// Get returns a connection.
// You must call Recycle on DBConn once done.
func (cp *ConnPool) Get(ctx context.Context) (*DBConn, error) {
	p := cp.pool()
	if p == nil {
		return nil, ErrConnPoolClosed
	}
	r, err := p.Get(ctx)
	if err != nil {
		return nil, err
	}
	return r.(*DBConn), nil
}

// Put puts a connection into the pool.
func (cp *ConnPool) Put(conn *DBConn) {
	p := cp.pool()
	if p == nil {
		panic(ErrConnPoolClosed)
	}
	if conn == nil {
		p.Put(nil)
	} else {
		p.Put(conn)
	}
}

// SetCapacity alters the size of the pool at runtime.
func (cp *ConnPool) SetCapacity(capacity int) (err error) {
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
func (cp *ConnPool) SetIdleTimeout(idleTimeout time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.connections != nil {
		cp.connections.SetIdleTimeout(idleTimeout)
	}
	cp.dbaPool.SetIdleTimeout(idleTimeout)
	cp.idleTimeout = idleTimeout
}

// StatsJSON returns the pool stats as a JSON object.
func (cp *ConnPool) StatsJSON() string {
	p := cp.pool()
	if p == nil {
		return "{}"
	}
	return p.StatsJSON()
}

// Capacity returns the pool capacity.
func (cp *ConnPool) Capacity() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Capacity()
}

// Available returns the number of available connections in the pool
func (cp *ConnPool) Available() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Available()
}

// MaxCap returns the maximum size of the pool
func (cp *ConnPool) MaxCap() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.MaxCap()
}

// WaitCount returns how many clients are waiting for a connection
func (cp *ConnPool) WaitCount() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.WaitCount()
}

// WaitTime return the pool WaitTime.
func (cp *ConnPool) WaitTime() time.Duration {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.WaitTime()
}

// IdleTimeout returns the idle timeout for the pool.
func (cp *ConnPool) IdleTimeout() time.Duration {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.IdleTimeout()
}
