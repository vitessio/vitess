// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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

	"github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/stats"
)

var (
	CONN_POOL_CLOSED_ERR = errors.New("connection pool is closed")
)

// PoolConnection is the interface implemented by users of this specialized pool.
type PoolConnection interface {
	ExecuteFetch(query string, maxrows int, wantfields bool) (*proto.QueryResult, error)
	ExecuteStreamFetch(query string, callback func(*proto.QueryResult) error, streamBufferSize int) error
	Id() int64
	Close()
	IsClosed() bool
	Recycle()
}

// CreateConnectionFunc is the factory method to create new connections
// within the passed ConnectionPool.
type CreateConnectionFunc func(*ConnectionPool) (connection PoolConnection, err error)

// ConnectionPool re-exposes ResourcePool as a pool of PoolConnection objects
type ConnectionPool struct {
	mu          sync.Mutex
	connections *pools.ResourcePool
	capacity    int
	idleTimeout time.Duration
}

// NewConnectionPool creates a new ConnectionPool. The name is used
// to publish stats only.
func NewConnectionPool(name string, capacity int, idleTimeout time.Duration) *ConnectionPool {
	cp := &ConnectionPool{capacity: capacity, idleTimeout: idleTimeout}
	if name == "" {
		return cp
	}
	stats.Publish(name+"Capacity", stats.IntFunc(cp.Capacity))
	stats.Publish(name+"Available", stats.IntFunc(cp.Available))
	stats.Publish(name+"MaxCap", stats.IntFunc(cp.MaxCap))
	stats.Publish(name+"WaitCount", stats.IntFunc(cp.WaitCount))
	stats.Publish(name+"WaitTime", stats.DurationFunc(cp.WaitTime))
	stats.Publish(name+"IdleTimeout", stats.DurationFunc(cp.IdleTimeout))
	return cp
}

func (cp *ConnectionPool) pool() (p *pools.ResourcePool) {
	cp.mu.Lock()
	p = cp.connections
	cp.mu.Unlock()
	return p
}

// Open must be call before starting to use the pool.
func (cp *ConnectionPool) Open(connFactory CreateConnectionFunc) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	f := func() (pools.Resource, error) {
		return connFactory(cp)
	}
	cp.connections = pools.NewResourcePool(f, cp.capacity, cp.capacity, cp.idleTimeout)
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
// You must call Recycle on the PoolConnection once done.
func (cp *ConnectionPool) Get(timeout time.Duration) (PoolConnection, error) {
	p := cp.pool()
	if p == nil {
		return nil, CONN_POOL_CLOSED_ERR
	}
	r, err := p.Get(timeout)
	if err != nil {
		return nil, err
	}
	return r.(PoolConnection), nil
}

// TryGet returns a connection, or nil.
// You must call Recycle on the PoolConnection once done.
func (cp *ConnectionPool) TryGet() (PoolConnection, error) {
	p := cp.pool()
	if p == nil {
		return nil, CONN_POOL_CLOSED_ERR
	}
	r, err := p.TryGet()
	if err != nil || r == nil {
		return nil, err
	}
	return r.(PoolConnection), nil
}

// Put puts a connection into the pool.
func (cp *ConnectionPool) Put(conn PoolConnection) {
	p := cp.pool()
	if p == nil {
		panic(CONN_POOL_CLOSED_ERR)
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
