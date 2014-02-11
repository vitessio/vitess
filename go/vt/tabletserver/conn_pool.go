// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"sync"
	"time"

	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/stats"
)

var (
	CONN_POOL_CLOSED_ERR = NewTabletError(FATAL, "connection pool is closed")
)

// ConnectionPool re-exposes ResourcePool as a pool of DBConnection objects
type ConnectionPool struct {
	mu          sync.Mutex
	connections *pools.ResourcePool
	capacity    int
	idleTimeout time.Duration
}

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

func (cp *ConnectionPool) Open(connFactory CreateConnectionFunc) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	f := func() (pools.Resource, error) {
		c, err := connFactory()
		if err != nil {
			return nil, err
		}
		return &pooledConnection{c, cp}, nil
	}
	cp.connections = pools.NewResourcePool(f, cp.capacity, cp.capacity, cp.idleTimeout)
}

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

// You must call Recycle on the PoolConnection once done.
func (cp *ConnectionPool) Get() PoolConnection {
	p := cp.pool()
	if p == nil {
		panic(CONN_POOL_CLOSED_ERR)
	}
	r, err := p.Get()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	return r.(*pooledConnection)
}

// You must call Recycle on the PoolConnection once done.
func (cp *ConnectionPool) SafeGet() (PoolConnection, error) {
	p := cp.pool()
	if p == nil {
		return nil, CONN_POOL_CLOSED_ERR
	}
	r, err := p.Get()
	if err != nil {
		return nil, err
	}
	return r.(*pooledConnection), nil
}

// You must call Recycle on the PoolConnection once done.
func (cp *ConnectionPool) TryGet() PoolConnection {
	p := cp.pool()
	if p == nil {
		panic(CONN_POOL_CLOSED_ERR)
	}
	r, err := p.TryGet()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	if r == nil {
		return nil
	}
	return r.(*pooledConnection)
}

func (cp *ConnectionPool) Put(conn PoolConnection) {
	p := cp.pool()
	if p == nil {
		panic(CONN_POOL_CLOSED_ERR)
	}
	p.Put(conn)
}

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

func (cp *ConnectionPool) SetIdleTimeout(idleTimeout time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.connections != nil {
		cp.connections.SetIdleTimeout(idleTimeout)
	}
	cp.idleTimeout = idleTimeout
}

func (cp *ConnectionPool) StatsJSON() string {
	p := cp.pool()
	if p == nil {
		return "{}"
	}
	return p.StatsJSON()
}

func (cp *ConnectionPool) Capacity() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Capacity()
}

func (cp *ConnectionPool) Available() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Available()
}

func (cp *ConnectionPool) MaxCap() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.MaxCap()
}

func (cp *ConnectionPool) WaitCount() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.WaitCount()
}

func (cp *ConnectionPool) WaitTime() time.Duration {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.WaitTime()
}

func (cp *ConnectionPool) IdleTimeout() time.Duration {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.IdleTimeout()
}

// pooledConnection re-exposes DBConnection as a PoolConnection
type pooledConnection struct {
	*DBConnection
	pool *ConnectionPool
}

func (pc *pooledConnection) Recycle() {
	if pc.IsClosed() {
		pc.pool.Put(nil)
	} else {
		pc.pool.Put(pc)
	}
}
