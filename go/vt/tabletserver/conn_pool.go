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

// ConnectionPool re-exposes ResourcePool as a pool of DBConnection objects
type ConnectionPool struct {
	pool        *pools.ResourcePool
	mu          sync.Mutex
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

func (cp *ConnectionPool) Open(connFactory CreateConnectionFunc) {
	f := func() (pools.Resource, error) {
		c, err := connFactory()
		if err != nil {
			return nil, err
		}
		return &pooledConnection{c, cp}, nil
	}
	cp.pool = pools.NewResourcePool(f, cp.capacity, cp.capacity, cp.idleTimeout)
}

func (cp *ConnectionPool) Close() {
	cp.pool.Close()
	cp.pool = nil
}

// You must call Recycle on the PoolConnection once done.
func (cp *ConnectionPool) Get() PoolConnection {
	r, err := cp.pool.Get()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	return r.(*pooledConnection)
}

// You must call Recycle on the PoolConnection once done.
func (cp *ConnectionPool) SafeGet() (PoolConnection, error) {
	r, err := cp.pool.Get()
	if err != nil {
		return nil, err
	}
	return r.(*pooledConnection), nil
}

// You must call Recycle on the PoolConnection once done.
func (cp *ConnectionPool) TryGet() PoolConnection {
	r, err := cp.pool.TryGet()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	if r == nil {
		return nil
	}
	return r.(*pooledConnection)
}

func (cp *ConnectionPool) Put(conn PoolConnection) {
	cp.pool.Put(conn)
}

func (cp *ConnectionPool) SetCapacity(capacity int) (err error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	err = cp.pool.SetCapacity(capacity)
	if err != nil {
		return err
	}
	cp.capacity = capacity
	return nil
}

func (cp *ConnectionPool) SetIdleTimeout(idleTimeout time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.pool.SetIdleTimeout(idleTimeout)
	cp.idleTimeout = idleTimeout
}

func (cp *ConnectionPool) StatsJSON() string {
	if cp.pool == nil {
		return "{}"
	}
	return cp.pool.StatsJSON()
}

func (cp *ConnectionPool) Capacity() int64 {
	if cp.pool == nil {
		return 0
	}
	return cp.pool.Capacity()
}

func (cp *ConnectionPool) Available() int64 {
	if cp.pool == nil {
		return 0
	}
	return cp.pool.Available()
}

func (cp *ConnectionPool) MaxCap() int64 {
	if cp.pool == nil {
		return 0
	}
	return cp.pool.MaxCap()
}

func (cp *ConnectionPool) WaitCount() int64 {
	if cp.pool == nil {
		return 0
	}
	return cp.pool.WaitCount()
}

func (cp *ConnectionPool) WaitTime() time.Duration {
	if cp.pool == nil {
		return 0
	}
	return cp.pool.WaitTime()
}

func (cp *ConnectionPool) IdleTimeout() time.Duration {
	if cp.pool == nil {
		return 0
	}
	return cp.pool.IdleTimeout()
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
