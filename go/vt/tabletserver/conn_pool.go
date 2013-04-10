// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"time"

	"code.google.com/p/vitess/go/pools"
)

// ConnectionPool re-exposes RoundRobin as a pool of DBConnection objects
type ConnectionPool struct {
	*pools.RoundRobin
}

func NewConnectionPool(capacity int, idleTimeout time.Duration) *ConnectionPool {
	return &ConnectionPool{pools.NewRoundRobin(capacity, idleTimeout)}
}

func (cp *ConnectionPool) Open(connFactory CreateConnectionFunc) {
	f := func() (pools.Resource, error) {
		c, err := connFactory()
		if err != nil {
			return nil, err
		}
		return &pooledConnection{c, cp}, nil
	}
	cp.RoundRobin.Open(f)
}

// You must call Recycle on the PoolConnection once done.
func (cp *ConnectionPool) Get() PoolConnection {
	r, err := cp.RoundRobin.Get()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	return r.(*pooledConnection)
}

// You must call Recycle on the PoolConnection once done.
func (cp *ConnectionPool) SafeGet() (PoolConnection, error) {
	r, err := cp.RoundRobin.Get()
	if err != nil {
		return nil, err
	}
	return r.(*pooledConnection), nil
}

// You must call Recycle on the PoolConnection once done.
func (cp *ConnectionPool) TryGet() PoolConnection {
	r, err := cp.RoundRobin.TryGet()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	if r == nil {
		return nil
	}
	return r.(*pooledConnection)
}

// pooledConnection re-exposes DBConnection as a PoolConnection
type pooledConnection struct {
	*DBConnection
	pool *ConnectionPool
}

func (pc *pooledConnection) Recycle() {
	pc.pool.Put(pc)
}
