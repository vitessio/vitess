// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"code.google.com/p/vitess/go/pools"
	"time"
)

// ConnectionPool re-exposes RoundRobin as a pool of DBConnection objects
type ConnectionPool struct {
	*pools.RoundRobin
}

func NewConnectionPool(capacity int, idleTimeout time.Duration) *ConnectionPool {
	return &ConnectionPool{pools.NewRoundRobin(capacity, idleTimeout)}
}

func (self *ConnectionPool) Open(connFactory CreateConnectionFunc) {
	f := func() (pools.Resource, error) {
		c, err := connFactory()
		if err != nil {
			return nil, err
		}
		return &pooledConnection{c, self}, nil
	}
	self.RoundRobin.Open(f)
}

// You must call Recycle on the PoolConnection once done.
func (self *ConnectionPool) Get() PoolConnection {
	r, err := self.RoundRobin.Get()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	return r.(*pooledConnection)
}

// You must call Recycle on the PoolConnection once done.
func (self *ConnectionPool) SafeGet() (PoolConnection, error) {
	r, err := self.RoundRobin.Get()
	if err != nil {
		return nil, err
	}
	return r.(*pooledConnection), nil
}

// You must call Recycle on the PoolConnection once done.
func (self *ConnectionPool) TryGet() PoolConnection {
	r, err := self.RoundRobin.TryGet()
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

func (self *pooledConnection) Recycle() {
	self.pool.Put(self)
}
