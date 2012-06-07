// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"code.google.com/p/vitess/go/pools"
	"sync/atomic"
	"time"
)

type ReservedPool struct {
	pool        *pools.Numbered
	lastId      int64
	connFactory CreateConnectionFunc
}

func NewReservedPool() *ReservedPool {
	return &ReservedPool{pool: pools.NewNumbered(), lastId: 1}
}

func (self *ReservedPool) Open(connFactory CreateConnectionFunc) {
	self.connFactory = connFactory
}

func (self *ReservedPool) Close() {
	for _, v := range self.pool.GetTimedout(time.Duration(0)) {
		conn := v.(*reservedConnection)
		conn.Close()
		self.pool.Unregister(conn.connectionId)
	}
}

func (self *ReservedPool) CreateConnection() (connectionId int64) {
	conn, err := self.connFactory()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	connectionId = atomic.AddInt64(&self.lastId, 1)
	rconn := &reservedConnection{DBConnection: conn, connectionId: connectionId, pool: self}
	self.pool.Register(connectionId, rconn)
	return connectionId
}

func (self *ReservedPool) CloseConnection(connectionId int64) {
	conn := self.Get(connectionId).(*reservedConnection)
	conn.Close()
	self.pool.Unregister(connectionId)
}

// You must call Recycle on the PoolConnection once done.
func (self *ReservedPool) Get(connectionId int64) PoolConnection {
	v, err := self.pool.Get(connectionId)
	if err != nil {
		panic(NewTabletError(FAIL, "Error getting connection %d: %v", connectionId, err))
	}
	return v.(*reservedConnection)
}

func (self *ReservedPool) StatsJSON() string {
	return self.pool.StatsJSON()
}

func (self *ReservedPool) Stats() (size int) {
	return self.pool.Stats()
}

type reservedConnection struct {
	*DBConnection
	connectionId int64
	pool         *ReservedPool
	inUse        bool
}

func (self *reservedConnection) Recycle() {
	if self.IsClosed() {
		self.pool.pool.Unregister(self.connectionId)
	} else {
		self.pool.pool.Put(self.connectionId)
	}
}
