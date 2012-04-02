/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

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
