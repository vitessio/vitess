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
	"vitess/relog"
	"sync/atomic"
	"time"
)

const MAX_POOL_SIZE = 5000

type ConnectionPool struct {
	Connections chan *PooledConnection
	Size, Count int64
	ConnFactory CreateConnectionFunc
	IdleTimeout time.Duration
}

func NewConnectionPool(size int, idleTimeout time.Duration) *ConnectionPool {
	if size <= 0 || size > MAX_POOL_SIZE {
		panic(NewTabletError(FAIL, "Size %v out of range", size))
	}
	return &ConnectionPool{Size: int64(size), IdleTimeout: idleTimeout}
}

func (self *ConnectionPool) Open(ConnFactory CreateConnectionFunc) {
	self.Connections = make(chan *PooledConnection, MAX_POOL_SIZE)
	self.Count = 0
	self.ConnFactory = ConnFactory
}

// We trust the caller to not close this pool while it's busy
func (self *ConnectionPool) Close() {
	for self.Count != 0 {
		conn := <-self.Connections
		conn.Close()
		atomic.AddInt64(&self.Count, -1)
	}
	self.Connections = nil
	self.ConnFactory = nil
}

func (self *ConnectionPool) connectAll() {
	for self.Count < self.Size {
		self.Connections <- NewPooledConnection(self)
		atomic.AddInt64(&self.Count, 1)
	}
}

func (self *ConnectionPool) Resize(size int) {
	if size <= 0 || size > MAX_POOL_SIZE {
		panic(NewTabletError(FAIL, "Size %v out of range", size))
	}
	self.Size = int64(size)
}

func (self *ConnectionPool) Get() PoolConnection {
	conn := self.TryGet()
	if conn == nil {
		defer waitStats.Record("ConnPool", time.Now())
		conn = <-self.Connections
	}
	return conn
}

func (self *ConnectionPool) TryGet() PoolConnection {
	if self.IdleTimeout <= 0 {
		return self.tryget()
	}
	for {
		conn := self.tryget()
		if conn == nil {
			return nil
		}
		if conn.TimeUsed.Add(self.IdleTimeout).Sub(time.Now()) < 0 {
			killStats.Add("Connections", 1)
			conn.Close()
			atomic.AddInt64(&self.Count, -1)
			relog.Info("discarding idle connection")
			continue
		}
		return conn
	}
	panic("unreachable")
}

func (self *ConnectionPool) tryget() (conn *PooledConnection) {
	select {
	case conn = <-self.Connections:
		return conn
	default:
		if self.Count < self.Size {
			conn = NewPooledConnection(self)
			atomic.AddInt64(&self.Count, 1)
			return conn
		}
	}
	// All connections in use
	return nil
}

// We trust the caller to always put() back what they Get() through conn.Recycle()
func (self *ConnectionPool) put(conn *PooledConnection) {
	if conn.Pool != self {
		panic(NewTabletError(FATAL, "Connection doesn't belong to pool"))
	}
	if self.Count > self.Size {
		conn.Close()
	}
	if conn.IsClosed {
		atomic.AddInt64(&self.Count, -1)
	} else {
		self.Connections <- conn
	}
}

func (self *ConnectionPool) SetIdleTimeout(idleTimeout time.Duration) {
	self.IdleTimeout = idleTimeout
}

type PooledConnection struct {
	*SmartConnection
	Pool     *ConnectionPool
	TimeUsed time.Time
}

func NewPooledConnection(pool *ConnectionPool) *PooledConnection {
	dbConn, err := pool.ConnFactory()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	return &PooledConnection{
		SmartConnection: dbConn,
		Pool:            pool,
		TimeUsed:        time.Now(),
	}
}

func (self *PooledConnection) Recycle() {
	self.TimeUsed = time.Now()
	self.Pool.put(self)
}

func (self *PooledConnection) Smart() *SmartConnection {
	return self.SmartConnection
}
