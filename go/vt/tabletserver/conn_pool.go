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
	"fmt"
	"sync/atomic"
	"time"
	"vitess/relog"
)

const MAX_POOL_CAP = 5000

type ConnectionPool struct {
	connections    chan *PooledConnection
	capacity, size int64 // Use sync/atomic for these
	connFactory    CreateConnectionFunc
	idleTimeout    int64 // Use sync/atomic
}

func NewConnectionPool(capacity int, idleTimeout time.Duration) *ConnectionPool {
	if capacity <= 0 || capacity > MAX_POOL_CAP {
		panic(NewTabletError(FAIL, "Capacity %v out of range", capacity))
	}
	return &ConnectionPool{capacity: int64(capacity), idleTimeout: int64(idleTimeout)}
}

func (self *ConnectionPool) Open(connFactory CreateConnectionFunc) {
	self.connections = make(chan *PooledConnection, MAX_POOL_CAP)
	self.size = 0
	self.connFactory = connFactory
}

// We trust the caller to not close this pool while it's busy
func (self *ConnectionPool) Close() {
	for atomic.LoadInt64(&self.size) > 0 {
		conn := <-self.connections
		conn.Close()
		atomic.AddInt64(&self.size, -1)
	}
	self.connections = nil
	self.connFactory = nil
}

func (self *ConnectionPool) SetCapacity(capacity int) {
	if capacity <= 0 || capacity > MAX_POOL_CAP {
		panic(NewTabletError(FAIL, "Capacity %v out of range", capacity))
	}
	atomic.StoreInt64(&self.capacity, int64(capacity))
}

func (self *ConnectionPool) Get() PoolConnection {
	conn := self.TryGet()
	if conn == nil {
		defer waitStats.Record("ConnPool", time.Now())
		conn = <-self.connections
	}
	return conn
}

func (self *ConnectionPool) TryGet() PoolConnection {
	idleTimeout := time.Duration(atomic.LoadInt64(&self.idleTimeout))
	if idleTimeout <= 0 {
		return self.tryget()
	}
	for {
		conn := self.tryget()
		if conn == nil {
			return nil
		}
		if conn.TimeUsed.Add(idleTimeout).Sub(time.Now()) < 0 {
			killStats.Add("Connections", 1)
			conn.Close()
			atomic.AddInt64(&self.size, -1)
			relog.Info("discarding idle connection")
			continue
		}
		return conn
	}
	panic("unreachable")
}

func (self *ConnectionPool) tryget() (conn *PooledConnection) {
	select {
	case conn = <-self.connections:
		return conn
	default:
		if atomic.LoadInt64(&self.size) < atomic.LoadInt64(&self.capacity) {
			// Prevent thundering herd: optimistically increment
			// size before opening connection
			atomic.AddInt64(&self.size, 1)
			var err error
			if conn, err = NewPooledConnection(self); err != nil {
				atomic.AddInt64(&self.size, -1)
				panic(NewTabletErrorSql(FATAL, err))
			}
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
	if atomic.LoadInt64(&self.size) > atomic.LoadInt64(&self.capacity) {
		conn.Close()
	}
	if conn.IsClosed {
		atomic.AddInt64(&self.size, -1)
	} else {
		self.connections <- conn
	}
}

func (self *ConnectionPool) SetIdleTimeout(idleTimeout time.Duration) {
	atomic.StoreInt64(&self.idleTimeout, int64(idleTimeout))
}

func (self *ConnectionPool) StatsJSON() string {
	s, c, a, i := self.Stats()
	return fmt.Sprintf("{\"Connections\": %v, \"Capacity\": %v, \"Available\": %v, \"IdleTimeout\": %v}", s, c, a, float64(i)/1e9)
}

func (self *ConnectionPool) Stats() (size, capacity, available int64, idleTimeout time.Duration) {
	return atomic.LoadInt64(&self.size), atomic.LoadInt64(&self.capacity), int64(len(self.connections)), time.Duration(atomic.LoadInt64(&self.idleTimeout))
}

type PooledConnection struct {
	*SmartConnection
	Pool     *ConnectionPool
	TimeUsed time.Time
}

func NewPooledConnection(pool *ConnectionPool) (*PooledConnection, error) {
	dbConn, err := pool.connFactory()
	if err != nil {
		return nil, err
	}
	p := &PooledConnection{
		SmartConnection: dbConn,
		Pool:            pool,
		TimeUsed:        time.Now(),
	}
	return p, nil
}

func (self *PooledConnection) Recycle() {
	self.TimeUsed = time.Now()
	self.Pool.put(self)
}

func (self *PooledConnection) Smart() *SmartConnection {
	return self.SmartConnection
}
