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
	"sync"
	"sync/atomic"
)

// This is a blend of ConnectionPool and TransactionPool
// We trust the caller to not close this pool while it's busy
type PersistentPool struct {
	sync.Mutex
	Connections map[int64]*PersistentConnection
	LastId      int64
	ConnFactory CreateConnectionFunc
}

func NewPersistentPool() *PersistentPool {
	return &PersistentPool{
		Connections: make(map[int64]*PersistentConnection),
		LastId:      1,
	}
}

func (self *PersistentPool) Open(ConnFactory CreateConnectionFunc) {
	self.ConnFactory = ConnFactory
	self.Connections = make(map[int64]*PersistentConnection)
}

func (self *PersistentPool) Close() {
	for _, conn := range self.Connections {
		conn.Close()
	}
	self.ConnFactory = nil
	self.Connections = nil
}

func (self *PersistentPool) CreateConnection() (connectionId int64) {
	conn, err := self.ConnFactory()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	self.Lock()
	defer self.Unlock()
	atomic.AddInt64(&self.LastId, 1)
	self.Connections[connectionId] = &PersistentConnection{SmartConnection: conn, ConnectionId: self.LastId, Pool: self}
	return connectionId
}

func (self *PersistentPool) CloseConnection(connectionId int64) {
	self.Lock()
	defer self.Unlock()
	conn, ok := self.Connections[connectionId]
	if !ok {
		return
	}
	if conn.InUse {
		panic(NewTabletError(FAIL, "Persistent connection %d is in use", connectionId))
	}
	conn.Close()
	delete(self.Connections, connectionId)
}

func (self *PersistentPool) Get(connectionId int64) PoolConnection {
	self.Lock()
	defer self.Unlock()
	conn, ok := self.Connections[connectionId]
	if !ok {
		panic(NewTabletError(FAIL, "Connection %d not found", connectionId))
	}
	if conn.InUse {
		panic(NewTabletError(FAIL, "Persistent connection %d is in use", connectionId))
	}
	conn.InUse = true
	return conn
}

// We trust the caller to always put() back what they Get() through conn.Recycle()
func (self *PersistentPool) put(connectionId int64) {
	self.Lock()
	defer self.Unlock()
	if conn, ok := self.Connections[connectionId]; ok {
		conn.InUse = false
	}
}

type PersistentConnection struct {
	*SmartConnection
	ConnectionId int64
	Pool         *PersistentPool
	InUse        bool
}

func (self *PersistentConnection) Recycle() {
	self.Pool.put(self.ConnectionId)
}

func (self *PersistentConnection) Smart() *SmartConnection {
	return self.SmartConnection
}
