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
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/timer"
	"fmt"
	"sync"
	"time"
)

type ActivePool struct {
	sync.Mutex
	connections map[int64]*ActiveConnection
	timeout     time.Duration
	pool        *ConnectionPool
	ticks       *timer.Timer
}

type ActiveConnection struct {
	id        int64
	startTime time.Time
}

func NewActivePool(queryTimeout, idleTimeout time.Duration) *ActivePool {
	return &ActivePool{
		timeout: queryTimeout,
		pool:    NewConnectionPool(1, idleTimeout),
		ticks:   timer.NewTimer(idleTimeout / 10),
	}
}

func (self *ActivePool) Open(ConnFactory CreateConnectionFunc) {
	self.connections = make(map[int64]*ActiveConnection)
	self.pool.Open(ConnFactory)
	go self.QueryKiller()
}

func (self *ActivePool) Close() {
	self.ticks.Close()
	self.pool.Close()
	self.Lock()
	defer self.Unlock()
	self.connections = nil
}

func (self *ActivePool) QueryKiller() {
	for self.ticks.Next() {
		for {
			connid := self.ScanForTimeout()
			if connid == 0 {
				break
			}
			killStats.Add("Queries", 1)
			self.kill(connid)
		}
	}
}

func (self *ActivePool) kill(connid int64) {
	relog.Info("killing query %d", connid)
	killConn := self.pool.Get()
	defer killConn.Recycle()
	sql := []byte(fmt.Sprintf("kill %d", connid))
	if _, err := killConn.Smart().ExecuteFetch(sql, 10000); err != nil {
		relog.Error("Could not kill query %d: %v", connid, err)
	}
}

func (self *ActivePool) ScanForTimeout() (id int64) {
	self.Lock()
	defer self.Unlock()
	t := time.Now()
	for _, conn := range self.connections {
		if conn.startTime.Add(self.timeout).Sub(t) < 0 {
			delete(self.connections, conn.id)
			return conn.id
		}
	}
	return 0
}

func (self *ActivePool) Put(id int64) {
	self.Lock()
	defer self.Unlock()
	self.connections[id] = &ActiveConnection{id, time.Now()}
}

func (self *ActivePool) Remove(id int64) {
	self.Lock()
	defer self.Unlock()
	delete(self.connections, id)
}

func (self *ActivePool) SetTimeout(timeout time.Duration) {
	self.Lock()
	defer self.Unlock()
	self.timeout = timeout
	self.ticks.SetInterval(timeout / 10)
}

func (self *ActivePool) SetIdleTimeout(idleTimeout time.Duration) {
	self.pool.SetIdleTimeout(idleTimeout)
}

func (self *ActivePool) StatsJSON() string {
	t := self.Stats()
	return fmt.Sprintf("{\"Timeout\": %v}", float64(t)/1e9)
}

func (self *ActivePool) Stats() (timeout time.Duration) {
	self.Lock()
	defer self.Unlock()
	return self.timeout
}
