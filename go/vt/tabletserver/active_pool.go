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
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/timer"
	"fmt"
	"sync/atomic"
	"time"
)

type ActivePool struct {
	pool     *pools.Numbered
	timeout  int64
	connPool *ConnectionPool
	ticks    *timer.Timer
}

func NewActivePool(queryTimeout, idleTimeout time.Duration) *ActivePool {
	return &ActivePool{
		pool:     pools.NewNumbered(),
		timeout:  int64(queryTimeout),
		connPool: NewConnectionPool(1, idleTimeout),
		ticks:    timer.NewTimer(idleTimeout / 10),
	}
}

func (self *ActivePool) Open(ConnFactory CreateConnectionFunc) {
	self.connPool.Open(ConnFactory)
	go self.QueryKiller()
}

func (self *ActivePool) Close() {
	self.ticks.Close()
	self.connPool.Close()
	self.pool = pools.NewNumbered()
}

func (self *ActivePool) QueryKiller() {
	for self.ticks.Next() {
		for _, v := range self.pool.GetTimedout(time.Duration(self.Timeout())) {
			self.kill(v.(int64))
		}
	}
}

func (self *ActivePool) kill(connid int64) {
	self.Remove(connid)
	killStats.Add("Queries", 1)
	relog.Info("killing query %d", connid)
	killConn := self.connPool.Get()
	defer killConn.Recycle()
	sql := []byte(fmt.Sprintf("kill %d", connid))
	if _, err := killConn.ExecuteFetch(sql, 10000, false); err != nil {
		relog.Error("Could not kill query %d: %v", connid, err)
	}
}

func (self *ActivePool) Put(id int64) {
	self.pool.Register(id, id)
}

func (self *ActivePool) Remove(id int64) {
	self.pool.Unregister(id)
}

func (self *ActivePool) Timeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&self.timeout))
}

func (self *ActivePool) SetTimeout(timeout time.Duration) {
	atomic.StoreInt64(&self.timeout, int64(timeout))
	self.ticks.SetInterval(timeout / 10)
}

func (self *ActivePool) SetIdleTimeout(idleTimeout time.Duration) {
	self.connPool.SetIdleTimeout(idleTimeout)
}

func (self *ActivePool) StatsJSON() string {
	s, t := self.Stats()
	return fmt.Sprintf("{\"Size\": %v, \"Timeout\": %v}", s, float64(t)/1e9)
}

func (self *ActivePool) Stats() (size int, timeout time.Duration) {
	return self.pool.Stats(), self.Timeout()
}
