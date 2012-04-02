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
