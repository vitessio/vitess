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
	"code.google.com/p/vitess/go/memcache"
	"code.google.com/p/vitess/go/pools"
	"code.google.com/p/vitess/go/relog"
	"time"
)

type CreateCacheFunc func() (*memcache.Connection, error)

// CachePool re-exposes RoundRobin as a pool of Memcache connection objects
type CachePool struct {
	*pools.RoundRobin
}

func NewCachePool(capacity int, idleTimeout time.Duration) *CachePool {
	return &CachePool{pools.NewRoundRobin(capacity, idleTimeout)}
}

func (self *CachePool) Open(connFactory CreateCacheFunc) {
	if connFactory == nil {
		return
	}
	f := func() (pools.Resource, error) {
		c, err := connFactory()
		if err != nil {
			return nil, err
		}
		return &Cache{c, self}, nil
	}
	self.RoundRobin.Open(f)
}

// You must call Recycle on the *Cache once done.
func (self *CachePool) Get() *Cache {
	r, err := self.RoundRobin.Get()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	return r.(*Cache)
}

// Cache re-exposes memcache.Connection
type Cache struct {
	*memcache.Connection
	pool *CachePool
}

func (self *Cache) Recycle() {
	self.pool.Put(self)
}

func CacheCreator(dbconfig map[string]interface{}) CreateCacheFunc {
	iaddress, ok := dbconfig["memcache"]
	if !ok {
		return nil
	}
	relog.Info("Row cache is enabled")
	address := iaddress.(string)
	return func() (*memcache.Connection, error) {
		return memcache.Connect(address)
	}
}
