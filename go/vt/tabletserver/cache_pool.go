// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"code.google.com/p/vitess/go/memcache"
	"code.google.com/p/vitess/go/pools"
	"code.google.com/p/vitess/go/relog"
	"net/http"
	"time"
)

const statsURL = "/debug/memcache/"

type CreateCacheFunc func() (*memcache.Connection, error)

// CachePool re-exposes RoundRobin as a pool of Memcache connection objects
type CachePool struct {
	*pools.RoundRobin
	DeleteExpiry uint64
}

func NewCachePool(capacity int, queryTimeout time.Duration, idleTimeout time.Duration) *CachePool {
	seconds := uint64(queryTimeout / time.Second)
	// Add an additional 15 second grace period for
	// memcache expiry of deleted items
	if seconds != 0 {
		seconds += 15
	}
	return &CachePool{pools.NewRoundRobin(capacity, idleTimeout), seconds}
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
	http.Handle(statsURL, self)
}

// You must call Recycle on the *Cache once done.
func (self *CachePool) Get() *Cache {
	r, err := self.RoundRobin.Get()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	return r.(*Cache)
}

func (self *CachePool) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	defer func() {
		if x := recover(); x != nil {
			response.Write(([]byte)(x.(error).Error()))
		}
	}()
	response.Header().Set("Content-Type", "text/plain")
	command := request.URL.Path[len(statsURL):]
	if command == "stats" {
		command = ""
	}
	conn := self.Get()
	defer conn.Recycle()
	r, err := conn.Stats(command)
	if err != nil {
		response.Write(([]byte)(err.Error()))
	} else {
		response.Write(r)
	}
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
