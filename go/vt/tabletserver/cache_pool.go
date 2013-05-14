// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"net/http"
	"sync"
	"time"

	"code.google.com/p/vitess/go/memcache"
	"code.google.com/p/vitess/go/pools"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/dbconfigs"
)

const statsURL = "/debug/memcache/"

type CreateCacheFunc func() (*memcache.Connection, error)

// CachePool re-exposes ResourcePool as a pool of Memcache connection objects
type CachePool struct {
	pool         *pools.ResourcePool
	mu           sync.Mutex
	capacity     int
	idleTimeout  time.Duration
	DeleteExpiry uint64
}

func NewCachePool(capacity int, queryTimeout time.Duration, idleTimeout time.Duration) *CachePool {
	seconds := uint64(queryTimeout / time.Second)
	// Add an additional 15 second grace period for
	// memcache expiry of deleted items
	if seconds != 0 {
		seconds += 15
	}
	cachePool := &CachePool{capacity: capacity, idleTimeout: idleTimeout, DeleteExpiry: seconds}
	http.Handle(statsURL, cachePool)
	return cachePool
}

func (cp *CachePool) Open(connFactory CreateCacheFunc) {
	if connFactory == nil {
		return
	}
	f := func() (pools.Resource, error) {
		c, err := connFactory()
		if err != nil {
			return nil, err
		}
		return &Cache{c, cp}, nil
	}
	cp.pool = pools.NewResourcePool(f, cp.capacity, cp.capacity, cp.idleTimeout)
}

func (cp *CachePool) Close() {
	cp.pool.Close()
	cp.pool = nil
}

func (cp *CachePool) IsClosed() bool {
	return cp.pool == nil
}

// You must call Recycle on the *Cache once done.
func (cp *CachePool) Get() *Cache {
	r, err := cp.pool.Get()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	return r.(*Cache)
}

func (cp *CachePool) Put(conn *Cache) {
	cp.pool.Put(conn)
}

func (cp *CachePool) SetCapacity(capacity int) (err error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	err = cp.pool.SetCapacity(capacity)
	if err != nil {
		return err
	}
	cp.capacity = capacity
	return nil
}

func (cp *CachePool) SetIdleTimeout(idleTimeout time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.pool.SetIdleTimeout(idleTimeout)
	cp.idleTimeout = idleTimeout
}

func (cp *CachePool) StatsJSON() string {
	if cp.pool == nil {
		return "{}"
	}
	return cp.pool.StatsJSON()
}

func (cp *CachePool) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	defer func() {
		if x := recover(); x != nil {
			response.Write(([]byte)(x.(error).Error()))
		}
	}()
	response.Header().Set("Content-Type", "text/plain")
	if cp.pool == nil {
		response.Write(([]byte)("closed"))
		return
	}
	command := request.URL.Path[len(statsURL):]
	if command == "stats" {
		command = ""
	}
	conn := cp.Get()
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

func (cache *Cache) Recycle() {
	if cache.IsClosed() {
		cache.pool.Put(nil)
	} else {
		cache.pool.Put(cache)
	}
}

func CacheCreator(dbconfig dbconfigs.DBConfig) CreateCacheFunc {
	if dbconfig.Memcache == "" {
		relog.Info("Row cache not enabled")
		return nil
	}
	relog.Info("Row cache is enabled")
	return func() (*memcache.Connection, error) {
		return memcache.Connect(dbconfig.Memcache)
	}
}
