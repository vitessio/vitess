// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"net/http"
	"os/exec"
	"strconv"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/memcache"
	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
)

const statsURL = "/debug/memcache/"

type CreateCacheFunc func() (*memcache.Connection, error)

// CachePool re-exposes ResourcePool as a pool of Memcache connection objects.
type CachePool struct {
	name          string
	pool          *pools.ResourcePool
	maxPrefix     sync2.AtomicInt64
	cmd           *exec.Cmd
	commandLine   []string
	capacity      int
	port          string
	idleTimeout   time.Duration
	DeleteExpiry  uint64
	memcacheStats *MemcacheStats
}

// Cache re-exposes memcache.Connection
// that can be recycled.
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

func NewCachePool(name string, commandLine []string, queryTimeout time.Duration, idleTimeout time.Duration) *CachePool {
	cp := &CachePool{name: name, idleTimeout: idleTimeout}
	if name != "" {
		stats.Publish(name+"Capacity", stats.IntFunc(cp.Capacity))
		stats.Publish(name+"Available", stats.IntFunc(cp.Available))
		stats.Publish(name+"MaxCap", stats.IntFunc(cp.MaxCap))
		stats.Publish(name+"WaitCount", stats.IntFunc(cp.WaitCount))
		stats.Publish(name+"WaitTime", stats.DurationFunc(cp.WaitTime))
		stats.Publish(name+"IdleTimeout", stats.DurationFunc(cp.IdleTimeout))
	}
	http.Handle(statsURL, cp)

	if len(commandLine) == 0 {
		return cp
	}
	cp.commandLine = commandLine

	// Start with memcached defaults
	cp.capacity = 1024 - 50
	cp.port = "11211"
	for i := 0; i < len(commandLine); i++ {
		switch commandLine[i] {
		case "-p", "-s":
			i++
			if i == len(commandLine) {
				log.Fatalf("expecting value after -p")
			}
			cp.port = commandLine[i]
		case "-c":
			i++
			if i == len(commandLine) {
				log.Fatalf("expecting value after -c")
			}
			capacity, err := strconv.Atoi(commandLine[i])
			if err != nil {
				log.Fatalf("%V", err)
			}
			if capacity <= 50 {
				log.Fatalf("insufficient capacity: %d", capacity)
			}
			cp.capacity = capacity - 50
		}
	}

	seconds := uint64(queryTimeout / time.Second)
	// Add an additional grace period for
	// memcache expiry of deleted items
	if seconds != 0 {
		cp.DeleteExpiry = 2*seconds + 15
	}
	return cp
}

func (cp *CachePool) Open() {
	if len(cp.commandLine) == 0 {
		log.Infof("rowcache not enabled")
		return
	}
	cp.startMemcache()
	log.Infof("rowcache is enabled")
	f := func() (pools.Resource, error) {
		c, err := memcache.Connect(cp.port)
		if err != nil {
			return nil, err
		}
		return &Cache{c, cp}, nil
	}
	cp.pool = pools.NewResourcePool(f, cp.capacity, cp.capacity, cp.idleTimeout)
	if cp.memcacheStats == nil {
		cp.memcacheStats = NewMemcacheStats(cp)
	} else {
		cp.memcacheStats.Start()
	}
}

func (cp *CachePool) startMemcache() {
	cp.cmd = exec.Command(cp.commandLine[0], cp.commandLine[1:]...)
	if err := cp.cmd.Start(); err != nil {
		panic(NewTabletError(FATAL, "can't start memcache: %v", err))
	}
	attempts := 0
	for {
		time.Sleep(50 * time.Millisecond)
		c, err := memcache.Connect(cp.port)
		if err != nil {
			attempts++
			if attempts >= 30 {
				cp.cmd.Process.Kill()
				// FIXME(sougou): Throw proper error if we can recover
				log.Fatal("Can't connect to memcache")
			}
			continue
		}
		if _, err = c.Set("health", 0, 0, []byte("ok")); err != nil {
			panic(NewTabletError(FATAL, "can't communicate with memcache: %v", err))
		}
		c.Close()
		break
	}
}

func (cp *CachePool) Close() {
	if cp.pool == nil {
		return
	}
	if cp.memcacheStats != nil {
		cp.memcacheStats.Stop()
	}
	cp.pool.Close()
	cp.cmd.Process.Kill()
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

func (cp *CachePool) StatsJSON() string {
	if cp.pool == nil {
		return "{}"
	}
	return cp.pool.StatsJSON()
}

func (cp *CachePool) Capacity() int64 {
	if cp.pool == nil {
		return 0
	}
	return cp.pool.Capacity()
}

func (cp *CachePool) Available() int64 {
	if cp.pool == nil {
		return 0
	}
	return cp.pool.Available()
}

func (cp *CachePool) MaxCap() int64 {
	if cp.pool == nil {
		return 0
	}
	return cp.pool.MaxCap()
}

func (cp *CachePool) WaitCount() int64 {
	if cp.pool == nil {
		return 0
	}
	return cp.pool.WaitCount()
}

func (cp *CachePool) WaitTime() time.Duration {
	if cp.pool == nil {
		return 0
	}
	return cp.pool.WaitTime()
}

func (cp *CachePool) IdleTimeout() time.Duration {
	if cp.pool == nil {
		return 0
	}
	return cp.pool.IdleTimeout()
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
