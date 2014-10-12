// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/memcache"
	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
)

const statsURL = "/debug/memcache/"

type CreateCacheFunc func() (*memcache.Connection, error)

// CachePool re-exposes ResourcePool as a pool of Memcache connection objects.
type CachePool struct {
	name           string
	pool           *pools.ResourcePool
	maxPrefix      sync2.AtomicInt64
	cmd            *exec.Cmd
	rowCacheConfig RowCacheConfig
	capacity       int
	port           string
	idleTimeout    time.Duration
	DeleteExpiry   uint64
	memcacheStats  *MemcacheStats
	mu             sync.Mutex
}

func NewCachePool(name string, rowCacheConfig RowCacheConfig, queryTimeout time.Duration, idleTimeout time.Duration) *CachePool {
	cp := &CachePool{name: name, idleTimeout: idleTimeout}
	if name != "" {
		cp.memcacheStats = NewMemcacheStats(cp, true, false, false)
		stats.Publish(name+"ConnPoolCapacity", stats.IntFunc(cp.Capacity))
		stats.Publish(name+"ConnPoolAvailable", stats.IntFunc(cp.Available))
		stats.Publish(name+"ConnPoolMaxCap", stats.IntFunc(cp.MaxCap))
		stats.Publish(name+"ConnPoolWaitCount", stats.IntFunc(cp.WaitCount))
		stats.Publish(name+"ConnPoolWaitTime", stats.DurationFunc(cp.WaitTime))
		stats.Publish(name+"ConnPoolIdleTimeout", stats.DurationFunc(cp.IdleTimeout))
	}
	http.Handle(statsURL, cp)

	if rowCacheConfig.Binary == "" {
		return cp
	}
	cp.rowCacheConfig = rowCacheConfig

	// Start with memcached defaults
	cp.capacity = 1024 - 50
	cp.port = "11211"
	if rowCacheConfig.Socket != "" {
		cp.port = rowCacheConfig.Socket
	}
	if rowCacheConfig.TcpPort > 0 {
		cp.port = strconv.Itoa(rowCacheConfig.TcpPort)
	}
	if rowCacheConfig.Connections > 0 {
		if rowCacheConfig.Connections <= 50 {
			log.Fatalf("insufficient capacity: %d", rowCacheConfig.Connections)
		}
		cp.capacity = rowCacheConfig.Connections - 50
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
	if cp.rowCacheConfig.Binary == "" {
		panic(NewTabletError(FATAL, "rowcache binary not specified"))
	}
	cp.startMemcache()
	log.Infof("rowcache is enabled")
	f := func() (pools.Resource, error) {
		return memcache.Connect(cp.port, 10*time.Second)
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.pool = pools.NewResourcePool(f, cp.capacity, cp.capacity, cp.idleTimeout)
	if cp.memcacheStats != nil {
		cp.memcacheStats.Open()
	}
}

func (cp *CachePool) startMemcache() {
	if strings.Contains(cp.port, "/") {
		_ = os.Remove(cp.port)
	}
	commandLine := cp.rowCacheConfig.GetSubprocessFlags()
	cp.cmd = exec.Command(commandLine[0], commandLine[1:]...)
	if err := cp.cmd.Start(); err != nil {
		panic(NewTabletError(FATAL, "can't start memcache: %v", err))
	}
	attempts := 0
	for {
		time.Sleep(100 * time.Millisecond)
		c, err := memcache.Connect(cp.port, 30*time.Millisecond)
		if err != nil {
			attempts++
			if attempts >= 50 {
				cp.cmd.Process.Kill()
				// Avoid zombies
				go cp.cmd.Wait()
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
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.pool == nil {
		return
	}
	if cp.memcacheStats != nil {
		cp.memcacheStats.Close()
	}
	cp.pool.Close()
	cp.cmd.Process.Kill()
	// Avoid zombies
	go cp.cmd.Wait()
	if strings.Contains(cp.port, "/") {
		_ = os.Remove(cp.port)
	}
	cp.pool = nil
}

func (cp *CachePool) IsClosed() bool {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.pool == nil
}

func (cp *CachePool) getPool() *pools.ResourcePool {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.pool
}

// You must call Put after Get.
func (cp *CachePool) Get() *memcache.Connection {
	pool := cp.getPool()
	if pool == nil {
		panic(NewTabletError(FATAL, "cache pool is not open"))
	}
	r, err := pool.Get()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	return r.(*memcache.Connection)
}

func (cp *CachePool) Put(conn *memcache.Connection) {
	pool := cp.getPool()
	if pool == nil {
		return
	}
	if conn == nil {
		pool.Put(nil)
	} else {
		pool.Put(conn)
	}
}

func (cp *CachePool) StatsJSON() string {
	pool := cp.getPool()
	if pool == nil {
		return "{}"
	}
	return pool.StatsJSON()
}

func (cp *CachePool) Capacity() int64 {
	pool := cp.getPool()
	if pool == nil {
		return 0
	}
	return pool.Capacity()
}

func (cp *CachePool) Available() int64 {
	pool := cp.getPool()
	if pool == nil {
		return 0
	}
	return pool.Available()
}

func (cp *CachePool) MaxCap() int64 {
	pool := cp.getPool()
	if pool == nil {
		return 0
	}
	return pool.MaxCap()
}

func (cp *CachePool) WaitCount() int64 {
	pool := cp.getPool()
	if pool == nil {
		return 0
	}
	return pool.WaitCount()
}

func (cp *CachePool) WaitTime() time.Duration {
	pool := cp.getPool()
	if pool == nil {
		return 0
	}
	return pool.WaitTime()
}

func (cp *CachePool) IdleTimeout() time.Duration {
	pool := cp.getPool()
	if pool == nil {
		return 0
	}
	return pool.IdleTimeout()
}

func (cp *CachePool) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.MONITORING); err != nil {
		acl.SendError(response, err)
		return
	}
	defer func() {
		if x := recover(); x != nil {
			response.Write(([]byte)(x.(error).Error()))
		}
	}()
	response.Header().Set("Content-Type", "text/plain")
	pool := cp.getPool()
	if pool == nil {
		response.Write(([]byte)("closed"))
		return
	}
	command := request.URL.Path[len(statsURL):]
	if command == "stats" {
		command = ""
	}
	conn := cp.Get()
	// This is not the same as defer rc.cachePool.Put(conn)
	defer func() { cp.Put(conn) }()
	r, err := conn.Stats(command)
	if err != nil {
		conn.Close()
		conn = nil
		response.Write(([]byte)(err.Error()))
	} else {
		response.Write(r)
	}
}
