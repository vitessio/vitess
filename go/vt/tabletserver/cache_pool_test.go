// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/tabletserver/fakecacheservice"
	"github.com/youtube/vitess/go/vt/tabletserver/fakesqldb"
	"golang.org/x/net/context"
)

func TestCachePoolWithEmptyBinary(t *testing.T) {
	fakecacheservice.Register()
	fakesqldb.Register()
	cachePool := newTestCachePool(RowCacheConfig{})
	cachePool.Close()
}

func TestCachePool(t *testing.T) {
	fakecacheservice.Register()
	fakesqldb.Register()
	rowCacheConfig := RowCacheConfig{
		Binary:      "ls",
		Connections: 100,
	}
	cachePool := newTestCachePool(rowCacheConfig)
	if !cachePool.IsClosed() {
		t.Fatalf("cache pool is not closed")
	}
	cachePool.Open()
	if cachePool.IsClosed() {
		t.Fatalf("cache pool is closed")
	}
	cachePool.Close()
	if !cachePool.IsClosed() {
		t.Fatalf("cache pool is not closed")
	}
}

func TestCachePoolOpenTwice(t *testing.T) {
	fakecacheservice.Register()
	fakesqldb.Register()
	rowCacheConfig := RowCacheConfig{
		Binary:      "ls",
		Connections: 100,
	}
	cachePool := newTestCachePool(rowCacheConfig)
	cachePool.Open()
	defer cachePool.Close()
	defer func() {
		if e := recover(); e == nil {
			t.Fatalf("open an opened cache pool should panic")
		}
	}()
	cachePool.Open()
}

func TestCachePoolOpenWithEmptyBinary(t *testing.T) {
	fakecacheservice.Register()
	fakesqldb.Register()
	rowCacheConfig := RowCacheConfig{
		Binary:      "ls",
		Connections: 100,
	}
	cachePool := newTestCachePool(rowCacheConfig)
	defer func() {
		if e := recover(); e == nil {
			t.Fatalf("open a cache pool with empty rowCacheConfig.Binary should panic")
		}
	}()
	cachePool.rowCacheConfig.Binary = ""
	cachePool.Open()
	cachePool.Close()
}

func TestCachePoolOpenWithInvalidBinary(t *testing.T) {
	fakecacheservice.Register()
	fakesqldb.Register()
	rowCacheConfig := RowCacheConfig{
		Binary:      "invalid_binary",
		Connections: 100,
	}
	cachePool := newTestCachePool(rowCacheConfig)
	defer func() {
		if e := recover(); e == nil {
			t.Fatalf("open a cache pool with an invalid rowCacheConfig.Binary should panic")
		}
	}()
	cachePool.Open()
	cachePool.Close()
}

func TestCachePoolState(t *testing.T) {
	fakecacheservice.Register()
	fakesqldb.Register()
	rowCacheConfig := RowCacheConfig{
		Binary:      "ls",
		Connections: 100,
	}
	cachePool := newTestCachePoolWithStats(rowCacheConfig)
	idleTimeout := 1 * time.Second
	cachePool.idleTimeout = idleTimeout
	cachePool.Open()
	cachePool.memcacheStats.update()
	defer cachePool.Close()
	if cachePool.Available() <= 0 {
		t.Fatalf("cache pool should have connections available")
	}
	if cachePool.Capacity() <= 0 {
		t.Fatalf("cache pool should have positive capacity")
	}
	if cachePool.MaxCap() <= 0 {
		t.Fatalf("cache pool should have positive max cap")
	}
	if cachePool.WaitCount() > 0 {
		t.Fatalf("cache pool has never waited for a connection, WaitCount should return 0")
	}
	if cachePool.WaitTime() > 0 {
		t.Fatalf("cache pool has never waited for a connection, WaitTime should return 0")
	}
	if cachePool.IdleTimeout() != idleTimeout {
		t.Fatalf("cache pool's idle timeout does not match the specified one")
	}
	if len(cachePool.StatsJSON()) <= 0 {
		t.Fatalf("cache pool stats json should return non empty result")
	}
}

func TestCachePoolStateWithoutOpen(t *testing.T) {
	fakecacheservice.Register()
	fakesqldb.Register()
	rowCacheConfig := RowCacheConfig{
		Binary:      "ls",
		Connections: 100,
	}
	cachePool := newTestCachePool(rowCacheConfig)
	idleTimeout := 1 * time.Second
	cachePool.idleTimeout = idleTimeout
	if cachePool.StatsJSON() != "{}" {
		t.Fatalf("cache pool StatsJSON() should return {}")
	}
	if cachePool.Capacity() != 0 {
		t.Fatalf("cache pool Capacity() should return 0")
	}
	if cachePool.Available() != 0 {
		t.Fatalf("cache pool Available() should return 0")
	}
	if cachePool.MaxCap() != 0 {
		t.Fatalf("cache pool MaxCap() should return 0")
	}
	if cachePool.WaitCount() != 0 {
		t.Fatalf("cache pool WaitCount() should return 0")
	}
	if cachePool.WaitTime() != 0 {
		t.Fatalf("cache pool WaitTime() should return 0")
	}
	if cachePool.IdleTimeout() != 0 {
		t.Fatalf("cache pool IdleTimeout() should return 0")
	}
	cachePool.Put(nil)
}

func TestCachePoolGetFailedBecauseCachePoolIsClosed(t *testing.T) {
	fakecacheservice.Register()
	fakesqldb.Register()
	rowCacheConfig := RowCacheConfig{
		Binary:      "ls",
		Connections: 100,
	}
	cachePool := newTestCachePool(rowCacheConfig)
	idleTimeout := 1 * time.Second
	cachePool.idleTimeout = idleTimeout
	ctx := context.Background()
	defer func() {
		if err := recover(); err == nil {
			t.Fatalf("Get should fail because cache pool is closed")
		}
	}()
	cachePool.Get(ctx)
}

func TestCachePoolStatsURL(t *testing.T) {
	fakecacheservice.Register()
	fakesqldb.Register()
	rowCacheConfig := RowCacheConfig{
		Binary:      "ls",
		Connections: 100,
	}
	cachePool := newTestCachePool(rowCacheConfig)
	idleTimeout := 1 * time.Second
	cachePool.idleTimeout = idleTimeout
	cachePool.Open()
	defer cachePool.Close()
	request, _ := http.NewRequest("GET", cachePool.statsURL, nil)
	response := httptest.NewRecorder()
	cachePool.ServeHTTP(response, request)
}

func newTestCachePoolWithStats(rowcacheConfig RowCacheConfig) *CachePool {
	randID := rand.Int63()
	name := fmt.Sprintf("TestCachePool-%d-", randID)
	statsURL := fmt.Sprintf("/debug/cache-%d", randID)
	return NewCachePool(name, rowcacheConfig, 1*time.Second, statsURL)
}

func newTestCachePool(rowcacheConfig RowCacheConfig) *CachePool {
	randID := rand.Int63()
	statsURL := fmt.Sprintf("/debug/cache-%d", randID)
	return NewCachePool("", rowcacheConfig, 1*time.Second, statsURL)
}
