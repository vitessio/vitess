// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"code.google.com/p/vitess/go/cache"
	"code.google.com/p/vitess/go/mysql/proto"
)

var (
	waitError = NewTabletError(FAIL, "Error waiting for consolidation")
)

type Consolidator struct {
	mu             sync.Mutex
	queries        map[string]*Result
	consolidations *cache.LRUCache
}

func NewConsolidator() *Consolidator {
	co := &Consolidator{queries: make(map[string]*Result), consolidations: cache.NewLRUCache(1000)}
	http.Handle("/debug/consolidations", co)
	return co
}

type Result struct {
	executing    sync.RWMutex
	consolidator *Consolidator
	sql          string
	Result       *proto.QueryResult
	Err          error
}

func (co *Consolidator) Create(sql string) (r *Result, created bool) {
	co.mu.Lock()
	defer co.mu.Unlock()
	if r, ok := co.queries[sql]; ok {
		return r, false
	}
	// Preset the error. If there was an unexpected panic during the main
	// query, then all those who waited will return the waitError.
	r = &Result{consolidator: co, sql: sql, Err: waitError}
	r.executing.Lock()
	co.queries[sql] = r
	return r, true
}

func (co *Consolidator) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	items := co.consolidations.Items()
	response.Header().Set("Content-Type", "text/plain")
	if items == nil {
		response.Write([]byte("empty\n"))
		return
	}
	response.Write([]byte(fmt.Sprintf("Length: %d\n", len(items))))
	for _, v := range items {
		response.Write([]byte(fmt.Sprintf("%v: %s\n", v.Value.(*ccount).Get(), v.Key)))
	}
}

func (co *Consolidator) record(sql string) {
	if v, ok := co.consolidations.Get(sql); ok {
		v.(*ccount).Add(1)
	} else {
		c := ccount(1)
		co.consolidations.Set(sql, &c)
	}
}

func (rs *Result) Broadcast() {
	rs.consolidator.mu.Lock()
	defer rs.consolidator.mu.Unlock()
	delete(rs.consolidator.queries, rs.sql)
	rs.executing.Unlock()
}

func (rs *Result) Wait() {
	rs.consolidator.record(rs.sql)
	defer waitStats.Record("Consolidations", time.Now())
	rs.executing.RLock()
}

type ccount int64

func (cc *ccount) Size() int {
	return 1
}

func (cc *ccount) Add(n int64) int64 {
	return atomic.AddInt64((*int64)(cc), n)
}

func (cc *ccount) Set(n int64) {
	atomic.StoreInt64((*int64)(cc), n)
}

func (cc *ccount) Get() int64 {
	return atomic.LoadInt64((*int64)(cc))
}
