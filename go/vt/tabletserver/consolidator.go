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

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/cache"
)

var (
	waitError = NewTabletError(ErrFail, "Error waiting for consolidation")
)

// Consolidator consolidates duplicate queries from executing simulaneously
// and shares results between them.
type Consolidator struct {
	mu             sync.Mutex
	queries        map[string]*Result
	consolidations *cache.LRUCache
}

// NewConsolidator creates a new Consolidator
func NewConsolidator(httpHandle bool) *Consolidator {
	co := &Consolidator{queries: make(map[string]*Result), consolidations: cache.NewLRUCache(1000)}
	if httpHandle {
		http.Handle("/debug/consolidations", co)
	}
	return co
}

// Result is a wrapper for result of a query.
type Result struct {
	executing    sync.RWMutex
	consolidator *Consolidator
	sql          string
	Result       interface{}
	Err          error
}

// Create adds a query to currently executing queries and acquires a
// lock on its Result if it is not already present. If the query is
// a duplicate, Create returns false.
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
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
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

// Broadcast removes the entry from current queries and releases the
// lock on its Result. Broadcast should be invoked when original
// query completes execution.
func (rs *Result) Broadcast() {
	rs.consolidator.mu.Lock()
	defer rs.consolidator.mu.Unlock()
	delete(rs.consolidator.queries, rs.sql)
	rs.executing.Unlock()
}

// Wait waits for the original query to complete execution. Wait should
// be invoked for duplicate queries.
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
