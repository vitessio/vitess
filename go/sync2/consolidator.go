// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sync2

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/cache"
)

// Consolidator consolidates duplicate queries from executing simulaneously
// and shares results between them.
type Consolidator struct {
	mu             sync.Mutex
	queries        map[string]*Result
	consolidations *cache.LRUCache
}

// NewConsolidator creates a new Consolidator
func NewConsolidator() *Consolidator {
	return &Consolidator{queries: make(map[string]*Result), consolidations: cache.NewLRUCache(1000)}
}

// Result is a wrapper for result of a query.
type Result struct {
	// executing is used to block additional requests.
	// The original request holds a write lock while additional ones are blocked
	// on acquiring a read lock (see Wait() below.)
	executing    sync.RWMutex
	consolidator *Consolidator
	query        string
	Result       interface{}
	Err          error
}

// Create adds a query to currently executing queries and acquires a
// lock on its Result if it is not already present. If the query is
// a duplicate, Create returns false.
func (co *Consolidator) Create(query string) (r *Result, created bool) {
	co.mu.Lock()
	defer co.mu.Unlock()
	if r, ok := co.queries[query]; ok {
		return r, false
	}
	r = &Result{consolidator: co, query: query}
	r.executing.Lock()
	co.queries[query] = r
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
		response.Write([]byte(fmt.Sprintf("%v: %s\n", v.Value.(*ccount).get(), v.Key)))
	}
}

func (co *Consolidator) record(query string) {
	if v, ok := co.consolidations.Get(query); ok {
		v.(*ccount).add(1)
	} else {
		c := ccount(1)
		co.consolidations.Set(query, &c)
	}
}

// Broadcast removes the entry from current queries and releases the
// lock on its Result. Broadcast should be invoked when original
// query completes execution.
func (rs *Result) Broadcast() {
	rs.consolidator.mu.Lock()
	defer rs.consolidator.mu.Unlock()
	delete(rs.consolidator.queries, rs.query)
	rs.executing.Unlock()
}

// Wait waits for the original query to complete execution. Wait should
// be invoked for duplicate queries.
func (rs *Result) Wait() {
	rs.consolidator.record(rs.query)
	rs.executing.RLock()
}

// ccount elements are used with a cache.LRUCache object to track if another
// request for the same query is already in progress.
type ccount int64

// Size always returns 1 because we use the cache only to track queries,
// independent of the number of requests waiting for them.
// This implements the cache.Value interface.
func (cc *ccount) Size() int {
	return 1
}

func (cc *ccount) add(n int64) int64 {
	return atomic.AddInt64((*int64)(cc), n)
}

func (cc *ccount) get() int64 {
	return atomic.LoadInt64((*int64)(cc))
}
