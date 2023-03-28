/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sync2

import (
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/sqltypes"
)

// Consolidator consolidates duplicate queries from executing simulaneously
// and shares results between them.
type Consolidator interface {
	Create(string) (PendingResult, bool)
	Items() []ConsolidatorCacheItem
	Record(query string)
}

// PendingResult is a wrapper for result of a query.
type PendingResult interface {
	Broadcast()
	Err() error
	SetErr(error)
	SetResult(*sqltypes.Result)
	Result() *sqltypes.Result
	Wait()
}

type consolidator struct {
	*ConsolidatorCache

	mu      sync.Mutex
	queries map[string]*pendingResult
}

// NewConsolidator creates a new Consolidator
func NewConsolidator() Consolidator {
	return &consolidator{
		ConsolidatorCache: NewConsolidatorCache(1000),
		queries:           make(map[string]*pendingResult),
	}
}

// pendingResult is a wrapper for result of a query.
type pendingResult struct {
	// executing is used to block additional requests.
	// The original request holds a write lock while additional ones are blocked
	// on acquiring a read lock (see Wait() below.)
	executing    sync.RWMutex
	consolidator *consolidator
	query        string
	result       *sqltypes.Result
	err          error
}

// Create adds a query to currently executing queries and acquires a
// lock on its Result if it is not already present. If the query is
// a duplicate, Create returns false.
func (co *consolidator) Create(query string) (PendingResult, bool) {
	co.mu.Lock()
	defer co.mu.Unlock()
	var r *pendingResult
	if r, ok := co.queries[query]; ok {
		return r, false
	}
	r = &pendingResult{consolidator: co, query: query}
	r.executing.Lock()
	co.queries[query] = r
	return r, true
}

// Broadcast removes the entry from current queries and releases the
// lock on its Result. Broadcast should be invoked when original
// query completes execution.
func (rs *pendingResult) Broadcast() {
	rs.consolidator.mu.Lock()
	defer rs.consolidator.mu.Unlock()
	delete(rs.consolidator.queries, rs.query)
	rs.executing.Unlock()
}

// Err returns any error returned by the query.
func (rs *pendingResult) Err() error {
	return rs.err
}

// Result returns any result returned by the query.
func (rs *pendingResult) Result() *sqltypes.Result {
	return rs.result
}

// SetErr sets any error returned by the query.
func (rs *pendingResult) SetErr(err error) {
	rs.err = err
}

// SetResult sets any result returned by the query.
func (rs *pendingResult) SetResult(res *sqltypes.Result) {
	rs.result = res
}

// Wait waits for the original query to complete execution. Wait should
// be invoked for duplicate queries.
func (rs *pendingResult) Wait() {
	rs.consolidator.Record(rs.query)
	rs.executing.RLock()
}

// ConsolidatorCache is a thread-safe object used for counting how often recent
// queries have been consolidated.
// It is also used by the txserializer package to count how often transactions
// have been queued and had to wait because they targeted the same row (range).
type ConsolidatorCache struct {
	*cache.LRUCache
}

// NewConsolidatorCache creates a new cache with the given capacity.
func NewConsolidatorCache(capacity int64) *ConsolidatorCache {
	return &ConsolidatorCache{cache.NewLRUCache(capacity, func(_ any) int64 {
		return 1
	})}
}

// Record increments the count for "query" by 1.
// If it's not in the cache yet, it will be added.
func (cc *ConsolidatorCache) Record(query string) {
	if v, ok := cc.Get(query); ok {
		v.(*ccount).add(1)
	} else {
		c := ccount(1)
		cc.Set(query, &c)
	}
}

// ConsolidatorCacheItem is a wrapper for the items in the consolidator cache
type ConsolidatorCacheItem struct {
	Query string
	Count int64
}

// Items returns the items in the cache as an array of String, int64 structs
func (cc *ConsolidatorCache) Items() []ConsolidatorCacheItem {
	items := cc.LRUCache.Items()
	ret := make([]ConsolidatorCacheItem, len(items))
	for i, v := range items {
		ret[i] = ConsolidatorCacheItem{Query: v.Key, Count: v.Value.(*ccount).get()}
	}
	return ret
}

// ccount elements are used with a cache.LRUCache object to track if another
// request for the same query is already in progress.
type ccount int64

func (cc *ccount) add(n int64) int64 {
	return atomic.AddInt64((*int64)(cc), n)
}

func (cc *ccount) get() int64 {
	return atomic.LoadInt64((*int64)(cc))
}
