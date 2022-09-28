/*
Copyright 2021 The Vitess Authors.

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

package srvtopo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
)

type queryEntry struct {
	// immutable values
	key fmt.Stringer

	// the mutex protects any access to this structure (read or write)
	mutex sync.Mutex

	// refreshingChan is used to synchronize requests and avoid hammering
	// the topo server
	refreshingChan chan struct{}

	insertionTime time.Time
	lastQueryTime time.Time
	value         any
	lastError     error
}

type resilientQuery struct {
	query func(ctx context.Context, entry *queryEntry) (any, error)

	counts               *stats.CountersWithSingleLabel
	cacheRefreshInterval time.Duration
	cacheTTL             time.Duration

	mutex   sync.Mutex
	entries map[string]*queryEntry
}

func (q *resilientQuery) getCurrentValue(ctx context.Context, wkey fmt.Stringer, staleOK bool) (any, error) {
	q.counts.Add(queryCategory, 1)

	// find the entry in the cache, add it if not there
	key := wkey.String()
	q.mutex.Lock()
	entry, ok := q.entries[key]
	if !ok {
		entry = &queryEntry{
			key: wkey,
		}
		q.entries[key] = entry
	}
	q.mutex.Unlock()

	// Lock the entry, and do everything holding the lock except
	// querying the underlying topo server.
	//
	// This means that even if the topo server is very slow, two concurrent
	// requests will only issue one underlying query.
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	cacheValid := entry.value != nil && (time.Since(entry.insertionTime) < q.cacheTTL)
	if !cacheValid && staleOK {
		// Only allow stale results for a bounded period
		cacheValid = entry.value != nil && (time.Since(entry.insertionTime) < (q.cacheTTL + 2*q.cacheRefreshInterval))
	}
	shouldRefresh := time.Since(entry.lastQueryTime) > q.cacheRefreshInterval

	// If it is not time to check again, then return either the cached
	// value or the cached error but don't ask topo again.
	if !shouldRefresh {
		if cacheValid {
			return entry.value, nil
		}
		return nil, entry.lastError
	}

	// Refresh the state in a background goroutine if no refresh is already
	// in progress. This way queries are not blocked while the cache is still
	// valid but past the refresh time, and avoids calling out to the topo
	// service while the lock is held.
	if entry.refreshingChan == nil {
		entry.refreshingChan = make(chan struct{})
		entry.lastQueryTime = time.Now()

		go func() {
			defer func() {
				if err := recover(); err != nil {
					log.Errorf("ResilientQuery uncaught panic, cell :%v, err :%v)", key, err)
				}
			}()

			newCtx, cancel := context.WithTimeout(ctx, srvTopoTimeout)
			defer cancel()

			result, err := q.query(newCtx, entry)

			entry.mutex.Lock()
			defer func() {
				close(entry.refreshingChan)
				entry.refreshingChan = nil
				entry.mutex.Unlock()
			}()

			if err == nil {
				// save the value we got and the current time in the cache
				entry.insertionTime = time.Now()
				// Avoid a tiny race if TTL == refresh time (the default)
				entry.lastQueryTime = entry.insertionTime
				entry.value = result
			} else {
				q.counts.Add(errorCategory, 1)
				if entry.insertionTime.IsZero() {
					log.Errorf("ResilientQuery(%v, %v) failed: %v (no cached value, caching and returning error)", ctx, wkey, err)
				} else if newCtx.Err() == context.DeadlineExceeded {
					log.Errorf("ResilientQuery(%v, %v) failed: %v (request timeout), (keeping cached value: %v)", ctx, wkey, err, entry.value)
				} else if entry.value != nil && time.Since(entry.insertionTime) < q.cacheTTL {
					q.counts.Add(cachedCategory, 1)
					log.Warningf("ResilientQuery(%v, %v) failed: %v (keeping cached value: %v)", ctx, wkey, err, entry.value)
				} else {
					log.Errorf("ResilientQuery(%v, %v) failed: %v (cached value expired)", ctx, wkey, err)
					entry.insertionTime = time.Time{}
					entry.value = nil
				}
			}

			entry.lastError = err
		}()
	}

	// If the cached entry is still valid then use it, otherwise wait
	// for the refresh attempt to complete to get a more up to date
	// response.
	//
	// In the event that the topo service is slow or unresponsive either
	// on the initial fetch or if the cache TTL expires, then several
	// requests could be blocked on refreshingChan waiting for the response
	// to come back.
	if cacheValid {
		return entry.value, nil
	}

	refreshingChan := entry.refreshingChan
	entry.mutex.Unlock()
	select {
	case <-refreshingChan:
	case <-ctx.Done():
		entry.mutex.Lock()
		return nil, ctx.Err()
	}
	entry.mutex.Lock()

	if entry.value != nil {
		return entry.value, nil
	}

	return nil, entry.lastError
}
