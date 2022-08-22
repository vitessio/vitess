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
	"vitess.io/vitess/go/vt/topo"
)

type watchState int

const (
	watchStateIdle watchState = iota
	watchStateStarting
	watchStateRunning
)

type watchEntry struct {
	// immutable values
	rw  *resilientWatcher
	key fmt.Stringer

	mutex      sync.Mutex
	watchState watchState

	watchStartingChan chan struct{}

	value     any
	lastError error

	lastValueTime time.Time
	lastErrorCtx  context.Context
	lastErrorTime time.Time

	listeners []func(any, error) bool
}

type resilientWatcher struct {
	watcher func(ctx context.Context, entry *watchEntry)

	counts               *stats.CountersWithSingleLabel
	cacheRefreshInterval time.Duration
	cacheTTL             time.Duration

	mutex   sync.Mutex
	entries map[string]*watchEntry
}

func (w *resilientWatcher) getEntry(wkey fmt.Stringer) *watchEntry {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	key := wkey.String()
	entry, ok := w.entries[key]
	if ok {
		return entry
	}

	entry = &watchEntry{
		rw:  w,
		key: wkey,
	}
	w.entries[key] = entry
	return entry
}

func (w *resilientWatcher) getValue(ctx context.Context, wkey fmt.Stringer) (any, error) {
	entry := w.getEntry(wkey)

	entry.mutex.Lock()
	defer entry.mutex.Unlock()
	return entry.currentValueLocked(ctx)
}

func (entry *watchEntry) addListener(ctx context.Context, callback func(any, error) bool) {
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	entry.listeners = append(entry.listeners, callback)
	v, err := entry.currentValueLocked(ctx)
	callback(v, err)
}

func (entry *watchEntry) ensureWatchingLocked(ctx context.Context) {
	switch entry.watchState {
	case watchStateRunning, watchStateStarting:
	case watchStateIdle:
		shouldRefresh := time.Since(entry.lastErrorTime) > entry.rw.cacheRefreshInterval || len(entry.listeners) > 0

		if shouldRefresh {
			entry.watchState = watchStateStarting
			entry.watchStartingChan = make(chan struct{})
			go entry.rw.watcher(ctx, entry)
		}
	}
}

func (entry *watchEntry) currentValueLocked(ctx context.Context) (any, error) {
	entry.rw.counts.Add(queryCategory, 1)

	if entry.watchState == watchStateRunning {
		return entry.value, entry.lastError
	}

	entry.ensureWatchingLocked(ctx)

	cacheValid := entry.value != nil && time.Since(entry.lastValueTime) < entry.rw.cacheTTL
	if cacheValid {
		entry.rw.counts.Add(cachedCategory, 1)
		return entry.value, nil
	}

	if entry.watchState == watchStateStarting {
		watchStartingChan := entry.watchStartingChan
		entry.mutex.Unlock()
		select {
		case <-watchStartingChan:
		case <-ctx.Done():
			entry.mutex.Lock()
			return nil, ctx.Err()
		}
		entry.mutex.Lock()
	}
	if entry.value != nil {
		return entry.value, nil
	}
	return nil, entry.lastError
}

func (entry *watchEntry) update(ctx context.Context, value any, err error, init bool) {
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	if err != nil {
		entry.onErrorLocked(ctx, err, init)
	} else {
		entry.onValueLocked(value)
	}

	listeners := entry.listeners
	entry.listeners = entry.listeners[:0]

	for _, callback := range listeners {
		if callback(entry.value, entry.lastError) {
			entry.listeners = append(entry.listeners, callback)
		}
	}
}

func (entry *watchEntry) onValueLocked(value any) {
	entry.watchState = watchStateRunning
	if entry.watchStartingChan != nil {
		close(entry.watchStartingChan)
		entry.watchStartingChan = nil
	}
	entry.value = value
	entry.lastValueTime = time.Now()

	entry.lastError = nil
	entry.lastErrorCtx = nil
	entry.lastErrorTime = time.Time{}
}

func (entry *watchEntry) onErrorLocked(callerCtx context.Context, err error, init bool) {
	entry.rw.counts.Add(errorCategory, 1)

	entry.lastErrorCtx = callerCtx
	entry.lastErrorTime = time.Now()

	// if the node disappears, delete the cached value
	if topo.IsErrType(err, topo.NoNode) {
		entry.value = nil
	}

	if init {
		entry.lastError = err

		// This watcher will able to continue to return the last value till it is not able to connect to the topo server even if the cache TTL is reached.
		// TTL cache is only checked if the error is a known error i.e topo.Error.
		_, isTopoErr := err.(topo.Error)
		if isTopoErr && time.Since(entry.lastValueTime) > entry.rw.cacheTTL {
			log.Errorf("WatchSrvKeyspace clearing cached entry for %v", entry.key)
			entry.value = nil
		}
	} else {
		entry.lastError = fmt.Errorf("ResilientWatch stream failed for %v: %w", entry.key, err)
		log.Errorf("%v", entry.lastError)

		// Even though we didn't get a new value, update the lastValueTime
		// here since the watch was successfully running before and we want
		// the value to be cached for the full TTL from here onwards.
		entry.lastValueTime = time.Now()
	}

	if entry.watchStartingChan != nil {
		close(entry.watchStartingChan)
		entry.watchStartingChan = nil
	}

	entry.watchState = watchStateIdle

	// only retry the watch if we haven't been explicitly interrupted
	if len(entry.listeners) > 0 && !topo.IsErrType(err, topo.Interrupted) {
		go func() {
			time.Sleep(entry.rw.cacheRefreshInterval)

			entry.mutex.Lock()
			entry.ensureWatchingLocked(context.Background())
			entry.mutex.Unlock()
		}()
	}
}
