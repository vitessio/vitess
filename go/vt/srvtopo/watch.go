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
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

type watchState int

const (
	watchStateIdle watchState = iota
	watchStateStarting
	watchStateRunning
	watchStateStopped
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

	lastErrorTime time.Time

	// staleSince is when the watch stopped running while a value was
	// cached. It is zero while the watch is running or no value is cached.
	staleSince time.Time

	listeners []func(any, error) bool
}

// SrvTopoWatchStaleSeconds reports, for every watched entry that holds a
// value, how many seconds that value has been served without a running watch.
// It is 0 while the watch is running.
var _ = stats.NewGaugesFuncWithMultiLabels(
	"SrvTopoWatchStaleSeconds",
	"Seconds a watched srvtopo value has been served without a running topo watch",
	[]string{"Type", "Cell", "Keyspace"},
	allWatchers.staleSeconds,
)

// allWatchers tracks every live resilientWatcher in the process for
// SrvTopoWatchStaleSeconds.
var allWatchers = &watcherRegistry{watchers: make(map[*resilientWatcher]struct{})}

type watcherRegistry struct {
	mu       sync.Mutex
	watchers map[*resilientWatcher]struct{}
}

func (r *watcherRegistry) add(ctx context.Context, w *resilientWatcher) {
	r.mu.Lock()
	r.watchers[w] = struct{}{}
	r.mu.Unlock()

	context.AfterFunc(ctx, func() {
		r.mu.Lock()
		delete(r.watchers, w)
		r.mu.Unlock()
	})
}

func (r *watcherRegistry) staleSeconds() map[string]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := make(map[string]int64)
	for w := range r.watchers {
		w.staleSeconds(result)
	}
	return result
}

type resilientWatcher struct {
	watcher func(entry *watchEntry)
	// kind is the Type label of SrvTopoWatchStaleSeconds for this watcher's
	// entries.
	kind string

	counts               *stats.CountersWithSingleLabel
	cacheRefreshInterval time.Duration

	mutex   sync.Mutex
	entries map[string]*watchEntry
}

func newResilientWatcher(ctx context.Context, kind string, watcher func(entry *watchEntry), counts *stats.CountersWithSingleLabel, cacheRefresh time.Duration) *resilientWatcher {
	rw := &resilientWatcher{
		watcher:              watcher,
		kind:                 kind,
		counts:               counts,
		cacheRefreshInterval: cacheRefresh,
		entries:              make(map[string]*watchEntry),
	}
	allWatchers.add(ctx, rw)
	return rw
}

// staleSeconds adds the staleness of every entry that holds a value to result,
// keyed by the SrvTopoWatchStaleSeconds labels. If several watchers in the process
// watch the same key, the largest staleness wins.
func (w *resilientWatcher) staleSeconds(result map[string]int64) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	for _, entry := range w.entries {
		entry.mutex.Lock()
		hasValue := entry.value != nil
		var stale int64
		if entry.watchState != watchStateRunning && !entry.staleSince.IsZero() {
			stale = int64(time.Since(entry.staleSince).Seconds())
		}
		entry.mutex.Unlock()

		if !hasValue {
			continue
		}

		var cell, keyspace string
		switch key := entry.key.(type) {
		case *srvKeyspaceKey:
			cell, keyspace = key.cell, key.keyspace
		case cellName:
			cell = string(key)
		}
		// Label values are joined with ".", so they must not contain one.
		labels := strings.Join([]string{w.kind, strings.ReplaceAll(cell, ".", "_"), strings.ReplaceAll(keyspace, ".", "_")}, ".")
		if stale >= result[labels] {
			result[labels] = stale
		}
	}
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
	if ctx.Err() != nil {
		return
	}
	switch entry.watchState {
	case watchStateRunning, watchStateStarting:
	case watchStateIdle:
		shouldRefresh := time.Since(entry.lastErrorTime) > entry.rw.cacheRefreshInterval || len(entry.listeners) > 0

		if shouldRefresh {
			entry.watchState = watchStateStarting
			entry.watchStartingChan = make(chan struct{})
			go entry.rw.watcher(entry)
		}
	}
}

func (entry *watchEntry) currentValueLocked(ctx context.Context) (any, error) {
	entry.rw.counts.Add(queryCategory, 1)

	if entry.watchState == watchStateRunning {
		return entry.value, entry.lastError
	}

	entry.ensureWatchingLocked(ctx)

	// While the watch is not running, serve the last known value rather than
	// waiting for the watch to be re-established. The topo server may be slow
	// or unreachable, and until it reports that the node is gone the cached
	// value is the best information available.
	if entry.value != nil {
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

	// Only notify listeners on success or when no cached value exists after error processing.
	// This prevents unnecessary notifications during topo outages when cached data is available.
	shouldNotifyListeners := err == nil || entry.value == nil

	if shouldNotifyListeners {
		listeners := entry.listeners
		entry.listeners = entry.listeners[:0]

		for _, callback := range listeners {
			if callback(entry.value, entry.lastError) {
				entry.listeners = append(entry.listeners, callback)
			}
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
	entry.staleSince = time.Time{}

	entry.lastError = nil
	entry.lastErrorTime = time.Time{}
}

func (entry *watchEntry) onErrorLocked(ctx context.Context, err error, init bool) {
	entry.rw.counts.Add(errorCategory, 1)

	entry.lastErrorTime = time.Now()

	// if the node disappears, delete the cached value. Any other error leaves
	// the cached value in place, to be served until the watch is
	// re-established.
	if topo.IsErrType(err, topo.NoNode) {
		entry.value = nil
	}

	if entry.value == nil {
		entry.staleSince = time.Time{}
	} else if entry.staleSince.IsZero() {
		entry.staleSince = entry.lastErrorTime
	}

	if init {
		entry.lastError = err
	} else if !topo.IsErrType(err, topo.Interrupted) {
		// No need to log if we're explicitly interrupted.
		entry.lastError = fmt.Errorf("ResilientWatch stream failed for %v: %w", entry.key, err)
		log.Error(fmt.Sprintf("%v", entry.lastError))
	}

	if entry.watchStartingChan != nil {
		close(entry.watchStartingChan)
		entry.watchStartingChan = nil
	}

	entry.watchState = watchStateIdle

	// only retry the watch if we haven't been explicitly interrupted

	if len(entry.listeners) > 0 && !topo.IsErrType(err, topo.Interrupted) {
		go func() {
			_ = timer.SleepContext(ctx, entry.rw.cacheRefreshInterval)
			entry.mutex.Lock()
			entry.ensureWatchingLocked(ctx)
			entry.mutex.Unlock()
		}()
	}
}
