package srvtopo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/topo"
)

type watchEntry struct {
	// unmutable values
	watcher *resilientWatcher
	key     fmt.Stringer

	mutex      sync.RWMutex
	watchState watchState

	watchStartingChan chan struct{}

	value     interface{}
	lastError error

	lastValueTime time.Time
	lastErrorCtx  context.Context
	lastErrorTime time.Time
}

type resilientWatcher struct {
	watch        func(ctx context.Context, entry *watchEntry)
	cacheRefresh time.Duration
	cacheTTL     time.Duration

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
		watcher: w,
		key:     wkey,
	}
	w.entries[key] = entry
	return entry
}

type SrvKeyspaceWatcher struct {
	w *resilientWatcher
}

type srvKeyspaceKey struct {
	cell, keyspace string
}

func (k *srvKeyspaceKey) String() string {
	return k.cell + "." + k.keyspace
}

func NewSrvKeyspaceWatcher(topoServer topo.Server, cacheRefresh, cacheTTL time.Duration) *SrvKeyspaceWatcher {
	watch := func(ctx context.Context, entry *watchEntry) {
		key := entry.key.(*srvKeyspaceKey)
		current, changes, cancel := topoServer.WatchSrvKeyspace(context.Background(), key.cell, key.keyspace)
		if !entry.update(ctx, current.Value, current.Err) {
			return
		}
		defer cancel()
		for c := range changes {
			if !entry.update(ctx, c.Value, c.Err) {
				return
			}
		}
	}

	w := &resilientWatcher{
		watch:        watch,
		cacheRefresh: cacheRefresh,
		cacheTTL:     cacheTTL,
		entries:      make(map[string]*watchEntry),
	}

	return &SrvKeyspaceWatcher{w}
}

func (w *resilientWatcher) GetValue(ctx context.Context, wkey fmt.Stringer) (interface{}, error) {
	entry := w.getEntry(wkey)
	return entry.currentValue(ctx)
}

func (entry *watchEntry) currentValue(ctx context.Context) (interface{}, error) {
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	if entry.watchState == watchStateRunning {
		return entry.value, entry.lastError
	}

	shouldRefresh := time.Since(entry.lastErrorTime) > entry.watcher.cacheRefresh
	if shouldRefresh && (entry.watchState == watchStateIdle) {
		entry.watchState = watchStateStarting
		entry.watchStartingChan = make(chan struct{})
		go entry.watcher.watch(ctx, entry)
	}

	cacheValid := entry.value != nil && time.Since(entry.lastValueTime) < entry.watcher.cacheTTL
	if cacheValid {
		entry.watcher.counts.Add(cachedCategory, 1)
		return entry.value, nil
	}

	if entry.watchState == watchStateStarting {
		watchStartingChan := entry.watchStartingChan
		entry.mutex.Unlock()
		select {
		case <-watchStartingChan:
		case <-ctx.Done():
			entry.mutex.Lock()
			return nil, fmt.Errorf("timed out waiting for keyspace")
		}
		entry.mutex.Lock()
	}

	if entry.value != nil {
		return entry.value, nil
	}
	return nil, entry.lastError
}

func (entry *watchEntry) update(ctx context.Context, value interface{}, err error) bool {
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	if err != nil {
		entry.onErrorLocked(ctx, err)
		return false
	}
	entry.onValueLocked(value)
	return true
}

func (entry *watchEntry) onValueLocked(value interface{}) {
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

func (entry *watchEntry) onErrorLocked(callerCtx context.Context, err error) {
	entry.lastError = err
	entry.lastErrorCtx = callerCtx
	entry.lastErrorTime = time.Now()

	// if the node disappears, delete the cached value
	if topo.IsErrType(err, topo.NoNode) {
		entry.value = nil
	}

	entry.watcher.counts.Add(errorCategory, 1)

	// This watcher will able to continue to return the last value till it is not able to connect to the topo server even if the cache TTL is reached.
	// TTL cache is only checked if the error is a known error i.e topo.Error.
	_, isTopoErr := err.(topo.Error)
	if isTopoErr && time.Since(entry.lastValueTime) > entry.watcher.cacheTTL {
		entry.value = nil
	}

	if entry.watchStartingChan != nil {
		close(entry.watchStartingChan)
		entry.watchStartingChan = nil
	}

	if entry.watchState == watchStateRunning {
		entry.lastValueTime = time.Now()
	}
	entry.watchState = watchStateIdle
}
