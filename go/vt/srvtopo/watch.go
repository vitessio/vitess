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
	rw  *resilientWatcher
	key fmt.Stringer

	mutex      sync.RWMutex
	watchState watchState

	watchStartingChan chan struct{}

	value     interface{}
	lastError error

	lastValueTime time.Time
	lastErrorCtx  context.Context
	lastErrorTime time.Time

	listeners map[interface{}]struct{}
}

type resilientWatcher struct {
	watch func(ctx context.Context, entry *watchEntry)
	event func(entry *watchEntry)

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
		rw:  w,
		key: wkey,
	}
	w.entries[key] = entry
	return entry
}

func (w *resilientWatcher) getValue(ctx context.Context, wkey fmt.Stringer) (interface{}, error) {
	entry := w.getEntry(wkey)
	return entry.currentValue(ctx)
}

func (entry *watchEntry) addListener(ctx context.Context, ch interface{}) {
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	entry.listeners[ch] = struct{}{}
	entry.ensureWatchingLocked(ctx)
}

func (entry *watchEntry) ensureWatchingLocked(ctx context.Context) {
	switch entry.watchState {
	case watchStateRunning, watchStateStarting:
	case watchStateIdle:
		entry.watchState = watchStateStarting
		entry.watchStartingChan = make(chan struct{})
		go entry.rw.watch(ctx, entry)
	}
}

func (entry *watchEntry) currentValue(ctx context.Context) (interface{}, error) {
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	if entry.watchState == watchStateRunning {
		return entry.value, entry.lastError
	}

	entry.ensureWatchingLocked(ctx)

	cacheValid := entry.value != nil && time.Since(entry.lastValueTime) < entry.rw.cacheTTL
	if cacheValid {
		// entry.rw.counts.Add(cachedCategory, 1)
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

func (entry *watchEntry) update(ctx context.Context, value interface{}, err error) {
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	if err != nil {
		entry.onErrorLocked(ctx, err)
		return
	}
	entry.onValueLocked(value)
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

	// entry.rw.counts.Add(errorCategory, 1)

	// This watcher will able to continue to return the last value till it is not able to connect to the topo server even if the cache TTL is reached.
	// TTL cache is only checked if the error is a known error i.e topo.Error.
	_, isTopoErr := err.(topo.Error)
	if isTopoErr && time.Since(entry.lastValueTime) > entry.rw.cacheTTL {
		entry.value = nil
	}

	if entry.watchStartingChan != nil {
		close(entry.watchStartingChan)
		entry.watchStartingChan = nil
	}

	if entry.watchState == watchStateRunning {
		entry.lastValueTime = time.Now()
	}

	entry.watchState = watchStateStarting
	entry.watchStartingChan = make(chan struct{})

	go func() {
		time.Sleep(5 * time.Second)
		entry.rw.watch(context.Background(), entry)
	}()
}
