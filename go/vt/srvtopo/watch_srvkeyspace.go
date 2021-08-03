package srvtopo

import (
	"context"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
)

type SrvKeyspaceWatcher struct {
	*resilientWatcher
}

type srvKeyspaceKey struct {
	cell, keyspace string
}

func (k *srvKeyspaceKey) String() string {
	return k.cell + "." + k.keyspace
}

func NewSrvKeyspaceWatcher(topoServer *topo.Server, counts *stats.CountersWithSingleLabel, cacheRefresh, cacheTTL time.Duration) *SrvKeyspaceWatcher {
	watch := func(ctx context.Context, entry *watchEntry) {
		key := entry.key.(*srvKeyspaceKey)
		current, changes, cancel := topoServer.WatchSrvKeyspace(context.Background(), key.cell, key.keyspace)

		entry.update(ctx, current.Value, current.Err, true)
		if current.Err != nil {
			return
		}

		defer cancel()
		for c := range changes {
			entry.update(ctx, c.Value, c.Err, false)
			if c.Err != nil {
				return
			}
		}
	}

	rw := &resilientWatcher{
		watcher:      watch,
		counts:       counts,
		cacheRefresh: cacheRefresh,
		cacheTTL:     cacheTTL,
		entries:      make(map[string]*watchEntry),
	}

	return &SrvKeyspaceWatcher{rw}
}

func (w *SrvKeyspaceWatcher) Get(ctx context.Context, cell, keyspace string) (*topodata.SrvKeyspace, error) {
	key := &srvKeyspaceKey{cell, keyspace}
	v, err := w.getValue(ctx, key)
	ks, _ := v.(*topodata.SrvKeyspace)
	return ks, err
}

func (w *SrvKeyspaceWatcher) Watch(ctx context.Context, cell, keyspace string, callback func(*topodata.SrvKeyspace, error)) {
	entry := w.getEntry(&srvKeyspaceKey{cell, keyspace})
	entry.addListener(ctx, func(v interface{}, err error) {
		srvkeyspace, _ := v.(*topodata.SrvKeyspace)
		callback(srvkeyspace, err)
	})
}

func (w *SrvKeyspaceWatcher) CacheStatus() (result []*SrvKeyspaceCacheStatus) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	for _, entry := range w.entries {
		entry.mutex.Lock()
		expirationTime := time.Now().Add(w.cacheTTL)
		if entry.watchState != watchStateRunning {
			expirationTime = entry.lastValueTime.Add(w.cacheTTL)
		}

		key := entry.key.(*srvKeyspaceKey)
		value, _ := entry.value.(*topodata.SrvKeyspace)

		result = append(result, &SrvKeyspaceCacheStatus{
			Cell:           key.cell,
			Keyspace:       key.keyspace,
			Value:          value,
			ExpirationTime: expirationTime,
			LastErrorTime:  entry.lastErrorTime,
			LastError:      entry.lastError,
			LastErrorCtx:   entry.lastErrorCtx,
		})
		entry.mutex.Unlock()
	}
	return
}
