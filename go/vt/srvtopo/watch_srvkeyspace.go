package srvtopo

import (
	"context"
	"time"

	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
)

type SrvKeyspaceWatcher struct {
	rw *resilientWatcher
}

type srvKeyspaceKey struct {
	cell, keyspace string
}

func (k *srvKeyspaceKey) String() string {
	return k.cell + "." + k.keyspace
}

func NewSrvKeyspaceWatcher(topoServer *topo.Server, cacheRefresh, cacheTTL time.Duration) *SrvKeyspaceWatcher {
	watch := func(ctx context.Context, entry *watchEntry) {
		key := entry.key.(*srvKeyspaceKey)
		current, changes, cancel := topoServer.WatchSrvKeyspace(context.Background(), key.cell, key.keyspace)

		entry.update(ctx, current.Value, current.Err)
		if current.Err != nil {
			return
		}

		defer cancel()
		for c := range changes {
			entry.update(ctx, c.Value, c.Err)
			if c.Err != nil {
				return
			}
		}
	}

	event := func(entry *watchEntry) {
		entry.mutex.Lock()
		defer entry.mutex.Unlock()

		if entry.lastError != nil {
			return
		}

		srvkeyspace := entry.value.(*topodata.SrvKeyspace)
		for watcher := range entry.listeners {
			ch := watcher.(chan *topodata.SrvKeyspace)
			select {
			case ch <- srvkeyspace:
			default:
			}
		}
	}

	rw := &resilientWatcher{
		watch:        watch,
		event:        event,
		cacheRefresh: cacheRefresh,
		cacheTTL:     cacheTTL,
		entries:      make(map[string]*watchEntry),
	}

	return &SrvKeyspaceWatcher{rw}
}

func (w *SrvKeyspaceWatcher) Get(ctx context.Context, cell, keyspace string) (*topodata.SrvKeyspace, error) {
	key := &srvKeyspaceKey{cell, keyspace}
	v, err := w.rw.getValue(ctx, key)
	if err != nil {
		return nil, err
	}
	return v.(*topodata.SrvKeyspace), nil
}

func (w *SrvKeyspaceWatcher) Watch(ctx context.Context, cell, keyspace string) chan *topodata.SrvKeyspace {
	key := &srvKeyspaceKey{cell, keyspace}
	newChan := make(chan *topodata.SrvKeyspace, 1)
	entry := w.rw.getEntry(key)
	entry.addListener(ctx, newChan)
	return newChan
}
