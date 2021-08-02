package srvtopo

import (
	"context"
	"time"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
)

type SrvVSchemaWatcher struct {
	rw *resilientWatcher
}

type srvVSchemaKey string

func (k srvVSchemaKey) String() string {
	return string(k)
}

func NewSrvVSchemaWatcher(topoServer *topo.Server, cacheRefresh, cacheTTL time.Duration) *SrvVSchemaWatcher {
	watch := func(ctx context.Context, entry *watchEntry) {
		key := entry.key.(srvVSchemaKey)
		current, changes, cancel := topoServer.WatchSrvVSchema(context.Background(), key.String())

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

		var lastErr error
		var vschema *vschemapb.SrvVSchema

		if entry.value != nil {
			vschema = entry.value.(*vschemapb.SrvVSchema)
		} else {
			lastErr = entry.lastError
		}

		for watcher := range entry.listeners {
			switch w := watcher.(type) {
			case chan *vschemapb.SrvVSchema:
				if vschema != nil {
					select {
					case w <- vschema:
					default:
					}
				}
			case *srvVSchemaCallback:
				w.callback(vschema, lastErr)
				if w.first != nil {
					close(w.first)
					w.first = nil
				}
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

	return &SrvVSchemaWatcher{rw}
}

func (w *SrvVSchemaWatcher) Get(ctx context.Context, cell string) (*vschemapb.SrvVSchema, error) {
	v, err := w.rw.getValue(ctx, srvVSchemaKey(cell))
	if err != nil {
		return nil, err
	}
	return v.(*vschemapb.SrvVSchema), nil
}

func (w *SrvVSchemaWatcher) Watch(ctx context.Context, cell string) chan *vschemapb.SrvVSchema {
	newChan := make(chan *vschemapb.SrvVSchema, 1)
	entry := w.rw.getEntry(srvVSchemaKey(cell))
	entry.addListener(ctx, newChan)
	return newChan
}

type srvVSchemaCallback struct {
	callback func(*vschemapb.SrvVSchema, error)
	first    chan<- struct{}
}

func (w *SrvVSchemaWatcher) WatchFunc(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error)) {
	first := make(chan struct{})
	entry := w.rw.getEntry(srvVSchemaKey(cell))
	entry.addListener(ctx, &srvVSchemaCallback{callback: callback, first: first})
	<-first
}
