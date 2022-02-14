package srvtopo

import (
	"context"
	"time"

	"vitess.io/vitess/go/stats"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
)

type SrvVSchemaWatcher struct {
	rw *resilientWatcher
}

type cellName string

func (k cellName) String() string {
	return string(k)
}

func NewSrvVSchemaWatcher(topoServer *topo.Server, counts *stats.CountersWithSingleLabel, cacheRefresh, cacheTTL time.Duration) *SrvVSchemaWatcher {
	watch := func(ctx context.Context, entry *watchEntry) {
		key := entry.key.(cellName)
		current, changes, cancel := topoServer.WatchSrvVSchema(context.Background(), key.String())

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
		watcher:              watch,
		counts:               counts,
		cacheRefreshInterval: cacheRefresh,
		cacheTTL:             cacheTTL,
		entries:              make(map[string]*watchEntry),
	}

	return &SrvVSchemaWatcher{rw}
}

func (w *SrvVSchemaWatcher) GetSrvVSchema(ctx context.Context, cell string) (*vschemapb.SrvVSchema, error) {
	v, err := w.rw.getValue(ctx, cellName(cell))
	vschema, _ := v.(*vschemapb.SrvVSchema)
	return vschema, err
}

func (w *SrvVSchemaWatcher) WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error) bool) {
	entry := w.rw.getEntry(cellName(cell))
	entry.addListener(ctx, func(v interface{}, err error) bool {
		vschema, _ := v.(*vschemapb.SrvVSchema)
		return callback(vschema, err)
	})
}
