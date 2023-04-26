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
	watch := func(entry *watchEntry) {
		key := entry.key.(cellName)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		current, changes, err := topoServer.WatchSrvVSchema(ctx, key.String())
		if err != nil {
			entry.update(nil, err, true)
			return
		}

		entry.update(current.Value, current.Err, true)
		if current.Err != nil {
			return
		}

		defer cancel()
		for c := range changes {
			entry.update(c.Value, c.Err, false)
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
	entry.addListener(ctx, func(v any, err error) bool {
		vschema, _ := v.(*vschemapb.SrvVSchema)
		return callback(vschema, err)
	})
}
