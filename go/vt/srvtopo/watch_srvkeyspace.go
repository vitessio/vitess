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

func NewSrvKeyspaceWatcher(ctx context.Context, topoServer *topo.Server, counts *stats.CountersWithSingleLabel, cacheRefresh time.Duration) *SrvKeyspaceWatcher {
	watch := func(entry *watchEntry) {
		key := entry.key.(*srvKeyspaceKey)
		requestCtx, requestCancel := context.WithCancel(ctx)
		defer requestCancel()

		current, changes, err := topoServer.WatchSrvKeyspace(requestCtx, key.cell, key.keyspace)
		if err != nil {
			entry.update(ctx, nil, err, true)
			return
		}

		entry.update(ctx, current.Value, current.Err, true)
		if current.Err != nil {
			return
		}

		for c := range changes {
			entry.update(ctx, c.Value, c.Err, false)
			if c.Err != nil {
				return
			}
		}
	}

	rw := newResilientWatcher(ctx, "SrvKeyspace", watch, counts, cacheRefresh)

	return &SrvKeyspaceWatcher{rw}
}

func (w *SrvKeyspaceWatcher) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodata.SrvKeyspace, error) {
	key := &srvKeyspaceKey{cell, keyspace}
	v, err := w.rw.getValue(ctx, key)
	ks, _ := v.(*topodata.SrvKeyspace)
	return ks, err
}

func (w *SrvKeyspaceWatcher) WatchSrvKeyspace(ctx context.Context, cell, keyspace string, callback func(*topodata.SrvKeyspace, error) bool) {
	entry := w.rw.getEntry(&srvKeyspaceKey{cell, keyspace})
	entry.addListener(ctx, func(v any, err error) bool {
		srvkeyspace, _ := v.(*topodata.SrvKeyspace)
		return callback(srvkeyspace, err)
	})
}

func (w *SrvKeyspaceWatcher) srvKeyspaceCacheStatus() (result []*SrvKeyspaceCacheStatus) {
	for _, entry := range w.rw.allEntries() {
		entry.mutex.Lock()
		var staleSince time.Time
		if entry.watchState != watchStateRunning {
			staleSince = entry.staleSince
		}

		key := entry.key.(*srvKeyspaceKey)
		value, _ := entry.value.(*topodata.SrvKeyspace)

		result = append(result, &SrvKeyspaceCacheStatus{
			Cell:          key.cell,
			Keyspace:      key.keyspace,
			Value:         value,
			StaleSince:    staleSince,
			LastErrorTime: entry.lastErrorTime,
			LastError:     entry.lastError,
		})
		entry.mutex.Unlock()
	}
	return
}
