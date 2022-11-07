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
	"vitess.io/vitess/go/vt/topo"
)

type SrvKeyspaceNamesQuery struct {
	rq *resilientQuery
}

func NewSrvKeyspaceNamesQuery(topoServer *topo.Server, counts *stats.CountersWithSingleLabel, cacheRefresh, cacheTTL time.Duration) *SrvKeyspaceNamesQuery {
	query := func(ctx context.Context, entry *queryEntry) (any, error) {
		cell := entry.key.(cellName)
		return topoServer.GetSrvKeyspaceNames(ctx, string(cell))
	}

	rq := &resilientQuery{
		query:                query,
		counts:               counts,
		cacheRefreshInterval: cacheRefresh,
		cacheTTL:             cacheTTL,
		entries:              make(map[string]*queryEntry),
	}

	return &SrvKeyspaceNamesQuery{rq}
}

func (q *SrvKeyspaceNamesQuery) GetSrvKeyspaceNames(ctx context.Context, cell string, staleOK bool) ([]string, error) {
	v, err := q.rq.getCurrentValue(ctx, cellName(cell), staleOK)
	names, _ := v.([]string)
	return names, err
}

func (q *SrvKeyspaceNamesQuery) srvKeyspaceNamesCacheStatus() (result []*SrvKeyspaceNamesCacheStatus) {
	q.rq.mutex.Lock()
	defer q.rq.mutex.Unlock()

	for _, entry := range q.rq.entries {
		entry.mutex.Lock()
		value, _ := entry.value.([]string)
		result = append(result, &SrvKeyspaceNamesCacheStatus{
			Cell:           entry.key.String(),
			Value:          value,
			ExpirationTime: entry.insertionTime.Add(q.rq.cacheTTL),
			LastQueryTime:  entry.lastQueryTime,
			LastError:      entry.lastError,
		})
		entry.mutex.Unlock()
	}
	return
}
