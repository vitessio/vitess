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
	query := func(ctx context.Context, entry *queryEntry) (interface{}, error) {
		cell := entry.key.(cellName)
		return topoServer.GetSrvKeyspaceNames(ctx, string(cell))
	}

	rq := &resilientQuery{
		query:        query,
		counts:       counts,
		cacheRefresh: cacheRefresh,
		cacheTTL:     cacheTTL,
		entries:      make(map[string]*queryEntry),
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
			LastErrorCtx:   entry.lastErrorCtx,
		})
		entry.mutex.Unlock()
	}
	return
}
