package vtctld

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctl"
)

// realtimeStats holds the objects needed to obtain realtime health stats of tablets.
type realtimeStats struct {
	healthCheck  discovery.HealthCheck
	tabletStats  tabletStatsCache
	cellWatchers []*discovery.TopologyWatcher
}

func newRealtimeStats(ts topo.Server) (realtimeStats, error) {

	hc := discovery.NewHealthCheck(*vtctl.HealthCheckTimeout, *vtctl.HealthcheckRetryDelay, *vtctl.HealthCheckTimeout)

	updates := tabletStatsCache{
		recentTabletStatuses: make(map[string]map[string]*discovery.TabletStats),
	}
	hc.SetListener(updates)

	r := realtimeStats{
		healthCheck: hc,
		tabletStats: updates,
	}

	// Creating a watcher for tablets in each cell.
	cells, err := ts.GetKnownCells(context.Background())
	if err != nil {
		return r, fmt.Errorf("error when getting cells: %v", err)
	}

	var watchers []*discovery.TopologyWatcher
	for _, cell := range cells {
		watcher := discovery.NewCellTabletsWatcher(ts, hc, cell, *vtctl.HealthCheckTopologyRefresh, discovery.DefaultTopoReadConcurrency)
		watchers = append(watchers, watcher)
	}
	r.cellWatchers = watchers

	return r, nil
}

func (r realtimeStats) Stop() error {
	for _, w := range r.cellWatchers {
		w.Stop()
	}
	if r.healthCheck != nil {
		if err := r.healthCheck.Close(); err != nil {
			return fmt.Errorf("healthCheck.Close() failed: %v", err)
		}
	}
	return nil
}

func (r realtimeStats) tabletStatuses(cell string, keyspace string, shard string, tabType string) map[string]*discovery.TabletStats {
	return r.tabletStats.tabletStatuses(cell, keyspace, shard, tabType)
}

func (r realtimeStats) mimicStatsUpdateForTesting(stats *discovery.TabletStats) {
	r.tabletStats.StatsUpdate(stats)
}
