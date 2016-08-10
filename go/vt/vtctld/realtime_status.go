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
	healthCheck discovery.HealthCheck
	*tabletStatsCache
	cellWatchers []*discovery.TopologyWatcher
}

func newRealtimeStats(ts topo.Server) (*realtimeStats, error) {
	hc := discovery.NewHealthCheck(*vtctl.HealthCheckTimeout, *vtctl.HealthcheckRetryDelay, *vtctl.HealthCheckTimeout)
	tabletStatsCache := newTabletStatsCache()
	// sendDownEvents is set to true here, as we want to receive
	// Up=False events for a tablet.
	hc.SetListener(tabletStatsCache, true)
	r := &realtimeStats{
		healthCheck:      hc,
		tabletStatsCache: tabletStatsCache,
	}

	// Get the list of all tablets from all cells and monitor the topology for added or removed tablets with a CellTabletsWatcher.
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

func (r *realtimeStats) Stop() error {
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
