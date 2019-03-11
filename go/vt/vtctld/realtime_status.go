/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtctld

import (
	"fmt"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl"
)

// realtimeStats holds the objects needed to obtain realtime health stats of tablets.
type realtimeStats struct {
	healthCheck discovery.HealthCheck
	*tabletStatsCache
	cellWatchers []*discovery.TopologyWatcher
}

func newRealtimeStats(ts *topo.Server) (*realtimeStats, error) {
	hc := discovery.NewHealthCheck(*vtctl.HealthcheckRetryDelay, *vtctl.HealthCheckTimeout)
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
		watcher := discovery.NewCellTabletsWatcher(context.Background(), ts, hc, cell, *vtctl.HealthCheckTopologyRefresh, true /* refreshKnownTablets */, discovery.DefaultTopoReadConcurrency)
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
