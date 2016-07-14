package vtctld

import (
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/discovery"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctl"
	"golang.org/x/net/context"
)

type realtimeStats struct {
	healthCheck discovery.HealthCheck
	tabletStats tabletStatsCache
}

func setUpFlagsForTests() {
	var temphcTR = 30 * time.Second
	vtctl.HealthCheckTopologyRefresh = &temphcTR
	var temphcRD = (5 * time.Second)
	vtctl.HealthcheckRetryDelay = &temphcRD
	var temphcT = (time.Minute)
	vtctl.HealthCheckTimeout = &temphcT
}

func initHealthCheck(ts topo.Server) realtimeStats {

	var r realtimeStats

	hc := discovery.NewHealthCheck(*vtctl.HealthCheckTimeout, *vtctl.HealthcheckRetryDelay, *vtctl.HealthCheckTimeout)

	//Creating a watcher for tablets in each cell
	listOfWatchers := make([]*discovery.TopologyWatcher, 0, 1)
	knownCells, err := ts.GetKnownCells(context.Background())
	if err != nil {
		log.Errorf("Error when getting cells")
		return r
	}

	var tabletTypes []topodatapb.TabletType
	tabletTypes = append(tabletTypes, topodatapb.TabletType_MASTER)
	tabletTypes = append(tabletTypes, topodatapb.TabletType_REPLICA)
	tabletTypes = append(tabletTypes, topodatapb.TabletType_RDONLY)

	for _, cell := range knownCells {
		watcher := discovery.NewCellTabletsWatcher(ts, hc, cell, *vtctl.HealthCheckTopologyRefresh, discovery.DefaultTopoReadConcurrency)
		err := discovery.WaitForAllServingTablets(context.Background(), hc, ts, cell, tabletTypes)
		if err != nil {
			log.Errorf("Error when waiting for tablets: %v", err)
			return r
		}
		listOfWatchers = append(listOfWatchers, watcher)
	}

	updates := tabletStatsCache{
		tabletUpdate: make(map[string]map[string]*querypb.RealtimeStats),
	}
	hc.SetListener(updates)

	r = realtimeStats{
		healthCheck: hc,
		tabletStats: updates,
	}

	return r
}

func (r realtimeStats) getUpdate(cell string, keyspace string, shard string, tabType string) map[string]*querypb.RealtimeStats {
	tabletUpdates := r.tabletStats
	return tabletUpdates.getTargetUpdates(cell, keyspace, shard, tabType)
}

func (r realtimeStats) mimicStatsUpdate(stats *discovery.TabletStats) {
	tabletStatsCache := r.tabletStats
	tabletStatsCache.StatsUpdate(stats)
}
