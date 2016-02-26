package discovery

import (
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// NewCellTabletsWatcher returns a TopologyWatcher that monitors all
// the tablets in a cell, and starts refreshing.
func NewCellTabletsWatcher(topoServer topo.Server, hc HealthCheck, cell string, refreshInterval time.Duration, topoReadConcurrency int) *TopologyWatcher {
	return NewTopologyWatcher(topoServer, hc, cell, refreshInterval, topoReadConcurrency, func(tw *TopologyWatcher) ([]*topodatapb.TabletAlias, error) {
		return tw.topoServer.GetTabletsByCell(tw.ctx, tw.cell)
	})
}

// NewShardReplicationWatcher returns a TopologyWatcher that
// monitors the tablets in a cell/keyspace/shard, and starts refreshing.
func NewShardReplicationWatcher(topoServer topo.Server, hc HealthCheck, cell, keyspace, shard string, refreshInterval time.Duration, topoReadConcurrency int) *TopologyWatcher {
	return NewTopologyWatcher(topoServer, hc, cell, refreshInterval, topoReadConcurrency, func(tw *TopologyWatcher) ([]*topodatapb.TabletAlias, error) {
		sri, err := tw.topoServer.GetShardReplication(tw.ctx, tw.cell, keyspace, shard)
		switch err {
		case nil:
			// we handle this case after this switch block
		case topo.ErrNoNode:
			// this is not an error
			return nil, nil
		default:
			return nil, err
		}

		result := make([]*topodatapb.TabletAlias, len(sri.Nodes))
		for i, node := range sri.Nodes {
			result[i] = node.TabletAlias
		}
		return result, nil
	})
}

// tabletEndPoint is used internally by the TopologyWatcher class
type tabletEndPoint struct {
	alias    string
	endPoint *topodatapb.EndPoint
}

// TopologyWatcher pulls endpoints from a configurable set of tablets
// periodically.
type TopologyWatcher struct {
	// set at construction time
	topoServer      topo.Server
	hc              HealthCheck
	cell            string
	refreshInterval time.Duration
	getTablets      func(tw *TopologyWatcher) ([]*topodatapb.TabletAlias, error)
	sem             chan int
	ctx             context.Context
	cancelFunc      context.CancelFunc

	// mu protects all variables below
	mu        sync.Mutex
	endPoints map[string]*tabletEndPoint
}

// NewTopologyWatcher returns a TopologyWatcher that monitors all
// the tablets in a cell, and starts refreshing.
func NewTopologyWatcher(topoServer topo.Server, hc HealthCheck, cell string, refreshInterval time.Duration, topoReadConcurrency int, getTablets func(tw *TopologyWatcher) ([]*topodatapb.TabletAlias, error)) *TopologyWatcher {
	tw := &TopologyWatcher{
		topoServer:      topoServer,
		hc:              hc,
		cell:            cell,
		refreshInterval: refreshInterval,
		getTablets:      getTablets,
		sem:             make(chan int, topoReadConcurrency),
		endPoints:       make(map[string]*tabletEndPoint),
	}
	tw.ctx, tw.cancelFunc = context.WithCancel(context.Background())
	go tw.watch()
	return tw
}

// watch pulls all endpoints and notifies HealthCheck by adding/removing endpoints.
func (tw *TopologyWatcher) watch() {
	ticker := time.NewTicker(tw.refreshInterval)
	defer ticker.Stop()
	for {
		tw.loadTablets()
		select {
		case <-tw.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// loadTablets reads all tablets from topology, converts to endpoints, and updates HealthCheck.
func (tw *TopologyWatcher) loadTablets() {
	var wg sync.WaitGroup
	newEndPoints := make(map[string]*tabletEndPoint)
	tabletAlias, err := tw.getTablets(tw)
	if err != nil {
		select {
		case <-tw.ctx.Done():
			return
		default:
		}
		log.Errorf("cannot get tablets for cell: %v: %v", tw.cell, err)
		return
	}
	for _, tAlias := range tabletAlias {
		wg.Add(1)
		go func(alias *topodatapb.TabletAlias) {
			defer wg.Done()
			tw.sem <- 1 // Wait for active queue to drain.
			tablet, err := tw.topoServer.GetTablet(tw.ctx, alias)
			<-tw.sem // Done; enable next request to run
			if err != nil {
				select {
				case <-tw.ctx.Done():
					return
				default:
				}
				log.Errorf("cannot get tablet for alias %v: %v", alias, err)
				return
			}
			endPoint, err := topo.TabletEndPoint(tablet.Tablet)
			if err != nil {
				log.Errorf("cannot get endpoint from tablet %v: %v", tablet, err)
				return
			}
			key := EndPointToMapKey(endPoint)
			tw.mu.Lock()
			newEndPoints[key] = &tabletEndPoint{
				alias:    topoproto.TabletAliasString(alias),
				endPoint: endPoint,
			}
			tw.mu.Unlock()
		}(tAlias)
	}

	wg.Wait()
	tw.mu.Lock()
	for key, tep := range newEndPoints {
		if _, ok := tw.endPoints[key]; !ok {
			tw.hc.AddEndPoint(tw.cell, tep.alias, tep.endPoint)
		}
	}
	for key, tep := range tw.endPoints {
		if _, ok := newEndPoints[key]; !ok {
			tw.hc.RemoveEndPoint(tep.endPoint)
		}
	}
	tw.endPoints = newEndPoints
	tw.mu.Unlock()
}

// Stop stops the watcher. It does not clean up the endpoints added to HealthCheck.
func (tw *TopologyWatcher) Stop() {
	tw.cancelFunc()
}
