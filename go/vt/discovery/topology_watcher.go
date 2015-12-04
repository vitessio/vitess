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
	ctw := &TopologyWatcher{
		topoServer:      topoServer,
		hc:              hc,
		cell:            cell,
		refreshInterval: refreshInterval,
		getTablets:      getTablets,
		sem:             make(chan int, topoReadConcurrency),
		endPoints:       make(map[string]*tabletEndPoint),
	}
	ctw.ctx, ctw.cancelFunc = context.WithCancel(context.Background())
	go ctw.watch()
	return ctw
}

// watch pulls all endpoints and notifies HealthCheck by adding/removing endpoints.
func (ctw *TopologyWatcher) watch() {
	ticker := time.NewTicker(ctw.refreshInterval)
	defer ticker.Stop()
	for {
		ctw.loadTablets()
		select {
		case <-ctw.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// loadTablets reads all tablets from topology, converts to endpoints, and updates HealthCheck.
func (ctw *TopologyWatcher) loadTablets() {
	var wg sync.WaitGroup
	newEndPoints := make(map[string]*tabletEndPoint)
	tabletAlias, err := ctw.getTablets(ctw)
	if err != nil {
		select {
		case <-ctw.ctx.Done():
			return
		default:
		}
		log.Errorf("cannot get tablets for cell: %v: %v", ctw.cell, err)
		return
	}
	for _, tAlias := range tabletAlias {
		wg.Add(1)
		go func(alias *topodatapb.TabletAlias) {
			defer wg.Done()
			ctw.sem <- 1 // Wait for active queue to drain.
			tablet, err := ctw.topoServer.GetTablet(ctw.ctx, alias)
			<-ctw.sem // Done; enable next request to run
			if err != nil {
				select {
				case <-ctw.ctx.Done():
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
			ctw.mu.Lock()
			newEndPoints[key] = &tabletEndPoint{
				alias:    topoproto.TabletAliasString(alias),
				endPoint: endPoint,
			}
			ctw.mu.Unlock()
		}(tAlias)
	}

	wg.Wait()
	ctw.mu.Lock()
	for key, tep := range newEndPoints {
		if _, ok := ctw.endPoints[key]; !ok {
			ctw.hc.AddEndPoint(ctw.cell, tep.alias, tep.endPoint)
		}
	}
	for key, tep := range ctw.endPoints {
		if _, ok := newEndPoints[key]; !ok {
			ctw.hc.RemoveEndPoint(tep.endPoint)
		}
	}
	ctw.endPoints = newEndPoints
	ctw.mu.Unlock()
}

// Stop stops the watcher. It does not clean up the endpoints added to HealthCheck.
func (ctw *TopologyWatcher) Stop() {
	ctw.cancelFunc()
}
