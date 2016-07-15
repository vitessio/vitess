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

// tabletInfo is used internally by the TopologyWatcher class
type tabletInfo struct {
	alias  string
	tablet *topodatapb.Tablet
}

// TopologyWatcher polls tablet from a configurable set of tablets
// periodically. When tablets are added / removed, it calls
// the HealthCheck AddTablet / RemoveTablet interface appropriately.
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
	// wg keeps track of all launched Go routines.
	wg sync.WaitGroup

	// mu protects all variables below
	mu      sync.Mutex
	tablets map[string]*tabletInfo
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
		tablets:         make(map[string]*tabletInfo),
	}
	tw.ctx, tw.cancelFunc = context.WithCancel(context.Background())
	tw.wg.Add(1)
	go tw.watch()
	return tw
}

// watch polls all tablets and notifies HealthCheck by adding/removing tablets.
func (tw *TopologyWatcher) watch() {
	defer tw.wg.Done()
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

// loadTablets reads all tablets from topology, and updates HealthCheck.
func (tw *TopologyWatcher) loadTablets() {
	var wg sync.WaitGroup
	newTablets := make(map[string]*tabletInfo)
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
			key := TabletToMapKey(tablet.Tablet)
			tw.mu.Lock()
			newTablets[key] = &tabletInfo{
				alias:  topoproto.TabletAliasString(alias),
				tablet: tablet.Tablet,
			}
			tw.mu.Unlock()
		}(tAlias)
	}

	wg.Wait()
	tw.mu.Lock()
	for key, tep := range newTablets {
		if _, ok := tw.tablets[key]; !ok {
			tw.hc.AddTablet(tw.cell, tep.alias, tep.tablet)
		}
	}
	for key, tep := range tw.tablets {
		if _, ok := newTablets[key]; !ok {
			tw.hc.RemoveTablet(tep.tablet)
		}
	}
	tw.tablets = newTablets
	tw.mu.Unlock()
}

// Stop stops the watcher. It does not clean up the tablets added to HealthCheck.
func (tw *TopologyWatcher) Stop() {
	tw.cancelFunc()
	// wait for watch goroutine to finish.
	tw.wg.Wait()
}
