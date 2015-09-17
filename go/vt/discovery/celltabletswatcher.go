package discovery

import (
	"sync"
	"time"

	log "github.com/golang/glog"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// NewCellTabletsWatcher returns a CellTabletsWatcher and starts refreshing.
func NewCellTabletsWatcher(topoServer topo.Server, hc HealthCheck, cell string, refreshInterval time.Duration, topoReadConcurrency int) *CellTabletsWatcher {
	ctw := &CellTabletsWatcher{
		topoServer:      topoServer,
		hc:              hc,
		cell:            cell,
		refreshInterval: refreshInterval,
		sem:             make(chan int, topoReadConcurrency),
		endPoints:       make(map[string]*pbt.EndPoint),
	}
	ctw.ctx, ctw.cancelFunc = context.WithCancel(context.Background())
	go ctw.watch()
	return ctw
}

// CellTabletsWatcher pulls endpoints of all running tablets periodically.
type CellTabletsWatcher struct {
	// set at construction time
	topoServer      topo.Server
	hc              HealthCheck
	cell            string
	refreshInterval time.Duration
	sem             chan int
	ctx             context.Context
	cancelFunc      context.CancelFunc

	// mu protects all variables below
	mu        sync.Mutex
	endPoints map[string]*pbt.EndPoint
}

// watch pulls all endpoints and notifies HealthCheck by adding/removing endpoints.
func (ctw *CellTabletsWatcher) watch() {
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
func (ctw *CellTabletsWatcher) loadTablets() {
	var wg sync.WaitGroup
	newEndPoints := make(map[string]*pbt.EndPoint)
	tabletAlias, err := ctw.topoServer.GetTabletsByCell(ctw.ctx, ctw.cell)
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
		go func(alias *pbt.TabletAlias) {
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
				log.Errorf("cannot get tablets for cell %v: %v", ctw.cell, err)
				return
			}
			endPoint, err := topo.TabletEndPoint(tablet.Tablet)
			if err != nil {
				log.Errorf("cannot get endpoint from tablet %v: %v", tablet, err)
				return
			}
			key := endPointToMapKey(endPoint)
			ctw.mu.Lock()
			newEndPoints[key] = endPoint
			ctw.mu.Unlock()
		}(tAlias)
	}

	wg.Wait()
	ctw.mu.Lock()
	for key, ep := range newEndPoints {
		if _, ok := ctw.endPoints[key]; !ok {
			ctw.hc.AddEndPoint(ctw.cell, ep)
		}
	}
	for key, ep := range ctw.endPoints {
		if _, ok := newEndPoints[key]; !ok {
			ctw.hc.RemoveEndPoint(ep)
		}
	}
	ctw.endPoints = newEndPoints
	ctw.mu.Unlock()
}

// Stop stops the watcher. It does not clean up the endpoints added to HealthCheck.
func (ctw *CellTabletsWatcher) Stop() {
	ctw.cancelFunc()
}
