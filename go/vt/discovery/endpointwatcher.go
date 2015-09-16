package discovery

import (
	"time"

	log "github.com/golang/glog"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// NewEndPointWatcher returns an EndPointWatcher and starts refreshing.
func NewEndPointWatcher(topoServer topo.Server, hc HealthCheck, cellsToWatch []string, refreshInterval time.Duration) *EndPointWatcher {
	epw := &EndPointWatcher{
		topoServer:      topoServer,
		hc:              hc,
		cellsToWatch:    cellsToWatch,
		refreshInterval: refreshInterval,
		endPoints:       make(map[string]*pbt.EndPoint),
	}
	epw.start()
	return epw
}

// EndPointWatcher pulls endpoints of all running tablets periodically.
type EndPointWatcher struct {
	topoServer      topo.Server
	hc              HealthCheck
	cellsToWatch    []string
	refreshInterval time.Duration
	cancelFunc      context.CancelFunc
	endPoints       map[string]*pbt.EndPoint
}

// start starts the pulling.
func (epw *EndPointWatcher) start() {
	ctx, cancelFunc := context.WithCancel(context.Background())
	epw.cancelFunc = cancelFunc
	go epw.watch(ctx)
}

// watch pulls all endpoints and notifies HealthCheck by adding/removing endpoints.
func (epw *EndPointWatcher) watch(ctx context.Context) {
	ticker := time.NewTicker(epw.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			watchEndPoints := make(map[string]*pbt.EndPoint)
			for _, cell := range epw.cellsToWatch {
				tabletAlias, err := epw.topoServer.GetTabletsByCell(ctx, cell)
				if err != nil {
					select {
					case <-ctx.Done():
						return
					default:
					}
					log.Errorf("cannot get tablets for cell: %v: %v", cell, err)
					continue
				}
				for _, alias := range tabletAlias {
					tablet, err := epw.topoServer.GetTablet(ctx, alias)
					if err != nil {
						select {
						case <-ctx.Done():
							return
						default:
						}
						log.Errorf("cannot get tablets for cell %v: %v", cell, err)
						continue
					}
					endPoint, err := topo.TabletEndPoint(tablet.Tablet)
					if err != nil {
						log.Errorf("cannot get endpoint from tablet %v: %v", tablet, err)
					}
					key := endPointToMapKey(endPoint)
					if _, ok := epw.endPoints[key]; !ok {
						// new endpoint
						epw.endPoints[key] = endPoint
						epw.hc.AddEndPoint(cell, endPoint)
					}
					watchEndPoints[key] = endPoint
				}
			}
			oldEndPoints := make(map[string]*pbt.EndPoint)
			for key, ep := range epw.endPoints {
				if _, ok := watchEndPoints[key]; !ok {
					oldEndPoints[key] = ep
				}
			}
			for key, ep := range oldEndPoints {
				delete(epw.endPoints, key)
				epw.hc.RemoveEndPoint(ep)
			}
		}
	}
}

// Stop stops the watcher.
func (epw *EndPointWatcher) Stop() {
	epw.cancelFunc()
}
