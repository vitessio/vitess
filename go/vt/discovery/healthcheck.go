package discovery

import (
	"fmt"
	"html/template"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

var (
	hcConnCounters  *stats.MultiCountersFunc
	hcErrorCounters *stats.MultiCounters
)

func init() {
	hcErrorCounters = stats.NewMultiCounters("HealthcheckErrors", []string{"keyspace", "shardname", "tablettype"})
}

// HealthCheckStatsListener is the listener to receive health check stats update.
type HealthCheckStatsListener interface {
	StatsUpdate(*EndPointStats)
}

// EndPointStats is returned when getting the set of endpoints.
type EndPointStats struct {
	EndPoint                            *topodatapb.EndPoint
	Name                                string // name is an optional tag (e.g. alternative address)
	Cell                                string
	Target                              *querypb.Target
	Up                                  bool // whether the endpoint is added
	Serving                             bool // whether the server is serving
	TabletExternallyReparentedTimestamp int64
	Stats                               *querypb.RealtimeStats
	LastError                           error
}

// HealthCheck defines the interface of health checking module.
type HealthCheck interface {
	// SetListener sets the listener for healthcheck updates. It should not block.
	SetListener(listener HealthCheckStatsListener)
	// AddEndPoint adds the endpoint, and starts health check.
	AddEndPoint(cell, name string, endPoint *topodatapb.EndPoint)
	// RemoveEndPoint removes the endpoint, and stops the health check.
	RemoveEndPoint(endPoint *topodatapb.EndPoint)
	// GetEndPointStatsFromKeyspaceShard returns all EndPointStats for the given keyspace/shard.
	GetEndPointStatsFromKeyspaceShard(keyspace, shard string) []*EndPointStats
	// GetEndPointStatsFromTarget returns all EndPointStats for the given target.
	GetEndPointStatsFromTarget(keyspace, shard string, tabletType topodatapb.TabletType) []*EndPointStats
	// GetConnection returns the TabletConn of the given endpoint.
	GetConnection(endPoint *topodatapb.EndPoint) tabletconn.TabletConn
	// CacheStatus returns a displayable version of the cache.
	CacheStatus() EndPointsCacheStatusList
	// Close stops the healthcheck.
	Close() error
}

// NewHealthCheck creates a new HealthCheck object.
func NewHealthCheck(connTimeout time.Duration, retryDelay time.Duration, healthCheckTimeout time.Duration, statsSuffix string) HealthCheck {
	hc := &HealthCheckImpl{
		addrToConns:        make(map[string]*healthCheckConn),
		targetToEPs:        make(map[string]map[string]map[topodatapb.TabletType][]*topodatapb.EndPoint),
		connTimeout:        connTimeout,
		retryDelay:         retryDelay,
		healthCheckTimeout: healthCheckTimeout,
		closeChan:          make(chan struct{}),
	}
	if hcConnCounters == nil {
		hcConnCounters = stats.NewMultiCountersFunc("HealthcheckConnections"+statsSuffix, []string{"keyspace", "shardname", "tablettype"}, hc.servingConnStats)
	}
	go func() {
		// Start another go routine to check timeout.
		// Currently vttablet sends healthcheck response every 20 seconds.
		// We set the default timeout to 1 minute (20s * 3),
		// and also perform the timeout check in sync with vttablet frequency.
		// When we change the healthcheck frequency on vttablet,
		// we should also adjust here.
		t := time.NewTicker(healthCheckTimeout / 3)
		defer t.Stop()
		for {
			select {
			case <-hc.closeChan:
				return
			case _, ok := <-t.C:
				if !ok {
					// the ticker stoped
					return
				}
				hc.checkHealthCheckTimeout()
			}
		}
	}()
	return hc
}

// HealthCheckImpl performs health checking and notifies downstream components about any changes.
type HealthCheckImpl struct {
	// set at construction time
	listener           HealthCheckStatsListener
	connTimeout        time.Duration
	retryDelay         time.Duration
	healthCheckTimeout time.Duration
	closeChan          chan struct{} // signals the process gorouting to terminate

	// mu protects all the following fields
	// when locking both mutex from HealthCheck and healthCheckConn, HealthCheck.mu goes first.
	mu          sync.RWMutex
	addrToConns map[string]*healthCheckConn                                            // addrToConns maps from address to the healthCheckConn object.
	targetToEPs map[string]map[string]map[topodatapb.TabletType][]*topodatapb.EndPoint // targetToEPs maps from keyspace/shard/tablettype to a list of endpoints.
}

// healthCheckConn contains details about an endpoint.
type healthCheckConn struct {
	// set at construction time
	cell       string
	name       string
	ctx        context.Context
	cancelFunc context.CancelFunc
	endPoint   *topodatapb.EndPoint

	// mu protects all the following fields
	// when locking both mutex from HealthCheck and healthCheckConn, HealthCheck.mu goes first.
	mu                                  sync.RWMutex
	conn                                tabletconn.TabletConn
	target                              *querypb.Target
	up                                  bool
	serving                             bool
	tabletExternallyReparentedTimestamp int64
	stats                               *querypb.RealtimeStats
	lastError                           error
	lastResponseTimestamp               time.Time // timestamp of the last healthcheck response
}

// servingConnStats returns the number of serving endpoints per keyspace/shard/tablet type.
func (hc *HealthCheckImpl) servingConnStats() map[string]int64 {
	res := make(map[string]int64)
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	for _, hcc := range hc.addrToConns {
		hcc.mu.RLock()
		if !hcc.up || !hcc.serving || hcc.lastError != nil {
			hcc.mu.RUnlock()
			continue
		}
		key := fmt.Sprintf("%s.%s.%s", hcc.target.Keyspace, hcc.target.Shard, strings.ToLower(hcc.target.TabletType.String()))
		hcc.mu.RUnlock()
		res[key]++
	}
	return res
}

// checkConn performs health checking on the given endpoint.
func (hc *HealthCheckImpl) checkConn(hcc *healthCheckConn, cell, name string, endPoint *topodatapb.EndPoint) {
	defer func() {
		hcc.mu.Lock()
		if hcc.conn != nil {
			hcc.conn.Close()
			hcc.conn = nil
		}
		hcc.mu.Unlock()
	}()
	// retry health check if it fails
	for {
		stream, err := hcc.connect(hc, endPoint)
		if err != nil {
			select {
			case <-hcc.ctx.Done():
				return
			default:
			}
			hcc.mu.Lock()
			hcc.serving = false
			hcc.lastError = err
			target := hcc.target
			hcc.mu.Unlock()
			hcErrorCounters.Add([]string{target.Keyspace, target.Shard, strings.ToLower(target.TabletType.String())}, 1)
			time.Sleep(hc.retryDelay)
			continue
		}
		for {
			reconnect, err := hcc.processResponse(hc, endPoint, stream)
			if err != nil {
				hcc.mu.Lock()
				hcc.serving = false
				hcc.lastError = err
				eps := &EndPointStats{
					EndPoint: endPoint,
					Cell:     hcc.cell,
					Name:     hcc.name,
					Target:   hcc.target,
					Up:       hcc.up,
					Serving:  hcc.serving,
					Stats:    hcc.stats,
					TabletExternallyReparentedTimestamp: hcc.tabletExternallyReparentedTimestamp,
					LastError:                           hcc.lastError,
				}
				target := hcc.target
				hcc.mu.Unlock()
				// notify downstream for serving status change
				if hc.listener != nil {
					hc.listener.StatsUpdate(eps)
				}
				select {
				case <-hcc.ctx.Done():
					return
				default:
				}
				hcErrorCounters.Add([]string{target.Keyspace, target.Shard, strings.ToLower(target.TabletType.String())}, 1)
				if reconnect {
					hcc.mu.Lock()
					hcc.conn.Close()
					hcc.conn = nil
					hcc.target = &querypb.Target{}
					hcc.mu.Unlock()
					time.Sleep(hc.retryDelay)
					break
				}
			}
		}
	}
}

// connect creates connection to the endpoint and starts streaming.
func (hcc *healthCheckConn) connect(hc *HealthCheckImpl, endPoint *topodatapb.EndPoint) (tabletconn.StreamHealthReader, error) {
	conn, err := tabletconn.GetDialer()(hcc.ctx, endPoint, "" /*keyspace*/, "" /*shard*/, topodatapb.TabletType_RDONLY, hc.connTimeout)
	if err != nil {
		return nil, err
	}
	stream, err := conn.StreamHealth(hcc.ctx)
	if err != nil {
		conn.Close()
		return nil, err
	}
	hcc.mu.Lock()
	hcc.conn = conn
	hcc.lastError = nil
	hcc.mu.Unlock()
	return stream, nil
}

// processResponse reads one health check response, and notifies HealthCheckStatsListener.
// It returns bool to indicate if the caller should reconnect. We do not need to reconnect when the streaming is working.
func (hcc *healthCheckConn) processResponse(hc *HealthCheckImpl, endPoint *topodatapb.EndPoint, stream tabletconn.StreamHealthReader) (bool, error) {
	select {
	case <-hcc.ctx.Done():
		return false, hcc.ctx.Err()
	default:
	}

	shr, err := stream.Recv()
	if err != nil {
		return true, err
	}
	// TODO(liang): remove after bug fix in tablet.
	if shr.Target == nil || shr.RealtimeStats == nil {
		return false, fmt.Errorf("health stats is not valid: %v", shr)
	}

	// an app-level error from tablet, force serving state.
	var healthErr error
	serving := shr.Serving
	if shr.RealtimeStats.HealthError != "" {
		healthErr = fmt.Errorf("vttablet error: %v", shr.RealtimeStats.HealthError)
		serving = false
	}

	if hcc.target.TabletType == topodatapb.TabletType_UNKNOWN {
		// The first time we see response for the endpoint.
		hcc.update(shr, serving, healthErr, true)
		hc.mu.Lock()
		hc.addEndPointToTargetProtected(hcc.target, endPoint)
		hc.mu.Unlock()
	} else if hcc.target.TabletType != shr.Target.TabletType {
		// tablet type changed for the tablet
		log.Infof("HealthCheckUpdate(Type Change): %v, EP: %v/%+v, target %+v => %+v, reparent time: %v", hcc.name, hcc.cell, endPoint, hcc.target, shr.Target, shr.TabletExternallyReparentedTimestamp)
		hc.mu.Lock()
		hc.deleteEndPointFromTargetProtected(hcc.target, endPoint)
		hcc.update(shr, serving, healthErr, true)
		hc.addEndPointToTargetProtected(shr.Target, endPoint)
		hc.mu.Unlock()
	} else {
		hcc.update(shr, serving, healthErr, false)
	}
	// notify downstream for tablettype and realtimestats change
	if hc.listener != nil {
		hcc.mu.RLock()
		eps := &EndPointStats{
			EndPoint: endPoint,
			Cell:     hcc.cell,
			Name:     hcc.name,
			Target:   hcc.target,
			Up:       hcc.up,
			Serving:  hcc.serving,
			Stats:    hcc.stats,
			TabletExternallyReparentedTimestamp: hcc.tabletExternallyReparentedTimestamp,
			LastError:                           hcc.lastError,
		}
		hcc.mu.RUnlock()
		hc.listener.StatsUpdate(eps)
	}
	return false, nil
}

func (hcc *healthCheckConn) update(shr *querypb.StreamHealthResponse, serving bool, healthErr error, setTarget bool) {
	hcc.mu.Lock()
	hcc.lastResponseTimestamp = time.Now()
	hcc.target = shr.Target
	hcc.serving = serving
	hcc.tabletExternallyReparentedTimestamp = shr.TabletExternallyReparentedTimestamp
	hcc.stats = shr.RealtimeStats
	hcc.lastError = healthErr
	if setTarget {
		hcc.conn.SetTarget(hcc.target.Keyspace, hcc.target.Shard, hcc.target.TabletType)
	}
	hcc.mu.Unlock()
}

func (hc *HealthCheckImpl) checkHealthCheckTimeout() {
	hc.mu.RLock()
	list := make([]*healthCheckConn, 0, len(hc.addrToConns))
	for _, hcc := range hc.addrToConns {
		list = append(list, hcc)
	}
	hc.mu.RUnlock()
	for _, hcc := range list {
		hcc.mu.RLock()
		if !hcc.serving {
			// ignore non-serving endpoint
			hcc.mu.RUnlock()
			continue
		}
		if time.Now().Sub(hcc.lastResponseTimestamp) < hc.healthCheckTimeout {
			// received a healthcheck response recently
			hcc.mu.RUnlock()
			continue
		}
		hcc.mu.RUnlock()
		// mark the endpoint non-serving as we have not seen a health check response for a long time
		hcc.mu.Lock()
		// check again to avoid race condition
		if !hcc.serving {
			// ignore non-serving endpoint
			hcc.mu.Unlock()
			continue
		}
		if time.Now().Sub(hcc.lastResponseTimestamp) < hc.healthCheckTimeout {
			// received a healthcheck response recently
			hcc.mu.Unlock()
			continue
		}
		hcc.serving = false
		hcc.lastError = fmt.Errorf("healthcheck timed out (latest %v)", hcc.lastResponseTimestamp)
		eps := &EndPointStats{
			EndPoint: hcc.endPoint,
			Cell:     hcc.cell,
			Name:     hcc.name,
			Target:   hcc.target,
			Up:       hcc.up,
			Serving:  hcc.serving,
			Stats:    hcc.stats,
			TabletExternallyReparentedTimestamp: hcc.tabletExternallyReparentedTimestamp,
			LastError:                           hcc.lastError,
		}
		target := hcc.target
		hcc.mu.Unlock()
		// notify downstream for serving status change
		if hc.listener != nil {
			hc.listener.StatsUpdate(eps)
		}
		hcErrorCounters.Add([]string{target.Keyspace, target.Shard, strings.ToLower(target.TabletType.String())}, 1)
	}
}

func (hc *HealthCheckImpl) deleteConn(endPoint *topodatapb.EndPoint) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	key := EndPointToMapKey(endPoint)
	hcc, ok := hc.addrToConns[key]
	if !ok {
		log.Warningf("deleting unknown endpoint: %+v", endPoint)
		return
	}
	hcc.mu.Lock()
	hcc.up = false
	hcc.mu.Unlock()
	hcc.cancelFunc()
	delete(hc.addrToConns, key)
	hc.deleteEndPointFromTargetProtected(hcc.target, endPoint)
}

// SetListener sets the listener for healthcheck updates. It should not block.
func (hc *HealthCheckImpl) SetListener(listener HealthCheckStatsListener) {
	hc.listener = listener
}

// AddEndPoint adds the endpoint, and starts health check.
// It does not block on making connection.
// name is an optional tag for the endpoint, e.g. an alternative address.
func (hc *HealthCheckImpl) AddEndPoint(cell, name string, endPoint *topodatapb.EndPoint) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	hcc := &healthCheckConn{
		cell:       cell,
		name:       name,
		ctx:        ctx,
		cancelFunc: cancelFunc,
		endPoint:   endPoint,
		target:     &querypb.Target{},
		up:         true,
	}
	key := EndPointToMapKey(endPoint)
	hc.mu.Lock()
	if _, ok := hc.addrToConns[key]; ok {
		hc.mu.Unlock()
		log.Warningf("adding duplicate endpoint %v for %v: %+v", name, cell, endPoint)
		return
	}
	hc.addrToConns[key] = hcc
	hc.mu.Unlock()

	go hc.checkConn(hcc, cell, name, endPoint)
}

// RemoveEndPoint removes the endpoint, and stops the health check.
// It does not block.
func (hc *HealthCheckImpl) RemoveEndPoint(endPoint *topodatapb.EndPoint) {
	go hc.deleteConn(endPoint)
}

// GetEndPointStatsFromKeyspaceShard returns all EndPointStats for the given keyspace/shard.
func (hc *HealthCheckImpl) GetEndPointStatsFromKeyspaceShard(keyspace, shard string) []*EndPointStats {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	shardMap, ok := hc.targetToEPs[keyspace]
	if !ok {
		return nil
	}
	ttMap, ok := shardMap[shard]
	if !ok {
		return nil
	}
	res := make([]*EndPointStats, 0, 1)
	for _, epList := range ttMap {
		for _, ep := range epList {
			key := EndPointToMapKey(ep)
			hcc, ok := hc.addrToConns[key]
			if !ok {
				continue
			}
			hcc.mu.RLock()
			eps := &EndPointStats{
				EndPoint: ep,
				Cell:     hcc.cell,
				Name:     hcc.name,
				Target:   hcc.target,
				Up:       hcc.up,
				Serving:  hcc.serving,
				Stats:    hcc.stats,
				TabletExternallyReparentedTimestamp: hcc.tabletExternallyReparentedTimestamp,
				LastError:                           hcc.lastError,
			}
			hcc.mu.RUnlock()
			res = append(res, eps)
		}
	}
	return res
}

// GetEndPointStatsFromTarget returns all EndPointStats for the given target.
func (hc *HealthCheckImpl) GetEndPointStatsFromTarget(keyspace, shard string, tabletType topodatapb.TabletType) []*EndPointStats {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	shardMap, ok := hc.targetToEPs[keyspace]
	if !ok {
		return nil
	}
	ttMap, ok := shardMap[shard]
	if !ok {
		return nil
	}
	epList, ok := ttMap[tabletType]
	if !ok {
		return nil
	}
	res := make([]*EndPointStats, 0, 1)
	for _, ep := range epList {
		key := EndPointToMapKey(ep)
		hcc, ok := hc.addrToConns[key]
		if !ok {
			continue
		}
		hcc.mu.RLock()
		eps := &EndPointStats{
			EndPoint: ep,
			Cell:     hcc.cell,
			Name:     hcc.name,
			Target:   hcc.target,
			Up:       hcc.up,
			Serving:  hcc.serving,
			Stats:    hcc.stats,
			TabletExternallyReparentedTimestamp: hcc.tabletExternallyReparentedTimestamp,
			LastError:                           hcc.lastError,
		}
		hcc.mu.RUnlock()
		res = append(res, eps)
	}
	return res
}

// GetConnection returns the TabletConn of the given endpoint.
func (hc *HealthCheckImpl) GetConnection(endPoint *topodatapb.EndPoint) tabletconn.TabletConn {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	hcc := hc.addrToConns[EndPointToMapKey(endPoint)]
	if hcc == nil {
		return nil
	}
	hcc.mu.RLock()
	defer hcc.mu.RUnlock()
	return hcc.conn
}

// addEndPointToTargetProtected adds the endpoint to the given target.
// LOCK_REQUIRED hc.mu
func (hc *HealthCheckImpl) addEndPointToTargetProtected(target *querypb.Target, endPoint *topodatapb.EndPoint) {
	shardMap, ok := hc.targetToEPs[target.Keyspace]
	if !ok {
		shardMap = make(map[string]map[topodatapb.TabletType][]*topodatapb.EndPoint)
		hc.targetToEPs[target.Keyspace] = shardMap
	}
	ttMap, ok := shardMap[target.Shard]
	if !ok {
		ttMap = make(map[topodatapb.TabletType][]*topodatapb.EndPoint)
		shardMap[target.Shard] = ttMap
	}
	epList, ok := ttMap[target.TabletType]
	if !ok {
		epList = make([]*topodatapb.EndPoint, 0, 1)
	}
	for _, ep := range epList {
		if topo.EndPointEquality(ep, endPoint) {
			log.Warningf("endpoint is already added: %+v", endPoint)
			return
		}
	}
	ttMap[target.TabletType] = append(epList, endPoint)
}

// deleteEndPointFromTargetProtected deletes the endpoint for the given target.
// LOCK_REQUIRED hc.mu
func (hc *HealthCheckImpl) deleteEndPointFromTargetProtected(target *querypb.Target, endPoint *topodatapb.EndPoint) {
	shardMap, ok := hc.targetToEPs[target.Keyspace]
	if !ok {
		return
	}
	ttMap, ok := shardMap[target.Shard]
	if !ok {
		return
	}
	epList, ok := ttMap[target.TabletType]
	if !ok {
		return
	}
	for i, ep := range epList {
		if topo.EndPointEquality(ep, endPoint) {
			epList = append(epList[:i], epList[i+1:]...)
			ttMap[target.TabletType] = epList
			return
		}
	}
}

// EndPointsCacheStatus is the current endpoints for a cell/target.
// TODO: change this to reflect the e2e information about the endpoints.
type EndPointsCacheStatus struct {
	Cell           string
	Target         *querypb.Target
	EndPointsStats EndPointStatsList
}

// EndPointStatsList is used for sorting.
type EndPointStatsList []*EndPointStats

// Len is part of sort.Interface.
func (epsl EndPointStatsList) Len() int {
	return len(epsl)
}

// Less is part of sort.Interface
func (epsl EndPointStatsList) Less(i, j int) bool {
	name1 := epsl[i].Name
	if name1 == "" {
		name1 = EndPointToMapKey(epsl[i].EndPoint)
	}
	name2 := epsl[j].Name
	if name2 == "" {
		name2 = EndPointToMapKey(epsl[j].EndPoint)
	}
	return name1 < name2
}

// Swap is part of sort.Interface
func (epsl EndPointStatsList) Swap(i, j int) {
	epsl[i], epsl[j] = epsl[j], epsl[i]
}

// StatusAsHTML returns an HTML version of the status.
func (epcs *EndPointsCacheStatus) StatusAsHTML() template.HTML {
	epLinks := make([]string, 0, 1)
	if epcs.EndPointsStats != nil {
		sort.Sort(epcs.EndPointsStats)
	}
	for _, eps := range epcs.EndPointsStats {
		vtPort := eps.EndPoint.PortMap["vt"]
		color := "green"
		extra := ""
		if eps.LastError != nil {
			color = "red"
			extra = fmt.Sprintf(" (%v)", eps.LastError)
		} else if !eps.Serving {
			color = "red"
			extra = " (Not Serving)"
		} else if !eps.Up {
			color = "red"
			extra = " (Down)"
		} else if eps.Target.TabletType == topodatapb.TabletType_MASTER {
			extra = fmt.Sprintf(" (MasterTS: %v)", eps.TabletExternallyReparentedTimestamp)
		} else {
			extra = fmt.Sprintf(" (RepLag: %v)", eps.Stats.SecondsBehindMaster)
		}
		name := eps.Name
		if name == "" {
			name = fmt.Sprintf("%v:%d", eps.EndPoint.Host, vtPort)
		}
		epLinks = append(epLinks, fmt.Sprintf(`<a href="http://%v:%d" style="color:%v">%v</a>%v`, eps.EndPoint.Host, vtPort, color, name, extra))
	}
	return template.HTML(strings.Join(epLinks, "<br>"))
}

// EndPointsCacheStatusList is used for sorting.
type EndPointsCacheStatusList []*EndPointsCacheStatus

// Len is part of sort.Interface.
func (epcsl EndPointsCacheStatusList) Len() int {
	return len(epcsl)
}

// Less is part of sort.Interface
func (epcsl EndPointsCacheStatusList) Less(i, j int) bool {
	return epcsl[i].Cell+"."+epcsl[i].Target.Keyspace+"."+epcsl[i].Target.Shard+"."+string(epcsl[i].Target.TabletType) <
		epcsl[j].Cell+"."+epcsl[j].Target.Keyspace+"."+epcsl[j].Target.Shard+"."+string(epcsl[j].Target.TabletType)
}

// Swap is part of sort.Interface
func (epcsl EndPointsCacheStatusList) Swap(i, j int) {
	epcsl[i], epcsl[j] = epcsl[j], epcsl[i]
}

// CacheStatus returns a displayable version of the cache.
func (hc *HealthCheckImpl) CacheStatus() EndPointsCacheStatusList {
	epcsMap := make(map[string]*EndPointsCacheStatus)
	hc.mu.RLock()
	for _, hcc := range hc.addrToConns {
		hcc.mu.RLock()
		key := fmt.Sprintf("%v.%v.%v.%v", hcc.cell, hcc.target.Keyspace, hcc.target.Shard, string(hcc.target.TabletType))
		var epcs *EndPointsCacheStatus
		var ok bool
		if epcs, ok = epcsMap[key]; !ok {
			epcs = &EndPointsCacheStatus{
				Cell:   hcc.cell,
				Target: hcc.target,
			}
			epcsMap[key] = epcs
		}
		stats := &EndPointStats{
			Cell:     hcc.cell,
			Name:     hcc.name,
			Target:   hcc.target,
			Up:       hcc.up,
			Serving:  hcc.serving,
			EndPoint: hcc.endPoint,
			Stats:    hcc.stats,
			TabletExternallyReparentedTimestamp: hcc.tabletExternallyReparentedTimestamp,
			LastError:                           hcc.lastError,
		}
		hcc.mu.RUnlock()
		epcs.EndPointsStats = append(epcs.EndPointsStats, stats)
	}
	hc.mu.RUnlock()
	epcsl := make(EndPointsCacheStatusList, 0, len(epcsMap))
	for _, epcs := range epcsMap {
		epcsl = append(epcsl, epcs)
	}
	sort.Sort(epcsl)
	return epcsl
}

// Close stops the healthcheck.
func (hc *HealthCheckImpl) Close() error {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	close(hc.closeChan)
	hc.listener = nil
	for _, hcc := range hc.addrToConns {
		hcc.cancelFunc()
	}
	hc.addrToConns = make(map[string]*healthCheckConn)
	hc.targetToEPs = make(map[string]map[string]map[topodatapb.TabletType][]*topodatapb.EndPoint)
	return nil
}

// EndPointToMapKey creates a key to the map from endpoint's host and ports.
// It should only be used in discovery and related module.
func EndPointToMapKey(endPoint *topodatapb.EndPoint) string {
	parts := make([]string, 0, 1)
	for name, port := range endPoint.PortMap {
		parts = append(parts, name+":"+fmt.Sprintf("%d", port))
	}
	sort.Strings(parts)
	parts = append([]string{endPoint.Host}, parts...)
	return strings.Join(parts, ":")
}
