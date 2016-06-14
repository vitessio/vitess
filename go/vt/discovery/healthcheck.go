// Package discovery provides a way to discover all tablets e.g. within a
// specific shard and monitor their current health.
//
// Use the HealthCheck object to query for tablets and their health.
//
// For an example how to use the HealthCheck object, see worker/topo_utils.go.
//
// Tablets have to be manually added to the HealthCheck using AddTablet().
// Alternatively, use a Watcher implementation which will constantly watch
// a source (e.g. the topology) and add and remove tablets as they are
// added or removed from the source.
// For a Watcher example have a look at NewShardReplicationWatcher().
//
// Note that the getter functions GetTabletStatsFrom* will always return
// an unfiltered list of all known tablets.
// Use the helper functions in utils.go to filter them e.g.
// RemoveUnhealthyTablets() or GetCurrentMaster().
// replicationlag.go contains a more advanced health filter which is used by
// vtgate.
//
// Internally, the HealthCheck module is connected to each tablet and has a
// streaming RPC (StreamHealth) open to receive periodic health infos.
package discovery

import (
	"fmt"
	"html/template"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/stats"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

var (
	hcErrorCounters *stats.MultiCounters
)

const (
	// DefaultTopoReadConcurrency can be used as default value for the topoReadConcurrency parameter of a TopologyWatcher.
	DefaultTopoReadConcurrency int = 5
)

func init() {
	hcErrorCounters = stats.NewMultiCounters("HealthcheckErrors", []string{"keyspace", "shardname", "tablettype"})
}

// HealthCheckStatsListener is the listener to receive health check stats update.
type HealthCheckStatsListener interface {
	StatsUpdate(*TabletStats)
}

// TabletStats is returned when getting the set of tablets.
type TabletStats struct {
	Tablet                              *topodatapb.Tablet
	Name                                string // name is an optional tag (e.g. alternative address)
	Target                              *querypb.Target
	Up                                  bool // whether the tablet is added
	Serving                             bool // whether the server is serving
	TabletExternallyReparentedTimestamp int64
	Stats                               *querypb.RealtimeStats
	LastError                           error
}

// String is defined because we want to print a []*TabletStats array nicely.
func (e *TabletStats) String() string {
	return fmt.Sprint(*e)
}

// HealthCheck defines the interface of health checking module.
type HealthCheck interface {
	// RegisterStats registers the connection counts stats.
	// It can only be called on one Healthcheck object per process.
	RegisterStats()
	// SetListener sets the listener for healthcheck updates. It should not block.
	// Note that the default implementation requires to set the listener before
	// any tablets are added to the healthcheck.
	SetListener(listener HealthCheckStatsListener)
	// AddTablet adds the tablet, and starts health check.
	AddTablet(cell, name string, tablet *topodatapb.Tablet)
	// RemoveTablet removes the tablet, and stops the health check.
	RemoveTablet(tablet *topodatapb.Tablet)
	// GetTabletStatsFromKeyspaceShard returns all TabletStats for the given keyspace/shard.
	GetTabletStatsFromKeyspaceShard(keyspace, shard string) []*TabletStats
	// GetTabletStatsFromTarget returns all TabletStats for the given target.
	// You can exclude unhealthy entries using the helper in utils.go.
	GetTabletStatsFromTarget(keyspace, shard string, tabletType topodatapb.TabletType) []*TabletStats
	// GetConnection returns the TabletConn of the given tablet.
	GetConnection(tablet *topodatapb.Tablet) tabletconn.TabletConn
	// CacheStatus returns a displayable version of the cache.
	CacheStatus() TabletsCacheStatusList
	// Close stops the healthcheck.
	Close() error
}

// NewHealthCheck creates a new HealthCheck object.
func NewHealthCheck(connTimeout time.Duration, retryDelay time.Duration, healthCheckTimeout time.Duration) HealthCheck {
	hc := &HealthCheckImpl{
		addrToConns:        make(map[string]*healthCheckConn),
		targetToTablets:    make(map[string]map[string]map[topodatapb.TabletType][]*topodatapb.Tablet),
		connTimeout:        connTimeout,
		retryDelay:         retryDelay,
		healthCheckTimeout: healthCheckTimeout,
		closeChan:          make(chan struct{}),
	}

	hc.wg.Add(1)
	go func() {
		defer hc.wg.Done()
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
	// Immutable fields set at construction time.
	listener           HealthCheckStatsListener
	connTimeout        time.Duration
	retryDelay         time.Duration
	healthCheckTimeout time.Duration
	closeChan          chan struct{} // signals the process gorouting to terminate
	// wg keeps track of all launched Go routines.
	wg sync.WaitGroup

	// mu protects all the following fields.
	// When locking both mutex from HealthCheck and healthCheckConn,
	// HealthCheck.mu goes first.
	mu sync.RWMutex

	// addrToConns maps from address to the healthCheckConn object.
	addrToConns map[string]*healthCheckConn

	// targetToTablets maps from keyspace/shard/tablettype to a
	// list of tablets.
	targetToTablets map[string]map[string]map[topodatapb.TabletType][]*topodatapb.Tablet
}

// healthCheckConn contains details about a tablet.
type healthCheckConn struct {
	// set at construction time
	cell       string
	name       string
	ctx        context.Context
	cancelFunc context.CancelFunc
	tablet     *topodatapb.Tablet

	// mu protects all the following fields.
	// When locking both mutex from HealthCheck and healthCheckConn,
	// HealthCheck.mu goes first.
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

// RegisterStats registers the connection counts stats
func (hc *HealthCheckImpl) RegisterStats() {
	stats.NewMultiCountersFunc("HealthcheckConnections", []string{"keyspace", "shardname", "tablettype"}, hc.servingConnStats)
}

// servingConnStats returns the number of serving tablets per keyspace/shard/tablet type.
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

// checkConn performs health checking on the given tablet.
func (hc *HealthCheckImpl) checkConn(hcc *healthCheckConn, cell, name string, tablet *topodatapb.Tablet) {
	defer hc.wg.Done()
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
		// Try to connect to the tablet.
		stream, err := hcc.connect(hc, tablet)
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

			// Sleep until the next retry is up or the context is done/canceled.
			select {
			case <-hcc.ctx.Done():
			case <-time.After(hc.retryDelay):
			}
			continue
		}

		// Read stream health responses.
		for {
			reconnect, err := hcc.processResponse(hc, tablet, stream)
			if err != nil {
				hcc.mu.Lock()
				hcc.serving = false
				hcc.lastError = err
				ts := &TabletStats{
					Tablet:  tablet,
					Name:    hcc.name,
					Target:  hcc.target,
					Up:      hcc.up,
					Serving: hcc.serving,
					Stats:   hcc.stats,
					TabletExternallyReparentedTimestamp: hcc.tabletExternallyReparentedTimestamp,
					LastError:                           hcc.lastError,
				}
				target := hcc.target
				hcc.mu.Unlock()
				// notify downstream for serving status change
				if hc.listener != nil {
					hc.listener.StatsUpdate(ts)
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

					// Sleep until the next retry is up or the context is done/canceled.
					select {
					case <-hcc.ctx.Done():
					case <-time.After(hc.retryDelay):
					}
					break
				}
			}
		}
	}
}

// connect creates connection to the tablet and starts streaming.
func (hcc *healthCheckConn) connect(hc *HealthCheckImpl, tablet *topodatapb.Tablet) (tabletconn.StreamHealthReader, error) {
	// Keyspace, shard and tabletType are the ones from the tablet
	// record, but they won't be used just yet.
	conn, err := tabletconn.GetDialer()(hcc.ctx, tablet, hc.connTimeout)
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
func (hcc *healthCheckConn) processResponse(hc *HealthCheckImpl, tablet *topodatapb.Tablet, stream tabletconn.StreamHealthReader) (bool, error) {
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
		// The first time we see response for the tablet.
		hcc.update(shr, serving, healthErr, true)
		hc.mu.Lock()
		hc.addTabletToTargetProtected(hcc.target, tablet)
		hc.mu.Unlock()
	} else if hcc.target.TabletType != shr.Target.TabletType {
		// tablet type changed for the tablet
		log.Infof("HealthCheckUpdate(Type Change): %v, tablet: %v/%+v, target %+v => %+v, reparent time: %v", hcc.name, hcc.cell, tablet, hcc.target, shr.Target, shr.TabletExternallyReparentedTimestamp)
		hc.mu.Lock()
		hc.deleteTabletFromTargetProtected(hcc.target, tablet)
		hcc.update(shr, serving, healthErr, true)
		hc.addTabletToTargetProtected(shr.Target, tablet)
		hc.mu.Unlock()
	} else {
		hcc.update(shr, serving, healthErr, false)
	}
	// notify downstream for tablettype and realtimestats change
	if hc.listener != nil {
		hcc.mu.RLock()
		ts := &TabletStats{
			Tablet:  tablet,
			Name:    hcc.name,
			Target:  hcc.target,
			Up:      hcc.up,
			Serving: hcc.serving,
			Stats:   hcc.stats,
			TabletExternallyReparentedTimestamp: hcc.tabletExternallyReparentedTimestamp,
			LastError:                           hcc.lastError,
		}
		hcc.mu.RUnlock()
		hc.listener.StatsUpdate(ts)
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
			// ignore non-serving tablet
			hcc.mu.RUnlock()
			continue
		}
		if time.Now().Sub(hcc.lastResponseTimestamp) < hc.healthCheckTimeout {
			// received a healthcheck response recently
			hcc.mu.RUnlock()
			continue
		}
		hcc.mu.RUnlock()
		// mark the tablet non-serving as we have not seen a health check response for a long time
		hcc.mu.Lock()
		// check again to avoid race condition
		if !hcc.serving {
			// ignore non-serving tablet
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
		ts := &TabletStats{
			Tablet:  hcc.tablet,
			Name:    hcc.name,
			Target:  hcc.target,
			Up:      hcc.up,
			Serving: hcc.serving,
			Stats:   hcc.stats,
			TabletExternallyReparentedTimestamp: hcc.tabletExternallyReparentedTimestamp,
			LastError:                           hcc.lastError,
		}
		target := hcc.target
		hcc.mu.Unlock()
		// notify downstream for serving status change
		if hc.listener != nil {
			hc.listener.StatsUpdate(ts)
		}
		hcErrorCounters.Add([]string{target.Keyspace, target.Shard, strings.ToLower(target.TabletType.String())}, 1)
	}
}

func (hc *HealthCheckImpl) deleteConn(tablet *topodatapb.Tablet) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	key := TabletToMapKey(tablet)
	hcc, ok := hc.addrToConns[key]
	if !ok {
		log.Warningf("deleting unknown tablet: %+v", tablet)
		return
	}
	hcc.mu.Lock()
	hcc.up = false
	hcc.mu.Unlock()
	hcc.cancelFunc()
	delete(hc.addrToConns, key)
	hc.deleteTabletFromTargetProtected(hcc.target, tablet)
}

// SetListener sets the listener for healthcheck updates.
// It should not block.
// It must be called after NewHealthCheck and before any tablets are added
// (either through AddTablet or through a Watcher).
func (hc *HealthCheckImpl) SetListener(listener HealthCheckStatsListener) {
	if hc.listener != nil {
		panic("must not call SetListener twice")
	}

	hc.mu.Lock()
	defer hc.mu.Unlock()
	if len(hc.addrToConns) > 0 {
		panic("must not call SetListener after tablets were added")
	}

	hc.listener = listener
}

// AddTablet adds the tablet, and starts health check.
// It does not block on making connection.
// name is an optional tag for the tablet, e.g. an alternative address.
func (hc *HealthCheckImpl) AddTablet(cell, name string, tablet *topodatapb.Tablet) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	hcc := &healthCheckConn{
		cell:       cell,
		name:       name,
		ctx:        ctx,
		cancelFunc: cancelFunc,
		tablet:     tablet,
		target:     &querypb.Target{},
		up:         true,
	}
	key := TabletToMapKey(tablet)
	hc.mu.Lock()
	if _, ok := hc.addrToConns[key]; ok {
		hc.mu.Unlock()
		log.Warningf("adding duplicate tablet %v for %v: %+v", name, cell, tablet)
		return
	}
	hc.addrToConns[key] = hcc
	hc.mu.Unlock()

	hc.wg.Add(1)
	go hc.checkConn(hcc, cell, name, tablet)
}

// RemoveTablet removes the tablet, and stops the health check.
// It does not block.
func (hc *HealthCheckImpl) RemoveTablet(tablet *topodatapb.Tablet) {
	go hc.deleteConn(tablet)
}

// GetTabletStatsFromKeyspaceShard returns all TabletStats for the given keyspace/shard.
func (hc *HealthCheckImpl) GetTabletStatsFromKeyspaceShard(keyspace, shard string) []*TabletStats {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	shardMap, ok := hc.targetToTablets[keyspace]
	if !ok {
		return nil
	}
	ttMap, ok := shardMap[shard]
	if !ok {
		return nil
	}
	res := make([]*TabletStats, 0, 1)
	for _, tList := range ttMap {
		for _, t := range tList {
			key := TabletToMapKey(t)
			hcc, ok := hc.addrToConns[key]
			if !ok {
				continue
			}
			hcc.mu.RLock()
			ts := &TabletStats{
				Tablet:  t,
				Name:    hcc.name,
				Target:  hcc.target,
				Up:      hcc.up,
				Serving: hcc.serving,
				Stats:   hcc.stats,
				TabletExternallyReparentedTimestamp: hcc.tabletExternallyReparentedTimestamp,
				LastError:                           hcc.lastError,
			}
			hcc.mu.RUnlock()
			res = append(res, ts)
		}
	}
	return res
}

// GetTabletStatsFromTarget returns all TabletStats for the given target.
func (hc *HealthCheckImpl) GetTabletStatsFromTarget(keyspace, shard string, tabletType topodatapb.TabletType) []*TabletStats {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	shardMap, ok := hc.targetToTablets[keyspace]
	if !ok {
		return nil
	}
	ttMap, ok := shardMap[shard]
	if !ok {
		return nil
	}
	tList, ok := ttMap[tabletType]
	if !ok {
		return nil
	}
	res := make([]*TabletStats, 0, 1)
	for _, t := range tList {
		key := TabletToMapKey(t)
		hcc, ok := hc.addrToConns[key]
		if !ok {
			continue
		}
		hcc.mu.RLock()
		ts := &TabletStats{
			Tablet:  t,
			Name:    hcc.name,
			Target:  hcc.target,
			Up:      hcc.up,
			Serving: hcc.serving,
			Stats:   hcc.stats,
			TabletExternallyReparentedTimestamp: hcc.tabletExternallyReparentedTimestamp,
			LastError:                           hcc.lastError,
		}
		hcc.mu.RUnlock()
		res = append(res, ts)
	}
	return res
}

// GetConnection returns the TabletConn of the given tablet.
func (hc *HealthCheckImpl) GetConnection(tablet *topodatapb.Tablet) tabletconn.TabletConn {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	hcc := hc.addrToConns[TabletToMapKey(tablet)]
	if hcc == nil {
		return nil
	}
	hcc.mu.RLock()
	defer hcc.mu.RUnlock()
	return hcc.conn
}

// addTabletToTargetProtected adds the tablet to the given target.
// LOCK_REQUIRED hc.mu
func (hc *HealthCheckImpl) addTabletToTargetProtected(target *querypb.Target, tablet *topodatapb.Tablet) {
	shardMap, ok := hc.targetToTablets[target.Keyspace]
	if !ok {
		shardMap = make(map[string]map[topodatapb.TabletType][]*topodatapb.Tablet)
		hc.targetToTablets[target.Keyspace] = shardMap
	}
	ttMap, ok := shardMap[target.Shard]
	if !ok {
		ttMap = make(map[topodatapb.TabletType][]*topodatapb.Tablet)
		shardMap[target.Shard] = ttMap
	}
	tList, ok := ttMap[target.TabletType]
	if !ok {
		tList = make([]*topodatapb.Tablet, 0, 1)
	}
	for _, t := range tList {
		if topo.TabletEquality(t, tablet) {
			log.Warningf("tablet is already added: %+v", tablet)
			return
		}
	}
	ttMap[target.TabletType] = append(tList, tablet)
}

// deleteTabletFromTargetProtected deletes the tablet for the given target.
// LOCK_REQUIRED hc.mu
func (hc *HealthCheckImpl) deleteTabletFromTargetProtected(target *querypb.Target, tablet *topodatapb.Tablet) {
	shardMap, ok := hc.targetToTablets[target.Keyspace]
	if !ok {
		return
	}
	ttMap, ok := shardMap[target.Shard]
	if !ok {
		return
	}
	tList, ok := ttMap[target.TabletType]
	if !ok {
		return
	}
	for i, t := range tList {
		if topo.TabletEquality(t, tablet) {
			tList = append(tList[:i], tList[i+1:]...)
			ttMap[target.TabletType] = tList
			return
		}
	}
}

// TabletsCacheStatus is the current tablets for a cell/target.
// TODO: change this to reflect the e2e information about the tablets.
type TabletsCacheStatus struct {
	Cell         string
	Target       *querypb.Target
	TabletsStats TabletStatsList
}

// TabletStatsList is used for sorting.
type TabletStatsList []*TabletStats

// Len is part of sort.Interface.
func (tsl TabletStatsList) Len() int {
	return len(tsl)
}

// Less is part of sort.Interface
func (tsl TabletStatsList) Less(i, j int) bool {
	name1 := tsl[i].Name
	if name1 == "" {
		name1 = TabletToMapKey(tsl[i].Tablet)
	}
	name2 := tsl[j].Name
	if name2 == "" {
		name2 = TabletToMapKey(tsl[j].Tablet)
	}
	return name1 < name2
}

// Swap is part of sort.Interface
func (tsl TabletStatsList) Swap(i, j int) {
	tsl[i], tsl[j] = tsl[j], tsl[i]
}

// StatusAsHTML returns an HTML version of the status.
func (tcs *TabletsCacheStatus) StatusAsHTML() template.HTML {
	tLinks := make([]string, 0, 1)
	if tcs.TabletsStats != nil {
		sort.Sort(tcs.TabletsStats)
	}
	for _, ts := range tcs.TabletsStats {
		vtPort := ts.Tablet.PortMap["vt"]
		color := "green"
		extra := ""
		if ts.LastError != nil {
			color = "red"
			extra = fmt.Sprintf(" (%v)", ts.LastError)
		} else if !ts.Serving {
			color = "red"
			extra = " (Not Serving)"
		} else if !ts.Up {
			color = "red"
			extra = " (Down)"
		} else if ts.Target.TabletType == topodatapb.TabletType_MASTER {
			extra = fmt.Sprintf(" (MasterTS: %v)", ts.TabletExternallyReparentedTimestamp)
		} else {
			extra = fmt.Sprintf(" (RepLag: %v)", ts.Stats.SecondsBehindMaster)
		}
		name := ts.Name
		addr := netutil.JoinHostPort(ts.Tablet.Hostname, vtPort)
		if name == "" {
			name = addr
		}
		tLinks = append(tLinks, fmt.Sprintf(`<a href="http://%s" style="color:%v">%v</a>%v`, addr, color, name, extra))
	}
	return template.HTML(strings.Join(tLinks, "<br>"))
}

// TabletsCacheStatusList is used for sorting.
type TabletsCacheStatusList []*TabletsCacheStatus

// Len is part of sort.Interface.
func (tcsl TabletsCacheStatusList) Len() int {
	return len(tcsl)
}

// Less is part of sort.Interface
func (tcsl TabletsCacheStatusList) Less(i, j int) bool {
	return tcsl[i].Cell+"."+tcsl[i].Target.Keyspace+"."+tcsl[i].Target.Shard+"."+string(tcsl[i].Target.TabletType) <
		tcsl[j].Cell+"."+tcsl[j].Target.Keyspace+"."+tcsl[j].Target.Shard+"."+string(tcsl[j].Target.TabletType)
}

// Swap is part of sort.Interface
func (tcsl TabletsCacheStatusList) Swap(i, j int) {
	tcsl[i], tcsl[j] = tcsl[j], tcsl[i]
}

// CacheStatus returns a displayable version of the cache.
func (hc *HealthCheckImpl) CacheStatus() TabletsCacheStatusList {
	tcsMap := make(map[string]*TabletsCacheStatus)
	hc.mu.RLock()
	for _, hcc := range hc.addrToConns {
		hcc.mu.RLock()
		key := fmt.Sprintf("%v.%v.%v.%v", hcc.cell, hcc.target.Keyspace, hcc.target.Shard, string(hcc.target.TabletType))
		var tcs *TabletsCacheStatus
		var ok bool
		if tcs, ok = tcsMap[key]; !ok {
			tcs = &TabletsCacheStatus{
				Cell:   hcc.cell,
				Target: hcc.target,
			}
			tcsMap[key] = tcs
		}
		stats := &TabletStats{
			Tablet:  hcc.tablet,
			Name:    hcc.name,
			Target:  hcc.target,
			Up:      hcc.up,
			Serving: hcc.serving,
			Stats:   hcc.stats,
			TabletExternallyReparentedTimestamp: hcc.tabletExternallyReparentedTimestamp,
			LastError:                           hcc.lastError,
		}
		hcc.mu.RUnlock()
		tcs.TabletsStats = append(tcs.TabletsStats, stats)
	}
	hc.mu.RUnlock()
	tcsl := make(TabletsCacheStatusList, 0, len(tcsMap))
	for _, tcs := range tcsMap {
		tcsl = append(tcsl, tcs)
	}
	sort.Sort(tcsl)
	return tcsl
}

// Close stops the healthcheck.
// After Close() returned, it's guaranteed that the listener won't be called
// anymore.
func (hc *HealthCheckImpl) Close() error {
	hc.mu.Lock()
	close(hc.closeChan)
	for _, hcc := range hc.addrToConns {
		hcc.cancelFunc()
	}
	hc.addrToConns = make(map[string]*healthCheckConn)
	hc.targetToTablets = make(map[string]map[string]map[topodatapb.TabletType][]*topodatapb.Tablet)
	// Release the lock early or a pending checkHealthCheckTimeout cannot get a
	// read lock on it.
	hc.mu.Unlock()

	// Wait for the checkHealthCheckTimeout Go routine and each Go routine per
	// tablet.
	hc.wg.Wait()

	return nil
}

// TabletToMapKey creates a key to the map from tablet's host and ports.
// It should only be used in discovery and related module.
func TabletToMapKey(tablet *topodatapb.Tablet) string {
	parts := make([]string, 0, 1)
	for name, port := range tablet.PortMap {
		parts = append(parts, netutil.JoinHostPort(name, port))
	}
	sort.Strings(parts)
	parts = append([]string{tablet.Hostname}, parts...)
	return strings.Join(parts, ",")
}
