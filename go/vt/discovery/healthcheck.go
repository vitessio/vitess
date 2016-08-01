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
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"
)

var (
	hcErrorCounters *stats.MultiCounters
)

const (
	// DefaultTopoReadConcurrency can be used as default value for the topoReadConcurrency parameter of a TopologyWatcher.
	DefaultTopoReadConcurrency int = 5

	// HealthCheckTemplate is the HTML code to display a TabletsCacheStatusList
	HealthCheckTemplate = `
<style>
  table {
    border-collapse: collapse;
  }
  td, th {
    border: 1px solid #999;
    padding: 0.2rem;
  }
</style>
<table>
  <tr>
    <th colspan="5">HealthCheck Tablet Cache</th>
  </tr>
  <tr>
    <th>Cell</th>
    <th>Keyspace</th>
    <th>Shard</th>
    <th>TabletType</th>
    <th>TabletStats</th>
  </tr>
  {{range $i, $ts := .}}
  <tr>
    <td>{{github_com_youtube_vitess_vtctld_srv_cell $ts.Cell}}</td>
    <td>{{github_com_youtube_vitess_vtctld_srv_keyspace $ts.Cell $ts.Target.Keyspace}}</td>
    <td>{{$ts.Target.Shard}}</td>
    <td>{{$ts.Target.TabletType}}</td>
    <td>{{$ts.StatusAsHTML}}</td>
  </tr>
  {{end}}
</table>
`
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
// The goal of this object is to maintain a streaming HealthCheck connection
// to a lot of tablets. Tablets are added / removed by calling the
// AddTablet / RemoveTablet methods (other discovery module objects
// can for instance watch the topology and call these).
//
// There are two ways to use this object:
// 1. register a Listener and get up / down / update notifications for tablets.
// 2. call GetTabletStatsFromTarget / GetConnection to use the tablets directly.
type HealthCheck interface {
	// RegisterStats registers the connection counts stats.
	// It can only be called on one Healthcheck object per process.
	RegisterStats()
	// SetListener sets the listener for healthcheck
	// updates. sendDownEvents is used when a tablet changes type
	// (from replica to master for instance). If the listener
	// wants two events (Up=false on old type, Up=True on new
	// type), sendDownEvents should be set. Otherwise, the
	// healthcheck will only send one event (Up=true on new type).
	//
	// Note that the default implementation requires to set the
	// listener before any tablets are added to the healthcheck.
	SetListener(listener HealthCheckStatsListener, sendDownEvents bool)
	// AddTablet adds the tablet, and starts health check.
	// Name is an alternate name, like an address.
	AddTablet(tablet *topodatapb.Tablet, name string)
	// RemoveTablet removes the tablet, and stops the health check.
	RemoveTablet(tablet *topodatapb.Tablet)
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

// healthCheckConn contains details about a tablet.
// It is used internally by HealthCheckImpl to keep all the info
// about a tablet.
type healthCheckConn struct {
	// set at construction time
	ctx        context.Context
	cancelFunc context.CancelFunc

	// mu protects all the following fields.
	// When locking both mutex from HealthCheck and healthCheckConn,
	// HealthCheck.mu goes first.
	// Note tabletStats.Tablet and tabletStats.Name are immutable.
	mu                    sync.RWMutex
	conn                  tabletconn.TabletConn
	tabletStats           TabletStats
	lastResponseTimestamp time.Time // timestamp of the last healthcheck response
}

// HealthCheckImpl performs health checking and notifies downstream components about any changes.
type HealthCheckImpl struct {
	// Immutable fields set at construction time.
	listener           HealthCheckStatsListener
	sendDownEvents     bool
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

	// targetToTablets maps from keyspace/shard/tabletType to a
	// list of tablets.
	targetToTablets map[string]map[string]map[topodatapb.TabletType][]*topodatapb.Tablet
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
		if !hcc.tabletStats.Up || !hcc.tabletStats.Serving || hcc.tabletStats.LastError != nil {
			hcc.mu.RUnlock()
			continue
		}
		key := fmt.Sprintf("%s.%s.%s", hcc.tabletStats.Target.Keyspace, hcc.tabletStats.Target.Shard, topoproto.TabletTypeLString(hcc.tabletStats.Target.TabletType))
		hcc.mu.RUnlock()
		res[key]++
	}
	return res
}

// checkConn performs health checking on the given tablet.
func (hc *HealthCheckImpl) checkConn(hcc *healthCheckConn, name string) {
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
		stream, err := hcc.connect(hc)
		if err != nil {
			select {
			case <-hcc.ctx.Done():
				return
			default:
			}
			hcc.mu.Lock()
			hcc.tabletStats.Serving = false
			hcc.tabletStats.LastError = err
			target := hcc.tabletStats.Target
			hcc.mu.Unlock()
			hcErrorCounters.Add([]string{target.Keyspace, target.Shard, topoproto.TabletTypeLString(target.TabletType)}, 1)

			// Sleep until the next retry is up or the context is done/canceled.
			select {
			case <-hcc.ctx.Done():
			case <-time.After(hc.retryDelay):
			}
			continue
		}

		// Read stream health responses.
		for {
			reconnect, err := hcc.processResponse(hc, stream)
			if err != nil {
				hcc.mu.Lock()
				hcc.tabletStats.Serving = false
				hcc.tabletStats.LastError = err
				ts := hcc.tabletStats
				hcc.mu.Unlock()
				// notify downstream for serving status change
				if hc.listener != nil {
					hc.listener.StatsUpdate(&ts)
				}
				select {
				case <-hcc.ctx.Done():
					return
				default:
				}
				hcErrorCounters.Add([]string{ts.Target.Keyspace, ts.Target.Shard, topoproto.TabletTypeLString(ts.Target.TabletType)}, 1)
				if reconnect {
					hcc.mu.Lock()
					hcc.conn.Close()
					hcc.conn = nil
					hcc.tabletStats.Target = &querypb.Target{}
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
func (hcc *healthCheckConn) connect(hc *HealthCheckImpl) (tabletconn.StreamHealthReader, error) {
	// Keyspace, shard and tabletType are the ones from the tablet
	// record, but they won't be used just yet.
	conn, err := tabletconn.GetDialer()(hcc.tabletStats.Tablet, hc.connTimeout)
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
	hcc.tabletStats.LastError = nil
	hcc.mu.Unlock()
	return stream, nil
}

// processResponse reads one health check response, and notifies HealthCheckStatsListener.
// It returns bool to indicate if the caller should reconnect. We do not need to reconnect when the streaming is working.
func (hcc *healthCheckConn) processResponse(hc *HealthCheckImpl, stream tabletconn.StreamHealthReader) (bool, error) {
	select {
	case <-hcc.ctx.Done():
		return false, hcc.ctx.Err()
	default:
	}

	shr, err := stream.Recv()
	if err != nil {
		return true, err
	}

	// Check for invalid data, better than panicking.
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

	var ts TabletStats
	if hcc.tabletStats.Target.TabletType == topodatapb.TabletType_UNKNOWN {
		// The first time we see response for the tablet.
		ts = hcc.update(shr, serving, healthErr)
		hc.mu.Lock()
		hc.addTabletToTargetProtected(hcc.tabletStats.Target, hcc.tabletStats.Tablet)
		hc.mu.Unlock()
	} else if hcc.tabletStats.Target.TabletType != shr.Target.TabletType {
		// The Tablet type changed for the tablet. Get old value.
		hcc.mu.RLock()
		oldTs := hcc.tabletStats
		hcc.mu.RUnlock()

		// Log and maybe notify
		log.Infof("HealthCheckUpdate(Type Change): %v, tablet: %v/%+v, target %+v => %+v, reparent time: %v", oldTs.Name, oldTs.Tablet.Alias.Cell, oldTs.Tablet, oldTs.Target, shr.Target, shr.TabletExternallyReparentedTimestamp)
		if hc.listener != nil && hc.sendDownEvents {
			oldTs.Up = false
			hc.listener.StatsUpdate(&oldTs)
		}

		hc.mu.Lock()
		hc.deleteTabletFromTargetProtected(hcc.tabletStats.Target, hcc.tabletStats.Tablet)
		ts = hcc.update(shr, serving, healthErr)
		hc.addTabletToTargetProtected(shr.Target, hcc.tabletStats.Tablet)
		hc.mu.Unlock()
	} else {
		ts = hcc.update(shr, serving, healthErr)
	}
	// notify downstream for tabletType and realtimeStats change
	if hc.listener != nil {
		hc.listener.StatsUpdate(&ts)
	}
	return false, nil
}

// update updates the stats of a healthCheckConn, and returns a copy
// of its tabletStats.
func (hcc *healthCheckConn) update(shr *querypb.StreamHealthResponse, serving bool, healthErr error) TabletStats {
	hcc.mu.Lock()
	defer hcc.mu.Unlock()
	hcc.lastResponseTimestamp = time.Now()
	hcc.tabletStats.Target = shr.Target
	hcc.tabletStats.Serving = serving
	hcc.tabletStats.TabletExternallyReparentedTimestamp = shr.TabletExternallyReparentedTimestamp
	hcc.tabletStats.Stats = shr.RealtimeStats
	hcc.tabletStats.LastError = healthErr
	return hcc.tabletStats
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
		if !hcc.tabletStats.Serving {
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
		if !hcc.tabletStats.Serving {
			// ignore non-serving tablet
			hcc.mu.Unlock()
			continue
		}
		if time.Now().Sub(hcc.lastResponseTimestamp) < hc.healthCheckTimeout {
			// received a healthcheck response recently
			hcc.mu.Unlock()
			continue
		}
		hcc.tabletStats.Serving = false
		hcc.tabletStats.LastError = fmt.Errorf("healthcheck timed out (latest %v)", hcc.lastResponseTimestamp)
		ts := hcc.tabletStats
		hcc.mu.Unlock()
		// notify downstream for serving status change
		if hc.listener != nil {
			hc.listener.StatsUpdate(&ts)
		}
		hcErrorCounters.Add([]string{ts.Target.Keyspace, ts.Target.Shard, topoproto.TabletTypeLString(ts.Target.TabletType)}, 1)
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
	hcc.tabletStats.Up = false
	hcc.mu.Unlock()
	hcc.cancelFunc()
	delete(hc.addrToConns, key)
	hc.deleteTabletFromTargetProtected(hcc.tabletStats.Target, tablet)
}

// SetListener sets the listener for healthcheck updates.
// It must be called after NewHealthCheck and before any tablets are added
// (either through AddTablet or through a Watcher).
func (hc *HealthCheckImpl) SetListener(listener HealthCheckStatsListener, sendDownEvents bool) {
	if hc.listener != nil {
		panic("must not call SetListener twice")
	}

	hc.mu.Lock()
	defer hc.mu.Unlock()
	if len(hc.addrToConns) > 0 {
		panic("must not call SetListener after tablets were added")
	}

	hc.listener = listener
	hc.sendDownEvents = sendDownEvents
}

// AddTablet adds the tablet, and starts health check.
// It does not block on making connection.
// name is an optional tag for the tablet, e.g. an alternative address.
func (hc *HealthCheckImpl) AddTablet(tablet *topodatapb.Tablet, name string) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	hcc := &healthCheckConn{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		tabletStats: TabletStats{
			Tablet: tablet,
			Name:   name,
			Target: &querypb.Target{},
			Up:     true,
		},
	}
	key := TabletToMapKey(tablet)
	hc.mu.Lock()
	if _, ok := hc.addrToConns[key]; ok {
		hc.mu.Unlock()
		log.Warningf("adding duplicate tablet %v for %v: %+v", name, tablet.Alias.Cell, tablet)
		return
	}
	hc.addrToConns[key] = hcc
	hc.mu.Unlock()

	hc.wg.Add(1)
	go hc.checkConn(hcc, name)
}

// RemoveTablet removes the tablet, and stops the health check.
// It does not block.
func (hc *HealthCheckImpl) RemoveTablet(tablet *topodatapb.Tablet) {
	go hc.deleteConn(tablet)
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
		ts := hcc.tabletStats
		hcc.mu.RUnlock()
		res = append(res, &ts)
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
		key := fmt.Sprintf("%v.%v.%v.%v", hcc.tabletStats.Tablet.Alias.Cell, hcc.tabletStats.Target.Keyspace, hcc.tabletStats.Target.Shard, string(hcc.tabletStats.Target.TabletType))
		var tcs *TabletsCacheStatus
		var ok bool
		if tcs, ok = tcsMap[key]; !ok {
			tcs = &TabletsCacheStatus{
				Cell:   hcc.tabletStats.Tablet.Alias.Cell,
				Target: hcc.tabletStats.Target,
			}
			tcsMap[key] = tcs
		}
		stats := hcc.tabletStats
		hcc.mu.RUnlock()
		tcs.TabletsStats = append(tcs.TabletsStats, &stats)
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
