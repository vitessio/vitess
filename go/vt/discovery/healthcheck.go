/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
// tabletStatsCache is one implementation, that caches the known tablets
// and the healthy ones per keyspace/shard/tabletType.
//
// Internally, the HealthCheck module is connected to each tablet and has a
// streaming RPC (StreamHealth) open to receive periodic health infos.
package discovery

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"html/template"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/vt/topo"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	hcErrorCounters          = stats.NewCountersWithMultiLabels("HealthcheckErrors", "Healthcheck Errors", []string{"Keyspace", "ShardName", "TabletType"})
	hcMasterPromotedCounters = stats.NewCountersWithMultiLabels("HealthcheckMasterPromoted", "Master promoted in keyspace/shard name because of health check errors", []string{"Keyspace", "ShardName"})
	healthcheckOnce          sync.Once

	// TabletURLTemplateString is a flag to generate URLs for the tablets that vtgate discovers.
	TabletURLTemplateString = flag.String("tablet_url_template", "http://{{.GetTabletHostPort}}", "format string describing debug tablet url formatting. See the Go code for getTabletDebugURL() how to customize this.")
	tabletURLTemplate       *template.Template

	//TODO(deepthi): change these vars back to unexported when discoveryGateway is removed

	// RefreshInterval is the interval at which healthcheck refreshes its list of tablets from topo
	RefreshInterval = flag.Duration("tablet_refresh_interval", 1*time.Minute, "tablet refresh interval")
	// RefreshKnownTablets tells us whether to process all tablets or only new tablets
	RefreshKnownTablets = flag.Bool("tablet_refresh_known_tablets", true, "tablet refresh reloads the tablet address/port map from topo in case it changes")
	// TopoReadConcurrency tells us how many topo reads are allowed in parallel
	TopoReadConcurrency = flag.Int("topo_read_concurrency", 32, "concurrent topo reads")
	// CellsToWatch is the list of cells this healthcheck operates over
	CellsToWatch = flag.String("cells_to_watch", "", "comma-separated list of cells for watching tablets")
	// AllowedTabletTypes is the list of allowed tablet types. e.g. {MASTER, REPLICA}
	AllowedTabletTypes []topodatapb.TabletType
	// TabletFilters are the keyspace|shard or keyrange filters to apply to the full set of tablets
	TabletFilters flagutil.StringListValue
	// KeyspacesToWatch - if provided this specifies which keyspaces should be
	// visible to a vtgate. By default the vtgate will allow access to any
	// keyspace.
	KeyspacesToWatch flagutil.StringListValue
)

// See the documentation for NewLegacyHealthCheck below for an explanation of these parameters.
const (
	DefaultHealthCheckRetryDelay = 5 * time.Second
	DefaultHealthCheckTimeout    = 1 * time.Minute

	// DefaultTopoReadConcurrency can be used as default value for the TopoReadConcurrency parameter of a LegacyTopologyWatcher.
	DefaultTopoReadConcurrency int = 5
	// DefaultTopologyWatcherRefreshInterval can be used as the default value for
	// the refresh interval of a topology watcher.
	DefaultTopologyWatcherRefreshInterval = 1 * time.Minute

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
    <th>tabletStats</th>
  </tr>
  {{range $i, $ts := .}}
  <tr>
    <td>{{github_com_vitessio_vitess_vtctld_srv_cell $ts.Cell}}</td>
    <td>{{github_com_vitessio_vitess_vtctld_srv_keyspace $ts.Cell $ts.Target.Keyspace}}</td>
    <td>{{$ts.Target.Shard}}</td>
    <td>{{$ts.Target.TabletType}}</td>
    <td>{{$ts.StatusAsHTML}}</td>
  </tr>
  {{end}}
</table>
`
)

// ParseTabletURLTemplateFromFlag loads or reloads the URL template.
func ParseTabletURLTemplateFromFlag() {
	tabletURLTemplate = template.New("")
	_, err := tabletURLTemplate.Parse(*TabletURLTemplateString)
	if err != nil {
		log.Exitf("error parsing template: %v", err)
	}
}

func init() {
	// Flags are not parsed at this point and the default value of the flag (just the hostname) will be used.
	ParseTabletURLTemplateFromFlag()
	flag.Var(&TabletFilters, "tablet_filters", "Specifies a comma-separated list of 'keyspace|shard_name or keyrange' values to filter the tablets to watch")
	topoproto.TabletTypeListVar(&AllowedTabletTypes, "allowed_tablet_types", "Specifies the tablet types this vtgate is allowed to route queries to")
	flag.Var(&KeyspacesToWatch, "keyspaces_to_watch", "Specifies which keyspaces this vtgate should have access to while routing queries or accessing the vschema")
}

// TabletRecorder is the part of the HealthCheck interface that can
// add or remove tablets. We define it as a sub-interface here so we
// can add filters on tablets if needed.
type TabletRecorder interface {
	// AddTablet adds the tablet.
	// Name is an alternate name, like an address.
	AddTablet(tablet *topodatapb.Tablet, name string)

	// RemoveTablet removes the tablet.
	RemoveTablet(tablet *topodatapb.Tablet)

	// ReplaceTablet does an AddTablet and RemoveTablet in one call, effectively replacing the old tablet with the new.
	ReplaceTablet(old, new *topodatapb.Tablet, name string)
}

// HealthCheck defines the interface of health checking module.
// The goal of this object is to maintain a StreamHealth RPC
// to a lot of tablets. Tablets are added / removed by calling the
// AddTablet / RemoveTablet methods (other discovery module objects
// can for instance watch the topology and call these).
//
// Updates to the health of all registered tablet can be watched by
// registering a listener. To get the underlying "TabletConn" object
// which is used for each tablet, use the "GetConnection()" method
// below and pass in the Key string which is also sent to the
// listener in each update (as it is part of tabletStats).
type HealthCheck interface {
	TabletRecorder
	// RegisterStats registers the connection counts and checksum stats.
	// It can only be called on one Healthcheck object per process.
	RegisterStats()
	// WaitForInitialStatsUpdates waits until all tablets added via
	// AddTablet() call were propagated to the listener via corresponding
	// StatsUpdate() calls. Note that code path from AddTablet() to
	// corresponding StatsUpdate() is asynchronous but not cancelable, thus
	// this function is also non-cancelable and can't return error. Also
	// note that all AddTablet() calls should happen before calling this
	// method. WaitForInitialStatsUpdates won't wait for StatsUpdate() calls
	// corresponding to AddTablet() calls made during its execution.
	WaitForInitialStatsUpdates()
	// GetConnection returns the TabletConn of the given tablet.
	GetConnection(key string) queryservice.QueryService
	// CacheStatus returns a displayable version of the cache.
	CacheStatus() TabletsCacheStatusList
	// Close stops the healthcheck.
	Close() error
	// GetTabletAndConnection gets a tablet and connection to execute a query on
	GetTabletAndConnection(target *querypb.Target, localCell string, invalidTablets map[string]bool) (string, queryservice.QueryService, error)
}

type tabletFilterFunc func(tablet *topodatapb.Tablet) bool

// HealthCheckImpl performs health checking and notifies downstream components about any changes.
// It contains a map of TabletHealth objects, each of which stores the health information for
// a tablet. A checkConn goroutine is spawned for each TabletHealth, which is responsible for
// keeping that TabletHealth up-to-date. This is done through callbacks to updateHealth.
// If checkConn terminates for any reason, it updates TabletHealth.Up as false. If a TabletHealth
// gets removed from the map, its cancelFunc gets called, which ensures that the associated
// checkConn goroutine eventually terminates.
type HealthCheckImpl struct {
	// Immutable fields set at construction time.
	retryDelay         time.Duration
	healthCheckTimeout time.Duration
	ts                 *topo.Server
	cell               string
	tscByCell          map[string]*tabletStatsCache

	// connsWG keeps track of all launched Go routines that monitor tablet connections.
	connsWG sync.WaitGroup

	// mu protects all the following fields.
	mu sync.Mutex

	// TODO(deepthi): verify all access to following fields is actually being protected by mu
	// if not needed, move them up

	// addrToHealth maps from address to TabletHealth.
	addrToHealth map[string]*tabletHealth

	// Wait group that's used to wait until all initial StatsUpdate() calls are made after the AddTablet() calls.
	initialUpdatesWG sync.WaitGroup

	tabletFilters []tabletFilterFunc

	topoWatchers []*TopologyWatcher

	// cellAliases is a cache of cell aliases
	cellAliases map[string]string
}

// HealthCheckConn is a structure that lives within the scope of
// the checkConn goroutine to maintain its internal state. Therefore,
// it does not require synchronization. Changes that are relevant to
// healthcheck are transmitted through calls to HealthCheckImpl.updateHealth.
// TODO(deepthi): replace with goroutine (already using a goroutine to update this)
type healthCheckConn struct {
	ctx context.Context

	conn                  queryservice.QueryService
	tabletStats           tabletStats
	loggedServingState    bool
	lastResponseTimestamp time.Time // timestamp of the last healthcheck response
}

// TabletHealth maintains the health status of a tablet. A map of this
// structure is maintained in HealthCheckImpl.
type tabletHealth struct {
	// cancelFunc must be called before discarding TabletHealth.
	// This will ensure that the associated checkConn goroutine will terminate.
	cancelFunc context.CancelFunc
	// conn is the connection associated with the tablet.
	conn queryservice.QueryService
	// latestTabletStats stores the latest health stats of the tablet.
	latestTabletStats tabletStats
}

// NewHealthCheck creates a new HealthCheck object.
// Parameters:
// retryDelay.
//   The duration to wait before retrying to connect (e.g. after a failed connection
//   attempt).
// healthCheckTimeout.
//   The duration for which we consider a health check response to be 'fresh'. If we don't get
//   a health check response from a tablet for more than this duration, we consider the tablet
//   not healthy.
func NewHealthCheck(ctx context.Context, retryDelay, healthCheckTimeout time.Duration, topoServer *topo.Server, localCell string) HealthCheck {
	log.Infof("loading tablets for cells: %v", *CellsToWatch)
	var filterFuncs []tabletFilterFunc
	tscMap := make(map[string]*tabletStatsCache)
	tscMap[localCell] = newTabletStatsCache()

	for _, c := range strings.Split(*CellsToWatch, ",") {
		if c == "" {
			continue
		}
		if len(TabletFilters) > 0 {
			if len(KeyspacesToWatch) > 0 {
				log.Exitf("Only one of -keyspaces_to_watch and -tablet_filters may be specified at a time")
			}

			fbs, err := NewFilterByShard(c, TabletFilters)
			if err != nil {
				log.Exitf("Cannot parse tablet_filters parameter: %v", err)
			}
			filterFuncs = append(filterFuncs, fbs.IsIncluded)
		} else if len(KeyspacesToWatch) > 0 {
			fbk := NewFilterByKeyspace(c, KeyspacesToWatch)
			filterFuncs = append(filterFuncs, fbk.IsIncluded)
		}
		if _, ok := tscMap[c]; !ok {
			tscMap[c] = newTabletStatsCache()
		}
		//ctw := NewCellTabletsWatcher(ctx, topoServer, hc, c, *RefreshInterval,
		//	*RefreshKnownTablets, *TopoReadConcurrency)
	}

	hc := &HealthCheckImpl{
		ts:                 topoServer,
		cell:               localCell,
		addrToHealth:       make(map[string]*tabletHealth),
		retryDelay:         retryDelay,
		healthCheckTimeout: healthCheckTimeout,
		tscByCell:          tscMap,
		tabletFilters:      filterFuncs,
		cellAliases:        make(map[string]string),
	}

	// create a go func per cell - call watchCell to watch topo and update list of tablets

	healthcheckOnce.Do(func() {
		http.Handle("/debug/gateway", hc)
	})

	return hc
}

// TODO(deepthi): implement the watch
//func watchCell(ctx context.Context, topoServer *topo.Server, hc *HealthCheck, cell string, refreshInterval time.Duration, refreshKnownTablets bool, topoReadConcurrency int) {
//
//}

// Open starts healthcheck
func (*HealthCheckImpl) Open() {
	// create a local cancelable context, cancel it in Close
}

// RegisterStats registers the connection counts stats
func (hc *HealthCheckImpl) RegisterStats() {
	stats.NewGaugeDurationFunc(
		"TopologyWatcherMaxRefreshLag",
		"maximum time since the topology watcher refreshed a cell",
		hc.topologyWatcherMaxRefreshLag,
	)

	stats.NewGaugeFunc(
		"TopologyWatcherChecksum",
		"crc32 checksum of the topology watcher state",
		hc.topologyWatcherChecksum,
	)

	stats.NewGaugesFuncWithMultiLabels(
		"HealthcheckConnections",
		"the number of healthcheck connections registered",
		[]string{"Keyspace", "ShardName", "TabletType"},
		hc.servingConnStats)

	stats.NewGaugeFunc(
		"HealthcheckChecksum",
		"crc32 checksum of the current healthcheck state",
		hc.stateChecksum)
}

// ServeHTTP is part of the http.Handler interface. It renders the current state of the discovery gateway tablet cache into json.
func (hc *HealthCheckImpl) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	status := hc.cacheStatusMap()
	b, err := json.MarshalIndent(status, "", " ")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	buf := bytes.NewBuffer(nil)
	json.HTMLEscape(buf, b)
	w.Write(buf.Bytes())
}

// servingConnStats returns the number of serving tablets per keyspace/shard/tablet type.
func (hc *HealthCheckImpl) servingConnStats() map[string]int64 {
	res := make(map[string]int64)
	hc.mu.Lock()
	defer hc.mu.Unlock()
	for _, th := range hc.addrToHealth {
		if !th.latestTabletStats.Up || !th.latestTabletStats.Serving || th.latestTabletStats.LastError != nil {
			continue
		}
		key := fmt.Sprintf("%s.%s.%s", th.latestTabletStats.Target.Keyspace, th.latestTabletStats.Target.Shard, topoproto.TabletTypeLString(th.latestTabletStats.Target.TabletType))
		res[key]++
	}
	return res
}

// stateChecksum returns a crc32 checksum of the healthcheck state
func (hc *HealthCheckImpl) stateChecksum() int64 {
	// CacheStatus is sorted so this should be stable across vtgates
	cacheStatus := hc.CacheStatus()
	var buf bytes.Buffer
	for _, st := range cacheStatus {
		fmt.Fprintf(&buf,
			"%v%v%v%v\n",
			st.Cell,
			st.Target.Keyspace,
			st.Target.Shard,
			st.Target.TabletType.String(),
		)
		sort.Sort(st.TabletsStats)
		for _, ts := range st.TabletsStats {
			fmt.Fprintf(&buf, "%v%v%v\n", ts.Up, ts.Serving, ts.MasterTermStartTime)
		}
	}

	return int64(crc32.ChecksumIEEE(buf.Bytes()))
}

// updateHealth updates the TabletHealth record and updates the tablet stats
func (hc *HealthCheckImpl) updateHealth(ts *tabletStats, conn queryservice.QueryService) {
	// update the stats cache
	hc.updateStatsCache(ts)

	hc.mu.Lock()
	th, ok := hc.addrToHealth[ts.Key]
	if !ok {
		// This can happen on delete because the entry is removed first,
		// or if HealthCheckImpl has been closed.
		hc.mu.Unlock()
		return
	}

	oldts := th.latestTabletStats
	th.latestTabletStats = *ts.Copy()
	th.conn = conn
	hc.mu.Unlock()

	// In the case where a tablet changes type (but not for the
	// initial message), we want to log it, and maybe advertise it too.
	if oldts.Target.TabletType != topodatapb.TabletType_UNKNOWN && oldts.Target.TabletType != ts.Target.TabletType {
		// Log and maybe notify
		log.Infof("HealthCheckUpdate(Type Change): %v, tablet: %s, target %+v => %+v, reparent time: %v",
			oldts.Name, topotools.TabletIdent(oldts.Tablet), topotools.TargetIdent(oldts.Target), topotools.TargetIdent(ts.Target), ts.MasterTermStartTime)
		oldts.Up = false
		hc.updateStatsCache(&oldts)

		// Track how often a tablet gets promoted to master. It is used for
		// comparing against the variables in go/vtgate/buffer/variables.go.
		if oldts.Target.TabletType != topodatapb.TabletType_MASTER && ts.Target.TabletType == topodatapb.TabletType_MASTER {
			hcMasterPromotedCounters.Add([]string{ts.Target.Keyspace, ts.Target.Shard}, 1)
		}
	}
}

func (hc *HealthCheckImpl) updateStatsCache(stats *tabletStats) {
	if stats.Target.TabletType != topodatapb.TabletType_MASTER &&
		stats.Tablet.Alias.Cell != hc.cell &&
		hc.getAliasByCell(stats.Tablet.Alias.Cell, hc.ts) != hc.getAliasByCell(hc.cell, hc.ts) {
		// this is for a non-master tablet in a different cell and a different alias, drop it
		return
	}

	// We assume that if we are getting an update for a tablet, then we are interested in that tablet
	// i.e. it belongs to a cell we are watching.
	tsc := hc.tscByCell[stats.Tablet.Alias.Cell]
	e := tsc.getOrCreateEntry(stats.Target)
	e.mu.Lock()
	defer e.mu.Unlock()

	// Update our full map.
	trivialNonMasterUpdate := false
	current, exists := e.all[stats.Key]

	switch {
	case exists && stats.Up:
		// We have an current entry, and a new entry.
		// Remember if they are both good (most common case).
		trivialNonMasterUpdate = current.LastError == nil && current.Serving && stats.LastError == nil &&
			stats.Serving && stats.Target.TabletType != topodatapb.TabletType_MASTER && current.TrivialStatsUpdate(stats)

		// We already have the entry, update the
		// values if necessary.  (will update both
		// 'all' and 'healthy' as they use pointers).
		if !trivialNonMasterUpdate {
			*current = *stats
		}
	case exists && !stats.Up:
		// We have an entry which we shouldn't. Remove it.
		delete(e.all, stats.Key)
	case !exists && stats.Up:
		// Add the entry.
		e.all[stats.Key] = stats
	case !exists && !stats.Up:
		// We were told to remove an entry which we
		// didn't have anyway, nothing should happen.
		return
	default:
		// Unreachable
	}

	// Update our healthy list.
	var allArray []*tabletStats
	if stats.Target.TabletType == topodatapb.TabletType_MASTER {
		// The healthy list is different for TabletType_MASTER: we
		// only keep the most recent one.
		e.updateHealthyMapForMaster(stats)
	} else {
		// For non-master, if it is a trivial update,
		// we just skip everything else. We don't even update the
		// aggregate stats.
		if trivialNonMasterUpdate {
			return
		}

		// Now we need to do some work. Recompute our healthy list.
		allArray = make([]*tabletStats, 0, len(e.all))
		for _, s := range e.all {
			allArray = append(allArray, s)
		}
		e.healthy = FilterStatsByReplicationLag(allArray)
	}
}

// finalizeConn closes the health checking connection and sends the final
// notification about the tablet to downstream. To be called only on exit from
// checkConn().
func (hc *HealthCheckImpl) finalizeConn(hcc *healthCheckConn) {
	hcc.tabletStats.Up = false
	hcc.setServingState(false, "finalizeConn closing connection")
	// Note: checkConn() exits only when hcc.ctx.Done() is closed. Thus it's
	// safe to simply get Err() value here and assign to LastError.
	hcc.tabletStats.LastError = hcc.ctx.Err()
	hc.updateHealth(hcc.tabletStats.Copy(), nil)
	if hcc.conn != nil {
		// Don't use hcc.ctx because it's already closed.
		// Use a separate context, and add a timeout to prevent unbounded waits.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		hcc.conn.Close(ctx)
		hcc.conn = nil
	}
}

// checkConn performs health checking on the given tablet.
func (hc *HealthCheckImpl) checkConn(hcc *healthCheckConn, name string) {
	defer hc.connsWG.Done()
	defer hc.finalizeConn(hcc)

	// Initial notification for downstream about the tablet existence.
	// do not copy
	hc.updateHealth(&hcc.tabletStats, hcc.conn)
	hc.initialUpdatesWG.Done()

	retryDelay := hc.retryDelay
	for {
		streamCtx, streamCancel := context.WithCancel(hcc.ctx)

		// Setup a watcher that restarts the timer every time an update is received.
		// If a timeout occurs for a serving tablet, we make it non-serving and send
		// a status update. The stream is also terminated so it can be retried.
		// servingStatus feeds into the serving var, which keeps track of the serving
		// status transmitted by the tablet.
		servingStatus := make(chan bool, 1)
		// timedout is accessed atomically because there could be a race
		// between the goroutine that sets it and the check for its value
		// later.
		timedout := sync2.NewAtomicBool(false)
		go func() {
			for {
				select {
				case <-servingStatus:
					continue
				case <-time.After(hc.healthCheckTimeout):
					timedout.Set(true)
					streamCancel()
					return
				case <-streamCtx.Done():
					// If the stream is done, stop watching.
					return
				}
			}
		}()

		// Read stream health responses.
		hcc.stream(streamCtx, hc, func(shr *querypb.StreamHealthResponse) error {
			// We received a message. Reset the back-off.
			retryDelay = hc.retryDelay
			// Don't block on send to avoid deadlocks.
			select {
			case servingStatus <- shr.Serving:
			default:
			}
			return hcc.processResponse(hc, shr)
		})

		// streamCancel to make sure the watcher goroutine terminates.
		streamCancel()

		// If there was a timeout send an error. We do this after stream has returned.
		// This will ensure that this update prevails over any previous message that
		// stream could have sent.
		if timedout.Get() {
			hcc.tabletStats.LastError = fmt.Errorf("healthcheck timed out (latest %v)", hcc.lastResponseTimestamp)
			hcc.setServingState(false, hcc.tabletStats.LastError.Error())
			hc.updateHealth(hcc.tabletStats.Copy(), hcc.conn)
			hcErrorCounters.Add([]string{hcc.tabletStats.Target.Keyspace, hcc.tabletStats.Target.Shard, topoproto.TabletTypeLString(hcc.tabletStats.Target.TabletType)}, 1)
		}

		// Streaming RPC failed e.g. because vttablet was restarted or took too long.
		// Sleep until the next retry is up or the context is done/canceled.
		select {
		case <-hcc.ctx.Done():
			return
		case <-time.After(retryDelay):
			// Exponentially back-off to prevent tight-loop.
			retryDelay *= 2
			// Limit the retry delay backoff to the health check timeout
			if retryDelay > hc.healthCheckTimeout {
				retryDelay = hc.healthCheckTimeout
			}
		}
	}
}

// setServingState sets the tablet state to the given value.
//
// If the state changes, it logs the change so that failures
// from the health check connection are logged the first time,
// but don't continue to log if the connection stays down.
//
// hcc.mu must be locked before calling this function
func (hcc *healthCheckConn) setServingState(serving bool, reason string) {
	if !hcc.loggedServingState || (serving != hcc.tabletStats.Serving) {
		// Emit the log from a separate goroutine to avoid holding
		// the hcc lock while logging is happening
		go log.Infof("HealthCheckUpdate(Serving State): %v, tablet: %v serving => %v for %v/%v (%v) reason: %s",
			hcc.tabletStats.Name,
			topotools.TabletIdent(hcc.tabletStats.Tablet),
			serving,
			hcc.tabletStats.Tablet.GetKeyspace(),
			hcc.tabletStats.Tablet.GetShard(),
			hcc.tabletStats.Target.GetTabletType(),
			reason,
		)
		hcc.loggedServingState = true
	}

	hcc.tabletStats.Serving = serving
}

// stream streams healthcheck responses to callback.
func (hcc *healthCheckConn) stream(ctx context.Context, hc *HealthCheckImpl, callback func(*querypb.StreamHealthResponse) error) {
	if hcc.conn == nil {
		conn, err := tabletconn.GetDialer()(hcc.tabletStats.Tablet, grpcclient.FailFast(true))
		if err != nil {
			hcc.tabletStats.LastError = err
			return
		}
		hcc.conn = conn
		hcc.tabletStats.LastError = nil
	}

	if err := hcc.conn.StreamHealth(ctx, callback); err != nil {
		log.Warningf("tablet %v healthcheck stream error: %v", hcc.tabletStats.Tablet.Alias, err)
		hcc.setServingState(false, err.Error())
		hcc.tabletStats.LastError = err
		// Send nil because we intend to close the connection.
		hc.updateHealth(hcc.tabletStats.Copy(), nil)
		hcc.conn.Close(ctx)
		hcc.conn = nil
	}
}

// processResponse reads one health check response, and updates health
func (hcc *healthCheckConn) processResponse(hc *HealthCheckImpl, shr *querypb.StreamHealthResponse) error {
	select {
	case <-hcc.ctx.Done():
		return hcc.ctx.Err()
	default:
	}

	// Check for invalid data, better than panicking.
	if shr.Target == nil || shr.RealtimeStats == nil {
		return fmt.Errorf("health stats is not valid: %v", shr)
	}

	// an app-level error from tablet, force serving state.
	var healthErr error
	serving := shr.Serving
	if shr.RealtimeStats.HealthError != "" {
		healthErr = fmt.Errorf("vttablet error: %v", shr.RealtimeStats.HealthError)
		serving = false
	}

	// hcc.tabletStats.Tablet.Alias.Uid may be 0 because the youtube internal mechanism uses a different
	// code path to initialize this value. If so, we should skip this check.
	if shr.TabletAlias != nil && hcc.tabletStats.Tablet.Alias.Uid != 0 && !proto.Equal(shr.TabletAlias, hcc.tabletStats.Tablet.Alias) {
		return fmt.Errorf("health stats mismatch, tablet %+v alias does not match response alias %v", hcc.tabletStats.Tablet, shr.TabletAlias)
	}

	// In this case where a new tablet is initialized or a tablet type changes, we want to
	// initialize the counter so the rate can be calculated correctly.
	if hcc.tabletStats.Target.TabletType != shr.Target.TabletType {
		hcErrorCounters.Add([]string{shr.Target.Keyspace, shr.Target.Shard, topoproto.TabletTypeLString(shr.Target.TabletType)}, 0)
	}

	// Update our record, and notify downstream for tabletType and
	// realtimeStats change.
	hcc.lastResponseTimestamp = time.Now()
	hcc.tabletStats.Target = shr.Target
	hcc.tabletStats.MasterTermStartTime = shr.TabletExternallyReparentedTimestamp
	hcc.tabletStats.Stats = shr.RealtimeStats
	hcc.tabletStats.LastError = healthErr
	reason := "healthCheck update"
	if healthErr != nil {
		reason = "healthCheck update error: " + healthErr.Error()
	}
	hcc.setServingState(serving, reason)

	// TODO(deepthi): do we need a copy? updateHealth should be done by HealthCheckConn, not healthCheck
	hc.updateHealth(hcc.tabletStats.Copy(), hcc.conn)
	return nil
}

func (hc *HealthCheckImpl) deleteConn(tablet *topodatapb.Tablet) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	key := TabletToMapKey(tablet)
	th, ok := hc.addrToHealth[key]
	if !ok {
		return
	}
	// Make sure the key still corresponds to the tablet we want to delete.
	// If it doesn't match, we should do nothing. The tablet we were asked to
	// delete is already gone, and some other tablet is using the key
	// (host:port) that the original tablet used to use, which is fine.
	if !topoproto.TabletAliasEqual(tablet.Alias, th.latestTabletStats.Tablet.Alias) {
		return
	}
	hc.deleteConnLocked(key, th)
}

func (hc *HealthCheckImpl) deleteConnLocked(key string, th *tabletHealth) {
	th.latestTabletStats.Up = false
	th.cancelFunc()
	delete(hc.addrToHealth, key)
}

// AddTablet adds the tablet, and starts health check.
// It does not block on making connection.
// name is an optional tag for the tablet, e.g. an alternative address.
func (hc *HealthCheckImpl) AddTablet(tablet *topodatapb.Tablet, name string) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	key := TabletToMapKey(tablet)
	hcc := &healthCheckConn{
		ctx: ctx,
		tabletStats: tabletStats{
			Key:    key,
			Tablet: tablet,
			Name:   name,
			Target: &querypb.Target{},
			Up:     true,
		},
	}
	hc.mu.Lock()
	if hc.addrToHealth == nil {
		// already closed.
		hc.mu.Unlock()
		return
	}
	if th, ok := hc.addrToHealth[key]; ok {
		// Something already exists at this key.
		// If it's the same tablet, something is wrong.
		if topoproto.TabletAliasEqual(th.latestTabletStats.Tablet.Alias, tablet.Alias) {
			hc.mu.Unlock()
			log.Warningf("refusing to add duplicate tablet %v for %v: %+v", name, tablet.Alias.Cell, tablet)
			return
		}
		// If it's a different tablet, then we trust this new tablet that claims
		// it has taken over the host:port that the old tablet used to be on.
		// Remove the old tablet to clear the way.
		hc.deleteConnLocked(key, th)
	}
	hc.addrToHealth[key] = &tabletHealth{
		cancelFunc:        cancelFunc,
		latestTabletStats: hcc.tabletStats,
	}
	hc.initialUpdatesWG.Add(1)
	hc.connsWG.Add(1)
	hc.mu.Unlock()

	// checkConn should take a tabletStats
	// should exit when healthcheck context is canceled
	go hc.checkConn(hcc, name)
}

// RemoveTablet removes the tablet, and stops the health check.
// It does not block.
func (hc *HealthCheckImpl) RemoveTablet(tablet *topodatapb.Tablet) {
	hc.deleteConn(tablet)
}

// ReplaceTablet removes the old tablet and adds the new tablet.
func (hc *HealthCheckImpl) ReplaceTablet(old, new *topodatapb.Tablet, name string) {
	hc.deleteConn(old)
	hc.AddTablet(new, name)
}

// WaitForInitialStatsUpdates waits until all tablets added via AddTablet() call
// were propagated to downstream via corresponding StatsUpdate() calls.
func (hc *HealthCheckImpl) WaitForInitialStatsUpdates() {
	hc.initialUpdatesWG.Wait()
}

// GetConnection returns the TabletConn of the given tablet.
func (hc *HealthCheckImpl) GetConnection(key string) queryservice.QueryService {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	th := hc.addrToHealth[key]
	if th == nil {
		return nil
	}
	return th.conn
}

// TabletsCacheStatus is the current tablets for a cell/target.
type TabletsCacheStatus struct {
	Cell         string
	Target       *querypb.Target
	TabletsStats TabletStatsList
}

// TabletStatsList is used for sorting.
type TabletStatsList []*tabletStats

// Len is part of sort.Interface.
func (tsl TabletStatsList) Len() int {
	return len(tsl)
}

// Less is part of sort.Interface
func (tsl TabletStatsList) Less(i, j int) bool {
	name1 := tsl[i].Name
	if name1 == "" {
		name1 = tsl[i].Key
	}
	name2 := tsl[j].Name
	if name2 == "" {
		name2 = tsl[j].Key
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
			extra = fmt.Sprintf(" (MasterTS: %v)", ts.MasterTermStartTime)
		} else {
			extra = fmt.Sprintf(" (RepLag: %v)", ts.Stats.SecondsBehindMaster)
		}
		name := ts.Name
		if name == "" {
			name = ts.GetTabletHostPort()
		}
		tLinks = append(tLinks, fmt.Sprintf(`<a href="%s" style="color:%v">%v</a>%v`, ts.getTabletDebugURL(), color, name, extra))
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
	tcsMap := hc.cacheStatusMap()
	tcsl := make(TabletsCacheStatusList, 0, len(tcsMap))
	for _, tcs := range tcsMap {
		tcsl = append(tcsl, tcs)
	}
	sort.Sort(tcsl)
	return tcsl
}

func (hc *HealthCheckImpl) cacheStatusMap() map[string]*TabletsCacheStatus {
	tcsMap := make(map[string]*TabletsCacheStatus)
	hc.mu.Lock()
	defer hc.mu.Unlock()
	for _, th := range hc.addrToHealth {
		key := fmt.Sprintf("%v.%v.%v.%v", th.latestTabletStats.Tablet.Alias.Cell, th.latestTabletStats.Target.Keyspace, th.latestTabletStats.Target.Shard, th.latestTabletStats.Target.TabletType.String())
		var tcs *TabletsCacheStatus
		var ok bool
		if tcs, ok = tcsMap[key]; !ok {
			tcs = &TabletsCacheStatus{
				Cell:   th.latestTabletStats.Tablet.Alias.Cell,
				Target: th.latestTabletStats.Target,
			}
			tcsMap[key] = tcs
		}
		stats := th.latestTabletStats
		tcs.TabletsStats = append(tcs.TabletsStats, &stats)
	}
	return tcsMap
}

// Close stops the healthcheck.
func (hc *HealthCheckImpl) Close() error {
	hc.mu.Lock()
	for _, th := range hc.addrToHealth {
		th.cancelFunc()
	}
	hc.addrToHealth = nil
	// Release the lock early or a pending checkHealthCheckTimeout
	// cannot get a read lock on it.
	hc.mu.Unlock()

	// Wait for the checkHealthCheckTimeout Go routine and each Go
	// routine per tablet.
	hc.connsWG.Wait()

	return nil
}

// topologyWatcherMaxRefreshLag returns the maximum lag since the watched
// cells were refreshed from the topo server
func (hc *HealthCheckImpl) topologyWatcherMaxRefreshLag() time.Duration {
	var lag time.Duration
	for _, tw := range hc.topoWatchers {
		cellLag := tw.RefreshLag()
		if cellLag > lag {
			lag = cellLag
		}
	}
	return lag
}

// topologyWatcherChecksum returns a checksum of the topology watcher state
func (hc *HealthCheckImpl) topologyWatcherChecksum() int64 {
	var checksum int64
	for _, tw := range hc.topoWatchers {
		checksum = checksum ^ int64(tw.TopoChecksum())
	}
	return checksum
}

// GetTabletAndConnection gets you a tablet connection and it's "Key" as produced by TabletToMapKey
// The Key is used by the caller to keep track of invalidTablets
func (hc *HealthCheckImpl) GetTabletAndConnection(target *querypb.Target, localCell string, invalidTablets map[string]bool) (string, queryservice.QueryService, error) {
	tablets := hc.getHealthyTabletStats(target.Keyspace, target.Shard, target.TabletType)
	if len(tablets) == 0 {
		// fail fast if there is no tablet
		err := vterrors.New(vtrpcpb.Code_UNAVAILABLE, "no valid tablet")
		return "", nil, err
	}
	hc.shuffleTablets(localCell, tablets)

	// skip tablets we tried before
	var ts *tabletStats
	for _, t := range tablets {
		if _, ok := invalidTablets[t.Key]; !ok {
			ts = &t
			conn := hc.GetConnection(ts.Key)
			if conn == nil {
				invalidTablets[ts.Key] = true
			} else {
				return ts.Key, conn, nil
			}
		}
	}
	err := vterrors.New(vtrpcpb.Code_UNAVAILABLE, "no available connection")
	return "", nil, err
}

// GetHealthyTabletStats returns only the healthy targets.
// The returned array is owned by the caller.
// For TabletType_MASTER, this will only return at most one entry,
// the most recent tablet of type master.
func (hc *HealthCheckImpl) getHealthyTabletStats(keyspace, shard string, tabletType topodatapb.TabletType) []tabletStats {
	var result []tabletStats
	// we check all tablet types in all cells because of cellAliases
	for _, tsc := range hc.tscByCell {
		e := tsc.getEntry(keyspace, shard, tabletType)
		if e != nil {
			result = append(result, e.getHealthyTabletStats()...)
		}
	}
	return result
}

func (e *tabletStatsCacheEntry) getHealthyTabletStats() []tabletStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	result := make([]tabletStats, len(e.healthy))
	for _, ts := range e.healthy {
		result = append(result, *ts)
	}
	return result
}

func (hc *HealthCheckImpl) shuffleTablets(cell string, tablets []tabletStats) {
	sameCell, diffCell, sameCellMax := 0, 0, -1
	length := len(tablets)

	// move all same cell tablets to the front, this is O(n)
	for {
		sameCellMax = diffCell - 1
		sameCell = hc.nextTablet(cell, tablets, sameCell, length, true)
		diffCell = hc.nextTablet(cell, tablets, diffCell, length, false)
		// either no more diffs or no more same cells should stop the iteration
		if sameCell < 0 || diffCell < 0 {
			break
		}

		if sameCell < diffCell {
			// fast forward the `sameCell` lookup to `diffCell + 1`, `diffCell` unchanged
			sameCell = diffCell + 1
		} else {
			// sameCell > diffCell, swap needed
			tablets[sameCell], tablets[diffCell] = tablets[diffCell], tablets[sameCell]
			sameCell++
			diffCell++
		}
	}

	//shuffle in same cell tablets
	for i := sameCellMax; i > 0; i-- {
		swap := rand.Intn(i + 1)
		tablets[i], tablets[swap] = tablets[swap], tablets[i]
	}

	//shuffle in diff cell tablets
	for i, diffCellMin := length-1, sameCellMax+1; i > diffCellMin; i-- {
		swap := rand.Intn(i-sameCellMax) + diffCellMin
		tablets[i], tablets[swap] = tablets[swap], tablets[i]
	}
}

func (hc *HealthCheckImpl) nextTablet(cell string, tablets []tabletStats, offset, length int, sameCell bool) int {
	for ; offset < length; offset++ {
		if (tablets[offset].Tablet.Alias.Cell == cell) == sameCell {
			return offset
		}
	}
	return -1
}

func (hc *HealthCheckImpl) getAliasByCell(cell string, topoServer *topo.Server) string {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if alias, ok := hc.cellAliases[cell]; ok {
		return alias
	}

	alias := topo.GetAliasByCell(context.Background(), topoServer, cell)
	hc.cellAliases[cell] = alias

	return alias
}
