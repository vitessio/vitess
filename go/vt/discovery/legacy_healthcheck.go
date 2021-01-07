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
// Deprecated
// Use the LegacyHealthCheck object to query for tablets and their health.
//
// For an example how to use the LegacyHealthCheck object, see worker/topo_utils.go.
//
// Tablets have to be manually added to the LegacyHealthCheck using AddTablet().
// Alternatively, use a Watcher implementation which will constantly watch
// a source (e.g. the topology) and add and remove tablets as they are
// added or removed from the source.
// For a Watcher example have a look at NewLegacyShardReplicationWatcher().
//
// Each LegacyHealthCheck has a LegacyHealthCheckStatsListener that will receive
// notification of when tablets go up and down.
// LegacyTabletStatsCache is one implementation, that caches the known tablets
// and the healthy ones per keyspace/shard/tabletType.
//
// Internally, the LegacyHealthCheck module is connected to each tablet and has a
// streaming RPC (StreamHealth) open to receive periodic health infos.
package discovery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	// LegacyHealthCheckTemplate is the HTML code to display a TabletsCacheStatusList
	LegacyHealthCheckTemplate = `
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

func init() {
	// Flags are not parsed at this point and the default value of the flag (just the hostname) will be used.
	ParseTabletURLTemplateFromFlag()
}

// LegacyHealthCheckStatsListener is the listener to receive health check stats update.
type LegacyHealthCheckStatsListener interface {
	// StatsUpdate is called when:
	// - a new tablet is known to the LegacyHealthCheck, and its first
	//   streaming healthcheck is returned. (then ts.Up is true).
	// - a tablet is removed from the list of tablets we watch
	//   (then ts.Up is false).
	// - a tablet dynamically changes its type. When registering the
	//   listener, if sendDownEvents is true, two events are generated
	//   (ts.Up false on the old type, ts.Up true on the new type).
	//   If it is false, only one event is sent (ts.Up true on the new
	//   type).
	StatsUpdate(*LegacyTabletStats)
}

// LegacyTabletStats is returned when getting the set of tablets.
type LegacyTabletStats struct {
	// Key uniquely identifies that serving tablet. It is computed
	// from the Tablet's record Hostname and PortMap. If a tablet
	// is restarted on different ports, its Key will be different.
	// Key is computed using the TabletToMapKey method below.
	// key can be used in GetConnection().
	Key string
	// Tablet is the tablet object that was sent to LegacyHealthCheck.AddTablet.
	Tablet *topodatapb.Tablet
	// Name is an optional tag (e.g. alternative address) for the
	// tablet.  It is supposed to represent the tablet as a task,
	// not as a process.  For instance, it can be a
	// cell+keyspace+shard+tabletType+taskIndex value.
	Name string
	// Target is the current target as returned by the streaming
	// StreamHealth RPC.
	Target *querypb.Target
	// Up describes whether the tablet is added or removed.
	Up bool
	// Serving describes if the tablet can be serving traffic.
	Serving bool
	// TabletExternallyReparentedTimestamp is the last timestamp
	// that this tablet was either elected the master, or received
	// a TabletExternallyReparented event. It is set to 0 if the
	// tablet doesn't think it's a master.
	TabletExternallyReparentedTimestamp int64
	// Stats is the current health status, as received by the
	// StreamHealth RPC (replication lag, ...).
	Stats *querypb.RealtimeStats
	// LastError is the error we last saw when trying to get the
	// tablet's healthcheck.
	LastError error
}

// String is defined because we want to print a []*LegacyTabletStats array nicely.
func (e *LegacyTabletStats) String() string {
	return fmt.Sprint(*e)
}

// DeepEqual compares two LegacyTabletStats. Since we include protos, we
// need to use proto.Equal on these.
func (e *LegacyTabletStats) DeepEqual(f *LegacyTabletStats) bool {
	return e.Key == f.Key &&
		proto.Equal(e.Tablet, f.Tablet) &&
		e.Name == f.Name &&
		proto.Equal(e.Target, f.Target) &&
		e.Up == f.Up &&
		e.Serving == f.Serving &&
		e.TabletExternallyReparentedTimestamp == f.TabletExternallyReparentedTimestamp &&
		proto.Equal(e.Stats, f.Stats) &&
		((e.LastError == nil && f.LastError == nil) ||
			(e.LastError != nil && f.LastError != nil && e.LastError.Error() == f.LastError.Error()))
}

// Copy produces a copy of LegacyTabletStats.
func (e *LegacyTabletStats) Copy() *LegacyTabletStats {
	ts := *e
	return &ts
}

// GetTabletHostPort formats a tablet host port address.
func (e LegacyTabletStats) GetTabletHostPort() string {
	vtPort := e.Tablet.PortMap["vt"]
	return netutil.JoinHostPort(e.Tablet.Hostname, vtPort)
}

// GetHostNameLevel returns the specified hostname level. If the level does not exist it will pick the closest level.
// This seems unused but can be utilized by certain url formatting templates. See getTabletDebugURL for more details.
func (e LegacyTabletStats) GetHostNameLevel(level int) string {
	chunkedHostname := strings.Split(e.Tablet.Hostname, ".")

	if level < 0 {
		return chunkedHostname[0]
	} else if level >= len(chunkedHostname) {
		return chunkedHostname[len(chunkedHostname)-1]
	} else {
		return chunkedHostname[level]
	}
}

// NamedStatusURL returns the URL for the case where a tablet server is named.
func (e LegacyTabletStats) NamedStatusURL() string {
	return "/" + topoproto.TabletAliasString(e.Tablet.Alias) + servenv.StatusURLPath()
}

// getTabletDebugURL formats a debug url to the tablet.
// It uses a format string that can be passed into the app to format
// the debug URL to accommodate different network setups. It applies
// the html/template string defined to a LegacyTabletStats object. The
// format string can refer to members and functions of LegacyTabletStats
// like a regular html/template string.
//
// For instance given a tablet with hostname:port of host.dc.domain:22
// could be configured as follows:
// http://{{.GetTabletHostPort}} -> http://host.dc.domain:22
// https://{{.Tablet.Hostname}} -> https://host.dc.domain
// https://{{.GetHostNameLevel 0}}.bastion.corp -> https://host.bastion.corp
// {{.NamedStatusURL}} -> test-0000000001/debug/status
func (e LegacyTabletStats) getTabletDebugURL() string {
	var buffer bytes.Buffer

	//Error logged
	if err := tabletURLTemplate.Execute(&buffer, e); err != nil {
		log.Errorf("tabletURLTemplate.Execute(&buffer, e) failed: %v", err)
	}
	return buffer.String()
}

// TrivialStatsUpdate returns true iff the old and new LegacyTabletStats
// haven't changed enough to warrant re-calling FilterLegacyStatsByReplicationLag.
func (e *LegacyTabletStats) TrivialStatsUpdate(n *LegacyTabletStats) bool {
	// Skip replag filter when replag remains in the low rep lag range,
	// which should be the case majority of the time.
	lowRepLag := lowReplicationLag.Seconds()
	oldRepLag := float64(e.Stats.SecondsBehindMaster)
	newRepLag := float64(n.Stats.SecondsBehindMaster)
	if oldRepLag <= lowRepLag && newRepLag <= lowRepLag {
		return true
	}

	// Skip replag filter when replag remains in the high rep lag range,
	// and did not change beyond +/- 10%.
	// when there is a high rep lag, it takes a long time for it to reduce,
	// so it is not necessary to re-calculate every time.
	// In that case, we won't save the new record, so we still
	// remember the original replication lag.
	if oldRepLag > lowRepLag && newRepLag > lowRepLag && newRepLag < oldRepLag*1.1 && newRepLag > oldRepLag*0.9 {
		return true
	}

	return false
}

// LegacyTabletRecorder is the part of the LegacyHealthCheck interface that can
// add or remove tablets. We define it as a sub-interface here so we
// can add filters on tablets if needed.
type LegacyTabletRecorder interface {
	// AddTablet adds the tablet.
	// Name is an alternate name, like an address.
	AddTablet(tablet *topodatapb.Tablet, name string)

	// RemoveTablet removes the tablet.
	RemoveTablet(tablet *topodatapb.Tablet)

	// ReplaceTablet does an AddTablet and RemoveTablet in one call, effectively replacing the old tablet with the new.
	ReplaceTablet(old, new *topodatapb.Tablet, name string)
}

// LegacyHealthCheck defines the interface of health checking module.
// The goal of this object is to maintain a StreamHealth RPC
// to a lot of tablets. Tablets are added / removed by calling the
// AddTablet / RemoveTablet methods (other discovery module objects
// can for instance watch the topology and call these).
//
// Updates to the health of all registered tablet can be watched by
// registering a listener. To get the underlying "TabletConn" object
// which is used for each tablet, use the "GetConnection()" method
// below and pass in the Key string which is also sent to the
// listener in each update (as it is part of LegacyTabletStats).
type LegacyHealthCheck interface {
	// LegacyTabletRecorder interface adds AddTablet and RemoveTablet methods.
	// AddTablet adds the tablet, and starts health check on it.
	// RemoveTablet removes the tablet, and stops its StreamHealth RPC.
	LegacyTabletRecorder

	// RegisterStats registers the connection counts and checksum stats.
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
	SetListener(listener LegacyHealthCheckStatsListener, sendDownEvents bool)
	// WaitForInitialStatsUpdates waits until all tablets added via
	// AddTablet() call were propagated to the listener via correspondingdiscovert
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
	CacheStatus() LegacyTabletsCacheStatusList
	// Close stops the healthcheck.
	Close() error
}

// LegacyHealthCheckImpl performs health checking and notifies downstream components about any changes.
// It contains a map of legacyTabletHealth objects, each of which stores the health information for
// a tablet. A checkConn goroutine is spawned for each legacyTabletHealth, which is responsible for
// keeping that legacyTabletHealth up-to-date. This is done through callbacks to updateHealth.
// If checkConn terminates for any reason, it updates legacyTabletHealth.Up as false. If a legacyTabletHealth
// gets removed from the map, its cancelFunc gets called, which ensures that the associated
// checkConn goroutine eventually terminates.
type LegacyHealthCheckImpl struct {
	// Immutable fields set at construction time.
	listener           LegacyHealthCheckStatsListener
	sendDownEvents     bool
	retryDelay         time.Duration
	healthCheckTimeout time.Duration
	// connsWG keeps track of all launched Go routines that monitor tablet connections.
	connsWG sync.WaitGroup

	// mu protects all the following fields.
	mu sync.Mutex

	// addrToHealth maps from address to legacyTabletHealth.
	addrToHealth map[string]*legacyTabletHealth

	// Wait group that's used to wait until all initial StatsUpdate() calls are made after the AddTablet() calls.
	initialUpdatesWG sync.WaitGroup
}

// legacyHealthCheckConn is a structure that lives within the scope of
// the checkConn goroutine to maintain its internal state. Therefore,
// it does not require synchronization. Changes that are relevant to
// healthcheck are transmitted through calls to LegacyHealthCheckImpl.updateHealth.
// TODO(sougou): move this and associated functions to a separate file.
type legacyHealthCheckConn struct {
	ctx context.Context

	conn                  queryservice.QueryService
	tabletStats           LegacyTabletStats
	loggedServingState    bool
	lastResponseTimestamp time.Time // timestamp of the last healthcheck response
}

// legacyTabletHealth maintains the health status of a tablet. A map of this
// structure is maintained in LegacyHealthCheckImpl.
type legacyTabletHealth struct {
	// cancelFunc must be called before discarding legacyTabletHealth.
	// This will ensure that the associated checkConn goroutine will terminate.
	cancelFunc context.CancelFunc
	// conn is the connection associated with the tablet.
	conn queryservice.QueryService
	// latestTabletStats stores the latest health stats of the tablet.
	latestTabletStats LegacyTabletStats
}

// NewLegacyDefaultHealthCheck creates a new LegacyHealthCheck object with a default configuration.
func NewLegacyDefaultHealthCheck() LegacyHealthCheck {
	return NewLegacyHealthCheck(DefaultHealthCheckRetryDelay, DefaultHealthCheckTimeout)
}

// NewLegacyHealthCheck creates a new LegacyHealthCheck object.
// Parameters:
// retryDelay.
//   The duration to wait before retrying to connect (e.g. after a failed connection
//   attempt).
// healthCheckTimeout.
//   The duration for which we consider a health check response to be 'fresh'. If we don't get
//   a health check response from a tablet for more than this duration, we consider the tablet
//   not healthy.
func NewLegacyHealthCheck(retryDelay, healthCheckTimeout time.Duration) LegacyHealthCheck {
	hc := &LegacyHealthCheckImpl{
		addrToHealth:       make(map[string]*legacyTabletHealth),
		retryDelay:         retryDelay,
		healthCheckTimeout: healthCheckTimeout,
	}

	healthcheckOnce.Do(func() {
		http.Handle("/debug/gateway", hc)
	})

	return hc
}

// RegisterStats registers the connection counts stats
func (hc *LegacyHealthCheckImpl) RegisterStats() {
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
func (hc *LegacyHealthCheckImpl) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
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
func (hc *LegacyHealthCheckImpl) servingConnStats() map[string]int64 {
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
func (hc *LegacyHealthCheckImpl) stateChecksum() int64 {
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
			fmt.Fprintf(&buf, "%v%v%v\n", ts.Up, ts.Serving, ts.TabletExternallyReparentedTimestamp)
		}
	}

	return int64(crc32.ChecksumIEEE(buf.Bytes()))
}

// updateHealth updates the legacyTabletHealth record and transmits the tablet stats
// to the listener.
func (hc *LegacyHealthCheckImpl) updateHealth(ts *LegacyTabletStats, conn queryservice.QueryService) {
	// Unconditionally send the received update at the end.
	defer func() {
		if hc.listener != nil {
			hc.listener.StatsUpdate(ts)
		}
	}()

	hc.mu.Lock()
	th, ok := hc.addrToHealth[ts.Key]
	if !ok {
		// This can happen on delete because the entry is removed first,
		// or if LegacyHealthCheckImpl has been closed.
		hc.mu.Unlock()
		return
	}
	oldts := th.latestTabletStats
	th.latestTabletStats = *ts
	th.conn = conn
	hc.mu.Unlock()

	// In the case where a tablet changes type (but not for the
	// initial message), we want to log it, and maybe advertise it too.
	if oldts.Target.TabletType != topodatapb.TabletType_UNKNOWN && oldts.Target.TabletType != ts.Target.TabletType {
		// Log and maybe notify
		log.Infof("HealthCheckUpdate(Type Change): %v, tablet: %s, target %+v => %+v, reparent time: %v",
			oldts.Name, topotools.TabletIdent(oldts.Tablet), topotools.TargetIdent(oldts.Target), topotools.TargetIdent(ts.Target), ts.TabletExternallyReparentedTimestamp)
		if hc.listener != nil && hc.sendDownEvents {
			oldts.Up = false
			hc.listener.StatsUpdate(&oldts)
		}

		// Track how often a tablet gets promoted to master. It is used for
		// comparing against the variables in go/vtgate/buffer/variables.go.
		if oldts.Target.TabletType != topodatapb.TabletType_MASTER && ts.Target.TabletType == topodatapb.TabletType_MASTER {
			hcMasterPromotedCounters.Add([]string{ts.Target.Keyspace, ts.Target.Shard}, 1)
		}
	}
}

// finalizeConn closes the health checking connection and sends the final
// notification about the tablet to downstream. To be called only on exit from
// checkConn().
func (hc *LegacyHealthCheckImpl) finalizeConn(hcc *legacyHealthCheckConn) {
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
func (hc *LegacyHealthCheckImpl) checkConn(hcc *legacyHealthCheckConn, name string) {
	defer hc.connsWG.Done()
	defer hc.finalizeConn(hcc)

	// Initial notification for downstream about the tablet existence.
	hc.updateHealth(hcc.tabletStats.Copy(), hcc.conn)
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
func (hcc *legacyHealthCheckConn) setServingState(serving bool, reason string) {
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
func (hcc *legacyHealthCheckConn) stream(ctx context.Context, hc *LegacyHealthCheckImpl, callback func(*querypb.StreamHealthResponse) error) {
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

// processResponse reads one health check response, and notifies LegacyHealthCheckStatsListener.
func (hcc *legacyHealthCheckConn) processResponse(hc *LegacyHealthCheckImpl, shr *querypb.StreamHealthResponse) error {
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

	// hcc.LegacyTabletStats.Tablet.Alias.Uid may be 0 because the youtube internal mechanism uses a different
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
	hcc.tabletStats.TabletExternallyReparentedTimestamp = shr.TabletExternallyReparentedTimestamp
	hcc.tabletStats.Stats = shr.RealtimeStats
	hcc.tabletStats.LastError = healthErr
	reason := "healthCheck update"
	if healthErr != nil {
		reason = "healthCheck update error: " + healthErr.Error()
	}
	hcc.setServingState(serving, reason)
	hc.updateHealth(hcc.tabletStats.Copy(), hcc.conn)
	return nil
}

func (hc *LegacyHealthCheckImpl) deleteConn(tablet *topodatapb.Tablet) {
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

func (hc *LegacyHealthCheckImpl) deleteConnLocked(key string, th *legacyTabletHealth) {
	th.latestTabletStats.Up = false
	th.cancelFunc()
	delete(hc.addrToHealth, key)
}

// SetListener sets the listener for healthcheck updates.
// It must be called after NewLegacyHealthCheck and before any tablets are added
// (either through AddTablet or through a Watcher).
func (hc *LegacyHealthCheckImpl) SetListener(listener LegacyHealthCheckStatsListener, sendDownEvents bool) {
	if hc.listener != nil {
		panic("must not call SetListener twice")
	}

	hc.mu.Lock()
	defer hc.mu.Unlock()
	if len(hc.addrToHealth) > 0 {
		panic("must not call SetListener after tablets were added")
	}

	hc.listener = listener
	hc.sendDownEvents = sendDownEvents
}

// AddTablet adds the tablet, and starts health check.
// It does not block on making connection.
// name is an optional tag for the tablet, e.g. an alternative address.
func (hc *LegacyHealthCheckImpl) AddTablet(tablet *topodatapb.Tablet, name string) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	key := TabletToMapKey(tablet)
	hcc := &legacyHealthCheckConn{
		ctx: ctx,
		tabletStats: LegacyTabletStats{
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
		cancelFunc()
		return
	}
	if th, ok := hc.addrToHealth[key]; ok {
		// Something already exists at this key.
		// If it's the same tablet, something is wrong.
		if topoproto.TabletAliasEqual(th.latestTabletStats.Tablet.Alias, tablet.Alias) {
			hc.mu.Unlock()
			log.Warningf("refusing to add duplicate tablet %v for %v: %+v", name, tablet.Alias.Cell, tablet)
			cancelFunc()
			return
		}
		// If it's a different tablet, then we trust this new tablet that claims
		// it has taken over the host:port that the old tablet used to be on.
		// Remove the old tablet to clear the way.
		hc.deleteConnLocked(key, th)
	}
	hc.addrToHealth[key] = &legacyTabletHealth{
		cancelFunc:        cancelFunc,
		latestTabletStats: hcc.tabletStats,
	}
	hc.initialUpdatesWG.Add(1)
	hc.connsWG.Add(1)
	hc.mu.Unlock()

	go hc.checkConn(hcc, name)
}

// RemoveTablet removes the tablet, and stops the health check.
// It does not block.
func (hc *LegacyHealthCheckImpl) RemoveTablet(tablet *topodatapb.Tablet) {
	hc.deleteConn(tablet)
}

// ReplaceTablet removes the old tablet and adds the new tablet.
func (hc *LegacyHealthCheckImpl) ReplaceTablet(old, new *topodatapb.Tablet, name string) {
	hc.deleteConn(old)
	hc.AddTablet(new, name)
}

// WaitForInitialStatsUpdates waits until all tablets added via AddTablet() call
// were propagated to downstream via corresponding StatsUpdate() calls.
func (hc *LegacyHealthCheckImpl) WaitForInitialStatsUpdates() {
	hc.initialUpdatesWG.Wait()
}

// GetConnection returns the TabletConn of the given tablet.
func (hc *LegacyHealthCheckImpl) GetConnection(key string) queryservice.QueryService {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	th := hc.addrToHealth[key]
	if th == nil {
		return nil
	}
	return th.conn
}

// LegacyTabletsCacheStatus is the current tablets for a cell/target.
type LegacyTabletsCacheStatus struct {
	Cell         string
	Target       *querypb.Target
	TabletsStats LegacyTabletStatsList
}

// LegacyTabletStatsList is used for sorting.
type LegacyTabletStatsList []*LegacyTabletStats

// Len is part of sort.Interface.
func (tsl LegacyTabletStatsList) Len() int {
	return len(tsl)
}

// Less is part of sort.Interface
func (tsl LegacyTabletStatsList) Less(i, j int) bool {
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
func (tsl LegacyTabletStatsList) Swap(i, j int) {
	tsl[i], tsl[j] = tsl[j], tsl[i]
}

// StatusAsHTML returns an HTML version of the status.
func (tcs *LegacyTabletsCacheStatus) StatusAsHTML() template.HTML {
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
			extra = fmt.Sprintf(" (MasterTS: %v)", ts.TabletExternallyReparentedTimestamp)
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

// LegacyTabletsCacheStatusList is used for sorting.
type LegacyTabletsCacheStatusList []*LegacyTabletsCacheStatus

// Len is part of sort.Interface.
func (tcsl LegacyTabletsCacheStatusList) Len() int {
	return len(tcsl)
}

// Less is part of sort.Interface
func (tcsl LegacyTabletsCacheStatusList) Less(i, j int) bool {
	return tcsl[i].Cell+"."+tcsl[i].Target.Keyspace+"."+tcsl[i].Target.Shard+"."+string(tcsl[i].Target.TabletType) <
		tcsl[j].Cell+"."+tcsl[j].Target.Keyspace+"."+tcsl[j].Target.Shard+"."+string(tcsl[j].Target.TabletType)
}

// Swap is part of sort.Interface
func (tcsl LegacyTabletsCacheStatusList) Swap(i, j int) {
	tcsl[i], tcsl[j] = tcsl[j], tcsl[i]
}

// CacheStatus returns a displayable version of the cache.
func (hc *LegacyHealthCheckImpl) CacheStatus() LegacyTabletsCacheStatusList {
	tcsMap := hc.cacheStatusMap()
	tcsl := make(LegacyTabletsCacheStatusList, 0, len(tcsMap))
	for _, tcs := range tcsMap {
		tcsl = append(tcsl, tcs)
	}
	sort.Sort(tcsl)
	return tcsl
}

func (hc *LegacyHealthCheckImpl) cacheStatusMap() map[string]*LegacyTabletsCacheStatus {
	tcsMap := make(map[string]*LegacyTabletsCacheStatus)
	hc.mu.Lock()
	defer hc.mu.Unlock()
	for _, th := range hc.addrToHealth {
		key := fmt.Sprintf("%v.%v.%v.%v", th.latestTabletStats.Tablet.Alias.Cell, th.latestTabletStats.Target.Keyspace, th.latestTabletStats.Target.Shard, th.latestTabletStats.Target.TabletType.String())
		var tcs *LegacyTabletsCacheStatus
		var ok bool
		if tcs, ok = tcsMap[key]; !ok {
			tcs = &LegacyTabletsCacheStatus{
				Cell:   th.latestTabletStats.Tablet.Alias.Cell,
				Target: th.latestTabletStats.Target,
			}
			tcsMap[key] = tcs
		}
		tabletStats := th.latestTabletStats
		tcs.TabletsStats = append(tcs.TabletsStats, &tabletStats)
	}
	return tcsMap
}

// Close stops the healthcheck.
// After Close() returned, it's guaranteed that the listener isn't
// currently executing and won't be called again.
func (hc *LegacyHealthCheckImpl) Close() error {
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
