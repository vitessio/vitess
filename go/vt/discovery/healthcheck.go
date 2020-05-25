/*
Copyright 2020 The Vitess Authors.

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
// For an example how to use the HealthCheck object, see vtgate/tabletgateway.go
//
// Tablets have to be manually added to the HealthCheck using AddTablet().
// Alternatively, use a Watcher implementation which will constantly watch
// a source (e.g. the topology) and add and remove tablets as they are
// added or removed from the source.
// For a Watcher example have a look at NewCellTabletsWatcher().
//
// Internally, the HealthCheck module is connected to each tablet and has a
// streaming RPC (StreamHealth) open to receive periodic health infos.
package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/flagutil"

	"vitess.io/vitess/go/vt/topo"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

var (
	hcErrorCounters          = stats.NewCountersWithMultiLabels("HealthcheckErrors", "Healthcheck Errors", []string{"Keyspace", "ShardName", "TabletType"})
	hcMasterPromotedCounters = stats.NewCountersWithMultiLabels("HealthcheckMasterPromoted", "Master promoted in keyspace/shard name because of health check errors", []string{"Keyspace", "ShardName"})
	healthcheckOnce          sync.Once

	// TabletURLTemplateString is a flag to generate URLs for the tablets that vtgate discovers.
	TabletURLTemplateString = flag.String("tablet_url_template", "http://{{.GetTabletHostPort}}", "format string describing debug tablet url formatting. See the Go code for getTabletDebugURL() how to customize this.")
	tabletURLTemplate       *template.Template

	//TODO(deepthi): change these vars back to unexported when discoveryGateway is removed

	// CellsToWatch is the list of cells the healthcheck operates over. If it is empty, only the local cell is watched
	CellsToWatch = flag.String("cells_to_watch", "", "comma-separated list of cells for watching tablets")
	// AllowedTabletTypes is the list of allowed tablet types. e.g. {MASTER, REPLICA}
	AllowedTabletTypes []topodata.TabletType
	// TabletFilters are the keyspace|shard or keyrange filters to apply to the full set of tablets
	TabletFilters flagutil.StringListValue
	// KeyspacesToWatch - if provided this specifies which keyspaces should be
	// visible to the healthcheck. By default the healthcheck will watch all keyspaces.
	KeyspacesToWatch flagutil.StringListValue
	// RefreshInterval is the interval at which healthcheck refreshes its list of tablets from topo
	RefreshInterval = flag.Duration("tablet_refresh_interval", 1*time.Minute, "tablet refresh interval")
	// RefreshKnownTablets tells us whether to process all tablets or only new tablets
	RefreshKnownTablets = flag.Bool("tablet_refresh_known_tablets", true, "tablet refresh reloads the tablet address/port map from topo in case it changes")
	// TopoReadConcurrency tells us how many topo reads are allowed in parallel
	TopoReadConcurrency = flag.Int("topo_read_concurrency", 32, "concurrent topo reads")
)

// See the documentation for NewHealthCheck below for an explanation of these parameters.
const (
	DefaultHealthCheckRetryDelay = 5 * time.Second
	DefaultHealthCheckTimeout    = 1 * time.Minute

	// DefaultTopoReadConcurrency is used as the default value for the TopoReadConcurrency parameter of a TopologyWatcher.
	DefaultTopoReadConcurrency int = 5
	// DefaultTopologyWatcherRefreshInterval is used as the default value for
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
    <th>TabletHealth</th>
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

// TabletRecorder is a sub interface of HealthCheck.
// It is separated out to enable unit testing.
type TabletRecorder interface {
	// AddTablet adds the tablet.
	AddTablet(tablet *topodata.Tablet)
	// RemoveTablet removes the tablet.
	RemoveTablet(tablet *topodata.Tablet)
	// ReplaceTablet does an AddTablet and RemoveTablet in one call, effectively replacing the old tablet with the new.
	ReplaceTablet(old, new *topodata.Tablet)
}

// HealthCheck performs health checking and stores the results.
// The goal of this object is to maintain a StreamHealth RPC
// to a lot of tablets. Tablets are added / removed by calling the
// AddTablet / RemoveTablet methods (other discovery module objects
// can for instance watch the topology and call these).
// It contains a map of tabletHealthCheck objects by Alias.
// Each tabletHealthCheck object stores the health information for one tablet.
// A checkConn goroutine is spawned for each tabletHealthCheck, which is responsible for
// keeping that tabletHealthCheck up-to-date.
// If checkConn terminates for any reason, then the corresponding tabletHealthCheck object
// is removed from the map. When a tabletHealthCheck
// gets removed from the map, its cancelFunc gets called, which ensures that the associated
// checkConn goroutine eventually terminates.
type HealthCheck struct {
	// Immutable fields set at construction time.
	retryDelay         time.Duration
	healthCheckTimeout time.Duration
	ts                 *topo.Server
	cell               string
	// mu protects all the following fields.
	mu sync.Mutex
	// authoritative map of tabletHealth by alias
	healthByAlias map[string]*tabletHealthCheck
	// a map keyed by keyspace.shard.tabletType
	// contains a map of tabletHealthCheck keyed by tablet alias for each tablet relevant to the keyspace.shard.tabletType
	// has to be kept in sync with healthByAlias
	healthData map[string]map[string]*tabletHealthCheck
	// another map keyed by keyspace.shard.tabletType, this one containing a sorted list of tabletHealthCheck
	healthy map[string][]*tabletHealthCheck
	// connsWG keeps track of all launched Go routines that monitor tablet connections.
	connsWG sync.WaitGroup
	// topology watchers that inform healthcheck of tablets being added and deleted
	topoWatchers []*TopologyWatcher
	// cellAliases is a cache of cell aliases
	cellAliases map[string]string
	// mutex to protect subscribers
	subMu sync.Mutex
	// subscribers
	subscribers map[chan *TabletHealth]struct{}
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
// topoServer.
//   The topology server that this healthcheck object can use to retrieve cell or tablet information
// localCell.
//   The localCell for this healthcheck
// callback.
//   A function to call when there is a master change. Used to notify vtgate's buffer to stop buffering.
func NewHealthCheck(ctx context.Context, retryDelay, healthCheckTimeout time.Duration, topoServer *topo.Server, localCell string) *HealthCheck {
	log.Infof("loading tablets for cells: %v", *CellsToWatch)

	hc := &HealthCheck{
		ts:                 topoServer,
		cell:               localCell,
		retryDelay:         retryDelay,
		healthCheckTimeout: healthCheckTimeout,
		healthByAlias:      make(map[string]*tabletHealthCheck),
		healthData:         make(map[string]map[string]*tabletHealthCheck),
		healthy:            make(map[string][]*tabletHealthCheck),
		subscribers:        make(map[chan *TabletHealth]struct{}),
		cellAliases:        make(map[string]string),
	}
	var topoWatchers []*TopologyWatcher
	var filter TabletFilter
	cells := strings.Split(*CellsToWatch, ",")
	if len(cells) == 0 {
		cells = append(cells, localCell)
	}
	for _, c := range cells {
		log.Infof("Setting up healthcheck for cell: %v", c)
		if c == "" {
			continue
		}
		if len(TabletFilters) > 0 {
			if len(KeyspacesToWatch) > 0 {
				log.Exitf("Only one of -keyspaces_to_watch and -tablet_filters may be specified at a time")
			}

			fbs, err := NewFilterByShard(TabletFilters)
			if err != nil {
				log.Exitf("Cannot parse tablet_filters parameter: %v", err)
			}
			filter = fbs
		} else if len(KeyspacesToWatch) > 0 {
			filter = NewFilterByKeyspace(KeyspacesToWatch)
		}
		topoWatchers = append(topoWatchers, NewCellTabletsWatcher(ctx, topoServer, hc, filter, c, *RefreshInterval, *RefreshKnownTablets, *TopoReadConcurrency))
	}

	hc.topoWatchers = topoWatchers
	healthcheckOnce.Do(func() {
		http.Handle("/debug/gateway", hc)
	})

	// start the topo watches here
	for _, tw := range hc.topoWatchers {
		go tw.Start()
	}

	return hc
}

// AddTablet adds the tablet, and starts health check.
// It does not block on making connection.
// name is an optional tag for the tablet, e.g. an alternative address.
func (hc *HealthCheck) AddTablet(tablet *topodata.Tablet) {
	log.Infof("Calling AddTablet for tablet: %v", tablet)
	// check whether we should really add this tablet
	if !hc.isIncluded(tablet) {
		return
	}
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if hc.healthByAlias == nil {
		// already closed.
		return
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	target := &query.Target{
		Keyspace:   tablet.Keyspace,
		Shard:      tablet.Shard,
		TabletType: tablet.Type,
	}
	th := &tabletHealthCheck{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		Tablet:     tablet,
		Target:     target,
	}

	// add to our datastore
	key := hc.keyFromTarget(target)
	tabletAlias := topoproto.TabletAliasString(tablet.Alias)
	// TODO: can this ever already exist?
	if _, ok := hc.healthByAlias[tabletAlias]; ok {
		log.Errorf("Program bug")
		return
	}
	hc.healthByAlias[tabletAlias] = th
	if ths, ok := hc.healthData[key]; !ok {
		hc.healthData[key] = make(map[string]*tabletHealthCheck)
		hc.healthData[key][tabletAlias] = th
	} else {
		// just overwrite it if it exists already?
		ths[tabletAlias] = th
	}

	res := th.SimpleCopy()
	hc.broadcast(res)
	hc.connsWG.Add(1)
	go th.checkConn(hc)
}

// RemoveTablet removes the tablet, and stops the health check.
// It does not block.
func (hc *HealthCheck) RemoveTablet(tablet *topodata.Tablet) {
	if !hc.isIncluded(tablet) {
		return
	}
	hc.deleteTablet(tablet)
}

// ReplaceTablet removes the old tablet and adds the new tablet.
func (hc *HealthCheck) ReplaceTablet(old, new *topodata.Tablet) {
	hc.deleteTablet(old)
	hc.AddTablet(new)
}

func (hc *HealthCheck) deleteTablet(tablet *topodata.Tablet) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	key := hc.keyFromTablet(tablet)
	tabletAlias := topoproto.TabletAliasString(tablet.Alias)
	// delete from authoritative map
	th, ok := hc.healthByAlias[tabletAlias]
	if !ok {
		log.Infof("We have no health data for tablet: %v, it might have been deleted already", tabletAlias)
		return
	}
	// calling this will end the context associated with th.checkConn
	// which will call finalizeConn, which will close the connection
	th.cancelFunc()
	delete(hc.healthByAlias, tabletAlias)
	// delete from map by keyspace.shard.tabletType
	ths, ok := hc.healthData[key]
	if !ok {
		log.Warningf("We have no health data for target: %v", key)
		return
	}
	delete(ths, tabletAlias)
}

func (hc *HealthCheck) updateHealth(th *tabletHealthCheck, shr *query.StreamHealthResponse, currentTarget *query.Target, trivialNonMasterUpdate bool, isMasterUpdate bool, isMasterChange bool) {
	// hc.healthByAlias is authoritative, it should be updated
	hc.mu.Lock()
	defer hc.mu.Unlock()

	tabletAlias := topoproto.TabletAliasString(shr.TabletAlias)
	// this will only change the first time, but it's easiest to set it always rather than check and set
	hc.healthByAlias[tabletAlias] = th

	hcErrorCounters.Add([]string{shr.Target.Keyspace, shr.Target.Shard, topoproto.TabletTypeLString(shr.Target.TabletType)}, 0)
	targetChanged := currentTarget.TabletType != shr.Target.TabletType || currentTarget.Keyspace != shr.Target.Keyspace || currentTarget.Shard != shr.Target.Shard
	if targetChanged {
		// keyspace and shard are not expected to change, but just in case ...
		// move this tabletHealthCheck to the correct map
		oldTargetKey := hc.keyFromTarget(currentTarget)
		newTargetKey := hc.keyFromTarget(shr.Target)
		delete(hc.healthData[oldTargetKey], tabletAlias)
		_, ok := hc.healthData[newTargetKey]
		if !ok {
			hc.healthData[newTargetKey] = make(map[string]*tabletHealthCheck)
		}
		hc.healthData[newTargetKey][tabletAlias] = th
	}

	targetKey := hc.keyFromTarget(shr.Target)
	if isMasterUpdate {
		if len(hc.healthy[targetKey]) == 0 {
			hc.healthy[targetKey] = append(hc.healthy[targetKey], th)
		} else {
			// We already have one up server, see if we
			// need to replace it.
			if shr.TabletExternallyReparentedTimestamp < hc.healthy[targetKey][0].MasterTermStartTime {
				log.Warningf("not marking healthy master %s as Up for %s because its MasterTermStartTime is smaller than the highest known timestamp from previous MASTERs %s: %d < %d ",
					topoproto.TabletAliasString(shr.TabletAlias),
					topoproto.KeyspaceShardString(shr.Target.Keyspace, shr.Target.Shard),
					topoproto.TabletAliasString(hc.healthy[targetKey][0].Tablet.Alias),
					shr.TabletExternallyReparentedTimestamp,
					hc.healthy[targetKey][0].MasterTermStartTime)
			} else {
				// Just replace it.
				hc.healthy[targetKey][0] = th
			}
		}
	}
	if !trivialNonMasterUpdate {
		if shr.Target.TabletType != topodata.TabletType_MASTER {
			all := hc.healthData[targetKey]
			allArray := make([]*tabletHealthCheck, 0, len(all))
			for _, s := range all {
				allArray = append(allArray, s)
			}
			hc.healthy[targetKey] = FilterStatsByReplicationLag(allArray)
		}
		if targetChanged && currentTarget.TabletType != topodata.TabletType_MASTER { // also recompute old target's healthy list
			oldTargetKey := hc.keyFromTarget(currentTarget)
			all := hc.healthData[oldTargetKey]
			allArray := make([]*tabletHealthCheck, 0, len(all))
			for _, s := range all {
				allArray = append(allArray, s)
			}
			hc.healthy[oldTargetKey] = FilterStatsByReplicationLag(allArray)
		}
	}
	result := th.SimpleCopy()
	if isMasterChange {
		log.Errorf("Adding 1 to MasterPromoted counter for tablet: %v, shr.Tablet: %v, shr.TabletType: %v", currentTarget, topoproto.TabletAliasString(shr.TabletAlias), shr.Target.TabletType)
		hcMasterPromotedCounters.Add([]string{shr.Target.Keyspace, shr.Target.Shard}, 1)
	}
	// broadcast to subscribers
	hc.broadcast(result)

}

// Subscribe adds a listener. Only used for testing right now
func (hc *HealthCheck) Subscribe() chan *TabletHealth {
	hc.subMu.Lock()
	defer hc.subMu.Unlock()
	c := make(chan *TabletHealth, 2)
	hc.subscribers[c] = struct{}{}
	return c
}

// Unsubscribe removes a listener. Only used for testing right now
func (hc *HealthCheck) Unsubscribe(c chan *TabletHealth) {
	hc.subMu.Lock()
	defer hc.subMu.Unlock()
	delete(hc.subscribers, c)
}

func (hc *HealthCheck) broadcast(th *TabletHealth) {
	hc.subMu.Lock()
	defer hc.subMu.Unlock()
	for c := range hc.subscribers {
		select {
		case c <- th:
		default:
		}
	}
}

// CacheStatus returns a displayable version of the cache.
func (hc *HealthCheck) CacheStatus() TabletsCacheStatusList {
	tcsMap := hc.cacheStatusMap()
	tcsl := make(TabletsCacheStatusList, 0, len(tcsMap))
	for _, tcs := range tcsMap {
		tcsl = append(tcsl, tcs)
	}
	sort.Sort(tcsl)
	return tcsl
}

func (hc *HealthCheck) cacheStatusMap() map[string]*TabletsCacheStatus {
	tcsMap := make(map[string]*TabletsCacheStatus)
	hc.mu.Lock()
	defer hc.mu.Unlock()
	for _, th := range hc.healthByAlias {
		key := fmt.Sprintf("%v.%v.%v.%v", th.Tablet.Alias.Cell, th.Target.Keyspace, th.Target.Shard, th.Target.TabletType.String())
		var tcs *TabletsCacheStatus
		var ok bool
		if tcs, ok = tcsMap[key]; !ok {
			tcs = &TabletsCacheStatus{
				Cell:   th.Tablet.Alias.Cell,
				Target: th.Target,
			}
			tcsMap[key] = tcs
		}
		tcs.TabletsStats = append(tcs.TabletsStats, th.SimpleCopy())
	}
	return tcsMap
}

// Close stops the healthcheck.
func (hc *HealthCheck) Close() error {
	hc.mu.Lock()
	for _, th := range hc.healthByAlias {
		th.cancelFunc()
	}
	hc.healthByAlias = nil
	hc.healthData = nil
	for _, tw := range hc.topoWatchers {
		tw.Stop()
	}
	for s := range hc.subscribers {
		close(s)
	}
	hc.subscribers = nil
	// Release the lock early or a pending checkHealthCheckTimeout
	// cannot get a read lock on it.
	hc.mu.Unlock()

	// Wait for the checkHealthCheckTimeout Go routine and each Go
	// routine per tablet.
	hc.connsWG.Wait()

	return nil
}

// GetHealthyTabletStats returns only the healthy tablets.
// The returned array is owned by the caller.
// For TabletType_MASTER, this will only return at most one entry,
// the most recent tablet of type master.
// This returns a copy of the data so that callers can access without
// synchronization
func (hc *HealthCheck) GetHealthyTabletStats(target *query.Target) []*TabletHealth {
	var result []*TabletHealth
	hc.mu.Lock()
	defer hc.mu.Unlock()
	for _, thc := range hc.healthy[hc.keyFromTarget(target)] {
		result = append(result, thc.SimpleCopy())
	}
	return result
}

// getTabletStats returns all tablets for the given target.
// The returned array is owned by the caller.
// For TabletType_MASTER, this will only return at most one entry,
// the most recent tablet of type master.
func (hc *HealthCheck) getTabletStats(target *query.Target) []*TabletHealth {
	var result []*TabletHealth
	hc.mu.Lock()
	defer hc.mu.Unlock()
	ths := hc.healthData[hc.keyFromTarget(target)]
	for _, th := range ths {
		result = append(result, th.SimpleCopy())
	}
	return result
}

// WaitForTablets waits for at least one tablet in the given
// keyspace / shard / tablet type before returning. The tablets do not
// have to be healthy.  It will return ctx.Err() if the context is canceled.
func (hc *HealthCheck) WaitForTablets(ctx context.Context, keyspace, shard string, tabletType topodata.TabletType) error {
	targets := []*query.Target{
		{
			Keyspace:   keyspace,
			Shard:      shard,
			TabletType: tabletType,
		},
	}
	return hc.waitForTablets(ctx, targets, false)
}

// WaitForAllServingTablets waits for at least one healthy serving tablet in
// each given target before returning.
// It will return ctx.Err() if the context is canceled.
// It will return an error if it can't read the necessary topology records.
func (hc *HealthCheck) WaitForAllServingTablets(ctx context.Context, targets []*query.Target) error {
	return hc.waitForTablets(ctx, targets, true)
}

// waitForTablets is the internal method that polls for tablets.
func (hc *HealthCheck) waitForTablets(ctx context.Context, targets []*query.Target, requireServing bool) error {
	for {
		// We nil targets as we find them.
		allPresent := true
		for i, target := range targets {
			if target == nil {
				continue
			}

			var stats []*TabletHealth
			if requireServing {
				stats = hc.GetHealthyTabletStats(target)
			} else {
				stats = hc.getTabletStats(target)
			}
			if len(stats) == 0 {
				allPresent = false
			} else {
				targets[i] = nil
			}
		}

		if allPresent {
			// we found everything we needed
			return nil
		}

		// Unblock after the sleep or when the context has expired.
		timer := time.NewTimer(waitAvailableTabletInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Target includes cell which we ignore here
// because tabletStatsCache is intended to be per-cell
func (hc *HealthCheck) keyFromTarget(target *query.Target) string {
	return fmt.Sprintf("%s.%s.%d", target.Keyspace, target.Shard, target.TabletType)
}

func (hc *HealthCheck) keyFromTablet(tablet *topodata.Tablet) string {
	return fmt.Sprintf("%s.%s.%d", tablet.Keyspace, tablet.Shard, tablet.Type)
}

func (hc *HealthCheck) getAliasByCell(cell string) string {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if alias, ok := hc.cellAliases[cell]; ok {
		return alias
	}

	alias := topo.GetAliasByCell(context.Background(), hc.ts, cell)
	// Currently cell aliases have to be non-overlapping.
	// If that changes, this will need to change to account for overlaps.
	hc.cellAliases[cell] = alias

	return alias
}

func (hc *HealthCheck) isIncluded(tablet *topodata.Tablet) bool {
	if tablet.Type == topodata.TabletType_MASTER {
		return true
	}
	if tablet.Alias.Cell == hc.cell {
		return true
	}
	if hc.getAliasByCell(tablet.Alias.Cell) == hc.getAliasByCell(hc.cell) {
		return true
	}
	return false
}

// topologyWatcherMaxRefreshLag returns the maximum lag since the watched
// cells were refreshed from the topo server
func (hc *HealthCheck) topologyWatcherMaxRefreshLag() time.Duration {
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
func (hc *HealthCheck) topologyWatcherChecksum() int64 {
	var checksum int64
	for _, tw := range hc.topoWatchers {
		checksum = checksum ^ int64(tw.TopoChecksum())
	}
	return checksum
}

// RegisterStats registers the connection counts stats
func (hc *HealthCheck) RegisterStats() {
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
func (hc *HealthCheck) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	status := hc.CacheStatus()
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
func (hc *HealthCheck) servingConnStats() map[string]int64 {
	res := make(map[string]int64)
	hc.mu.Lock()
	defer hc.mu.Unlock()
	for _, th := range hc.healthByAlias {
		th.mu.Lock()
		if !th.Serving || th.LastError != nil {
			th.mu.Unlock()
			continue
		}
		key := fmt.Sprintf("%s.%s.%s", th.Target.Keyspace, th.Target.Shard, topoproto.TabletTypeLString(th.Target.TabletType))
		th.mu.Unlock()
		res[key]++
	}
	return res
}

// stateChecksum returns a crc32 checksum of the healthcheck state
func (hc *HealthCheck) stateChecksum() int64 {
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
			fmt.Fprintf(&buf, "%v%v\n", ts.Serving, ts.MasterTermStartTime)
		}
	}

	return int64(crc32.ChecksumIEEE(buf.Bytes()))
}
