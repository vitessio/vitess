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

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

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

type keyspaceShardTabletType string
type tabletAliasString string

//HealthCheck declares what the TabletGateway needs from the HealthCheck
type HealthCheck interface {
	// CacheStatus returns a displayable version of the health check cache.
	CacheStatus() TabletsCacheStatusList

	// Close stops the healthcheck.
	Close() error

	// WaitForAllServingTablets waits for at least one healthy serving tablet in
	// each given target before returning.
	// It will return ctx.Err() if the context is canceled.
	// It will return an error if it can't read the necessary topology records.
	WaitForAllServingTablets(ctx context.Context, targets []*query.Target) error

	// TabletConnection returns the TabletConn of the given tablet.
	TabletConnection(alias *topodata.TabletAlias) (queryservice.QueryService, error)

	// RegisterStats registers the connection counts stats
	RegisterStats()

	// GetHealthyTabletStats returns only the healthy tablets.
	// The returned array is owned by the caller.
	// For TabletType_MASTER, this will only return at most one entry,
	// the most recent tablet of type master.
	// This returns a copy of the data so that callers can access without
	// synchronization
	GetHealthyTabletStats(target *query.Target) []*TabletHealth

	// Subscribe adds a listener. Used by vtgate buffer to learn about master changes.
	Subscribe() chan *TabletHealth

	// Unsubscribe removes a listener.
	Unsubscribe(c chan *TabletHealth)
}

var _ HealthCheck = (*HealthCheckImpl)(nil)

// HealthCheckImpl performs health checking and stores the results.
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
type HealthCheckImpl struct {
	// Immutable fields set at construction time.
	retryDelay         time.Duration
	healthCheckTimeout time.Duration
	ts                 *topo.Server
	cell               string
	// mu protects all the following fields.
	mu sync.Mutex
	// authoritative map of tabletHealth by alias
	healthByAlias map[tabletAliasString]*tabletHealthCheck
	// a map keyed by keyspace.shard.tabletType
	// contains a map of TabletHealth keyed by tablet alias for each tablet relevant to the keyspace.shard.tabletType
	// has to be kept in sync with healthByAlias
	healthData map[keyspaceShardTabletType]map[tabletAliasString]*TabletHealth
	// another map keyed by keyspace.shard.tabletType, this one containing a sorted list of TabletHealth
	healthy map[keyspaceShardTabletType][]*TabletHealth
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
func NewHealthCheck(ctx context.Context, retryDelay, healthCheckTimeout time.Duration, topoServer *topo.Server, localCell, cellsToWatch string) *HealthCheckImpl {
	log.Infof("loading tablets for cells: %v", cellsToWatch)

	hc := &HealthCheckImpl{
		ts:                 topoServer,
		cell:               localCell,
		retryDelay:         retryDelay,
		healthCheckTimeout: healthCheckTimeout,
		healthByAlias:      make(map[tabletAliasString]*tabletHealthCheck),
		healthData:         make(map[keyspaceShardTabletType]map[tabletAliasString]*TabletHealth),
		healthy:            make(map[keyspaceShardTabletType][]*TabletHealth),
		subscribers:        make(map[chan *TabletHealth]struct{}),
		cellAliases:        make(map[string]string),
	}
	var topoWatchers []*TopologyWatcher
	var filter TabletFilter
	cells := strings.Split(cellsToWatch, ",")
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
func (hc *HealthCheckImpl) AddTablet(tablet *topodata.Tablet) {
	// check whether grpc port is present on tablet, if not return
	if tablet.PortMap["grpc"] == 0 {
		return
	}

	log.Infof("Adding tablet to healthcheck: %v", tablet)
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
	thc := &tabletHealthCheck{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		Tablet:     tablet,
		Target:     target,
	}

	// add to our datastore
	key := hc.keyFromTarget(target)
	tabletAlias := topoproto.TabletAliasString(tablet.Alias)
	if _, ok := hc.healthByAlias[tabletAliasString(tabletAlias)]; ok {
		// We should not add a tablet that we already have
		log.Errorf("Program bug: tried to add existing tablet: %v to healthcheck", tabletAlias)
		return
	}
	hc.healthByAlias[tabletAliasString(tabletAlias)] = thc
	res := thc.SimpleCopy()
	if ths, ok := hc.healthData[key]; !ok {
		hc.healthData[key] = make(map[tabletAliasString]*TabletHealth)
		hc.healthData[key][tabletAliasString(tabletAlias)] = res
	} else {
		// just overwrite it if it exists already
		ths[tabletAliasString(tabletAlias)] = res
	}

	hc.broadcast(res)
	hc.connsWG.Add(1)
	go thc.checkConn(hc)
}

// RemoveTablet removes the tablet, and stops the health check.
// It does not block.
func (hc *HealthCheckImpl) RemoveTablet(tablet *topodata.Tablet) {
	hc.deleteTablet(tablet)
}

// ReplaceTablet removes the old tablet and adds the new tablet.
func (hc *HealthCheckImpl) ReplaceTablet(old, new *topodata.Tablet) {
	hc.RemoveTablet(old)
	hc.AddTablet(new)
}

func (hc *HealthCheckImpl) deleteTablet(tablet *topodata.Tablet) {
	log.Infof("Removing tablet from healthcheck: %v", tablet)
	hc.mu.Lock()
	defer hc.mu.Unlock()

	key := hc.keyFromTablet(tablet)
	tabletAlias := tabletAliasString(topoproto.TabletAliasString(tablet.Alias))
	// delete from authoritative map
	th, ok := hc.healthByAlias[tabletAlias]
	if !ok {
		log.Infof("We have no health data for tablet: %v, it might have been deleted already", tabletAlias)
		return
	}
	// Calling this will end the context associated with th.checkConn,
	// which will call finalizeConn, which will close the connection.
	th.cancelFunc()
	delete(hc.healthByAlias, tabletAlias)
	// delete from map by keyspace.shard.tabletType
	ths, ok := hc.healthData[key]
	if !ok {
		log.Warningf("We have no health data for target: %v", key)
		return
	}
	delete(ths, tabletAlias)
	// delete from healthy list
	healthy, ok := hc.healthy[key]
	if ok && len(healthy) > 0 {
		hc.recomputeHealthy(key)
	}
}

func (hc *HealthCheckImpl) updateHealth(th *TabletHealth, prevTarget *query.Target, trivialUpdate bool, isPrimaryUp bool) {
	// hc.healthByAlias is authoritative, it should be updated
	hc.mu.Lock()
	defer hc.mu.Unlock()

	tabletAlias := tabletAliasString(topoproto.TabletAliasString(th.Tablet.Alias))
	targetKey := hc.keyFromTarget(th.Target)
	targetChanged := prevTarget.TabletType != th.Target.TabletType || prevTarget.Keyspace != th.Target.Keyspace || prevTarget.Shard != th.Target.Shard
	if targetChanged {
		// Error counter has to be set here in case we get a new tablet type for the first time in a stream response
		hcErrorCounters.Add([]string{th.Target.Keyspace, th.Target.Shard, topoproto.TabletTypeLString(th.Target.TabletType)}, 0)
		// keyspace and shard are not expected to change, but just in case ...
		// move this tabletHealthCheck to the correct map
		oldTargetKey := hc.keyFromTarget(prevTarget)
		delete(hc.healthData[oldTargetKey], tabletAlias)
		_, ok := hc.healthData[targetKey]
		if !ok {
			hc.healthData[targetKey] = make(map[tabletAliasString]*TabletHealth)
		}
	}
	// add it to the map by target
	hc.healthData[targetKey][tabletAlias] = th

	isPrimary := th.Target.TabletType == topodata.TabletType_MASTER
	switch {
	case isPrimary && isPrimaryUp:
		if len(hc.healthy[targetKey]) == 0 {
			hc.healthy[targetKey] = append(hc.healthy[targetKey], th)
		} else {
			// We already have one up server, see if we
			// need to replace it.
			if th.MasterTermStartTime < hc.healthy[targetKey][0].MasterTermStartTime {
				log.Warningf("not marking healthy master %s as Up for %s because its MasterTermStartTime is smaller than the highest known timestamp from previous MASTERs %s: %d < %d ",
					topoproto.TabletAliasString(th.Tablet.Alias),
					topoproto.KeyspaceShardString(th.Target.Keyspace, th.Target.Shard),
					topoproto.TabletAliasString(hc.healthy[targetKey][0].Tablet.Alias),
					th.MasterTermStartTime,
					hc.healthy[targetKey][0].MasterTermStartTime)
			} else {
				// Just replace it.
				hc.healthy[targetKey][0] = th
			}
		}
	case isPrimary && !isPrimaryUp:
		// No healthy master tablet
		hc.healthy[targetKey] = []*TabletHealth{}
	}

	if !trivialUpdate {
		// We re-sort the healthy tablet list whenever we get a health update for tablets we can route to.
		// Tablets from other cells for non-master targets should not trigger a re-sort;
		// they should also be excluded from healthy list.
		if th.Target.TabletType != topodata.TabletType_MASTER && hc.isIncluded(th.Target.TabletType, th.Tablet.Alias) {
			hc.recomputeHealthy(targetKey)
		}
		if targetChanged && prevTarget.TabletType != topodata.TabletType_MASTER && hc.isIncluded(th.Target.TabletType, th.Tablet.Alias) { // also recompute old target's healthy list
			oldTargetKey := hc.keyFromTarget(prevTarget)
			hc.recomputeHealthy(oldTargetKey)
		}
	}

	isNewPrimary := isPrimary && prevTarget.TabletType != topodata.TabletType_MASTER
	if isNewPrimary {
		log.Errorf("Adding 1 to MasterPromoted counter for target: %v, tablet: %v, tabletType: %v", prevTarget, topoproto.TabletAliasString(th.Tablet.Alias), th.Target.TabletType)
		hcMasterPromotedCounters.Add([]string{th.Target.Keyspace, th.Target.Shard}, 1)
	}

	// broadcast to subscribers
	hc.broadcast(th)
}

func (hc *HealthCheckImpl) recomputeHealthy(key keyspaceShardTabletType) {
	all := hc.healthData[key]
	allArray := make([]*TabletHealth, 0, len(all))
	for _, s := range all {
		// Only tablets in same cell / cellAlias are included in healthy list.
		if hc.isIncluded(s.Tablet.Type, s.Tablet.Alias) {
			allArray = append(allArray, s)
		}
	}
	hc.healthy[key] = FilterStatsByReplicationLag(allArray)
}

// Subscribe adds a listener. Used by vtgate buffer to learn about master changes.
func (hc *HealthCheckImpl) Subscribe() chan *TabletHealth {
	hc.subMu.Lock()
	defer hc.subMu.Unlock()
	c := make(chan *TabletHealth, 2)
	hc.subscribers[c] = struct{}{}
	return c
}

// Unsubscribe removes a listener.
func (hc *HealthCheckImpl) Unsubscribe(c chan *TabletHealth) {
	hc.subMu.Lock()
	defer hc.subMu.Unlock()
	delete(hc.subscribers, c)
}

func (hc *HealthCheckImpl) broadcast(th *TabletHealth) {
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
	for _, ths := range hc.healthData {
		for _, th := range ths {
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
			tcs.TabletsStats = append(tcs.TabletsStats, th)
		}
	}
	return tcsMap
}

// Close stops the healthcheck.
func (hc *HealthCheckImpl) Close() error {
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
func (hc *HealthCheckImpl) GetHealthyTabletStats(target *query.Target) []*TabletHealth {
	var result []*TabletHealth
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if target.Shard == "" {
		target.Shard = "0"
	}
	return append(result, hc.healthy[hc.keyFromTarget(target)]...)
}

// getTabletStats returns all tablets for the given target.
// The returned array is owned by the caller.
// For TabletType_MASTER, this will only return at most one entry,
// the most recent tablet of type master.
func (hc *HealthCheckImpl) getTabletStats(target *query.Target) []*TabletHealth {
	var result []*TabletHealth
	hc.mu.Lock()
	defer hc.mu.Unlock()
	ths := hc.healthData[hc.keyFromTarget(target)]
	for _, th := range ths {
		result = append(result, th)
	}
	return result
}

// WaitForTablets waits for at least one tablet in the given
// keyspace / shard / tablet type before returning. The tablets do not
// have to be healthy.  It will return ctx.Err() if the context is canceled.
func (hc *HealthCheckImpl) WaitForTablets(ctx context.Context, keyspace, shard string, tabletType topodata.TabletType) error {
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
func (hc *HealthCheckImpl) WaitForAllServingTablets(ctx context.Context, targets []*query.Target) error {
	return hc.waitForTablets(ctx, targets, true)
}

// FilterTargetsByKeyspaces only returns the targets that are part of the provided keyspaces
func FilterTargetsByKeyspaces(keyspaces []string, targets []*query.Target) []*query.Target {
	filteredTargets := make([]*query.Target, 0)

	// Keep them all if there are no keyspaces to watch
	if len(KeyspacesToWatch) == 0 {
		return append(filteredTargets, targets...)
	}

	// Let's remove from the target shards that are not in the keyspaceToWatch list.
	for _, target := range targets {
		for _, keyspaceToWatch := range keyspaces {
			if target.Keyspace == keyspaceToWatch {
				filteredTargets = append(filteredTargets, target)
			}
		}
	}
	return filteredTargets
}

// waitForTablets is the internal method that polls for tablets.
func (hc *HealthCheckImpl) waitForTablets(ctx context.Context, targets []*query.Target, requireServing bool) error {
	targets = FilterTargetsByKeyspaces(KeyspacesToWatch, targets)

	for {
		// We nil targets as we find them.
		allPresent := true
		for i, target := range targets {
			if target == nil {
				continue
			}

			var tabletHealths []*TabletHealth
			if requireServing {
				tabletHealths = hc.GetHealthyTabletStats(target)
			} else {
				tabletHealths = hc.getTabletStats(target)
			}
			if len(tabletHealths) == 0 {
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
			for _, target := range targets {
				if target != nil {
					log.Infof("couldn't find tablets for target: %v", target)
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// TabletConnection returns the Connection to a given tablet.
func (hc *HealthCheckImpl) TabletConnection(alias *topodata.TabletAlias) (queryservice.QueryService, error) {
	hc.mu.Lock()
	thc := hc.healthByAlias[tabletAliasString(topoproto.TabletAliasString(alias))]
	hc.mu.Unlock()
	if thc == nil || thc.Conn == nil {
		//TODO: test that throws this error
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "tablet: %v is either down or nonexistent", alias)
	}
	return thc.Connection(), nil
}

// Target includes cell which we ignore here
// because tabletStatsCache is intended to be per-cell
func (hc *HealthCheckImpl) keyFromTarget(target *query.Target) keyspaceShardTabletType {
	return keyspaceShardTabletType(fmt.Sprintf("%s.%s.%s", target.Keyspace, target.Shard, topoproto.TabletTypeLString(target.TabletType)))
}

func (hc *HealthCheckImpl) keyFromTablet(tablet *topodata.Tablet) keyspaceShardTabletType {
	return keyspaceShardTabletType(fmt.Sprintf("%s.%s.%s", tablet.Keyspace, tablet.Shard, topoproto.TabletTypeLString(tablet.Type)))
}

// getAliasByCell should only be called while holding hc.mu
func (hc *HealthCheckImpl) getAliasByCell(cell string) string {
	if alias, ok := hc.cellAliases[cell]; ok {
		return alias
	}

	alias := topo.GetAliasByCell(context.Background(), hc.ts, cell)
	// Currently cell aliases have to be non-overlapping.
	// If that changes, this will need to change to account for overlaps.
	hc.cellAliases[cell] = alias

	return alias
}

func (hc *HealthCheckImpl) isIncluded(tabletType topodata.TabletType, tabletAlias *topodata.TabletAlias) bool {
	if tabletType == topodata.TabletType_MASTER {
		return true
	}
	if tabletAlias.Cell == hc.cell {
		return true
	}
	if hc.getAliasByCell(tabletAlias.Cell) == hc.getAliasByCell(hc.cell) {
		return true
	}
	return false
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
	status := hc.CacheStatus()
	b, err := json.MarshalIndent(status, "", " ")
	if err != nil {
		//Error logged
		if _, err := w.Write([]byte(err.Error())); err != nil {
			log.Errorf("write to buffer error failed: %v", err)
		}

		return
	}

	buf := bytes.NewBuffer(nil)
	json.HTMLEscape(buf, b)

	//Error logged
	if _, err := w.Write(buf.Bytes()); err != nil {
		log.Errorf("write to buffer bytes failed: %v", err)
	}
}

// servingConnStats returns the number of serving tablets per keyspace/shard/tablet type.
func (hc *HealthCheckImpl) servingConnStats() map[string]int64 {
	res := make(map[string]int64)
	hc.mu.Lock()
	defer hc.mu.Unlock()
	for key, ths := range hc.healthData {
		for _, th := range ths {
			if th.Serving && th.LastError == nil {
				res[string(key)]++
			}
		}
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
			fmt.Fprintf(&buf, "%v%v\n", ts.Serving, ts.MasterTermStartTime)
		}
	}

	return int64(crc32.ChecksumIEEE(buf.Bytes()))
}
