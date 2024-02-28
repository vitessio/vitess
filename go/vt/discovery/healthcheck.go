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
// For a Watcher example have a look at NewTopologyWatcher().
//
// Internally, the HealthCheck module is connected to each tablet and has a
// streaming RPC (StreamHealth) open to receive periodic health infos.
package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/safehtml/template"
	"github.com/google/safehtml/template/uncheckedconversions"
	"github.com/spf13/pflag"
	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

var (
	hcErrorCounters = stats.NewCountersWithMultiLabels("HealthcheckErrors", "Healthcheck Errors", []string{"Keyspace", "ShardName", "TabletType"})

	hcPrimaryPromotedCounters = stats.NewCountersWithMultiLabels("HealthcheckPrimaryPromoted", "Primary promoted in keyspace/shard name because of health check errors", []string{"Keyspace", "ShardName"})
	healthcheckOnce           sync.Once

	// TabletURLTemplateString is a flag to generate URLs for the tablets that vtgate discovers.
	TabletURLTemplateString = "http://{{.GetTabletHostPort}}"
	tabletURLTemplate       *template.Template

	// AllowedTabletTypes is the list of allowed tablet types. e.g. {PRIMARY, REPLICA}.
	AllowedTabletTypes []topodata.TabletType

	// KeyspacesToWatch - if provided this specifies which keyspaces should be
	// visible to the healthcheck. By default the healthcheck will watch all keyspaces.
	KeyspacesToWatch []string

	// tabletFilters are the keyspace|shard or keyrange filters to apply to the full set of tablets.
	tabletFilters []string

	// refreshInterval is the interval at which healthcheck refreshes its list of tablets from topo.
	refreshInterval = 1 * time.Minute

	// refreshKnownTablets tells us whether to process all tablets or only new tablets.
	refreshKnownTablets = true

	// healthCheckDialConcurrency tells us how many healthcheck connections can be opened to tablets at once. This should be less than the golang max thread limit of 10000.
	healthCheckDialConcurrency int64 = 1024

	// How much to sleep between each check.
	waitAvailableTabletInterval = 100 * time.Millisecond

	// HealthCheckCacheTemplate uses healthCheckTemplate with the `HealthCheck Tablet - Cache` title to create the
	// HTML code required to render the cache of the HealthCheck.
	HealthCheckCacheTemplate = fmt.Sprintf(healthCheckTemplate, "HealthCheck - Cache")

	// HealthCheckHealthyTemplate uses healthCheckTemplate with the `HealthCheck Tablet - Healthy Tablets` title to
	// create the HTML code required to render the list of healthy tablets from the HealthCheck.
	HealthCheckHealthyTemplate = fmt.Sprintf(healthCheckTemplate, "HealthCheck - Healthy Tablets")
)

// See the documentation for NewHealthCheck below for an explanation of these parameters.
const (
	DefaultHealthCheckRetryDelay = 5 * time.Second
	DefaultHealthCheckTimeout    = 1 * time.Minute

	// healthCheckTemplate is the HTML code to display a TabletsCacheStatusList, it takes a parameter for the title
	// as the template can be used for both HealthCheck's cache and healthy tablets list.
	healthCheckTemplate = `
<style>
  table {
    border-collapse: collapse;
  }
  td, th {
    border: 1px solid #999;
    padding: 0.2rem;
  }
</style>
<table class="refreshRequired">
  <tr>
    <th colspan="5">%s</th>
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
    <td>{{$ts.Cell}}</td>
    <td>{{$ts.Target.Keyspace}}</td>
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
	_, err := tabletURLTemplate.ParseFromTrustedTemplate(uncheckedconversions.TrustedTemplateFromStringKnownToSatisfyTypeContract(TabletURLTemplateString))
	if err != nil {
		log.Exitf("error parsing template: %v", err)
	}
}

func init() {
	for _, cmd := range []string{"vtgate", "vtcombo"} {
		servenv.OnParseFor(cmd, registerDiscoveryFlags)
		servenv.OnParseFor(cmd, registerWebUIFlags)
	}

	servenv.OnParseFor("vtctld", registerWebUIFlags)
}

func registerDiscoveryFlags(fs *pflag.FlagSet) {
	fs.StringSliceVar(&tabletFilters, "tablet_filters", []string{}, "Specifies a comma-separated list of 'keyspace|shard_name or keyrange' values to filter the tablets to watch.")
	fs.Var((*topoproto.TabletTypeListFlag)(&AllowedTabletTypes), "allowed_tablet_types", "Specifies the tablet types this vtgate is allowed to route queries to. Should be provided as a comma-separated set of tablet types.")
	fs.StringSliceVar(&KeyspacesToWatch, "keyspaces_to_watch", []string{}, "Specifies which keyspaces this vtgate should have access to while routing queries or accessing the vschema.")
}

func registerWebUIFlags(fs *pflag.FlagSet) {
	fs.StringVar(&TabletURLTemplateString, "tablet_url_template", "http://{{.GetTabletHostPort}}", "Format string describing debug tablet url formatting. See getTabletDebugURL() for how to customize this.")
	fs.DurationVar(&refreshInterval, "tablet_refresh_interval", 1*time.Minute, "Tablet refresh interval.")
	fs.BoolVar(&refreshKnownTablets, "tablet_refresh_known_tablets", true, "Whether to reload the tablet's address/port map from topo in case they change.")
	fs.Int64Var(&healthCheckDialConcurrency, "healthcheck-dial-concurrency", 1024, "Maximum concurrency of new healthcheck connections. This should be less than the golang max thread limit of 10000.")
	ParseTabletURLTemplateFromFlag()
}

// FilteringKeyspaces returns true if any keyspaces have been configured to be filtered.
func FilteringKeyspaces() bool {
	return len(KeyspacesToWatch) > 0
}

type KeyspaceShardTabletType string
type tabletAliasString string

// HealthCheck declares what the TabletGateway needs from the HealthCheck
type HealthCheck interface {
	// AddTablet adds the tablet.
	AddTablet(tablet *topodata.Tablet)

	// RemoveTablet removes the tablet.
	RemoveTablet(tablet *topodata.Tablet)

	// ReplaceTablet does an AddTablet and RemoveTablet in one call, effectively replacing the old tablet with the new.
	ReplaceTablet(old, new *topodata.Tablet)

	// CacheStatus returns a displayable version of the health check cache.
	CacheStatus() TabletsCacheStatusList

	// HealthyStatus returns a displayable version of the health check healthy list.
	HealthyStatus() TabletsCacheStatusList

	// CacheStatusMap returns a map of the health check cache.
	CacheStatusMap() map[string]*TabletsCacheStatus

	// Close stops the healthcheck.
	Close() error

	// WaitForAllServingTablets waits for at least one healthy serving tablet in
	// each given target before returning.
	// It will return ctx.Err() if the context is canceled.
	// It will return an error if it can't read the necessary topology records.
	WaitForAllServingTablets(ctx context.Context, targets []*query.Target) error

	// TabletConnection returns the TabletConn of the given tablet.
	TabletConnection(alias *topodata.TabletAlias, target *query.Target) (queryservice.QueryService, error)

	// RegisterStats registers the connection counts stats
	RegisterStats()

	// GetHealthyTabletStats returns only the healthy tablets.
	// The returned array is owned by the caller.
	// For TabletType_PRIMARY, this will only return at most one entry,
	// the most recent tablet of type primary.
	// This returns a copy of the data so that callers can access without
	// synchronization
	GetHealthyTabletStats(target *query.Target) []*TabletHealth

	// GetTabletHealth results the TabletHealth of the tablet that matches the given alias
	GetTabletHealth(kst KeyspaceShardTabletType, alias *topodata.TabletAlias) (*TabletHealth, error)

	// GetTabletHealthByAlias results the TabletHealth of the tablet that matches the given alias
	GetTabletHealthByAlias(alias *topodata.TabletAlias) (*TabletHealth, error)

	// Subscribe adds a listener. Used by vtgate buffer to learn about primary changes.
	Subscribe() chan *TabletHealth

	// Unsubscribe removes a listener.
	Unsubscribe(c chan *TabletHealth)

	// GetLoadTabletsTrigger returns a channel that is used to inform when to load tablets.
	GetLoadTabletsTrigger() chan struct{}
}

var _ HealthCheck = (*HealthCheckImpl)(nil)

// KeyFromTarget includes cell which we ignore here
// because tabletStatsCache is intended to be per-cell
func KeyFromTarget(target *query.Target) KeyspaceShardTabletType {
	return KeyspaceShardTabletType(fmt.Sprintf("%s.%s.%s", target.Keyspace, target.Shard, topoproto.TabletTypeLString(target.TabletType)))
}

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
	healthData map[KeyspaceShardTabletType]map[tabletAliasString]*TabletHealth
	// another map keyed by keyspace.shard.tabletType, this one containing a sorted list of TabletHealth
	healthy map[KeyspaceShardTabletType][]*TabletHealth
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
	// loadTablets trigger is used to immediately load a new primary tablet when the current one has been demoted
	loadTabletsTrigger chan struct{}
	// healthCheckDialSem is used to limit how many healthcheck connections can be opened to tablets at once.
	healthCheckDialSem *semaphore.Weighted
}

// NewHealthCheck creates a new HealthCheck object.
// Parameters:
// retryDelay.
//
//	The duration to wait before retrying to connect (e.g. after a failed connection
//	attempt).
//
// healthCheckTimeout.
//
//	The duration for which we consider a health check response to be 'fresh'. If we don't get
//	a health check response from a tablet for more than this duration, we consider the tablet
//	not healthy.
//
// topoServer.
//
//	The topology server that this healthcheck object can use to retrieve cell or tablet information
//
// localCell.
//
//	The localCell for this healthcheck
//
// callback.
//
//	A function to call when there is a primary change. Used to notify vtgate's buffer to stop buffering.
func NewHealthCheck(ctx context.Context, retryDelay, healthCheckTimeout time.Duration, topoServer *topo.Server, localCell, cellsToWatch string) *HealthCheckImpl {
	log.Infof("loading tablets for cells: %v", cellsToWatch)

	hc := &HealthCheckImpl{
		ts:                 topoServer,
		cell:               localCell,
		retryDelay:         retryDelay,
		healthCheckTimeout: healthCheckTimeout,
		healthCheckDialSem: semaphore.NewWeighted(healthCheckDialConcurrency),
		healthByAlias:      make(map[tabletAliasString]*tabletHealthCheck),
		healthData:         make(map[KeyspaceShardTabletType]map[tabletAliasString]*TabletHealth),
		healthy:            make(map[KeyspaceShardTabletType][]*TabletHealth),
		subscribers:        make(map[chan *TabletHealth]struct{}),
		cellAliases:        make(map[string]string),
		loadTabletsTrigger: make(chan struct{}),
	}
	var topoWatchers []*TopologyWatcher
	var filter TabletFilter
	cells := strings.Split(cellsToWatch, ",")
	if cellsToWatch == "" {
		cells = append(cells, localCell)
	}

	for _, c := range cells {
		log.Infof("Setting up healthcheck for cell: %v", c)
		if c == "" {
			continue
		}
		if len(tabletFilters) > 0 {
			if len(KeyspacesToWatch) > 0 {
				log.Exitf("Only one of -keyspaces_to_watch and -tablet_filters may be specified at a time")
			}

			fbs, err := NewFilterByShard(tabletFilters)
			if err != nil {
				log.Exitf("Cannot parse tablet_filters parameter: %v", err)
			}
			filter = fbs
		} else if len(KeyspacesToWatch) > 0 {
			filter = NewFilterByKeyspace(KeyspacesToWatch)
		}
		topoWatchers = append(topoWatchers, NewTopologyWatcher(ctx, topoServer, hc, filter, c, refreshInterval, refreshKnownTablets, topo.DefaultConcurrency))
	}

	hc.topoWatchers = topoWatchers
	healthcheckOnce.Do(func() {
		servenv.HTTPHandle("/debug/gateway", hc)
	})

	// start the topo watches here
	for _, tw := range hc.topoWatchers {
		tw.Start()
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
	key := KeyFromTarget(target)
	tabletAlias := topoproto.TabletAliasString(tablet.Alias)
	if _, ok := hc.healthByAlias[tabletAliasString(tabletAlias)]; ok {
		// We should not add a tablet that we already have
		log.Errorf("Program bug: tried to add existing tablet: %v to healthcheck", tabletAlias)
		return
	}
	hc.healthByAlias[tabletAliasString(tabletAlias)] = thc
	res := thc.SimpleCopy()
	if _, ok := hc.healthData[key]; !ok {
		hc.healthData[key] = make(map[tabletAliasString]*TabletHealth)
	}
	hc.healthData[key][tabletAliasString(tabletAlias)] = res

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

	tabletAlias := tabletAliasString(topoproto.TabletAliasString(tablet.Alias))
	defer func() {
		// We want to be sure the tablet is gone from the secondary
		// maps even if it's already gone from the authoritative map.
		// The tablet's type also may have recently changed as well,
		// so ensure that the tablet we're removing is removed from
		// any possible secondary map keys:
		// key: keyspace.shard.tabletType -> val: map[tabletAlias]tabletHealth
		for _, tabletType := range topoproto.AllTabletTypes {
			key := KeyspaceShardTabletType(fmt.Sprintf("%s.%s.%s", tablet.Keyspace, tablet.Shard, topoproto.TabletTypeLString(tabletType)))
			// delete from map by keyspace.shard.tabletType
			ths, ok := hc.healthData[key]
			if !ok {
				continue
			}
			delete(ths, tabletAlias)
			// delete from healthy list
			healthy, ok := hc.healthy[key]
			if ok && len(healthy) > 0 {
				hc.recomputeHealthy(key)
			}
		}
	}()
	// delete from authoritative map
	th, ok := hc.healthByAlias[tabletAlias]
	if !ok {
		log.Infof("We have no health data for tablet: %v, it might have been deleted already", tablet)
		return
	}
	// Calling this will end the context associated with th.checkConn,
	// which will call finalizeConn, which will close the connection.
	th.cancelFunc()
	delete(hc.healthByAlias, tabletAlias)
}

func (hc *HealthCheckImpl) updateHealth(th *TabletHealth, prevTarget *query.Target, trivialUpdate bool, up bool) {
	// hc.healthByAlias is authoritative, it should be updated
	hc.mu.Lock()
	defer hc.mu.Unlock()

	tabletAlias := tabletAliasString(topoproto.TabletAliasString(th.Tablet.Alias))
	// let's be sure that this tablet hasn't been deleted from the authoritative map
	// so that we're not racing to update it and in effect re-adding a copy of the
	// tablet record that was deleted
	if _, ok := hc.healthByAlias[tabletAlias]; !ok {
		log.Infof("Tablet %v has been deleted, skipping health update", th.Tablet)
		return
	}

	targetKey := KeyFromTarget(th.Target)
	targetChanged := prevTarget.TabletType != th.Target.TabletType || prevTarget.Keyspace != th.Target.Keyspace || prevTarget.Shard != th.Target.Shard
	if targetChanged {
		// Error counter has to be set here in case we get a new tablet type for the first time in a stream response
		hcErrorCounters.Add([]string{th.Target.Keyspace, th.Target.Shard, topoproto.TabletTypeLString(th.Target.TabletType)}, 0)
		// keyspace and shard are not expected to change, but just in case ...
		// move this tabletHealthCheck to the correct map
		oldTargetKey := KeyFromTarget(prevTarget)
		delete(hc.healthData[oldTargetKey], tabletAlias)
		_, ok := hc.healthData[targetKey]
		if !ok {
			hc.healthData[targetKey] = make(map[tabletAliasString]*TabletHealth)
		}

		// If the previous tablet type was primary, we need to check if the next new primary has already been assigned.
		// If no new primary has been assigned, we will trigger a `loadTablets` call to immediately redirect traffic to the new primary.
		//
		// This is to avoid a situation where a newly primary tablet for a shard has just been started and the tableRefreshInterval has not yet passed,
		// causing an interruption where no primary is assigned to the shard.
		if prevTarget.TabletType == topodata.TabletType_PRIMARY {
			if primaries := hc.healthData[oldTargetKey]; len(primaries) == 0 {
				log.Infof("We will have no health data for the next new primary tablet after demoting the tablet: %v, so start loading tablets now", topotools.TabletIdent(th.Tablet))
				hc.loadTabletsTrigger <- struct{}{}
			}
		}
	}
	// add it to the map by target and create the map record if needed
	if _, ok := hc.healthData[targetKey]; !ok {
		hc.healthData[targetKey] = make(map[tabletAliasString]*TabletHealth)
	}
	hc.healthData[targetKey][tabletAlias] = th

	isPrimary := th.Target.TabletType == topodata.TabletType_PRIMARY
	switch {
	case isPrimary && up:
		if len(hc.healthy[targetKey]) == 0 {
			hc.healthy[targetKey] = append(hc.healthy[targetKey], th)
		} else {
			// We already have one up server, see if we
			// need to replace it.
			if th.PrimaryTermStartTime < hc.healthy[targetKey][0].PrimaryTermStartTime {
				log.Warningf("not marking healthy primary %s as Up for %s because its PrimaryTermStartTime is smaller than the highest known timestamp from previous PRIMARYs %s: %d < %d ",
					topoproto.TabletAliasString(th.Tablet.Alias),
					topoproto.KeyspaceShardString(th.Target.Keyspace, th.Target.Shard),
					topoproto.TabletAliasString(hc.healthy[targetKey][0].Tablet.Alias),
					th.PrimaryTermStartTime,
					hc.healthy[targetKey][0].PrimaryTermStartTime)
			} else {
				// Just replace it.
				hc.healthy[targetKey][0] = th
			}
		}
	case isPrimary && !up:
		if healthy, ok := hc.healthy[targetKey]; ok && len(healthy) > 0 {
			// isPrimary is true here therefore we should only have 1 tablet in healthy
			alias := tabletAliasString(topoproto.TabletAliasString(healthy[0].Tablet.Alias))
			// Clear healthy list for primary if the existing tablet is down
			if alias == tabletAlias {
				hc.healthy[targetKey] = []*TabletHealth{}
			}
		}
	}

	if !trivialUpdate {
		// We re-sort the healthy tablet list whenever we get a health update for tablets we can route to.
		// Tablets from other cells for non-primary targets should not trigger a re-sort;
		// they should also be excluded from healthy list.
		if th.Target.TabletType != topodata.TabletType_PRIMARY && hc.isIncluded(th.Target.TabletType, th.Tablet.Alias) {
			hc.recomputeHealthy(targetKey)
		}
		if targetChanged && prevTarget.TabletType != topodata.TabletType_PRIMARY && hc.isIncluded(th.Target.TabletType, th.Tablet.Alias) { // also recompute old target's healthy list
			oldTargetKey := KeyFromTarget(prevTarget)
			hc.recomputeHealthy(oldTargetKey)
		}
	}

	isNewPrimary := isPrimary && prevTarget.TabletType != topodata.TabletType_PRIMARY
	if isNewPrimary {
		log.Errorf("Adding 1 to PrimaryPromoted counter for target: %v, tablet: %v, tabletType: %v", prevTarget, topoproto.TabletAliasString(th.Tablet.Alias), th.Target.TabletType)
		hcPrimaryPromotedCounters.Add([]string{th.Target.Keyspace, th.Target.Shard}, 1)
	}

	// broadcast to subscribers
	hc.broadcast(th)
}

func (hc *HealthCheckImpl) recomputeHealthy(key KeyspaceShardTabletType) {
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

// Subscribe adds a listener. Used by vtgate buffer to learn about primary changes.
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

// GetLoadTabletsTrigger returns a channel that is used to inform when to load tablets.
func (hc *HealthCheckImpl) GetLoadTabletsTrigger() chan struct{} {
	return hc.loadTabletsTrigger
}

// CacheStatus returns a displayable version of the cache.
func (hc *HealthCheckImpl) CacheStatus() TabletsCacheStatusList {
	tcsMap := hc.CacheStatusMap()
	tcsl := make(TabletsCacheStatusList, 0, len(tcsMap))
	for _, tcs := range tcsMap {
		tcsl = append(tcsl, tcs)
	}
	sort.Sort(tcsl)
	return tcsl
}

// HealthyStatus returns a displayable version of the cache.
func (hc *HealthCheckImpl) HealthyStatus() TabletsCacheStatusList {
	tcsMap := hc.HealthyStatusMap()
	tcsl := make(TabletsCacheStatusList, 0, len(tcsMap))
	for _, tcs := range tcsMap {
		tcsl = append(tcsl, tcs)
	}
	sort.Sort(tcsl)
	return tcsl
}

func (hc *HealthCheckImpl) CacheStatusMap() map[string]*TabletsCacheStatus {
	tcsMap := make(map[string]*TabletsCacheStatus)
	hc.mu.Lock()
	defer hc.mu.Unlock()
	for _, ths := range hc.healthData {
		for _, th := range ths {
			tabletHealthToTabletCacheStatus(th, tcsMap)
		}
	}
	return tcsMap
}

func (hc *HealthCheckImpl) HealthyStatusMap() map[string]*TabletsCacheStatus {
	tcsMap := make(map[string]*TabletsCacheStatus)
	hc.mu.Lock()
	defer hc.mu.Unlock()
	for _, ths := range hc.healthy {
		for _, th := range ths {
			tabletHealthToTabletCacheStatus(th, tcsMap)
		}
	}
	return tcsMap
}

func tabletHealthToTabletCacheStatus(th *TabletHealth, tcsMap map[string]*TabletsCacheStatus) {
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
// For TabletType_PRIMARY, this will only return at most one entry,
// the most recent tablet of type primary.
// This returns a copy of the data so that callers can access without
// synchronization
func (hc *HealthCheckImpl) GetHealthyTabletStats(target *query.Target) []*TabletHealth {
	var result []*TabletHealth
	hc.mu.Lock()
	defer hc.mu.Unlock()
	return append(result, hc.healthy[KeyFromTarget(target)]...)
}

// GetTabletStats returns all tablets for the given target.
// The returned array is owned by the caller.
// For TabletType_PRIMARY, this will only return at most one entry,
// the most recent tablet of type primary.
func (hc *HealthCheckImpl) GetTabletStats(target *query.Target) []*TabletHealth {
	var result []*TabletHealth
	hc.mu.Lock()
	defer hc.mu.Unlock()
	ths := hc.healthData[KeyFromTarget(target)]
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

// waitForTablets is the internal method that polls for tablets.
func (hc *HealthCheckImpl) waitForTablets(ctx context.Context, targets []*query.Target, requireServing bool) error {
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
				tabletHealths = hc.GetTabletStats(target)
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

// GetTabletHealthByAlias results the TabletHealth of the tablet that matches the given alias
func (hc *HealthCheckImpl) GetTabletHealthByAlias(alias *topodata.TabletAlias) (*TabletHealth, error) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if hd, ok := hc.healthByAlias[tabletAliasString(topoproto.TabletAliasString(alias))]; ok {
		return hd.SimpleCopy(), nil
	}
	return nil, fmt.Errorf("could not find tablet: %s", alias.String())
}

// GetTabletHealth results the TabletHealth of the tablet that matches the given alias and KeyspaceShardTabletType
// The data is retrieved from the healthData map.
func (hc *HealthCheckImpl) GetTabletHealth(kst KeyspaceShardTabletType, alias *topodata.TabletAlias) (*TabletHealth, error) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if hd, ok := hc.healthData[kst]; ok {
		if th, ok := hd[tabletAliasString(topoproto.TabletAliasString(alias))]; ok {
			return th, nil
		}
	}
	return nil, fmt.Errorf("could not find tablet: %s", alias.String())
}

// TabletConnection returns the Connection to a given tablet.
func (hc *HealthCheckImpl) TabletConnection(alias *topodata.TabletAlias, target *query.Target) (queryservice.QueryService, error) {
	hc.mu.Lock()
	thc := hc.healthByAlias[tabletAliasString(topoproto.TabletAliasString(alias))]
	hc.mu.Unlock()
	if thc == nil || thc.Conn == nil {
		// TODO: test that throws this error
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "tablet: %v is either down or nonexistent", alias)
	}
	return thc.Connection(hc), nil
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
	if tabletType == topodata.TabletType_PRIMARY {
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
		// Error logged
		if _, err := w.Write([]byte(err.Error())); err != nil {
			log.Errorf("write to buffer error failed: %v", err)
		}

		return
	}

	buf := bytes.NewBuffer(nil)
	json.HTMLEscape(buf, b)

	// Error logged
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
			fmt.Fprintf(&buf, "%v%v\n", ts.Serving, ts.PrimaryTermStartTime)
		}
	}

	return int64(crc32.ChecksumIEEE(buf.Bytes()))
}

// TabletToMapKey creates a key to the map from tablet's host and ports.
// It should only be used in discovery and related module.
func TabletToMapKey(tablet *topodata.Tablet) string {
	parts := make([]string, 0, 1)
	for name, port := range tablet.PortMap {
		parts = append(parts, netutil.JoinHostPort(name, port))
	}
	sort.Strings(parts)
	parts = append([]string{tablet.Hostname}, parts...)
	return strings.Join(parts, ",")
}
