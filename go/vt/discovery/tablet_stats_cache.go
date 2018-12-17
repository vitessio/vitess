/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package discovery

import (
	"math"
	"sync"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
)

// TabletStatsCache is a HealthCheckStatsListener that keeps both the
// current list of available TabletStats, and a serving list:
// - for master tablets, only the current master is kept.
// - for non-master tablets, we filter the list using FilterByReplicationLag.
// It keeps entries for all tablets in the cell(s) it's configured to serve for,
// and for the master independently of which cell it's in.
// Note the healthy tablet computation is done when we receive a tablet
// update only, not at serving time.
// Also note the cache may not have the last entry received by the tablet.
// For instance, if a tablet was healthy, and is still healthy, we do not
// keep its new update.
type TabletStatsCache struct {
	// cell is the cell we are keeping all tablets for.
	// Note we keep track of all master tablets in all cells.
	cell string
	// ts is the topo server in use.
	ts *topo.Server
	// aggregatesChan is used to send notifications to listeners.
	aggregatesChan chan []*srvtopo.TargetStatsEntry
	// mu protects the following fields. It does not protect individual
	// entries in the entries map.
	mu sync.RWMutex
	// entries maps from keyspace/shard/tabletType to our cache.
	entries map[string]map[string]map[topodatapb.TabletType]*tabletStatsCacheEntry
	// tsm is a helper to broadcast aggregate stats.
	tsm srvtopo.TargetStatsMultiplexer
}

// tabletStatsCacheEntry is the per keyspace/shard/tabletType
// entry of the in-memory map for TabletStatsCache.
type tabletStatsCacheEntry struct {
	// mu protects the rest of this structure.
	mu sync.RWMutex
	// all has the valid tablets, indexed by TabletToMapKey(ts.Tablet),
	// as it is the index used by HealthCheck.
	all map[string]*TabletStats
	// healthy only has the healthy ones.
	healthy []*TabletStats
	// aggregates has the per-region aggregates.
	aggregates map[string]*querypb.AggregateStats
}

func (e *tabletStatsCacheEntry) updateHealthyMapForMaster(ts *TabletStats) {
	if ts.Up {
		// We have an Up master.
		if len(e.healthy) == 0 {
			// We have a new Up server, just remember it.
			e.healthy = append(e.healthy, ts)
			return
		}

		// We already have one up server, see if we
		// need to replace it.
		if ts.TabletExternallyReparentedTimestamp < e.healthy[0].TabletExternallyReparentedTimestamp {
			log.Warningf("not marking healthy master %s as Up for %s because its externally reparented timestamp is smaller than the highest known timestamp from previous MASTERs %s: %d < %d ",
				topoproto.TabletAliasString(ts.Tablet.Alias),
				topoproto.KeyspaceShardString(ts.Target.Keyspace, ts.Target.Shard),
				topoproto.TabletAliasString(e.healthy[0].Tablet.Alias),
				ts.TabletExternallyReparentedTimestamp,
				e.healthy[0].TabletExternallyReparentedTimestamp)
			return
		}

		// Just replace it.
		e.healthy[0] = ts
		return
	}

	// We have a Down master, remove it only if it's exactly the same.
	if len(e.healthy) != 0 {
		if ts.Key == e.healthy[0].Key {
			// Same guy, remove it.
			e.healthy = nil
		}
	}
}

// NewTabletStatsCache creates a TabletStatsCache, and registers
// it as HealthCheckStatsListener of the provided healthcheck.
// Note we do the registration in this code to guarantee we call
// SetListener with sendDownEvents=true, as we need these events
// to maintain the integrity of our cache.
func NewTabletStatsCache(hc HealthCheck, ts *topo.Server, cell string) *TabletStatsCache {
	return newTabletStatsCache(hc, ts, cell, true /* setListener */)
}

// NewTabletStatsCacheDoNotSetListener is identical to NewTabletStatsCache
// but does not automatically set the returned object as listener for "hc".
// Instead, it's up to the caller to ensure that TabletStatsCache.StatsUpdate()
// gets called properly. This is useful for chaining multiple listeners.
// When the caller sets its own listener on "hc", they must make sure that they
// set the parameter  "sendDownEvents" to "true" or this cache won't properly
// remove tablets whose tablet type changes.
func NewTabletStatsCacheDoNotSetListener(ts *topo.Server, cell string) *TabletStatsCache {
	return newTabletStatsCache(nil, ts, cell, false /* setListener */)
}

func newTabletStatsCache(hc HealthCheck, ts *topo.Server, cell string, setListener bool) *TabletStatsCache {
	tc := &TabletStatsCache{
		cell:           cell,
		ts:             ts,
		aggregatesChan: make(chan []*srvtopo.TargetStatsEntry, 100),
		entries:        make(map[string]map[string]map[topodatapb.TabletType]*tabletStatsCacheEntry),
		tsm:            srvtopo.NewTargetStatsMultiplexer(),
	}

	if setListener {
		// We need to set sendDownEvents=true to get the deletes from the map
		// upon type change.
		hc.SetListener(tc, true /*sendDownEvents*/)
	}
	return tc
}

// getEntry returns an existing tabletStatsCacheEntry in the cache, or nil
// if the entry does not exist. It only takes a Read lock on mu.
func (tc *TabletStatsCache) getEntry(keyspace, shard string, tabletType topodatapb.TabletType) *tabletStatsCacheEntry {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	if s, ok := tc.entries[keyspace]; ok {
		if t, ok := s[shard]; ok {
			if e, ok := t[tabletType]; ok {
				return e
			}
		}
	}
	return nil
}

// getOrCreateEntry returns an existing tabletStatsCacheEntry from the cache,
// or creates it if it doesn't exist.
func (tc *TabletStatsCache) getOrCreateEntry(target *querypb.Target) *tabletStatsCacheEntry {
	// Fast path (most common path too): Read-lock, return the entry.
	if e := tc.getEntry(target.Keyspace, target.Shard, target.TabletType); e != nil {
		return e
	}

	// Slow path: Lock, will probably have to add the entry at some level.
	tc.mu.Lock()
	defer tc.mu.Unlock()

	s, ok := tc.entries[target.Keyspace]
	if !ok {
		s = make(map[string]map[topodatapb.TabletType]*tabletStatsCacheEntry)
		tc.entries[target.Keyspace] = s
	}
	t, ok := s[target.Shard]
	if !ok {
		t = make(map[topodatapb.TabletType]*tabletStatsCacheEntry)
		s[target.Shard] = t
	}
	e, ok := t[target.TabletType]
	if !ok {
		e = &tabletStatsCacheEntry{
			all:        make(map[string]*TabletStats),
			aggregates: make(map[string]*querypb.AggregateStats),
		}
		t[target.TabletType] = e
	}
	return e
}

func (tc *TabletStatsCache) getRegionByCell(cell string) string {
	return topo.GetRegionByCell(context.Background(), tc.ts, cell)
}

// StatsUpdate is part of the HealthCheckStatsListener interface.
func (tc *TabletStatsCache) StatsUpdate(ts *TabletStats) {
	if ts.Target.TabletType != topodatapb.TabletType_MASTER && ts.Tablet.Alias.Cell != tc.cell && tc.getRegionByCell(ts.Tablet.Alias.Cell) != tc.getRegionByCell(tc.cell) {
		// this is for a non-master tablet in a different cell and a different region, drop it
		return
	}

	e := tc.getOrCreateEntry(ts.Target)
	e.mu.Lock()
	defer e.mu.Unlock()

	// Update our full map.
	trivialNonMasterUpdate := false
	if existing, ok := e.all[ts.Key]; ok {
		if ts.Up {
			// We have an existing entry, and a new entry.
			// Remember if they are both good (most common case).
			trivialNonMasterUpdate = existing.LastError == nil && existing.Serving && ts.LastError == nil && ts.Serving && ts.Target.TabletType != topodatapb.TabletType_MASTER && TrivialStatsUpdate(existing, ts)

			// We already have the entry, update the
			// values if necessary.  (will update both
			// 'all' and 'healthy' as they use pointers).
			if !trivialNonMasterUpdate {
				*existing = *ts
			}
		} else {
			// We have an entry which we shouldn't. Remove it.
			delete(e.all, ts.Key)
		}
	} else {
		if ts.Up {
			// Add the entry.
			e.all[ts.Key] = ts
		} else {
			// We were told to remove an entry which we
			// didn't have anyway, nothing should happen.
			return
		}
	}

	// Update our healthy list.
	var allArray []*TabletStats
	if ts.Target.TabletType == topodatapb.TabletType_MASTER {
		// The healthy list is different for TabletType_MASTER: we
		// only keep the most recent one.
		e.updateHealthyMapForMaster(ts)
		for _, s := range e.all {
			allArray = append(allArray, s)
		}
	} else {
		// For non-master, if it is a trivial update,
		// we just skip everything else. We don't even update the
		// aggregate stats.
		if trivialNonMasterUpdate {
			return
		}

		// Now we need to do some work. Recompute our healthy list.
		allArray = make([]*TabletStats, 0, len(e.all))
		for _, s := range e.all {
			allArray = append(allArray, s)
		}
		e.healthy = FilterByReplicationLag(allArray)
	}

	tc.updateAggregateMap(ts.Target.Keyspace, ts.Target.Shard, ts.Target.TabletType, e, allArray)
}

// makeAggregateMap takes a list of TabletStats and builds a per-region
// AggregateStats map.
func (tc *TabletStatsCache) makeAggregateMap(stats []*TabletStats) map[string]*querypb.AggregateStats {
	result := make(map[string]*querypb.AggregateStats)
	for _, ts := range stats {
		region := tc.getRegionByCell(ts.Tablet.Alias.Cell)
		agg, ok := result[region]
		if !ok {
			agg = &querypb.AggregateStats{
				SecondsBehindMasterMin: math.MaxUint32,
			}
			result[region] = agg
		}

		if ts.Serving && ts.LastError == nil {
			agg.HealthyTabletCount++
			if ts.Stats.SecondsBehindMaster < agg.SecondsBehindMasterMin {
				agg.SecondsBehindMasterMin = ts.Stats.SecondsBehindMaster
			}
			if ts.Stats.SecondsBehindMaster > agg.SecondsBehindMasterMax {
				agg.SecondsBehindMasterMax = ts.Stats.SecondsBehindMaster
			}
		} else {
			agg.UnhealthyTabletCount++
		}
	}
	return result
}

// updateAggregateMap will update the aggregate map for the
// tabletStatsCacheEntry. It may broadcast the changes too if we have listeners.
// e.mu needs to be locked.
func (tc *TabletStatsCache) updateAggregateMap(keyspace, shard string, tabletType topodatapb.TabletType, e *tabletStatsCacheEntry, stats []*TabletStats) {
	// Save the new value
	e.aggregates = tc.makeAggregateMap(stats)
}

// GetTabletStats returns the full list of available targets.
// The returned array is owned by the caller.
func (tc *TabletStatsCache) GetTabletStats(keyspace, shard string, tabletType topodatapb.TabletType) []TabletStats {
	e := tc.getEntry(keyspace, shard, tabletType)
	if e == nil {
		return nil
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]TabletStats, 0, len(e.all))
	for _, s := range e.all {
		result = append(result, *s)
	}
	return result
}

// GetHealthyTabletStats returns only the healthy targets.
// The returned array is owned by the caller.
// For TabletType_MASTER, this will only return at most one entry,
// the most recent tablet of type master.
func (tc *TabletStatsCache) GetHealthyTabletStats(keyspace, shard string, tabletType topodatapb.TabletType) []TabletStats {
	e := tc.getEntry(keyspace, shard, tabletType)
	if e == nil {
		return nil
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]TabletStats, len(e.healthy))
	for i, ts := range e.healthy {
		result[i] = *ts
	}
	return result
}

// InStatsCache returns true if the tablet with the given key is in the cache
func (tc *TabletStatsCache) InStatsCache(keyspace, shard string, tabletType topodatapb.TabletType, key string) bool {
	e := tc.getEntry(keyspace, shard, tabletType)
	if e == nil {
		return false
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, ts := range e.healthy {
		if ts.Key == key {
			return true
		}
	}
	return false
}

// ResetForTesting is for use in tests only.
func (tc *TabletStatsCache) ResetForTesting() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.entries = make(map[string]map[string]map[topodatapb.TabletType]*tabletStatsCacheEntry)
}

// GetAggregateStats is part of the TargetStatsListener interface.
func (tc *TabletStatsCache) GetAggregateStats(target *querypb.Target) (*querypb.AggregateStats, error) {
	e := tc.getEntry(target.Keyspace, target.Shard, target.TabletType)
	if e == nil {
		return nil, topo.NewError(topo.NoNode, topotools.TargetIdent(target))
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	if target.TabletType == topodatapb.TabletType_MASTER {
		if len(e.aggregates) == 0 {
			return nil, topo.NewError(topo.NoNode, topotools.TargetIdent(target))
		}
		for _, agg := range e.aggregates {
			return agg, nil
		}
	}
	targetRegion := tc.getRegionByCell(target.Cell)
	agg, ok := e.aggregates[targetRegion]
	if !ok {
		return nil, topo.NewError(topo.NoNode, topotools.TargetIdent(target))
	}
	return agg, nil
}

// GetMasterCell is part of the TargetStatsListener interface.
func (tc *TabletStatsCache) GetMasterCell(keyspace, shard string) (cell string, err error) {
	e := tc.getEntry(keyspace, shard, topodatapb.TabletType_MASTER)
	if e == nil {
		return "", topo.NewError(topo.NoNode, topotools.TargetIdent(&querypb.Target{
			Keyspace:   keyspace,
			Shard:      shard,
			TabletType: topodatapb.TabletType_MASTER,
		}))
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	for cell := range e.aggregates {
		return cell, nil
	}
	return "", topo.NewError(topo.NoNode, topotools.TargetIdent(&querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: topodatapb.TabletType_MASTER,
	}))
}

// Compile-time interface check.
var _ HealthCheckStatsListener = (*TabletStatsCache)(nil)
