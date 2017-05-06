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
	"sync"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// TabletStatsCache is a HealthCheckStatsListener that keeps both the
// current list of available TabletStats, and a serving list:
// - for master tablets, only the current master is kept.
// - for non-master tablets, we filter the list using FilterByReplicationLag.
// It keeps entries for all tablets in the cell it's configured to serve for,
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

	// mu protects the entries map. It does not protect individual
	// entries in the map.
	mu sync.RWMutex
	// entries maps from keyspace/shard/tabletType to our cache.
	entries map[string]map[string]map[topodatapb.TabletType]*tabletStatsCacheEntry
}

// tabletStatsCacheEntry is the per keyspace/shard/tabaletType
// entry of the in-memory map for TabletStatsCache.
type tabletStatsCacheEntry struct {
	// mu protects the rest of this structure.
	mu sync.RWMutex
	// all has the valid tablets, indexed by TabletToMapKey(ts.Tablet),
	// as it is the index used by HealthCheck.
	all map[string]*TabletStats
	// healthy only has the healthy ones.
	healthy []*TabletStats
}

// NewTabletStatsCache creates a TabletStatsCache, and registers
// it as HealthCheckStatsListener of the provided healthcheck.
// Note we do the registration in this code to guarantee we call
// SetListener with sendDownEvents=true, as we need these events
// to maintain the integrity of our cache.
func NewTabletStatsCache(hc HealthCheck, cell string) *TabletStatsCache {
	return newTabletStatsCache(hc, cell, true /* setListener */)
}

// NewTabletStatsCacheDoNotSetListener is identical to NewTabletStatsCache
// but does not automatically set the returned object as listener for "hc".
// Instead, it's up to the caller to ensure that TabletStatsCache.StatsUpdate()
// gets called properly. This is useful for chaining multiple listeners.
// When the caller sets its own listener on "hc", they must make sure that they
// set the parameter  "sendDownEvents" to "true" or this cache won't properly
// remove tablets whose tablet type changes.
func NewTabletStatsCacheDoNotSetListener(cell string) *TabletStatsCache {
	return newTabletStatsCache(nil, cell, false /* setListener */)
}

func newTabletStatsCache(hc HealthCheck, cell string, setListener bool) *TabletStatsCache {
	tc := &TabletStatsCache{
		cell:    cell,
		entries: make(map[string]map[string]map[topodatapb.TabletType]*tabletStatsCacheEntry),
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
			all: make(map[string]*TabletStats),
		}
		t[target.TabletType] = e
	}
	return e
}

// StatsUpdate is part of the HealthCheckStatsListener interface.
func (tc *TabletStatsCache) StatsUpdate(ts *TabletStats) {
	if ts.Target.TabletType != topodatapb.TabletType_MASTER && ts.Tablet.Alias.Cell != tc.cell {
		// this is for a non-master tablet in a different cell, drop it
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

	// The healthy list is different for TabletType_MASTER: we
	// only keep the most recent one.
	if ts.Target.TabletType == topodatapb.TabletType_MASTER {
		if ts.Up {
			// We have an Up master
			if len(e.healthy) == 0 {
				// We have a new Up server, just remember it.
				e.healthy = append(e.healthy, ts)
				return
			}

			// We already have one up server, see if we
			// need to replace it.
			if e.healthy[0].TabletExternallyReparentedTimestamp > ts.TabletExternallyReparentedTimestamp {
				// The notification we just got is older than
				// the one we had before, discard it.
				return
			}

			// Just replace it
			e.healthy[0] = ts
		} else {
			// We have a Down master, remove it only if
			// it's exactly the same
			if len(e.healthy) != 0 {
				if ts.Key == e.healthy[0].Key {
					// same guy, remove it
					e.healthy = nil
				}
			}
		}
		return
	}

	// For non-master, we just recompute the healthy list
	// using FilterByReplicationLag, if we need to.
	if trivialNonMasterUpdate {
		return
	}
	allArray := make([]*TabletStats, 0, len(e.all))
	for _, s := range e.all {
		allArray = append(allArray, s)
	}
	e.healthy = FilterByReplicationLag(allArray)
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

// ResetForTesting is for use in tests only.
func (tc *TabletStatsCache) ResetForTesting() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.entries = make(map[string]map[string]map[topodatapb.TabletType]*tabletStatsCacheEntry)
}

// Compile-time interface check.
var _ HealthCheckStatsListener = (*TabletStatsCache)(nil)
