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

package discovery

import (
	"sync"

	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// tabletStatsCache is an internal data structure that keeps both the
// current list of available tabletStats, and a serving list:
// - for master tablets, only the current master is kept.
// - for non-master tablets, we filter the list using FilterStatsByReplicationLag.
// It keeps entries for all tablets in the cell(s) it's configured to serve for,
// and for the master independently of which cell it's in.
// Note the healthy tablet computation is done when we receive a tablet
// update only, not at serving time.
// Also note the cache may not have the last entry received from the tablet.
// For instance, if a tablet was healthy, and is still healthy, we do not
// keep its new update.
type tabletStatsCache struct {
	// mu protects the following fields. It does not protect individual
	// entries in the entries map.
	mu sync.Mutex
	// entries maps from keyspace/shard/tabletType to our cache.
	entries map[string]map[string]map[topodatapb.TabletType]*tabletStatsCacheEntry
}

func newTabletStatsCache() *tabletStatsCache {
	tc := &tabletStatsCache{
		entries: make(map[string]map[string]map[topodatapb.TabletType]*tabletStatsCacheEntry),
	}
	return tc
}

// getEntry returns an existing TabletStatsCacheEntry in the cache, or nil
// if the entry does not exist. It only takes a Read lock on mu.
func (tc *tabletStatsCache) getEntry(keyspace, shard string, tabletType topodatapb.TabletType) *tabletStatsCacheEntry {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if s, ok := tc.entries[keyspace]; ok {
		if t, ok := s[shard]; ok {
			if e, ok := t[tabletType]; ok {
				return e
			}
		}
	}
	return nil
}

// getOrCreateEntry returns an existing TabletStatsCacheEntry from the cache,
// or creates it if it doesn't exist.
func (tc *tabletStatsCache) getOrCreateEntry(target *querypb.Target) *tabletStatsCacheEntry {
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
			all: make(map[string]*tabletStats),
		}
		t[target.TabletType] = e
	}
	return e
}

// tabletStatsCacheEntry is the per keyspace/shard/tabletType
// entry of the in-memory map for tabletStatsCache.
type tabletStatsCacheEntry struct {
	// mu protects the rest of this structure.
	mu sync.Mutex
	// all has the valid tablets, indexed by TabletToMapKey(ts.Tablet),
	// as it is the index used by HealthCheck.
	all map[string]*tabletStats
	// healthy only has the healthy ones.
	healthy []*tabletStats
}

func (e *tabletStatsCacheEntry) updateHealthyMapForMaster(ts *tabletStats) {
	if ts.Target.TabletType != topodatapb.TabletType_MASTER {
		panic("program bug")
	}
	if ts.Up {
		// We have an Up master.
		if len(e.healthy) == 0 {
			// We have a new Up server, just remember it.
			e.healthy = append(e.healthy, ts)
			return
		}

		// We already have one up server, see if we
		// need to replace it.
		if ts.MasterTermStartTime < e.healthy[0].MasterTermStartTime {
			log.Warningf("not marking healthy master %s as Up for %s because its externally reparented timestamp is smaller than the highest known timestamp from previous MASTERs %s: %d < %d ",
				topoproto.TabletAliasString(ts.Tablet.Alias),
				topoproto.KeyspaceShardString(ts.Target.Keyspace, ts.Target.Shard),
				topoproto.TabletAliasString(e.healthy[0].Tablet.Alias),
				ts.MasterTermStartTime,
				e.healthy[0].MasterTermStartTime)
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

func (e *tabletStatsCacheEntry) getHealthyTabletStats() []tabletStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	result := make([]tabletStats, len(e.healthy))
	for i, ts := range e.healthy {
		result[i] = *ts
	}
	return result
}

func (e *tabletStatsCacheEntry) getTabletStats() []tabletStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	result := make([]tabletStats, 0, len(e.all))
	for _, ts := range e.all {
		result = append(result, *ts)
	}
	return result
}

// ResetForTesting is for use in tests only.
func (tc *tabletStatsCache) ResetForTesting() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.entries = make(map[string]map[string]map[topodatapb.TabletType]*tabletStatsCacheEntry)
}
