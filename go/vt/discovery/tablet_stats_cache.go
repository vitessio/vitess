package discovery

import (
	"sync"

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
type TabletStatsCache struct {
	// cell is the cell we are keeping all tablets for.
	// Note we keep track of all master tablets in all cells.
	cell string

	// mu protects the following fields.
	mu sync.RWMutex
	// entries maps from keyspace/shard/tabletType to our cache.
	entries map[string]map[string]map[topodatapb.TabletType]*tabletStatsCacheEntry
}

// tabletStatsCacheEntry is the per keyspace/shard/tabaletType
// entry of the in-memory map for TabletStatsCache.
type tabletStatsCacheEntry struct {
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
	tc := &TabletStatsCache{
		cell:    cell,
		entries: make(map[string]map[string]map[topodatapb.TabletType]*tabletStatsCacheEntry),
	}

	// We need to set sendDownEvents=true to get the deletes from the map
	// upon type change.
	hc.SetListener(tc, true /*sendDownEvents*/)
	return tc
}

// StatsUpdate is part of the HealthCheckStatsListener interface.
func (tc *TabletStatsCache) StatsUpdate(ts *TabletStats) {
	if ts.Target.TabletType != topodatapb.TabletType_MASTER && ts.Tablet.Alias.Cell != tc.cell {
		// this is for a non-master tablet in a different cell, drop it
		return
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	s, ok := tc.entries[ts.Target.Keyspace]
	if !ok {
		s = make(map[string]map[topodatapb.TabletType]*tabletStatsCacheEntry)
		tc.entries[ts.Target.Keyspace] = s
	}
	t, ok := s[ts.Target.Shard]
	if !ok {
		t = make(map[topodatapb.TabletType]*tabletStatsCacheEntry)
		s[ts.Target.Shard] = t
	}
	e, ok := t[ts.Target.TabletType]
	if !ok {
		e = &tabletStatsCacheEntry{
			all: make(map[string]*TabletStats),
		}
		t[ts.Target.TabletType] = e
	}

	// Update our full map.
	if existing, ok := e.all[ts.Key]; ok {
		if ts.Up {
			// We already have the entry, update the values.
			*existing = *ts
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
	// using FilterByReplicationLag.
	allArray := make([]*TabletStats, 0, len(e.all))
	for _, s := range e.all {
		allArray = append(allArray, s)
	}
	e.healthy = FilterByReplicationLag(allArray)
}

// GetTabletStats returns the full list of available targets.
// The returned array is owned by the caller.
func (tc *TabletStatsCache) GetTabletStats(keyspace, shard string, tabletType topodatapb.TabletType) []TabletStats {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	s, ok := tc.entries[keyspace]
	if !ok {
		return nil
	}
	t, ok := s[shard]
	if !ok {
		return nil
	}
	e, ok := t[tabletType]
	if !ok {
		return nil
	}

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
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	s, ok := tc.entries[keyspace]
	if !ok {
		return nil
	}
	t, ok := s[shard]
	if !ok {
		return nil
	}
	e, ok := t[tabletType]
	if !ok {
		return nil
	}

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
