package vtctld

import (
	"strconv"
	"sync"

	"github.com/youtube/vitess/go/vt/discovery"
)

// tabletStatsCache holds the most recent status update received for
// each tablet. The tablets are indexed by uid, so it is different
// than discovery.TabletStatsCache.
type tabletStatsCache struct {
	// mu guards access to the fields below.
	mu sync.Mutex

	// statuses keeps a map of TabletStats.
	// First map key is "cell- keyspace- shard- tabletType".
	// Second map key is the string representation of tablet's uid.
	// Both keys are strings to allow exposing this map as a JSON object
	// in api.go.
	statuses map[string]map[string]*discovery.TabletStats
}

// StatsUpdate is part of the discovery.HealthCheckStatsListener interface.
func (t *tabletStatsCache) StatsUpdate(stats *discovery.TabletStats) {
	target := stats.Target
	tabletAlias := stats.Tablet.Alias
	currentTarget := createTargetMapKey(stats.Tablet.Alias.Cell, target.Keyspace, target.Shard, target.TabletType.String())

	t.mu.Lock()
	defer t.mu.Unlock()
	tablets, ok := t.statuses[currentTarget]
	if !ok {
		if !stats.Up {
			// We're told a tablet is gone, and we don't have
			// a map for it anyway, nothing to do.
			return
		}
		tablets = make(map[string]*discovery.TabletStats)
		t.statuses[currentTarget] = tablets
	}
	key := strconv.FormatUint(uint64(tabletAlias.Uid), 10)
	if stats.Up {
		tablets[key] = stats
	} else {
		delete(tablets, key)
	}
}

func (t *tabletStatsCache) tabletStatuses(cell, keyspace, shard, tabletType string) map[string]*discovery.TabletStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	statsMapCopy := make(map[string]*discovery.TabletStats)
	for tabletUid, tabletStatus := range t.statuses[createTargetMapKey(cell, keyspace, shard, tabletType)] {
		statsMapCopy[tabletUid] = tabletStatus
	}
	return statsMapCopy
}

func createTargetMapKey(cell, keyspace, shard, tabletType string) string {
	return (cell + "-" + keyspace + "-" + shard + "-" + tabletType)
}

// compile-time interface check
var _ discovery.HealthCheckStatsListener = (*tabletStatsCache)(nil)
