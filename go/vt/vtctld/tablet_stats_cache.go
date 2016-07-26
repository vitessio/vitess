package vtctld

import (
	"strconv"
	"sync"

	"github.com/youtube/vitess/go/vt/discovery"
)

// tabletStatsCache holds the most recent status update received for each tablet.
type tabletStatsCache struct {
	// mu guards access to the fields below.
	mu sync.Mutex
	// First map key is "cell- keyspace- shard- tabletType".
	// Second map key is the string representation of tablet's uid.
	// Both keys are strings to allow exposing this map as a JSON object in api.go.
	statuses map[string]map[string]*discovery.TabletStats
}

func (t *tabletStatsCache) StatsUpdate(stats *discovery.TabletStats) {
	target := stats.Target
	tabletAlias := stats.Tablet.Alias
	currentTarget := createTargetMapKey(stats.Tablet.Alias.Cell, target.Keyspace, target.Shard, target.TabletType.String())

	t.mu.Lock()
	defer t.mu.Unlock()
	tablets, ok := t.statuses[currentTarget]
	if !ok {
		tablets = make(map[string]*discovery.TabletStats)
		t.statuses[currentTarget] = tablets
	}
	tablets[strconv.FormatUint(uint64(tabletAlias.Uid), 10)] = stats
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
