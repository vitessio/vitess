package vtctld

import (
	"strconv"
	"sync"

	"github.com/youtube/vitess/go/vt/discovery"
)

// tabletStatsCache holds the most recent status update received for each tablet.
type tabletStatsCache struct {
	// First map key is "cellName - keyspaceName - shardName - tabType".
	// Second map key is the string representation of tablet's uid.
	// Both keys are strings to allow exposing this map as a JSON object in api.go.
	recentTabletStatuses map[string]map[string]*discovery.TabletStats
}

var mu = &sync.Mutex{}

func (t tabletStatsCache) StatsUpdate(stats *discovery.TabletStats) {
	target := stats.Target
	tabletAlias := stats.Tablet.Alias
	currentTarget := createTargetMapKey(stats.Tablet.Alias.Cell, target.Keyspace, target.Shard, target.TabletType.String())

	// The mutex object is locked to protect the map from changing while
	// update is in progress.
	mu.Lock()
	tablets, ok := t.recentTabletStatuses[currentTarget]
	if !ok {
		tablets = make(map[string]*discovery.TabletStats)
		t.recentTabletStatuses[currentTarget] = tablets
	}
	tablets[strconv.FormatUint(uint64(tabletAlias.Uid), 10)] = stats
	mu.Unlock()
}

func (t *tabletStatsCache) tabletStatuses(cell, keyspace, shard, tabType string) map[string]*discovery.TabletStats {
	// The mutex object is locked to protect the map from changing while
	// sending the update.
	mu.Lock()
	defer mu.Unlock()
	return t.recentTabletStatuses[createTargetMapKey(cell, keyspace, shard, tabType)]
}

func createTargetMapKey(cell, keyspace, shard, tabType string) string {
	return cell + "-" + keyspace + "-" + shard + "-" + tabType
}
