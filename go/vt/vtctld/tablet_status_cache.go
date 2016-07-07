package vtctld

import (
	"strconv"

	"github.com/youtube/vitess/go/vt/discovery"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

type tabletStatsCache struct {
	tabletUpdate map[string]map[string]*querypb.RealtimeStats
}

func (t tabletStatsCache) StatsUpdate(stats *discovery.TabletStats) {
	target := stats.Target
	tablet := stats.Tablet.Alias
	currTarget := createTargetMapKey(tablet.Cell, target.Keyspace, target.Shard, target.TabletType.String())
	currTablet := strconv.FormatUint(uint64(tablet.Uid), 10)
	update := stats.Stats
	if t.tabletUpdate[currTarget] == nil {
		t.tabletUpdate[currTarget] = make(map[string]*querypb.RealtimeStats)
	}
	(t.tabletUpdate[currTarget])[currTablet] = update
}

func (t *tabletStatsCache) getTargetUpdates(cell, keyspace, shard, tabType string) map[string]*querypb.RealtimeStats {
	currTarget := createTargetMapKey(cell, keyspace, shard, tabType)
	return t.tabletUpdate[currTarget]
}

func createTargetMapKey(cell, keyspace, shard, tabType string) string {
	return cell + "-" + keyspace + "-" + shard + "-" + tabType
}
