package vtctld

import (
	"reflect"
	"sort"
	"strconv"
	"sync"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// yLabel is used to keep track of the outer and inner labels of the heatmap.
// For example: { label: 'cell1', nestedLabels: ['2', 'master', '1', 'replica', '1'] } means that
// the heatmap would have an outer label of cell1 with rowspan of 2 and two inner labels of
// master and replica both with rowspan of 1.
type yLabel struct {
	Label        string
	NestedLabels []string
}

type byTabletUid []*discovery.TabletStats

func (a byTabletUid) Len() int           { return len(a) }
func (a byTabletUid) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byTabletUid) Less(i, j int) bool { return a[i].Tablet.Alias.Uid < a[j].Tablet.Alias.Uid }

const tabletMissing int = -1

// tabletStatsCache holds the most recent status update received for
// each tablet. The tablets are indexed by uid, so it is different
// than discovery.TabletStatsCache.
type tabletStatsCache struct {
	// mu guards access to the fields below.
	mu sync.Mutex
	// statuses keeps a map of TabletStats.
	// The first key is the keyspace, the second key is the shard,
	// the third key is the cell, the last key is the tabletType.
	// The keys are strings to allow exposing this map as a JSON object in api.go.
	statuses map[string]map[string]map[string]map[topodatapb.TabletType][]*discovery.TabletStats
	// statusesByAlias is a copy of statuses and will be updated simultaneously.
	// The first key is the string representation of the tablet alias.
	statusesByAlias map[string]*discovery.TabletStats
	// Cells keeps track of the cells found so far.
	cells map[string]bool
}

// StatsUpdate is part of the discovery.HealthCheckStatsListener interface.
// Upon receiving a new TabletStats, it updates the two maps in tablet_stats_cache.
func (c *tabletStatsCache) StatsUpdate(stats *discovery.TabletStats) {
	c.mu.Lock()
	defer c.mu.Unlock()

	keyspace := stats.Target.Keyspace
	shard := stats.Target.Shard
	cell := stats.Tablet.Alias.Cell
	c.cells[cell] = true
	tabletType := stats.Target.TabletType

	if stats.Up {
		_, ok := c.statusesByAlias[stats.Tablet.Alias.String()]
		if ok {
			// If the tablet already exists, then just update the stats of that tablet.
			for index, tabletStat := range c.statuses[keyspace][shard][cell][tabletType] {
				if reflect.DeepEqual(stats.Tablet.Alias, tabletStat.Tablet.Alias) {
					c.statuses[keyspace][shard][cell][tabletType][index] = stats
				}
			}
			c.statusesByAlias[stats.Tablet.Alias.String()] = stats
		} else {
			// If the tablet isn't already there, add it to the map.
			shards, ok := c.statuses[keyspace]
			if !ok {
				shards = make(map[string]map[string]map[topodatapb.TabletType][]*discovery.TabletStats)
				c.statuses[keyspace] = shards
			}

			cells, ok := c.statuses[keyspace][shard]
			if !ok {
				cells = make(map[string]map[topodatapb.TabletType][]*discovery.TabletStats)
				c.statuses[keyspace][shard] = cells
			}

			types, ok := c.statuses[keyspace][shard][cell]
			if !ok {
				types = make(map[topodatapb.TabletType][]*discovery.TabletStats)
				c.statuses[keyspace][shard][cell] = types
			}

			tablets, ok := c.statuses[keyspace][shard][cell][tabletType]
			if !ok {
				if !stats.Up {
					// We're told a tablet is gone, and we don't have
					// a map for it anyway, nothing to do.
					return
				}
				tablets = make([]*discovery.TabletStats, 0)
				c.statuses[keyspace][shard][cell][tabletType] = tablets
			}
			c.statuses[keyspace][shard][cell][tabletType] = append(c.statuses[keyspace][shard][cell][tabletType], stats)
			sort.Sort(byTabletUid(c.statuses[keyspace][shard][cell][tabletType]))

			c.statusesByAlias[stats.Tablet.Alias.String()] = stats
		}
	} else {
		// If !Stats.Up then remove the stats from the cache.
		c.statuses[keyspace][shard][cell][tabletType] = remove(c.statuses[keyspace][shard][cell][tabletType], stats.Tablet.Alias)
		delete(c.statusesByAlias, stats.Tablet.Alias.String())
	}
}

// remove takes in an array and returns it with the specified element removed
// (leaves the array unchanged if element isn't in the array).
func remove(tablets []*discovery.TabletStats, tabletAlias *topodata.TabletAlias) []*discovery.TabletStats {
	for index, tablet := range tablets {
		if topoproto.TabletAliasEqual(tablet.Tablet.Alias, tabletAlias) {
			if len(tablets) == index+1 {
				// The element to be removed is the last element in the slice so just remove it.
				tablets = append(tablets[0:index])
			} else {
				// There are more elements after the element to be removed so copy all elements before and after.
				tablets = append(tablets[0:index], tablets[(index+1):]...)
			}
			return tablets
		}
	}
	return tablets
}

// heatmapData returns a 2D array of data (based on the specified metric) as well as the labels for the heatmap.
func (c *tabletStatsCache) heatmapData(keyspace, cell, tabletType, metric string) ([][]float64, [][]*topodata.TabletAlias, []yLabel) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var cells []string
	for cell := range c.cells {
		cells = append(cells, cell)
	}
	sort.Strings(cells)

	var shards []string
	for s := range c.statuses[keyspace] {
		shards = append(shards, s)
	}
	sort.Strings(shards)

	types := []topodatapb.TabletType{topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY}

	// TODO(pkulshre): Generalize the following algorithm to support any combination of keyspace-cell-type.
	var heatmapData [][]float64
	var heatmapTabletAliases [][]*topodata.TabletAlias
	var yLabels []yLabel
	// The loop goes through every outer label (in this case, cell).
	for _, cell := range cells {
		perCellYLabel := yLabel{
			Label: cell,
		}
		labelSpan := 0

		// This loop goes through every nested label (in this case, tablet type).
		for _, tabletType := range types {
			maxRowLength := 0

			// The loop calculates the maximum number of rows needed.
			for _, shard := range shards {
				tabletsCount := len(c.statuses[keyspace][shard][cell][tabletType])
				if maxRowLength < tabletsCount {
					maxRowLength = tabletsCount
				}
			}

			// dataRowsPerType is a 2D array that will hold the data of the tablets of one (cell, type) combination.
			dataRowsPerType := make([][]float64, maxRowLength)
			// aliasRowsPerType is a 2D array that will hold the aliases of the tablets of one (cell, type) combination.
			aliasRowsPerType := make([][]*topodata.TabletAlias, maxRowLength)
			for i := range dataRowsPerType {
				dataRowsPerType[i] = make([]float64, len(shards))
				aliasRowsPerType[i] = make([]*topodata.TabletAlias, len(shards))
			}

			// Filling in the 2D array with tablet data by columns.
			for shardIndex, shard := range shards {
				for tabletIndex := 0; tabletIndex < maxRowLength; tabletIndex++ {
					// If the key doesn't exist then the tablet must not exist so that data is set to -1 (tabletMissing).
					if tabletIndex < len(c.statuses[keyspace][shard][cell][tabletType]) {
						dataRowsPerType[tabletIndex][shardIndex] = metricValue(c.statuses[keyspace][shard][cell][tabletType][tabletIndex], metric)
						aliasRowsPerType[tabletIndex][shardIndex] = c.statuses[keyspace][shard][cell][tabletType][tabletIndex].Tablet.Alias
					} else {
						dataRowsPerType[tabletIndex][shardIndex] = float64(tabletMissing)
						aliasRowsPerType[tabletIndex][shardIndex] = nil
					}

					// Adding the labels for the yaxis only if it is the first column.
					if shardIndex == 0 {
						// Adding the label to the array.
						if tabletIndex == 0 {
							perCellYLabel.NestedLabels = append(perCellYLabel.NestedLabels, tabletType.String())
						}
						// Adding the number of rows needed for the previously added label.
						if tabletIndex == (maxRowLength - 1) {
							perCellYLabel.NestedLabels = append(perCellYLabel.NestedLabels, strconv.FormatInt(int64(maxRowLength), 10))
							labelSpan += maxRowLength
						}
					}
				}
			}

			for i := 0; i < len(dataRowsPerType); i++ {
				heatmapData = append(heatmapData, dataRowsPerType[i])
				heatmapTabletAliases = append(heatmapTabletAliases, aliasRowsPerType[i])
			}
		}
		perCellYLabel.NestedLabels = append([]string{strconv.FormatInt(int64(labelSpan), 10)}, perCellYLabel.NestedLabels...)
		yLabels = append(yLabels, perCellYLabel)
	}

	return heatmapData, heatmapTabletAliases, yLabels
}

func metricValue(stat *discovery.TabletStats, metric string) float64 {
	switch metric {
	case "lag":
		return float64(stat.Stats.SecondsBehindMaster)
	case "cpu":
		return stat.Stats.CpuUsage
	case "qps":
		return stat.Stats.Qps
	}
	return -1
}

// compile-time interface check
var _ discovery.HealthCheckStatsListener = (*tabletStatsCache)(nil)
