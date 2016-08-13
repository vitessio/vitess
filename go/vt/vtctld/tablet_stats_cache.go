package vtctld

import (
	"fmt"
	"sort"
	"sync"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// yLabel is used to keep track of the outer and inner labels of the heatmap.
type yLabel struct {
	Label        label
	NestedLabels []label
}

// label is used to keep track of one label of a heatmap and how many rows it should span.
type label struct {
	Name    string
	Rowspan int
}

// heatmap stores all the needed info to construct the heatmap.
type heatmap struct {
	// Labels has the outer and inner labels for each row.
	Labels []yLabel
	// Data is a 2D array of values of the specified metric.
	Data [][]float64
	// Aliases is a 2D array holding references to the tablet aliases.
	Aliases [][]*topodata.TabletAlias
}

type byTabletUID []*discovery.TabletStats

func (a byTabletUID) Len() int           { return len(a) }
func (a byTabletUID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byTabletUID) Less(i, j int) bool { return a[i].Tablet.Alias.Uid < a[j].Tablet.Alias.Uid }

const tabletMissing = -1

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
	// cells counts the number of tablets per cell.
	tabletCountsByCell map[string]int
}

func newTabletStatsCache() *tabletStatsCache {
	return &tabletStatsCache{
		statuses:           make(map[string]map[string]map[string]map[topodatapb.TabletType][]*discovery.TabletStats),
		statusesByAlias:    make(map[string]*discovery.TabletStats),
		tabletCountsByCell: make(map[string]int),
	}
}

// StatsUpdate is part of the discovery.HealthCheckStatsListener interface.
// Upon receiving a new TabletStats, it updates the two maps in tablet_stats_cache.
func (c *tabletStatsCache) StatsUpdate(stats *discovery.TabletStats) {
	c.mu.Lock()
	defer c.mu.Unlock()

	keyspace := stats.Target.Keyspace
	shard := stats.Target.Shard
	cell := stats.Tablet.Alias.Cell
	tabletType := stats.Target.TabletType

	aliasKey := tabletToMapKey(stats)
	ts, ok := c.statusesByAlias[aliasKey]
	if !stats.Up {
		if !ok {
			// Tablet doesn't exist and was recently deleted or changed its type. Panic as this is unexpected behavior.
			panic(fmt.Sprintf("BUG: tablet (%v) doesn't exist", aliasKey))
		}
		// The tablet still exists in our cache but was recently deleted or changed its type. Delete it now.
		c.statuses[keyspace][shard][cell][tabletType] = remove(c.statuses[keyspace][shard][cell][tabletType], stats.Tablet.Alias)
		delete(c.statusesByAlias, aliasKey)
		c.tabletCountsByCell[cell]--
		if c.tabletCountsByCell[cell] == 0 {
			delete(c.tabletCountsByCell, cell)
		}
		return
	}

	if !ok {
		// Tablet isn't tracked yet so just add it.
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
			tablets = make([]*discovery.TabletStats, 0)
			c.statuses[keyspace][shard][cell][tabletType] = tablets
		}

		c.statuses[keyspace][shard][cell][tabletType] = append(c.statuses[keyspace][shard][cell][tabletType], stats)
		sort.Sort(byTabletUID(c.statuses[keyspace][shard][cell][tabletType]))
		c.statusesByAlias[aliasKey] = stats
		c.tabletCountsByCell[cell]++
		return
	}

	// Tablet already exists so just update it in the cache.
	*ts = *stats
}

func tabletToMapKey(stats *discovery.TabletStats) string {
	return stats.Tablet.Alias.String()
}

// remove takes in an array and returns it with the specified element removed
// (leaves the array unchanged if element isn't in the array).
func remove(tablets []*discovery.TabletStats, tabletAlias *topodata.TabletAlias) []*discovery.TabletStats {
	filteredTablets := tablets[:0]
	for _, tablet := range tablets {
		if !topoproto.TabletAliasEqual(tablet.Tablet.Alias, tabletAlias) {
			filteredTablets = append(filteredTablets, tablet)
		}
	}
	return filteredTablets
}

// heatmapData returns a 2D array of data (based on the specified metric) as well as the labels for the heatmap.
func (c *tabletStatsCache) heatmapData(keyspace, cell, tabletType, metric string) (heatmap, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get the metric data.
	var metricFunc func(stats *discovery.TabletStats) float64
	switch metric {
	case "lag":
		metricFunc = replicationLag
	case "cpu":
		metricFunc = cpu
	case "qps":
		metricFunc = qps
	default:
		return heatmap{}, fmt.Errorf("invalid metric: %v Select 'lag', 'cpu', or 'qps'", metric)
	}

	var cells []string
	for cell := range c.tabletCountsByCell {
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
		perCellYLabel := yLabel{}
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
						dataRowsPerType[tabletIndex][shardIndex] = metricFunc(c.statuses[keyspace][shard][cell][tabletType][tabletIndex])
						aliasRowsPerType[tabletIndex][shardIndex] = c.statuses[keyspace][shard][cell][tabletType][tabletIndex].Tablet.Alias
					} else {
						dataRowsPerType[tabletIndex][shardIndex] = tabletMissing
						aliasRowsPerType[tabletIndex][shardIndex] = nil
					}

					// Adding the labels for the yaxis only if it is the first column.
					if shardIndex == 0 && tabletIndex == (maxRowLength-1) {
						tempLabel := label{
							Name:    tabletType.String(),
							Rowspan: maxRowLength,
						}
						perCellYLabel.NestedLabels = append(perCellYLabel.NestedLabels, tempLabel)
						labelSpan += maxRowLength
					}
				}
			}

			for i := 0; i < len(dataRowsPerType); i++ {
				heatmapData = append(heatmapData, dataRowsPerType[i])
				heatmapTabletAliases = append(heatmapTabletAliases, aliasRowsPerType[i])
			}
		}
		perCellYLabel.Label = label{
			Name:    cell,
			Rowspan: labelSpan,
		}
		yLabels = append(yLabels, perCellYLabel)
	}

	return heatmap{
		Data:    heatmapData,
		Labels:  yLabels,
		Aliases: heatmapTabletAliases,
	}, nil
}

func (c *tabletStatsCache) tabletStatsByAlias(tabletAlias *topodatapb.TabletAlias) discovery.TabletStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	ts, ok := c.statusesByAlias[tabletAlias.String()]
	if !ok {
		return discovery.TabletStats{}
	}
	return *ts
}

func (c *tabletStatsCache) tabletHealth(cell string, uid uint32) (*discovery.TabletStats, error) {
	tabletAlias := topodatapb.TabletAlias{
		Cell: cell,
		Uid:  uid,
	}
	stat, ok := c.statusesByAlias[tabletAlias.String()]
	if !ok {
		return nil, fmt.Errorf("tablet %v doesn't exist in cell %v", uid, cell)
	}
	return stat, nil
}

func replicationLag(stat *discovery.TabletStats) float64 {
	return float64(stat.Stats.SecondsBehindMaster)
}

func cpu(stat *discovery.TabletStats) float64 {
	return stat.Stats.CpuUsage
}

func qps(stat *discovery.TabletStats) float64 {
	return stat.Stats.Qps
}

// compile-time interface check
var _ discovery.HealthCheckStatsListener = (*tabletStatsCache)(nil)
