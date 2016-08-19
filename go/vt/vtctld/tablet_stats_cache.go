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
	CellLabel  label
	TypeLabels []label
}

// label is used to keep track of one label of a heatmap and how many rows it should span.
type label struct {
	Name    string
	Rowspan int
}

// heatmap stores all the needed info to construct the heatmap.
type heatmap struct {

	// Data is a 2D array of values of the specified metric.
	Data [][]float64
	// Aliases is a 2D array holding references to the tablet aliases.
	Aliases [][]*topodata.TabletAlias

	KeyspaceLabel     label
	CellAndTypeLabels []yLabel
	ShardLabels       []string
}

type byTabletUID []*discovery.TabletStats

func (a byTabletUID) Len() int           { return len(a) }
func (a byTabletUID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byTabletUID) Less(i, j int) bool { return a[i].Tablet.Alias.Uid < a[j].Tablet.Alias.Uid }

// This value represents a missing/non-existent tablet for any metric.
const tabletMissing = -1

// This value is needed to indicate when there's data but no labels are needed.
const empty = "EMPTY"

// These values represent the threshold for replication lag.
const lagThresholdDegraded = 60
const lagThresholdUnhealthy = 120

// These values represent the health of the tablet - 1 is healthy, 2 is degraded, 3 is unhealthy
const tabletHealthy = 0
const tabletDegraded = 1
const tabletUnhealthy = 2

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

func (c *tabletStatsCache) keyspaces(keyspace string) []string {
	if keyspace != "all" {
		return []string{keyspace}
	}
	var keyspaces []string
	for ks := range c.statuses {
		keyspaces = append(keyspaces, ks)
	}
	sort.Strings(keyspaces)
	return keyspaces
}

func (c *tabletStatsCache) cells(cell string) []string {
	if cell != "all" {
		return []string{cell}
	}
	var cells []string
	for cl := range c.tabletCountsByCell {
		cells = append(cells, cl)
	}
	sort.Strings(cells)
	return cells
}

func tabletTypes(tabletType string) []topodata.TabletType {
	if tabletType != "all" {
		tabletTypeObj, _ := topoproto.ParseTabletType(tabletType)
		return []topodata.TabletType{tabletTypeObj}
	}
	return []topodata.TabletType{topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY}
}

func (c *tabletStatsCache) shards(keyspace string) []string {
	var shards []string
	for s := range c.statuses[keyspace] {
		shards = append(shards, s)
	}
	sort.Strings(shards)
	return shards
}

// heatmapData returns a 2D array of data (based on the specified metric) as well as the labels for the heatmap.
func (c *tabletStatsCache) heatmapData(selectedKeyspace, selectedCell, selectedTabletType, selectedMetric string) ([]heatmap, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get the metric data.
	var metricFunc func(stats *discovery.TabletStats) float64
	switch selectedMetric {
	case "lag":
		metricFunc = replicationLag
	case "qps":
		metricFunc = qps
	case "healthy":
		metricFunc = healthy
	default:
		return nil, fmt.Errorf("invalid metric: %v Select 'lag', 'cpu', or 'qps'", selectedMetric)
	}

	// Get the proper data (unaggregated tablets or aggregated tablets by types)
	aggregated := false
	if selectedKeyspace == "all" && selectedTabletType == "all" {
		aggregated = true
	}

	keyspaces := c.keyspaces(selectedKeyspace)
	cells := c.cells(selectedCell)
	var listOfHeatmaps []heatmap
	for _, keyspace := range keyspaces {
		var heatmapData [][]float64
		var heatmapTabletAliases [][]*topodata.TabletAlias
		var heatmapCellAndTypeLabels []yLabel
		shards := c.shards(keyspace)
		keyspaceLabelSpan := 0
		// The loop goes through every outer label (in this case, cell).
		for _, cell := range cells {
			var cellData [][]float64
			var cellAliases [][]*topodata.TabletAlias
			var cellLabel yLabel

			if aggregated {
				cellData, cellAliases, cellLabel = c.aggregatedData(keyspace, cell, selectedTabletType, selectedMetric, metricFunc)
			} else {
				cellData, cellAliases, cellLabel = c.unaggregatedData(keyspace, cell, selectedTabletType, metricFunc)
			}

			if cellLabel.CellLabel.Rowspan > 0 {
				// Adding the data in reverse to match the format that the plotly map takes in.
				for i := 0; i < len(cellData); i++ {
					heatmapData = append([][]float64{cellData[i]}, heatmapData...)
					if cellAliases == nil {
						continue
					}
					heatmapTabletAliases = append([][]*topodata.TabletAlias{cellAliases[i]}, heatmapTabletAliases...)
				}
				heatmapCellAndTypeLabels = append(heatmapCellAndTypeLabels, cellLabel)
			}
			keyspaceLabelSpan += cellLabel.CellLabel.Rowspan
		}

		heatmapKeyspaceLabel := label{Name: keyspace, Rowspan: keyspaceLabelSpan}
		currHeatmap := heatmap{
			Data:              heatmapData,
			Aliases:           heatmapTabletAliases,
			KeyspaceLabel:     heatmapKeyspaceLabel,
			CellAndTypeLabels: heatmapCellAndTypeLabels,
			ShardLabels:       shards,
		}
		listOfHeatmaps = append(listOfHeatmaps, currHeatmap)
	}

	return listOfHeatmaps, nil
}

func (c *tabletStatsCache) unaggregatedData(keyspace, cell, selectedType string, metricFunc func(stats *discovery.TabletStats) float64) ([][]float64, [][]*topodata.TabletAlias, yLabel) {
	// This loop goes through every nested label (in this case, tablet type).
	var heatmapData [][]float64
	var heatmapTabletAliases [][]*topodata.TabletAlias
	var cellLabel yLabel
	cellLabelSpan := 0
	tabletTypes := tabletTypes(selectedType)
	shards := c.shards(keyspace)
	for _, tabletType := range tabletTypes {
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
			}
		}

		if maxRowLength > 0 {
			cellLabel.TypeLabels = append(cellLabel.TypeLabels, label{Name: tabletType.String(), Rowspan: maxRowLength})
		}
		cellLabelSpan += maxRowLength

		for i := 0; i < len(dataRowsPerType); i++ {
			heatmapData = append(heatmapData, dataRowsPerType[i])
			heatmapTabletAliases = append(heatmapTabletAliases, aliasRowsPerType[i])
		}
	}

	cellLabel.CellLabel = label{Name: cell, Rowspan: cellLabelSpan}

	return heatmapData, heatmapTabletAliases, cellLabel
}

func (c *tabletStatsCache) aggregatedData(keyspace, cell, selectedType, selectedMetric string, metricFunc func(stats *discovery.TabletStats) float64) ([][]float64, [][]*topodata.TabletAlias, yLabel) {
	shards := c.shards(keyspace)
	tabletTypes := tabletTypes(selectedType)

	var heatmapData [][]float64
	dataRow := make([]float64, len(shards))
	// This loop goes through each shard in the (keyspace-cell) combination.
	for shardIndex, shard := range shards {
		var sum, count float64 = 0, 0
		hasTablets := false
		unhealthyFound := false
		// Going through all the types of tablets and aggregating their information
		for _, tabletType := range tabletTypes {
			tablets, ok := c.statuses[keyspace][shard][cell][tabletType]
			if !ok {
				continue
			}
			for _, tablet := range tablets {
				hasTablets = true
				// If even one tablet is unhealthy then the entire groups becomes unhealthy.
				if (selectedMetric == "healthy" && metricFunc(tablet) == tabletUnhealthy) ||
					(selectedMetric == "lag" && metricFunc(tablet) > lagThresholdUnhealthy) {
					sum = metricFunc(tablet)
					count = 1
					unhealthyFound = true
					break
				}
				sum += metricFunc(tablet)
				count++
			}
			if unhealthyFound == true {
				break
			}
		}
		if hasTablets == true {
			dataRow[shardIndex] = (sum / count)
		} else {
			dataRow[shardIndex] = tabletMissing
		}
	}
	heatmapData = append(heatmapData, dataRow)
	var heatmapCellLabel yLabel
	heatmapCellLabel.CellLabel = label{Name: cell, Rowspan: 1}

	return heatmapData, nil, heatmapCellLabel
}

func (c *tabletStatsCache) tabletStats(tabletAlias *topodatapb.TabletAlias) (discovery.TabletStats, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ts, ok := c.statusesByAlias[tabletAlias.String()]
	if !ok {
		return discovery.TabletStats{}, fmt.Errorf("could not find tablet: %v", tabletAlias)
	}
	return *ts, nil
}

func healthy(stat *discovery.TabletStats) float64 {
	health := tabletHealthy
	// The tablet is unhealthy if there is a health error.
	if stat.Stats.HealthError != "" {
		return tabletUnhealthy
	}

	// The tablet is degraded if there was an error previously.
	if stat.LastError != nil {
		if health < tabletDegraded {
			health = tabletDegraded
		}
	}

	// The tablet is healthy/degraded/unheathy depending on the lag.
	lag := stat.Stats.SecondsBehindMaster
	if lag < lagThresholdDegraded {
		if health < tabletHealthy {
			health = tabletHealthy
		}
	} else if lag < lagThresholdUnhealthy {
		if health < tabletDegraded {
			health = tabletDegraded
		}
	} else {
		return tabletUnhealthy
	}

	// The tablet is healthy or degraded based on serving status
	if stat.Serving == true {
		if health < tabletHealthy {
			health = tabletHealthy
		}
	} else {
		if health < tabletDegraded {
			health = tabletDegraded
		}
	}
	return float64(health)
}

func replicationLag(stat *discovery.TabletStats) float64 {
	return float64(stat.Stats.SecondsBehindMaster)
}

func qps(stat *discovery.TabletStats) float64 {
	return stat.Stats.Qps
}

// compile-time interface check
var _ discovery.HealthCheckStatsListener = (*tabletStatsCache)(nil)
