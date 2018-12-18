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

package vtctld

import (
	"fmt"
	"sort"
	"sync"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// yLabel is used to keep track of the cell and type labels of the heatmap.
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
	Aliases           [][]*topodatapb.TabletAlias
	KeyspaceLabel     label
	CellAndTypeLabels []yLabel
	ShardLabels       []string

	// YGridLines is used to draw gridLines on the map in the right places.
	YGridLines []float64
}

type byTabletUID []*discovery.TabletStats

func (a byTabletUID) Len() int           { return len(a) }
func (a byTabletUID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byTabletUID) Less(i, j int) bool { return a[i].Tablet.Alias.Uid < a[j].Tablet.Alias.Uid }

// tabletMissing represents a missing/non-existent tablet for any metric.
const tabletMissing = -1

// These values represent the threshold for replication lag.
const lagThresholdDegraded = 60
const lagThresholdUnhealthy = 120

// These values represent the health of the tablet - 1 is healthy, 2 is degraded, 3 is unhealthy
const tabletHealthy = 0
const tabletDegraded = 1
const tabletUnhealthy = 2

// availableTabletTypes is an array of tabletTypes that are being considered to display on the heatmap.
// Note: this list must always be sorted by the order they should appear (i.e. MASTER first, then REPLICA, then RDONLY)
var availableTabletTypes = []topodatapb.TabletType{topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY}

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
}

type topologyInfo struct {
	Keyspaces   []string
	Cells       []string
	TabletTypes []string
}

func newTabletStatsCache() *tabletStatsCache {
	return &tabletStatsCache{
		statuses:        make(map[string]map[string]map[string]map[topodatapb.TabletType][]*discovery.TabletStats),
		statusesByAlias: make(map[string]*discovery.TabletStats),
	}
}

// StatsUpdate is part of the discovery.HealthCheckStatsListener interface.
// Upon receiving a new TabletStats, it updates the two maps in tablet_stats_cache.
func (c *tabletStatsCache) StatsUpdate(stats *discovery.TabletStats) {
	c.mu.Lock()
	defer c.mu.Unlock()

	keyspace := stats.Tablet.Keyspace
	shard := stats.Tablet.Shard
	cell := stats.Tablet.Alias.Cell
	tabletType := stats.Tablet.Type

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
		return
	}

	// Tablet already exists so just update it in the cache.
	*ts = *stats
}

// InStatsCache is part of the HealthCheckStatsListener interface
func (c *tabletStatsCache) InStatsCache(keyspace, shard string, tabletType topodatapb.TabletType, key string) bool {
	return false
}

func tabletToMapKey(stats *discovery.TabletStats) string {
	return stats.Tablet.Alias.String()
}

// remove takes in an array and returns it with the specified element removed
// (leaves the array unchanged if element isn't in the array).
func remove(tablets []*discovery.TabletStats, tabletAlias *topodatapb.TabletAlias) []*discovery.TabletStats {
	filteredTablets := tablets[:0]
	for _, tablet := range tablets {
		if !topoproto.TabletAliasEqual(tablet.Tablet.Alias, tabletAlias) {
			filteredTablets = append(filteredTablets, tablet)
		}
	}
	return filteredTablets
}

func (c *tabletStatsCache) topologyInfo(selectedKeyspace, selectedCell string) *topologyInfo {
	c.mu.Lock()
	defer c.mu.Unlock()

	return &topologyInfo{
		Keyspaces:   c.keyspacesLocked("all"),
		Cells:       c.cellsInTopology(selectedKeyspace),
		TabletTypes: makeStringTypeList(c.typesInTopology(selectedKeyspace, selectedCell)),
	}
}

func makeStringTypeList(types []topodatapb.TabletType) []string {
	var list []string
	for _, t := range types {
		list = append(list, t.String())
	}
	return list
}

// keyspacesLocked returns the keyspaces to be displayed in the heatmap based on the dropdown filters.
// It returns one keyspace if a specific one was chosen or returns all of them if 'all' is chosen.
// This method is used by heatmapData to traverse over desired keyspaces and
// topologyInfo to send all available options for the keyspace dropdown.
func (c *tabletStatsCache) keyspacesLocked(keyspace string) []string {
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

// cellsLocked returns the cells needed to be displayed in the heatmap based on the dropdown filters.
// returns one cell if a specific one was chosen or returns all of them if 'all' is chosen.
// This method is used by heatmapData to traverse over the desired cells.
func (c *tabletStatsCache) cellsLocked(keyspace, cell string) []string {
	if cell != "all" {
		return []string{cell}
	}
	return c.cellsInTopology(keyspace)
}

// tabletTypesLocked returns the tablet types needed to be displayed in the heatmap based on the dropdown filters.
// It returns tablet type if a specific one was chosen or returns all of them if 'all' is chosen for keyspace and/or cell.
// This method is used by heatmapData to traverse over the desired tablet types.
func (c *tabletStatsCache) tabletTypesLocked(keyspace, cell, tabletType string) []topodatapb.TabletType {
	if tabletType != "all" {
		tabletTypeObj, _ := topoproto.ParseTabletType(tabletType)
		return []topodatapb.TabletType{tabletTypeObj}
	}
	return c.typesInTopology(keyspace, cell)
}

// cellsInTopology returns all the cells in the given keyspace.
// If all keyspaces is chosen, it returns the cells from every keyspace.
// This method is used by topologyInfo to send all available options for the cell dropdown
func (c *tabletStatsCache) cellsInTopology(keyspace string) []string {
	keyspaces := c.keyspacesLocked(keyspace)
	cells := make(map[string]bool)
	// Going through all shards in each keyspace to get all existing cells
	for _, ks := range keyspaces {
		shardsPerKeyspace := c.statuses[ks]
		for s := range shardsPerKeyspace {
			cellsInKeyspace := c.statuses[ks][s]
			for cl := range cellsInKeyspace {
				cells[cl] = true
			}
		}
	}
	var cellList []string
	for cell := range cells {
		cellList = append(cellList, cell)
	}
	sort.Strings(cellList)
	return cellList
}

// typesInTopology returns all the types in the given keyspace and cell.
// If all keyspaces and cells is chosen, it returns the types from every cell in every keyspace.
// This method is used by topologyInfo to send all available options for the tablet type dropdown
func (c *tabletStatsCache) typesInTopology(keyspace, cell string) []topodatapb.TabletType {
	keyspaces := c.keyspacesLocked(keyspace)
	types := make(map[topodatapb.TabletType]bool)
	// Going through the shards in every cell in every keyspace to get existing tablet types
	for _, ks := range keyspaces {
		cellsPerKeyspace := c.cellsLocked(ks, cell)
		for _, cl := range cellsPerKeyspace {
			shardsPerKeyspace := c.statuses[ks]
			for s := range shardsPerKeyspace {
				typesPerShard := c.statuses[ks][s][cl]
				for t := range typesPerShard {
					types[t] = true
					if len(types) == len(availableTabletTypes) {
						break
					}
				}
			}
		}
	}
	typesList := sortTypes(types)
	return typesList
}

func sortTypes(types map[topodatapb.TabletType]bool) []topodatapb.TabletType {
	var listOfTypes []topodatapb.TabletType
	for _, tabType := range availableTabletTypes {
		if t, _ := types[tabType]; t {
			listOfTypes = append(listOfTypes, tabType)
		}
	}
	return listOfTypes
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
	case "health":
		metricFunc = health
	default:
		return nil, fmt.Errorf("invalid metric: %v Select 'lag', 'cpu', or 'qps'", selectedMetric)
	}

	// Get the proper data (unaggregated tablets or aggregated tablets by types)
	aggregated := false
	if selectedKeyspace == "all" && selectedTabletType == "all" {
		aggregated = true
	}

	keyspaces := c.keyspacesLocked(selectedKeyspace)
	var heatmaps []heatmap
	for _, keyspace := range keyspaces {
		var h heatmap
		h.ShardLabels = c.shards(keyspace)
		keyspaceLabelSpan := 0

		cells := c.cellsLocked(keyspace, selectedCell)
		// The loop goes through every outer label (in this case, cell).
		for _, cell := range cells {
			var cellData [][]float64
			var cellAliases [][]*topodatapb.TabletAlias
			var cellLabel yLabel

			if aggregated {
				cellData, cellAliases, cellLabel = c.aggregatedData(keyspace, cell, selectedTabletType, selectedMetric, metricFunc)
			} else {
				cellData, cellAliases, cellLabel = c.unaggregatedData(keyspace, cell, selectedTabletType, metricFunc)
			}

			if cellLabel.CellLabel.Rowspan > 0 {
				// Iterating over the rows of data for the current cell.
				for i := 0; i < len(cellData); i++ {
					// Adding the data in reverse to match the format that the plotly map takes in.
					h.Data = append([][]float64{cellData[i]}, h.Data...)
					if cellAliases != nil {
						h.Aliases = append([][]*topodatapb.TabletAlias{cellAliases[i]}, h.Aliases...)
					}
				}
				h.CellAndTypeLabels = append(h.CellAndTypeLabels, cellLabel)
			}
			keyspaceLabelSpan += cellLabel.CellLabel.Rowspan
		}

		// Setting the values for the yGridLines by going in reverse and subtracting 0.5 as an offset.
		sum := 0
		for c := len(h.CellAndTypeLabels) - 1; c >= 0; c-- {
			// If the current view is aggregated then we need to traverse the cell labels
			// to calculate the values for the grid line since that is the innermost label.
			// For example if h.CellAndTypeLabels =
			//   { CellLabel: {Name: 'cell1', Rowspan: 2}, TypeLabels: nil },
			//   { CellLabel: {Name: 'cell2', Rowspan: 3}, TypeLabels: nil },
			// then the resulting array will be [2.5, 4.5] which specifies the grid line indexes
			// starting from 0 which is at the bottom of the heatmap.
			if h.CellAndTypeLabels[c].TypeLabels == nil {
				sum += h.CellAndTypeLabels[c].CellLabel.Rowspan
				h.YGridLines = append(h.YGridLines, (float64(sum) - 0.5))
				continue
			}
			// Otherwise traverse the type labels because that is the innermost label.
			// For example if h.CellAndTypeLabels =
			//   { CellLabel: {Name: 'cell1', Rowspan: 3}, TypeLabels: [{Name: 'Master', Rowspan: 1},  {Name: 'Replica', Rowspan: 2}] },
			//   { CellLabel: {Name: 'cell2', Rowspan: 3}, TypeLabels: [{Name: 'Master', Rowspan: 1},  {Name: 'Replica', Rowspan: 2}] },
			// then the resulting array will be [1.5, 2.5, 4.5, 5.5] which specifies the grid line indexes
			// starting from 0 which is at the bottom of the heatmap.
			for t := len(h.CellAndTypeLabels[c].TypeLabels) - 1; t >= 0; t-- {
				sum += h.CellAndTypeLabels[c].TypeLabels[t].Rowspan
				h.YGridLines = append(h.YGridLines, (float64(sum) - 0.5))
			}
		}

		h.KeyspaceLabel = label{Name: keyspace, Rowspan: keyspaceLabelSpan}

		heatmaps = append(heatmaps, h)
	}

	return heatmaps, nil
}

func (c *tabletStatsCache) unaggregatedData(keyspace, cell, selectedType string, metricFunc func(stats *discovery.TabletStats) float64) ([][]float64, [][]*topodatapb.TabletAlias, yLabel) {
	// This loop goes through every nested label (in this case, tablet type).
	var cellData [][]float64
	var cellAliases [][]*topodatapb.TabletAlias
	var cellLabel yLabel
	cellLabelSpan := 0
	tabletTypes := c.tabletTypesLocked(keyspace, cell, selectedType)
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
		aliasRowsPerType := make([][]*topodatapb.TabletAlias, maxRowLength)
		for i := range dataRowsPerType {
			dataRowsPerType[i] = make([]float64, len(shards))
			aliasRowsPerType[i] = make([]*topodatapb.TabletAlias, len(shards))
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
			cellData = append(cellData, dataRowsPerType[i])
			cellAliases = append(cellAliases, aliasRowsPerType[i])
		}
	}

	cellLabel.CellLabel = label{Name: cell, Rowspan: cellLabelSpan}

	return cellData, cellAliases, cellLabel
}

// aggregatedData gets heatmapData by taking the average of the metric value of all tablets within the keyspace and cell of the
// specified type (or from all types if 'all' was selected).
func (c *tabletStatsCache) aggregatedData(keyspace, cell, selectedType, selectedMetric string, metricFunc func(stats *discovery.TabletStats) float64) ([][]float64, [][]*topodatapb.TabletAlias, yLabel) {
	shards := c.shards(keyspace)
	tabletTypes := c.tabletTypesLocked(keyspace, cell, selectedType)

	var cellData [][]float64
	dataRow := make([]float64, len(shards))
	// This loop goes through each shard in the (keyspace-cell) combination.
	for shardIndex, shard := range shards {
		var sum, count float64
		hasTablets := false
		unhealthyFound := false
		// Going through all the types of tablets and aggregating their information.
		for _, tabletType := range tabletTypes {
			tablets, ok := c.statuses[keyspace][shard][cell][tabletType]
			if !ok {
				continue
			}
			for _, tablet := range tablets {
				hasTablets = true
				// If even one tablet is unhealthy then the entire group becomes unhealthy.
				metricVal := metricFunc(tablet)
				if (selectedMetric == "health" && metricVal == tabletUnhealthy) ||
					(selectedMetric == "lag" && metricVal > lagThresholdUnhealthy) {
					sum = metricVal
					count = 1
					unhealthyFound = true
					break
				}
				sum += metricVal
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
	cellData = append(cellData, dataRow)
	cellLabel := yLabel{
		CellLabel: label{Name: cell, Rowspan: 1},
	}

	return cellData, nil, cellLabel
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

func health(stat *discovery.TabletStats) float64 {
	// The tablet is unhealthy if there is an health error.
	if stat.Stats.HealthError != "" {
		return tabletUnhealthy
	}

	// The tablet is healthy/degraded/unheathy depending on the lag.
	lag := stat.Stats.SecondsBehindMaster
	switch {
	case lag >= lagThresholdUnhealthy:
		return tabletUnhealthy
	case lag >= lagThresholdDegraded:
		return tabletDegraded
	}

	// The tablet is degraded if there was an error previously.
	if stat.LastError != nil {
		return tabletDegraded
	}

	// The tablet is healthy or degraded based on serving status.
	if !stat.Serving {
		return tabletDegraded
	}

	// All else is ok so tablet is healthy.
	return tabletHealthy
}

func replicationLag(stat *discovery.TabletStats) float64 {
	return float64(stat.Stats.SecondsBehindMaster)
}

func qps(stat *discovery.TabletStats) float64 {
	return stat.Stats.Qps
}

// compile-time interface check
var _ discovery.HealthCheckStatsListener = (*tabletStatsCache)(nil)
