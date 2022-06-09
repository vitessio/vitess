/*
Copyright 2022 The Vitess Authors.

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

package vtctld

import (
	"fmt"
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/discovery"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

const (
	// tabletMissing represents a missing/non-existent tablet for any metric.
	tabletMissing = -1
	// These values represent the threshold for replication lag.
	lagThresholdDegraded  = 60
	lagThresholdUnhealthy = 120
	// These values represent the health of the tablet - 1 is healthy, 2 is degraded, 3 is unhealthy
	tabletHealthy   = 0
	tabletDegraded  = 1
	tabletUnhealthy = 2
)

type (
	// yLabel is used to keep track of the cell and type labels of the heatmap.
	yLabel struct {
		CellLabel  label
		TypeLabels []label
	}

	// label is used to keep track of one label of a heatmap and how many rows it should span.
	label struct {
		Name    string
		Rowspan int
	}

	// heatmap stores all the needed info to construct the heatmap.
	heatmap struct {
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

	topologyInfo struct {
		Keyspaces   []string
		Cells       []string
		TabletTypes []string
	}
)

// availableTabletTypes is an array of tabletTypes that are being considered to display on the heatmap.
// Note: this list must always be sorted by the order they should appear (i.e. PRIMARY first, then REPLICA, then RDONLY)
var availableTabletTypes = []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY}

func makeStringTypeList(types []topodatapb.TabletType) []string {
	var list []string
	for _, t := range types {
		list = append(list, t.String())
	}
	return list
}

func sortTypes(types map[topodatapb.TabletType]bool) []topodatapb.TabletType {
	var listOfTypes []topodatapb.TabletType
	for _, tabType := range availableTabletTypes {
		if t := types[tabType]; t {
			listOfTypes = append(listOfTypes, tabType)
		}
	}
	return listOfTypes
}

func health(stat *discovery.TabletHealth) float64 {
	// The tablet is unhealthy if there is an health error.
	if stat.Stats.HealthError != "" {
		return tabletUnhealthy
	}

	// The tablet is healthy/degraded/unheathy depending on the lag.
	lag := stat.Stats.ReplicationLagSeconds
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

func replicationLag(stat *discovery.TabletHealth) float64 {
	return float64(stat.Stats.ReplicationLagSeconds)
}

func qps(stat *discovery.TabletHealth) float64 {
	return stat.Stats.Qps
}

func getTabletHealthWithCellFilter(hc discovery.HealthCheck, ks, shard, cell string, tabletType topodatapb.TabletType) []*discovery.TabletHealth {
	tabletTypeStr := topoproto.TabletTypeLString(tabletType)
	m := hc.CacheStatusMap()
	key := fmt.Sprintf("%v.%v.%v.%v", cell, ks, shard, strings.ToUpper(tabletTypeStr))
	if _, ok := m[key]; !ok {
		return nil
	}
	return m[key].TabletsStats
}

func getShardInKeyspace(hc discovery.HealthCheck, ks string) []string {
	shards := []string{}
	shardsMap := map[string]bool{}
	cache := hc.CacheStatus()
	for _, status := range cache {
		if status.Target.Keyspace != ks {
			continue
		}
		if ok := shardsMap[status.Target.Shard]; !ok {
			shardsMap[status.Target.Shard] = true
			shards = append(shards, status.Target.Shard)
		}
	}
	return shards
}

func getTabletTypesForKeyspaceShardAndCell(hc discovery.HealthCheck, ks, shard, cell string) []topodatapb.TabletType {
	tabletTypes := []topodatapb.TabletType{}
	tabletTypeMap := map[topodatapb.TabletType]bool{}
	cache := hc.CacheStatus()
	for _, status := range cache {
		if status.Target.Keyspace != ks || status.Cell != cell || status.Target.Shard != shard {
			continue
		}
		if ok := tabletTypeMap[status.Target.TabletType]; !ok {
			tabletTypeMap[status.Target.TabletType] = true
			tabletTypes = append(tabletTypes, status.Target.TabletType)
		}
	}
	return tabletTypes
}

func getTopologyInfo(healthcheck discovery.HealthCheck, selectedKeyspace, selectedCell string) *topologyInfo {
	return &topologyInfo{
		Keyspaces:   keyspacesLocked(healthcheck, "all"),
		Cells:       cellsInTopology(healthcheck, selectedKeyspace),
		TabletTypes: makeStringTypeList(typesInTopology(healthcheck, selectedKeyspace, selectedCell)),
	}
}

// keyspacesLocked returns the keyspaces to be displayed in the heatmap based on the dropdown filters.
// It returns one keyspace if a specific one was chosen or returns all of them if 'all' is chosen.
// This method is used by heatmapData to traverse over desired keyspaces and
// topologyInfo to send all available options for the keyspace dropdown.
func keyspacesLocked(healthcheck discovery.HealthCheck, keyspace string) []string {
	if keyspace != "all" {
		return []string{keyspace}
	}
	seenKs := map[string]bool{}
	keyspaces := []string{}
	cache := healthcheck.CacheStatus()
	for _, status := range cache {
		if _, ok := seenKs[status.Target.Keyspace]; !ok {
			seenKs[status.Target.Keyspace] = true
			keyspaces = append(keyspaces, status.Target.Keyspace)
		}
	}
	sort.Strings(keyspaces)
	return keyspaces
}

func getShardsForKeyspace(healthcheck discovery.HealthCheck, keyspace string) []string {
	seenShards := map[string]bool{}
	shards := []string{}
	cache := healthcheck.CacheStatus()
	for _, status := range cache {
		if status.Target.Keyspace != keyspace {
			continue
		}
		if _, ok := seenShards[status.Target.Shard]; !ok {
			seenShards[status.Target.Shard] = true
			shards = append(shards, status.Target.Shard)
		}
	}
	sort.Strings(shards)
	return shards
}

// cellsInTopology returns all the cells in the given keyspace.
// If all keyspaces is chosen, it returns the cells from every keyspace.
// This method is used by topologyInfo to send all available options for the cell dropdown
func cellsInTopology(healthcheck discovery.HealthCheck, keyspace string) []string {
	kss := []string{keyspace}
	if keyspace == "all" {
		kss = keyspacesLocked(healthcheck, keyspace)
	}
	cells := map[string]bool{}
	cache := healthcheck.CacheStatus()
	for _, status := range cache {
		found := false
		for _, ks := range kss {
			if status.Target.Keyspace == ks {
				found = true
				break
			}
		}
		if !found {
			continue
		}
		if _, ok := cells[status.Cell]; !ok {
			cells[status.Cell] = true
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
func typesInTopology(healthcheck discovery.HealthCheck, keyspace, cell string) []topodatapb.TabletType {
	keyspaces := keyspacesLocked(healthcheck, keyspace)
	types := make(map[topodatapb.TabletType]bool)
	// Going through the shards in every cell in every keyspace to get existing tablet types
	for _, ks := range keyspaces {
		cellsPerKeyspace := cellsLocked(healthcheck, ks, cell)
		for _, cl := range cellsPerKeyspace {
			shardsPerKeyspace := getShardInKeyspace(healthcheck, ks)
			for _, s := range shardsPerKeyspace {
				typesPerShard := getTabletTypesForKeyspaceShardAndCell(healthcheck, ks, s, cl)
				for _, t := range typesPerShard {
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

// tabletTypesLocked returns the tablet types needed to be displayed in the heatmap based on the dropdown filters.
// It returns tablet type if a specific one was chosen or returns all of them if 'all' is chosen for keyspace and/or cell.
// This method is used by heatmapData to traverse over the desired tablet types.
func tabletTypesLocked(healthcheck discovery.HealthCheck, keyspace, cell, tabletType string) []topodatapb.TabletType {
	if tabletType != "all" {
		tabletTypeObj, _ := topoproto.ParseTabletType(tabletType)
		return []topodatapb.TabletType{tabletTypeObj}
	}
	return typesInTopology(healthcheck, keyspace, cell)
}

// cellsLocked returns the cells needed to be displayed in the heatmap based on the dropdown filters.
// returns one cell if a specific one was chosen or returns all of them if 'all' is chosen.
// This method is used by heatmapData to traverse over the desired cells.
func cellsLocked(healthcheck discovery.HealthCheck, keyspace, cell string) []string {
	if cell != "all" {
		return []string{cell}
	}
	return cellsInTopology(healthcheck, keyspace)
}

// aggregatedData gets heatmapData by taking the average of the metric value of all tablets within the keyspace and cell of the
// specified type (or from all types if 'all' was selected).
func aggregatedData(healthcheck discovery.HealthCheck, keyspace, cell, selectedType, selectedMetric string, metricFunc func(stats *discovery.TabletHealth) float64) ([][]float64, [][]*topodatapb.TabletAlias, yLabel) {
	shards := getShardsForKeyspace(healthcheck, keyspace)
	tabletTypes := tabletTypesLocked(healthcheck, keyspace, cell, selectedType)

	var cellData [][]float64
	dataRow := make([]float64, len(shards))
	// This loop goes through each shard in the (keyspace-cell) combination.
	for shardIndex, shard := range shards {
		var sum, count float64
		hasTablets := false
		unhealthyFound := false
		// Going through all the types of tablets and aggregating their information.
		for _, tabletType := range tabletTypes {
			tablets := getTabletHealthWithCellFilter(healthcheck, keyspace, shard, cell, tabletType)
			if len(tablets) == 0 {
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
			if unhealthyFound {
				break
			}
		}
		if hasTablets {
			dataRow[shardIndex] = sum / count
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

func unaggregatedData(healthcheck discovery.HealthCheck, keyspace, cell, selectedType string, metricFunc func(stats *discovery.TabletHealth) float64) ([][]float64, [][]*topodatapb.TabletAlias, yLabel) {
	// This loop goes through every nested label (in this case, tablet type).
	var cellData [][]float64
	var cellAliases [][]*topodatapb.TabletAlias
	var cellLabel yLabel
	cellLabelSpan := 0
	tabletTypes := tabletTypesLocked(healthcheck, keyspace, cell, selectedType)
	shards := getShardsForKeyspace(healthcheck, keyspace)
	for _, tabletType := range tabletTypes {
		maxRowLength := 0

		// The loop calculates the maximum number of rows needed.
		for _, shard := range shards {
			tabletsCount := len(getTabletHealthWithCellFilter(healthcheck, keyspace, shard, cell, tabletType))
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
				filteredHealthData := getTabletHealthWithCellFilter(healthcheck, keyspace, shard, cell, tabletType)
				if tabletIndex < len(filteredHealthData) {
					dataRowsPerType[tabletIndex][shardIndex] = metricFunc(filteredHealthData[tabletIndex])
					aliasRowsPerType[tabletIndex][shardIndex] = filteredHealthData[tabletIndex].Tablet.Alias
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

// heatmapData returns a 2D array of data (based on the specified metric) as well as the labels for the heatmap.
func heatmapData(healthcheck discovery.HealthCheck, selectedKeyspace, selectedCell, selectedTabletType, selectedMetric string) ([]heatmap, error) {
	// Get the metric data.
	var metricFunc func(stats *discovery.TabletHealth) float64
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

	keyspaces := keyspacesLocked(healthcheck, selectedKeyspace)
	var heatmaps []heatmap
	for _, keyspace := range keyspaces {
		var h heatmap
		h.ShardLabels = getShardsForKeyspace(healthcheck, keyspace)
		keyspaceLabelSpan := 0

		cells := cellsLocked(healthcheck, keyspace, selectedCell)
		// The loop goes through every outer label (in this case, cell).
		for _, cell := range cells {
			var cellData [][]float64
			var cellAliases [][]*topodatapb.TabletAlias
			var cellLabel yLabel

			if aggregated {
				cellData, cellAliases, cellLabel = aggregatedData(healthcheck, keyspace, cell, selectedTabletType, selectedMetric, metricFunc)
			} else {
				cellData, cellAliases, cellLabel = unaggregatedData(healthcheck, keyspace, cell, selectedTabletType, metricFunc)
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
				h.YGridLines = append(h.YGridLines, float64(sum)-0.5)
				continue
			}
			// Otherwise traverse the type labels because that is the innermost label.
			// For example if h.CellAndTypeLabels =
			//   { CellLabel: {Name: 'cell1', Rowspan: 3}, TypeLabels: [{Name: 'Primary', Rowspan: 1},  {Name: 'Replica', Rowspan: 2}] },
			//   { CellLabel: {Name: 'cell2', Rowspan: 3}, TypeLabels: [{Name: 'Primary', Rowspan: 1},  {Name: 'Replica', Rowspan: 2}] },
			// then the resulting array will be [1.5, 2.5, 4.5, 5.5] which specifies the grid line indexes
			// starting from 0 which is at the bottom of the heatmap.
			for t := len(h.CellAndTypeLabels[c].TypeLabels) - 1; t >= 0; t-- {
				sum += h.CellAndTypeLabels[c].TypeLabels[t].Rowspan
				h.YGridLines = append(h.YGridLines, float64(sum)-0.5)
			}
		}

		h.KeyspaceLabel = label{Name: keyspace, Rowspan: keyspaceLabelSpan}

		heatmaps = append(heatmaps, h)
	}

	return heatmaps, nil
}
