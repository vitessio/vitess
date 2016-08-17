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

// This value represents a missing/non-existent tablet for the healthy metric.
const tabletMissingHealthy = 0

// This value represents a missing/non-existent tablet for the other metrics.
const tabletMissingOther = -1

// This value is needed to indicate when there's data but no labels are needed.
const empty = "EMPTY"

// These values represent the two thresholds for qps and replication lag.
const qpsThresholdDegraded = 1000
const qpsThresholdUnhealthy = 2000
const lagThresholdDegraded = 60
const lagThresholdUnhealthy = 120

// These values represent the health of the tablet - 1 is healthy, 2 is degraded, 3 is unhealthy
const tabletHealthy = 1
const tabletDegraded = 2
const tabletUnhealthy = 3

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

func (c *tabletStatsCache) keyspaces() []string {
	var keyspaces []string
	for ks := range c.statuses {
		keyspaces = append(keyspaces, ks)
	}
	sort.Strings(keyspaces)
	return keyspaces
}

func (c *tabletStatsCache) cells() []string {
	var cells []string
	for cell := range c.tabletCountsByCell {
		cells = append(cells, cell)
	}
	sort.Strings(cells)
	return cells
}

func tabletTypes() []string {
	return []string{topodatapb.TabletType_MASTER.String(), topodatapb.TabletType_REPLICA.String(), topodatapb.TabletType_RDONLY.String()}
}

func (c *tabletStatsCache) shards() []string {
	var keyspace string
	for ks := range c.statuses {
		keyspace = ks
		break
	}
	var shards []string
	for s := range c.statuses[keyspace] {
		shards = append(shards, s)
	}
	sort.Strings(shards)
	return shards
}

func (c *tabletStatsCache) tablets(outerType, innerType, outer, inner, keyspace, cell, shard, tabletType string) []*discovery.TabletStats {
	var tabletsPerGroup []*discovery.TabletStats
	if outerType == "keyspace" {
		if innerType == "cell" {
			tabletTypeObj, _ := topoproto.ParseTabletType(tabletType)
			tabletsPerGroup = c.statuses[outer][shard][inner][tabletTypeObj]
		} else if innerType == "tabletType" {
			tabletTypeObj, _ := topoproto.ParseTabletType(inner)
			tabletsPerGroup = c.statuses[outer][shard][cell][tabletTypeObj]
		} else if innerType == "" {
			tabletTypeObj, _ := topoproto.ParseTabletType(tabletType)
			tabletsPerGroup = c.statuses[outer][shard][cell][tabletTypeObj]
		}
	} else if outerType == "cell" {
		if innerType == "tabletType" {
			tabletTypeObj, _ := topoproto.ParseTabletType(inner)
			tabletsPerGroup = c.statuses[keyspace][shard][outer][tabletTypeObj]
		} else if innerType == "" {
			tabletTypeObj, _ := topoproto.ParseTabletType(tabletType)
			tabletsPerGroup = c.statuses[keyspace][shard][outer][tabletTypeObj]
		}
	} else if outerType == "tabletType" {
		tabletTypeObj, _ := topoproto.ParseTabletType(outer)
		tabletsPerGroup = c.statuses[keyspace][shard][cell][tabletTypeObj]
	} else {
		tabletTypeObj, _ := topoproto.ParseTabletType(tabletType)
		tabletsPerGroup = c.statuses[keyspace][shard][cell][tabletTypeObj]
	}
	return tabletsPerGroup
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
	case "qps":
		metricFunc = qps
	case "healthy":
		metricFunc = healthy
	default:
		return heatmap{}, fmt.Errorf("invalid metric: %v Select 'lag', 'cpu', or 'qps'", metric)
	}

	var outerType string
	var innerType string
	var outerLabels []string
	var innerLabels []string
	var shards []string

	// Setting the correct outer and inner labels depending on the parameters
	if keyspace == "all" {
		outerType = "keyspace"
		outerLabels = c.keyspaces()
	}
	if cell == "all" {
		if outerType == "" {
			outerType = "cell"
			outerLabels = c.cells()
		} else {
			innerType = "cell"
			innerLabels = c.cells()
		}
	}
	if tabletType == "all" {
		if outerType == "" {
			outerType = "tabletType"
			outerLabels = tabletTypes()
		} else {
			innerType = "tabletType"
			innerLabels = tabletTypes()
		}
	}

	// tabletMissing is the correct value for a missing/non-existent tablet based on the metric
	var tabletMissing float64 = tabletMissingOther
	if metric == "healthy" {
		tabletMissing = tabletMissingHealthy
	}

	// outerLabels and innerLabels must not be empty to gaurentee the execution of the loop at least once.
	if len(outerLabels) == 0 {
		outerLabels = []string{empty}
	}
	if len(innerLabels) == 0 {
		innerLabels = []string{empty}
	}

	shards = c.shards()

	var heatmapData [][]float64
	var heatmapTabletAliases [][]*topodata.TabletAlias
	var heatmapLabels []yLabel

	for _, outer := range outerLabels {
		labelsPerGroup := yLabel{}
		labelSpan := 0
		for _, inner := range innerLabels {
			maxRowLength := 0

			for _, shard := range shards {
				// tabletCount is the length of the tablets in this group.
				tabletsPerGroup := c.tablets(outerType, innerType, outer, inner, keyspace, cell, shard, tabletType)
				tabletCount := len(tabletsPerGroup)
				if maxRowLength < tabletCount {
					maxRowLength = tabletCount
				}
			}

			// The two arrays hold the tablet data and alias data respectively for a (outer, inner) combination
			dataPerGroup := make([][]float64, maxRowLength)
			aliasPerGroup := make([][]*topodata.TabletAlias, maxRowLength)
			for i := range dataPerGroup {
				dataPerGroup[i] = make([]float64, len(shards))
				aliasPerGroup[i] = make([]*topodata.TabletAlias, len(shards))
			}

			for shardIndex, shard := range shards {
				for tabletIndex := 0; tabletIndex < maxRowLength; tabletIndex++ {
					tabletsPerGroup := c.tablets(outerType, innerType, outer, inner, keyspace, cell, shard, tabletType)
					if tabletIndex < len(tabletsPerGroup) {
						dataPerGroup[tabletIndex][shardIndex] = metricFunc(tabletsPerGroup[tabletIndex])
						aliasPerGroup[tabletIndex][shardIndex] = tabletsPerGroup[tabletIndex].Tablet.Alias
					} else {
						// If the key doesn't exist then the tablet must not exist so that data is set to -1 (tabletMissing).
						dataPerGroup[tabletIndex][shardIndex] = tabletMissing
						aliasPerGroup[tabletIndex][shardIndex] = nil
					}

					// Adding the labels for the yaxis only if it is the first column and first row of the group to avoid duplication.
					if shardIndex == 0 && tabletIndex == 0 {
						if inner != empty {
							tempLabel := label{
								Name:    inner,
								Rowspan: maxRowLength,
							}
							labelsPerGroup.NestedLabels = append(labelsPerGroup.NestedLabels, tempLabel)
						}
						labelSpan += maxRowLength
					}
				}
			}

			// The loop adds each group to heatmapData in reverse order following the format of the heatmap.
			for i := 0; i < len(dataPerGroup); i++ {
				heatmapData = append([][]float64{dataPerGroup[i]}, heatmapData...)
				heatmapTabletAliases = append([][]*topodata.TabletAlias{aliasPerGroup[i]}, heatmapTabletAliases...)
			}
		}
		if outer != empty && labelSpan > 0 {
			labelsPerGroup.Label = label{
				Name:    outer,
				Rowspan: labelSpan,
			}
			heatmapLabels = append(heatmapLabels, labelsPerGroup)
		}
	}

	return heatmap{
		Data:    heatmapData,
		Labels:  heatmapLabels,
		Aliases: heatmapTabletAliases,
	}, nil
}

func (c *tabletStatsCache) aggregatedHeatmapData(metric string) (heatmap, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	var metricFunc func(stats *discovery.TabletStats) float64
	switch metric {
	case "lag":
		metricFunc = replicationLag
	case "qps":
		metricFunc = qps
	case "healthy":
		metricFunc = healthy
	default:
		return heatmap{}, fmt.Errorf("invalid metric: %v Select 'lag', 'cpu', or 'qps'", metric)
	}

	var tabletMissing float64 = tabletMissingOther
	if metric == "healthy" {
		tabletMissing = tabletMissingHealthy
	}

	keyspaces := c.keyspaces()
	cells := c.cells()
	shards := c.shards()
	tabletTypes := tabletTypes()

	var heatmapData [][]float64
	var heatmapLabels []yLabel
	for _, keyspace := range keyspaces {
		labelPerGroup := yLabel{}
		labelSpan := 0
		for _, cell := range cells {
			dataRow := make([]float64, len(shards))
			for shardIndex, shard := range shards {
				var sum, count float64 = 0, 0
				hasTablets := false
				unhealthyFound := false
				for _, tabletType := range tabletTypes {
					tabletTypeObj, _ := topoproto.ParseTabletType(tabletType)
					tablets, ok := c.statuses[keyspace][shard][cell][tabletTypeObj]
					if !ok {
						continue
					}
					for _, tablet := range tablets {
						hasTablets = true
						// If even one tablet is unhealthy then the entire groups becomes unhealthy.
						if metricFunc(tablet) == tabletUnhealthy {
							sum = tabletUnhealthy
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
			heatmapData = append([][]float64{dataRow}, heatmapData...)
			labelSpan++
		}
		tempLabel := label{
			Name:    keyspace,
			Rowspan: labelSpan,
		}
		labelPerGroup.Label = tempLabel
		heatmapLabels = append(heatmapLabels, labelPerGroup)
	}

	return heatmap{
		Data:    heatmapData,
		Labels:  heatmapLabels,
		Aliases: nil,
	}, nil
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
