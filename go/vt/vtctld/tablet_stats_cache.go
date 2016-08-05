package vtctld

import (
	"sort"
	"sync"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

type yLabel struct {
	label        string
	nestedLabels []string
}

type ByTabletUid []*discovery.TabletStats

func (a ByTabletUid) Len() int           { return len(a) }
func (a ByTabletUid) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTabletUid) Less(i, j int) bool { return a[i].Tablet.Alias.Uid < a[j].Tablet.Alias.Uid }

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
	statuses map[string]map[string]map[string]map[string][]*discovery.TabletStats

	// The first key is the string representation of the tablet alias
	copyStatuses map[string]*discovery.TabletStats
}

// StatsUpdate is part of the discovery.HealthCheckStatsListener interface.
func (t *tabletStatsCache) StatsUpdate(stats *discovery.TabletStats) {

	t.mu.Lock()
	defer t.mu.Unlock()
	cell := stats.Tablet.Alias.Cell
	keyspace := stats.Target.Keyspace
	tabletType := stats.Target.TabletType.String()
	shard := stats.Target.Shard

	shards, ok := t.statuses[keyspace]
	if !ok {
		shards = make(map[string]map[string]map[string][]*discovery.TabletStats)
		t.statuses[keyspace] = shards
	}

	cells, ok := t.statuses[keyspace][shard]
	if !ok {
		cells = make(map[string]map[string][]*discovery.TabletStats)
		t.statuses[keyspace][shard] = cells
	}

	types, ok := t.statuses[keyspace][shard][cell]
	if !ok {
		types = make(map[string][]*discovery.TabletStats)
		t.statuses[keyspace][shard][cell] = types
	}

	tablets, ok := t.statuses[keyspace][shard][cell][tabletType]
	if !ok {
		if !stats.Up {
			// We're told a tablet is gone, and we don't have
			// a map for it anyway, nothing to do.
			return
		}
		tablets = make([]*discovery.TabletStats, 0)
		t.statuses[keyspace][shard][cell][tabletType] = tablets
	}

	if stats.Up {
		t.statuses[keyspace][shard][cell][tabletType] = remove(t.statuses[keyspace][shard][cell][tabletType], stats.Tablet.Alias)
		t.statuses[keyspace][shard][cell][tabletType] = append(t.statuses[keyspace][shard][cell][tabletType], stats)
		sort.Sort(ByTabletUid(t.statuses[keyspace][shard][cell][tabletType]))
	} else {
		t.statuses[keyspace][shard][cell][tabletType] = remove(t.statuses[keyspace][shard][cell][tabletType], stats.Tablet.Alias)
	}

	t.copyStatuses[stats.Tablet.Alias.String()] = stats
}

// heatmapData returns a 2D array of data (based on the specified metric) as well as the labels for the heatmap.
func (tsc *tabletStatsCache) heatmapData(metric, keyspace, cell, tabletType string) ([][]float64, []yLabel) {
	tsc.mu.Lock()
	defer tsc.mu.Unlock()

	// knownCells is an array of cells from the current tablet stats cache.
	knownCells := make([]string, 0)
	for _, cellMap := range tsc.statuses[keyspace] {
		for c := range cellMap {
			if !contains(knownCells, c) {
				knownCells = append(knownCells, c)
			}
		}
	}
	sort.Strings(knownCells)

	// knownShards is an array of shards from the current tablet stats cache.
	knownShards := make([]string, 0)
	for s := range tsc.statuses[keyspace] {
		knownShards = append(knownShards, s)
	}
	sort.Strings(knownShards)

	// knownTypes is an array of tablet types present in the current tablet stats cache.
	knownTypes := []string{topodatapb.TabletType_MASTER.String(), topodatapb.TabletType_REPLICA.String(), topodatapb.TabletType_RDONLY.String()}

	heatmapData := make([][]float64, 0)
	yLabels := make([]yLabel, 0)
	// The loop goes through every outer label (in this case, cell).
	for _, cell := range knownCells {
		tempyLabel := yLabel{
			label:        cell,
			nestedLabels: make([]string, 0),
		}
		// This loop goes through every nested label (in this case, tablet type).
		for _, tabletType := range knownTypes {
			maxRowLength := 0

			// The loop calculates the maximum number of rows needed.
			for _, shard := range knownShards {
				currLength := len(tsc.statuses[keyspace][shard][cell][tabletType])
				if maxRowLength < currLength {
					maxRowLength = currLength
				}
			}

			// currentRows is 2D array that will hold the tablets of one (cell, type) combination.
			currentRows := make([][]float64, maxRowLength)
			for i := range currentRows {
				currentRows[i] = make([]float64, len(knownShards))
			}

			// Filling in the 2D array with tablet data by columns.
			firstRow := true
			firstShard := true
			shardIndex := 0
			for _, shard := range knownShards {
				for i := 0; i < maxRowLength; i++ {
					// If the key doesn't exist then the tablet must not exist so that data is set to -1.
					if i < len(tsc.statuses[keyspace][shard][cell][tabletType]) {
						currentRows[i][shardIndex] = float64(tsc.statuses[keyspace][shard][cell][tabletType][i].Stats.SecondsBehindMaster)
					} else {
						currentRows[i][shardIndex] = -1
					}

					// Adding the labels for the yaxis only if it is the first column.
					if firstShard == true && firstRow == true {
						tempyLabel.nestedLabels = append(tempyLabel.nestedLabels, tabletType)
						firstRow = false
					} else if firstShard == true {
						tempyLabel.nestedLabels = append(tempyLabel.nestedLabels, "")
					}
				}
				shardIndex++
				firstShard = false
			}

			for _, row := range currentRows {
				heatmapData = append(heatmapData, row)
			}
		}
		yLabels = append(yLabels, tempyLabel)
	}

	return heatmapData, yLabels
}

// remove takes in an array and returns it with the specified element removed
// (leaves the array unchanged if element isn't in the array).
func remove(tabletArray []*discovery.TabletStats, tabletAlias *topodata.TabletAlias) []*discovery.TabletStats {
	for index, tablet := range tabletArray {
		if tablet == nil || topoproto.TabletAliasEqual(tablet.Tablet.Alias, tabletAlias) {
			if len(tabletArray) > index+1 {
				tabletArray = append(tabletArray[0:index], tabletArray[index+1])
			} else {
				tabletArray = append(tabletArray[0:index])
			}
			return tabletArray
		}
	}
	return tabletArray
}

// contains checks whether a specific string is already in an array of strings.
func contains(strArray []string, wantStr string) bool {
	for _, str := range strArray {
		if str == wantStr {
			return true
		}
	}
	return false
}

// compile-time interface check
var _ discovery.HealthCheckStatsListener = (*tabletStatsCache)(nil)
