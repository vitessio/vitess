/*
Copyright 2019 The Vitess Authors.

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

package discovery

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type TabletPickerCellPreference int

const (
	// PreferLocalWithAlias gives preference to the local cell first, then specified cells, if any.
	// This is the default when no other option is provided.
	TabletPickerCellPreference_PreferLocalWithAlias TabletPickerCellPreference = iota
	// OnlySpecified only picks tablets from the list of cells given.
	TabletPickerCellPreference_OnlySpecified
)

type TabletPickerTabletOrder int

const (
	// All provided tablet types are given equal priority. This is the default.
	TabletPickerTabletOrder_Any TabletPickerTabletOrder = iota
	// Provided tablet types are expected to be prioritized in the given order.
	TabletPickerTabletOrder_InOrder
	InOrderHint = "in_order:"
)

var (
	tabletPickerRetryDelay   = 30 * time.Second
	muTabletPickerRetryDelay sync.Mutex
	globalTPStats            *tabletPickerStats

	tabletPickerCellPreferenceMap = map[string]TabletPickerCellPreference{
		"preferlocalwithalias": TabletPickerCellPreference_PreferLocalWithAlias,
		"onlyspecified":        TabletPickerCellPreference_OnlySpecified,
	}

	tabletPickerTabletOrderMap = map[string]TabletPickerTabletOrder{
		"any":     TabletPickerTabletOrder_Any,
		"inorder": TabletPickerTabletOrder_InOrder,
	}
)

// GetTabletPickerRetryDelay synchronizes changes to tabletPickerRetryDelay. Used in tests only at the moment
func GetTabletPickerRetryDelay() time.Duration {
	muTabletPickerRetryDelay.Lock()
	defer muTabletPickerRetryDelay.Unlock()
	return tabletPickerRetryDelay
}

// SetTabletPickerRetryDelay synchronizes reads for tabletPickerRetryDelay. Used in tests only at the moment
func SetTabletPickerRetryDelay(delay time.Duration) {
	muTabletPickerRetryDelay.Lock()
	defer muTabletPickerRetryDelay.Unlock()
	tabletPickerRetryDelay = delay
}

type TabletPickerOptions struct {
	CellPreference           string
	TabletOrder              string
	IncludeNonServingTablets bool
}

func parseTabletPickerCellPreferenceString(str string) (TabletPickerCellPreference, error) {
	// return default if blank
	if str == "" {
		return TabletPickerCellPreference_PreferLocalWithAlias, nil
	}

	if c, ok := tabletPickerCellPreferenceMap[strings.ToLower(str)]; ok {
		return c, nil
	}

	return -1, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid cell preference: %v", str)
}

func parseTabletPickerTabletOrderString(str string) (TabletPickerTabletOrder, error) {
	// return default if blank
	if str == "" {
		return TabletPickerTabletOrder_Any, nil
	}

	if o, ok := tabletPickerTabletOrderMap[strings.ToLower(str)]; ok {
		return o, nil
	}

	return -1, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid tablet order type: %v", str)
}

type localCellInfo struct {
	localCell    string
	cellsInAlias map[string]string
}

// TabletPicker gives a simplified API for picking tablets.
type TabletPicker struct {
	ts            *topo.Server
	cells         []string
	keyspace      string
	shard         string
	tabletTypes   []topodatapb.TabletType
	inOrder       bool
	cellPref      TabletPickerCellPreference
	localCellInfo localCellInfo
	// This map is keyed on the results of TabletAlias.String().
	ignoreTablets map[string]struct{}
	options       TabletPickerOptions
}

// NewTabletPicker returns a TabletPicker.
func NewTabletPicker(
	ctx context.Context,
	ts *topo.Server,
	cells []string,
	localCell, keyspace, shard, tabletTypesStr string,
	options TabletPickerOptions,
	ignoreTablets ...*topodatapb.TabletAlias,
) (*TabletPicker, error) {
	// Keep inOrder parsing here for backward compatability until TabletPickerTabletOrder is fully adopted.
	if tabletTypesStr == "" {
		tabletTypesStr = "replica,rdonly,primary"
	}
	tabletTypes, inOrder, err := ParseTabletTypesAndOrder(tabletTypesStr)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "failed to parse list of tablet types: %v", tabletTypesStr)
	}
	var missingFields []string
	if keyspace == "" {
		missingFields = append(missingFields, "Keyspace")
	}
	if shard == "" {
		missingFields = append(missingFields, "Shard")
	}
	if len(cells) == 0 {
		missingFields = append(missingFields, "Cells")
	}
	if len(missingFields) > 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("Missing required field(s) for tablet picker: %s", strings.Join(missingFields, ", ")))
	}

	// Resolve tablet picker options
	cellPref, err := parseTabletPickerCellPreferenceString(options.CellPreference)
	if err != nil {
		return nil, err
	}

	// For backward compatibility only parse the options for tablet ordering
	// if the in_order hint wasn't already specified. Otherwise it could be overridden.
	// We can remove this check once the in_order hint is deprecated.
	if !inOrder {
		order, err := parseTabletPickerTabletOrderString(options.TabletOrder)
		if err != nil {
			return nil, err
		}
		switch order {
		case TabletPickerTabletOrder_Any:
			inOrder = false
		case TabletPickerTabletOrder_InOrder:
			inOrder = true
		}
	}

	aliasCellMap := make(map[string]string)
	if cellPref == TabletPickerCellPreference_PreferLocalWithAlias {
		if localCell == "" {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot have local cell preference without local cell")
		}

		// Add local cell to the list of cells for tablet picking.
		// This will be de-duped later if the local cell already exists in the original list - see: dedupeCells()
		cells = append(cells, localCell)
		aliasName := topo.GetAliasByCell(ctx, ts, localCell)

		// If an alias exists
		if aliasName != localCell {
			alias, err := ts.GetCellsAlias(ctx, aliasName, false)
			if err != nil {
				return nil, vterrors.Wrap(err, "error fetching local cell alias")
			}

			// Add the aliasName to the list of cells for tablet picking.
			cells = append(cells, aliasName)

			// Create a map of the cells in the alias to make lookup faster later when we're giving preference to these.
			// see prioritizeTablets()
			for _, c := range alias.Cells {
				aliasCellMap[c] = c
			}
		}
	}

	tp := &TabletPicker{
		ts:            ts,
		cells:         dedupeCells(cells),
		localCellInfo: localCellInfo{localCell: localCell, cellsInAlias: aliasCellMap},
		keyspace:      keyspace,
		shard:         shard,
		tabletTypes:   tabletTypes,
		inOrder:       inOrder,
		cellPref:      cellPref,
		ignoreTablets: make(map[string]struct{}, len(ignoreTablets)),
		options:       options,
	}

	for _, ignoreTablet := range ignoreTablets {
		tp.ignoreTablets[ignoreTablet.String()] = struct{}{}
	}

	return tp, nil

}

// dedupeCells is used to remove duplicates in the cell list in case it is passed in
// and exists in the local cell's alias. Can happen if CellPreference is PreferLocalWithAlias.
func dedupeCells(cells []string) []string {
	keys := make(map[string]bool)
	dedupedCells := []string{}

	for _, c := range cells {
		if _, value := keys[c]; !value {
			keys[c] = true
			dedupedCells = append(dedupedCells, c)
		}
	}
	return dedupedCells
}

// prioritizeTablets orders the candidate pool of tablets based on CellPreference.
// If CellPreference is PreferLocalWithAlias then tablets in the local cell will be prioritized for selection,
// followed by the tablets within the local cell's alias, and finally any others specified by the client.
// If CellPreference is OnlySpecified, then tablets will only be selected randomly from the cells specified by the client.
func (tp *TabletPicker) prioritizeTablets(candidates []*topo.TabletInfo) (sameCell, sameAlias, allOthers []*topo.TabletInfo) {
	for _, c := range candidates {
		if c.Alias.Cell == tp.localCellInfo.localCell {
			sameCell = append(sameCell, c)
		} else if _, ok := tp.localCellInfo.cellsInAlias[c.Alias.Cell]; ok {
			sameAlias = append(sameAlias, c)
		} else {
			allOthers = append(allOthers, c)
		}
	}

	return sameCell, sameAlias, allOthers
}

func (tp *TabletPicker) orderByTabletType(candidates []*topo.TabletInfo) []*topo.TabletInfo {
	// Sort candidates slice such that tablets appear in same tablet type order as in tp.tabletTypes
	orderMap := map[topodatapb.TabletType]int{}
	for i, t := range tp.tabletTypes {
		orderMap[t] = i
	}
	sort.Slice(candidates, func(i, j int) bool {
		if orderMap[candidates[i].Type] == orderMap[candidates[j].Type] {
			// identical tablet types: randomize order of tablets for this type
			return rand.Intn(2) == 0 // 50% chance
		}
		return orderMap[candidates[i].Type] < orderMap[candidates[j].Type]
	})

	return candidates
}

func (tp *TabletPicker) sortCandidates(ctx context.Context, candidates []*topo.TabletInfo) []*topo.TabletInfo {
	if tp.cellPref == TabletPickerCellPreference_PreferLocalWithAlias {
		sameCellCandidates, sameAliasCandidates, allOtherCandidates := tp.prioritizeTablets(candidates)

		if tp.inOrder {
			sameCellCandidates = tp.orderByTabletType(sameCellCandidates)
			sameAliasCandidates = tp.orderByTabletType(sameAliasCandidates)
			allOtherCandidates = tp.orderByTabletType(allOtherCandidates)
		} else {
			// Randomize candidates
			rand.Shuffle(len(sameCellCandidates), func(i, j int) {
				sameCellCandidates[i], sameCellCandidates[j] = sameCellCandidates[j], sameCellCandidates[i]
			})
			rand.Shuffle(len(sameAliasCandidates), func(i, j int) {
				sameAliasCandidates[i], sameAliasCandidates[j] = sameAliasCandidates[j], sameAliasCandidates[i]
			})
			rand.Shuffle(len(allOtherCandidates), func(i, j int) {
				allOtherCandidates[i], allOtherCandidates[j] = allOtherCandidates[j], allOtherCandidates[i]
			})
		}

		candidates = append(sameCellCandidates, sameAliasCandidates...)
		candidates = append(candidates, allOtherCandidates...)
	} else if tp.inOrder {
		candidates = tp.orderByTabletType(candidates)
	} else {
		// Randomize candidates.
		rand.Shuffle(len(candidates), func(i, j int) {
			candidates[i], candidates[j] = candidates[j], candidates[i]
		})
	}
	return candidates
}

// PickForStreaming picks a tablet that is healthy and serving.
// Selection is based on CellPreference.
// See prioritizeTablets for prioritization logic.
func (tp *TabletPicker) PickForStreaming(ctx context.Context) (*topodatapb.Tablet, error) {
	// Keep trying at intervals (tabletPickerRetryDelay) until a healthy
	// serving tablet is found or the context is cancelled.
	for {
		select {
		case <-ctx.Done():
			return nil, vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
		default:
		}
		candidates := tp.GetMatchingTablets(ctx)
		candidates = tp.sortCandidates(ctx, candidates)
		if len(candidates) == 0 {
			// If no viable candidates were found, sleep and try again.
			tp.incNoTabletFoundStat()
			log.Infof("No healthy serving tablet found for streaming, shard %s.%s, cells %v, tabletTypes %v, sleeping for %.3f seconds.",
				tp.keyspace, tp.shard, tp.cells, tp.tabletTypes, float64(GetTabletPickerRetryDelay().Milliseconds())/1000.0)
			timer := time.NewTimer(GetTabletPickerRetryDelay())
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
			case <-timer.C:
			}
			continue
		}
		log.Infof("Tablet picker found a healthy tablet for streaming: %s", candidates[0].Tablet.String())
		return candidates[0].Tablet, nil
	}
}

// GetMatchingTablets returns a list of TabletInfo for healthy
// serving tablets that match the cells, keyspace, shard and
// tabletTypes for this TabletPicker.
func (tp *TabletPicker) GetMatchingTablets(ctx context.Context) []*topo.TabletInfo {
	// Special handling for PRIMARY tablet type: since there is only
	// one primary per shard, we ignore cell and find the primary.
	aliases := make([]*topodatapb.TabletAlias, 0)
	if len(tp.tabletTypes) == 1 && tp.tabletTypes[0] == topodatapb.TabletType_PRIMARY {
		shortCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
		defer cancel()
		si, err := tp.ts.GetShard(shortCtx, tp.keyspace, tp.shard)
		if err != nil {
			log.Errorf("Error getting shard %s/%s: %v", tp.keyspace, tp.shard, err)
			return nil
		}
		if _, ignore := tp.ignoreTablets[si.PrimaryAlias.String()]; !ignore {
			aliases = append(aliases, si.PrimaryAlias)
		}
	} else {
		actualCells := make([]string, 0)
		for _, cell := range tp.cells {
			// Check if cell is actually an alias; using a
			// non-blocking read so that this is fast.
			shortCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
			defer cancel()
			_, err := tp.ts.GetCellInfo(shortCtx, cell, false)
			if err != nil {
				// Not a valid cell, check whether it is a cell alias...
				shortCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
				defer cancel()
				alias, err := tp.ts.GetCellsAlias(shortCtx, cell, false)
				// If we get an error, either cellAlias doesn't exist or
				// it isn't a cell alias at all; ignore and continue.
				if err == nil {
					actualCells = append(actualCells, alias.Cells...)
				} else {
					log.Infof("Unable to resolve cell %s, ignoring", cell)
				}
			} else {
				// Valid cell, add it to our list.
				actualCells = append(actualCells, cell)
			}
		}

		for _, cell := range actualCells {
			shortCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
			defer cancel()
			// Match cell, keyspace, and shard.
			sri, err := tp.ts.GetShardReplication(shortCtx, cell, tp.keyspace, tp.shard)
			if err != nil {
				continue
			}
			for _, node := range sri.Nodes {
				if _, ignore := tp.ignoreTablets[node.TabletAlias.String()]; !ignore {
					aliases = append(aliases, node.TabletAlias)
				}
			}
		}
	}

	if len(aliases) == 0 {
		return nil
	}

	shortCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()
	tabletMap, err := tp.ts.GetTabletMap(shortCtx, aliases, nil)
	if err != nil {
		log.Warningf("Error fetching tablets from topo: %v", err)
		// If we get a partial result we can still use it, otherwise return.
		if len(tabletMap) == 0 {
			return nil
		}
	}

	tablets := make([]*topo.TabletInfo, 0, len(aliases))
	for _, tabletAlias := range aliases {
		tabletInfo, ok := tabletMap[topoproto.TabletAliasString(tabletAlias)]
		if !ok {
			// Either tablet disappeared on us, or we got a partial result
			// (GetTabletMap ignores topo.ErrNoNode); just log a warning.
			log.Warningf("Tablet picker failed to load tablet %v", tabletAlias)
		} else if topoproto.IsTypeInList(tabletInfo.Type, tp.tabletTypes) {
			// Try to connect to the tablet and confirm that it's usable.
			if conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, grpcclient.FailFast(true)); err == nil {
				// Ensure that the tablet is healthy and serving.
				shortCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
				defer cancel()
				if err := conn.StreamHealth(shortCtx, func(shr *querypb.StreamHealthResponse) error {
					if shr != nil &&
						(shr.Serving || tp.options.IncludeNonServingTablets) &&
						shr.RealtimeStats != nil &&
						shr.RealtimeStats.HealthError == "" {
						return io.EOF // End the stream
					}
					return vterrors.New(vtrpcpb.Code_INTERNAL, "tablet is not healthy and serving")
				}); err == nil || err == io.EOF {
					tablets = append(tablets, tabletInfo)
				}
				_ = conn.Close(ctx)
			}
		}
	}
	return tablets
}

func init() {
	globalTPStats = newTabletPickerStats()
}

type tabletPickerStats struct {
	mu                 sync.Mutex
	noTabletFoundError *stats.CountersWithMultiLabels
}

func newTabletPickerStats() *tabletPickerStats {
	tpStats := &tabletPickerStats{}
	tpStats.noTabletFoundError = stats.NewCountersWithMultiLabels("TabletPickerNoTabletFoundErrorCount", "", []string{"cells", "keyspace", "shard", "types"})
	return tpStats
}

func (tp *TabletPicker) incNoTabletFoundStat() {
	globalTPStats.mu.Lock()
	defer globalTPStats.mu.Unlock()
	cells := strings.Join(tp.cells, "_")
	tabletTypes := strings.ReplaceAll(topoproto.MakeStringTypeCSV(tp.tabletTypes), ",", "_")
	labels := []string{cells, tp.keyspace, tp.shard, tabletTypes}
	globalTPStats.noTabletFoundError.Add(labels, 1)
}
