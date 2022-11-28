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
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/stats"

	"vitess.io/vitess/go/vt/topo/topoproto"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	"vitess.io/vitess/go/vt/log"

	"context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	tabletPickerRetryDelay   = 30 * time.Second
	muTabletPickerRetryDelay sync.Mutex
	globalTPStats            *tabletPickerStats
	inOrderHint              = "in_order:"
	localPreferenceHint      = "local:"
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

// TabletPicker gives a simplified API for picking tablets.
type TabletPicker struct {
	ts              *topo.Server
	cells           []string
	keyspace        string
	shard           string
	tabletTypes     []topodatapb.TabletType
	inOrder         bool
	localPreference string
}

// NewTabletPicker returns a TabletPicker.
func NewTabletPicker(ts *topo.Server, cells []string, keyspace, shard, tabletTypesStr string) (*TabletPicker, error) {
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

	localPreference := ""
	if strings.HasPrefix(cells[0], localPreferenceHint) {
		localPreference = cells[0][len(localPreferenceHint):]
		cells = cells[1:]
		// Add the local cell to the list of cells
		// This may result in the local cell appearing twice if it already exists as part of an alias,
		// but cells will get deduped during tablet selection. See GetMatchingTablets() -> tp.dedupeCells()
		cells = append(cells, localPreference)
	}

	return &TabletPicker{
		ts:              ts,
		cells:           cells,
		keyspace:        keyspace,
		shard:           shard,
		tabletTypes:     tabletTypes,
		inOrder:         inOrder,
		localPreference: localPreference,
	}, nil
}

func (tp *TabletPicker) prioritizeTablets(candidates []*topo.TabletInfo) (sameCell, allOthers []*topo.TabletInfo) {
	for _, c := range candidates {
		if c.Alias.Cell == tp.localPreference {
			sameCell = append(sameCell, c)
		} else {
			allOthers = append(allOthers, c)
		}
	}

	return sameCell, allOthers
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

// PickForStreaming picks an available tablet.
// All tablets that belong to tp.cells are evaluated and one is
// chosen at random, unless local preference is given
func (tp *TabletPicker) PickForStreaming(ctx context.Context) (*topodatapb.Tablet, error) {
	rand.Seed(time.Now().UnixNano())
	// keep trying at intervals (tabletPickerRetryDelay) until a tablet is found
	// or the context is canceled
	for {
		select {
		case <-ctx.Done():
			return nil, vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
		default:
		}
		candidates := tp.GetMatchingTablets(ctx)
		// we'd like to prioritize same cell tablets
		if tp.localPreference != "" {
			sameCellCandidates, allOtherCandidates := tp.prioritizeTablets(candidates)

			// order same cell and all others by tablet type separately
			// combine with same cell in front
			if tp.inOrder {
				sameCellCandidates = tp.orderByTabletType(sameCellCandidates)
				allOtherCandidates = tp.orderByTabletType(allOtherCandidates)
			}
			candidates = append(sameCellCandidates, allOtherCandidates...)
		} else if tp.inOrder {
			candidates = tp.orderByTabletType(candidates)
		} else {
			// Randomize candidates
			rand.Shuffle(len(candidates), func(i, j int) {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			})
		}
		if len(candidates) == 0 {
			// if no candidates were found, sleep and try again
			tp.incNoTabletFoundStat()
			log.Infof("No tablet found for streaming, shard %s.%s, cells %v, tabletTypes %v, sleeping for %.3f seconds",
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
		for _, ti := range candidates {
			// try to connect to tablet
			if conn, err := tabletconn.GetDialer()(ti.Tablet, true); err == nil {
				// OK to use ctx here because it is not actually used by the underlying Close implementation
				_ = conn.Close(ctx)
				log.Infof("tablet picker found tablet %s", ti.Tablet.String())
				return ti.Tablet, nil
			}
			// err found
			log.Warningf("unable to connect to tablet for alias %v", ti.Alias)
		}
		// Got here? Means we iterated all tablets and did not find a healthy one
		tp.incNoTabletFoundStat()
	}
}

// GetMatchingTablets returns a list of TabletInfo for tablets
// that match the cells, keyspace, shard and tabletTypes for this TabletPicker
func (tp *TabletPicker) GetMatchingTablets(ctx context.Context) []*topo.TabletInfo {
	// Special handling for PRIMARY tablet type
	// Since there is only one primary, we ignore cell and find the primary
	aliases := make([]*topodatapb.TabletAlias, 0)
	if len(tp.tabletTypes) == 1 && tp.tabletTypes[0] == topodatapb.TabletType_PRIMARY {
		shortCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
		defer cancel()
		si, err := tp.ts.GetShard(shortCtx, tp.keyspace, tp.shard)
		if err != nil {
			log.Errorf("error getting shard %s/%s: %s", tp.keyspace, tp.shard, err.Error())
			return nil
		}
		aliases = append(aliases, si.PrimaryAlias)
	} else {
		actualCells := make([]string, 0)
		for _, cell := range tp.cells {
			// check if cell is actually an alias
			// non-blocking read so that this is fast
			shortCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
			defer cancel()
			_, err := tp.ts.GetCellInfo(shortCtx, cell, false)
			if err != nil {
				// not a valid cell, check whether it is a cell alias
				shortCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
				defer cancel()
				alias, err := tp.ts.GetCellsAlias(shortCtx, cell, false)
				// if we get an error, either cellAlias doesn't exist or it isn't a cell alias at all. Ignore and continue
				if err == nil {
					actualCells = append(actualCells, alias.Cells...)
				} else {
					log.Infof("Unable to resolve cell %s, ignoring", cell)
				}
			} else {
				// valid cell, add it to our list
				actualCells = append(actualCells, cell)
			}
		}
		// Just in case a cell was passed in addition to its alias.
		// Can happen if localPreference is not "". See NewTabletPicker
		actualCells = tp.dedupeCells(actualCells)

		for _, cell := range actualCells {
			shortCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
			defer cancel()
			// match cell, keyspace and shard
			sri, err := tp.ts.GetShardReplication(shortCtx, cell, tp.keyspace, tp.shard)
			if err != nil {
				continue
			}

			for _, node := range sri.Nodes {
				aliases = append(aliases, node.TabletAlias)
			}
		}
	}

	if len(aliases) == 0 {
		return nil
	}
	shortCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()
	tabletMap, err := tp.ts.GetTabletMap(shortCtx, aliases)
	if err != nil {
		log.Warningf("error fetching tablets from topo: %v", err)
		// If we get a partial result we can still use it, otherwise return
		if len(tabletMap) == 0 {
			return nil
		}
	}
	tablets := make([]*topo.TabletInfo, 0, len(aliases))
	for _, tabletAlias := range aliases {
		tabletInfo, ok := tabletMap[topoproto.TabletAliasString(tabletAlias)]
		if !ok {
			// Either tablet disappeared on us, or we got a partial result (GetTabletMap ignores
			// topo.ErrNoNode). Just log a warning
			log.Warningf("failed to load tablet %v", tabletAlias)
		} else if topoproto.IsTypeInList(tabletInfo.Type, tp.tabletTypes) {
			tablets = append(tablets, tabletInfo)
		}
	}
	return tablets
}

func (tp *TabletPicker) dedupeCells(cells []string) []string {
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

func init() {
	// TODO(sougou): consolidate this call to be once per process.
	rand.Seed(time.Now().UnixNano())
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
	tabletTypes := strings.Join(topoproto.MakeStringTypeList(tp.tabletTypes), "_")
	labels := []string{cells, tp.keyspace, tp.shard, tabletTypes}
	globalTPStats.noTabletFoundError.Add(labels, 1)
}
