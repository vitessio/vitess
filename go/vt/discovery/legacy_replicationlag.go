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
	"sort"
)

// LegacyIsReplicationLagHigh verifies that the given LegacyTabletStats refers to a tablet with high
// replication lag, i.e. higher than the configured discovery_low_replication_lag flag.
func LegacyIsReplicationLagHigh(tabletStats *LegacyTabletStats) bool {
	return float64(tabletStats.Stats.SecondsBehindMaster) > lowReplicationLag.Seconds()
}

// LegacyIsReplicationLagVeryHigh verifies that the given LegacyTabletStats refers to a tablet with very high
// replication lag, i.e. higher than the configured discovery_high_replication_lag_minimum_serving flag.
func LegacyIsReplicationLagVeryHigh(tabletStats *LegacyTabletStats) bool {
	return float64(tabletStats.Stats.SecondsBehindMaster) > highReplicationLagMinServing.Seconds()
}

// FilterLegacyStatsByReplicationLag filters the list of LegacyTabletStats by LegacyTabletStats.Stats.SecondsBehindMaster.
// Note that LegacyTabletStats that is non-serving or has error is ignored.
//
// The simplified logic:
// - Return tablets that have lag <= lowReplicationLag.
// - Make sure we return at least minNumTablets tablets, if there are enough one with lag <= highReplicationLagMinServing.
// For example, with the default of 30s / 2h / 2, this means:
// - lags of (5s, 10s, 15s, 120s) return the first three
// - lags of (30m, 35m, 40m, 45m) return the first two
// - lags of (2h, 3h, 4h, 5h) return the first one
//
// The legacy algorithm (default for now):
// - Return the list if there is 0 or 1 tablet.
// - Return the list if all tablets have <=30s lag.
// - Filter by replication lag: for each tablet, if the mean value without it is more than 0.7 of the mean value across all tablets, it is valid.
// - Make sure we return at least minNumTablets tablets (if there are enough one with only low replication lag).
// - If one tablet is removed, run above steps again in case there are two tablets with high replication lag. (It should cover most cases.)
// For example, lags of (5s, 10s, 15s, 120s) return the first three;
// lags of (30m, 35m, 40m, 45m) return all.
//
// One thing to know about this code: vttablet also has a couple flags that impact the logic here:
// * unhealthy_threshold: if replication lag is higher than this, a tablet will be reported as unhealthy.
//   The default for this is 2h, same as the discovery_high_replication_lag_minimum_serving here.
// * degraded_threshold: this is only used by vttablet for display. It should match
//   discovery_low_replication_lag here, so the vttablet status display matches what vtgate will do of it.
func FilterLegacyStatsByReplicationLag(tabletStatsList []*LegacyTabletStats) []*LegacyTabletStats {
	if !*legacyReplicationLagAlgorithm {
		return filterLegacyStatsByLag(tabletStatsList)
	}

	res := filterLegacyStatsByLagWithLegacyAlgorithm(tabletStatsList)
	// run the filter again if exactly one tablet is removed,
	// and we have spare tablets.
	if len(res) > *minNumTablets && len(res) == len(tabletStatsList)-1 {
		res = filterLegacyStatsByLagWithLegacyAlgorithm(res)
	}
	return res
}

func filterLegacyStatsByLag(tabletStatsList []*LegacyTabletStats) []*LegacyTabletStats {
	list := make([]legacyTabletLagSnapshot, 0, len(tabletStatsList))
	// filter non-serving tablets and those with very high replication lag
	for _, ts := range tabletStatsList {
		if !ts.Serving || ts.LastError != nil || ts.Stats == nil || LegacyIsReplicationLagVeryHigh(ts) {
			continue
		}
		// Pull the current replication lag for a stable sort later.
		list = append(list, legacyTabletLagSnapshot{
			ts:     ts,
			replag: ts.Stats.SecondsBehindMaster})
	}

	// Sort by replication lag.
	sort.Sort(byLegacyReplag(list))

	// Pick those with low replication lag, but at least minNumTablets tablets regardless.
	res := make([]*LegacyTabletStats, 0, len(list))
	for i := 0; i < len(list); i++ {
		if !LegacyIsReplicationLagHigh(list[i].ts) || i < *minNumTablets {
			res = append(res, list[i].ts)
		}
	}
	return res
}

func filterLegacyStatsByLagWithLegacyAlgorithm(tabletStatsList []*LegacyTabletStats) []*LegacyTabletStats {
	list := make([]*LegacyTabletStats, 0, len(tabletStatsList))
	// filter non-serving tablets
	for _, ts := range tabletStatsList {
		if !ts.Serving || ts.LastError != nil || ts.Stats == nil {
			continue
		}
		list = append(list, ts)
	}
	if len(list) <= 1 {
		return list
	}
	// if all have low replication lag (<=30s), return all tablets.
	allLowLag := true
	for _, ts := range list {
		if LegacyIsReplicationLagHigh(ts) {
			allLowLag = false
			break
		}
	}
	if allLowLag {
		return list
	}
	// filter those affecting "mean" lag significantly
	// calculate mean for all tablets
	res := make([]*LegacyTabletStats, 0, len(list))
	m, _ := legacyMean(list, -1)
	for i, ts := range list {
		// calculate mean by excluding ith tablet
		mi, _ := legacyMean(list, i)
		if float64(mi) > float64(m)*0.7 {
			res = append(res, ts)
		}
	}
	if len(res) >= *minNumTablets {
		return res
	}
	// return at least minNumTablets tablets to avoid over loading,
	// if there is enough tablets with replication lag < highReplicationLagMinServing.
	// Pull the current replication lag for a stable sort.
	snapshots := make([]legacyTabletLagSnapshot, 0, len(list))
	for _, ts := range list {
		if !LegacyIsReplicationLagVeryHigh(ts) {
			snapshots = append(snapshots, legacyTabletLagSnapshot{
				ts:     ts,
				replag: ts.Stats.SecondsBehindMaster})
		}
	}
	if len(snapshots) == 0 {
		// We get here if all tablets are over the high
		// replication lag threshold, and their lag is
		// different enough that the 70% mean computation up
		// there didn't find them all in a group. For
		// instance, if *minNumTablets = 2, and we have two
		// tablets with lag of 3h and 30h.  In that case, we
		// just use them all.
		for _, ts := range list {
			snapshots = append(snapshots, legacyTabletLagSnapshot{
				ts:     ts,
				replag: ts.Stats.SecondsBehindMaster})
		}
	}

	// Sort by replication lag.
	sort.Sort(byLegacyReplag(snapshots))

	// Pick the first minNumTablets tablets.
	res = make([]*LegacyTabletStats, 0, *minNumTablets)
	for i := 0; i < min(*minNumTablets, len(snapshots)); i++ {
		res = append(res, snapshots[i].ts)
	}
	return res
}

type legacyTabletLagSnapshot struct {
	ts     *LegacyTabletStats
	replag uint32
}
type byLegacyReplag []legacyTabletLagSnapshot

func (a byLegacyReplag) Len() int           { return len(a) }
func (a byLegacyReplag) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byLegacyReplag) Less(i, j int) bool { return a[i].replag < a[j].replag }

// mean calculates the mean value over the given list,
// while excluding the item with the specified index.
func legacyMean(tabletStatsList []*LegacyTabletStats, idxExclude int) (uint64, error) {
	var sum uint64
	var count uint64
	for i, ts := range tabletStatsList {
		if i == idxExclude {
			continue
		}
		sum = sum + uint64(ts.Stats.SecondsBehindMaster)
		count++
	}
	if count == 0 {
		return 0, fmt.Errorf("empty list")
	}
	return sum / count, nil
}
