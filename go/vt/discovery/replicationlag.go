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
	"flag"
	"sort"
	"time"
)

var (
	// lowReplicationLag defines the duration that replication lag is low enough that the VTTablet is considered healthy.
	lowReplicationLag            = flag.Duration("discovery_low_replication_lag", 30*time.Second, "the replication lag that is considered low enough to be healthy")
	highReplicationLagMinServing = flag.Duration("discovery_high_replication_lag_minimum_serving", 2*time.Hour, "the replication lag that is considered too high when selecting the minimum num vttablets for serving")
	minNumTablets                = flag.Int("min_number_serving_vttablets", 2, "the minimum number of vttablets that will be continue to be used even with low replication lag")
)

// IsReplicationLagHigh verifies that the given LegacyTabletStats refers to a tablet with high
// replication lag, i.e. higher than the configured discovery_low_replication_lag flag.
func IsReplicationLagHigh(tabletStats *TabletStats) bool {
	return float64(tabletStats.Stats.SecondsBehindMaster) > lowReplicationLag.Seconds()
}

// IsReplicationLagVeryHigh verifies that the given LegacyTabletStats refers to a tablet with very high
// replication lag, i.e. higher than the configured discovery_high_replication_lag_minimum_serving flag.
func IsReplicationLagVeryHigh(tabletStats *TabletStats) bool {
	return float64(tabletStats.Stats.SecondsBehindMaster) > highReplicationLagMinServing.Seconds()
}

// FilterStatsByReplicationLag filters the list of LegacyTabletStats by LegacyTabletStats.Stats.SecondsBehindMaster.
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
func FilterStatsByReplicationLag(tabletStatsList []*TabletStats) []*TabletStats {
	return filterStatsByLag(tabletStatsList)
}

func filterStatsByLag(tabletStatsList []*TabletStats) []*TabletStats {
	list := make([]tabletLagSnapshot, 0, len(tabletStatsList))
	// filter non-serving tablets and those with very high replication lag
	for _, ts := range tabletStatsList {
		if !ts.Serving || ts.LastError != nil || ts.Stats == nil || IsReplicationLagVeryHigh(ts) {
			continue
		}
		// Pull the current replication lag for a stable sort later.
		list = append(list, tabletLagSnapshot{
			ts:     ts,
			replag: ts.Stats.SecondsBehindMaster})
	}

	// Sort by replication lag.
	sort.Sort(tabletLagSnapshotList(list))

	// Pick those with low replication lag, but at least minNumTablets tablets regardless.
	res := make([]*TabletStats, 0, len(list))
	for i := 0; i < len(list); i++ {
		if !IsReplicationLagHigh(list[i].ts) || i < *minNumTablets {
			res = append(res, list[i].ts)
		}
	}
	return res
}

type tabletLagSnapshot struct {
	ts     *TabletStats
	replag uint32
}
type tabletLagSnapshotList []tabletLagSnapshot

func (a tabletLagSnapshotList) Len() int           { return len(a) }
func (a tabletLagSnapshotList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a tabletLagSnapshotList) Less(i, j int) bool { return a[i].replag < a[j].replag }

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}
