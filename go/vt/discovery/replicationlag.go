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

package discovery

import (
	"flag"
	"fmt"
	"math"
	"time"
)

var (
	// lowReplicationLag defines the duration that replication lag is low enough that the VTTablet is considered healthy.
	lowReplicationLag            = flag.Duration("discovery_low_replication_lag", 30*time.Second, "the replication lag that is considered low enough to be healthy")
	highReplicationLagMinServing = flag.Duration("discovery_high_replication_lag_minimum_serving", 2*time.Hour, "the replication lag that is considered too high when selecting miminum 2 vttablets for serving")
)

// IsReplicationLagHigh verifies that the given TabletStats refers to a tablet with high
// replication lag, i.e. higher than the configured discovery_low_replication_lag flag.
func IsReplicationLagHigh(tabletStats *TabletStats) bool {
	return float64(tabletStats.Stats.SecondsBehindMaster) > lowReplicationLag.Seconds()
}

// FilterByReplicationLag filters the list of TabletStats by TabletStats.Stats.SecondsBehindMaster.
// The algorithm (TabletStats that is non-serving or has error is ignored):
// - Return the list if there is 0 or 1 tablet.
// - Return the list if all tablets have <=30s lag.
// - Filter by replication lag: for each tablet, if the mean value without it is more than 0.7 of the mean value across all tablets, it is valid.
// - Make sure we return at least two tablets (if there is one with <2h replication lag).
// - If one tablet is removed, run above steps again in case there are two tablets with high replication lag. (It should cover most cases.)
// For example, lags of (5s, 10s, 15s, 120s) return the first three;
// lags of (30m, 35m, 40m, 45m) return all.
func FilterByReplicationLag(tabletStatsList []*TabletStats) []*TabletStats {
	res := filterByLag(tabletStatsList)
	// run the filter again if exact one tablet is removed.
	if len(res) > 2 && len(res) == len(tabletStatsList)-1 {
		res = filterByLag(res)
	}
	return res
}

func filterByLag(tabletStatsList []*TabletStats) []*TabletStats {
	list := make([]*TabletStats, 0, len(tabletStatsList))
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
		if IsReplicationLagHigh(ts) {
			allLowLag = false
			break
		}
	}
	if allLowLag {
		return list
	}
	// filter those affecting "mean" lag significantly
	// calculate mean for all tablets
	res := make([]*TabletStats, 0, len(list))
	m, _ := mean(list, -1)
	for i, ts := range list {
		// calculate mean by excluding ith tablet
		mi, _ := mean(list, i)
		if float64(mi) > float64(m)*0.7 {
			res = append(res, ts)
		}
	}
	// return at least 2 tablets to avoid over loading,
	// if there is another tablet with replication lag < highReplicationLagMinServing.
	if len(res) == 0 {
		return list
	}
	if len(res) == 1 && len(list) > 1 {
		minLag := uint32(math.MaxUint32)
		idx := -1
		for i, ts := range list {
			if ts == res[0] {
				continue
			}
			if ts.Stats.SecondsBehindMaster < minLag {
				idx = i
				minLag = ts.Stats.SecondsBehindMaster
			}
		}
		if idx >= 0 && minLag <= uint32(highReplicationLagMinServing.Seconds()) {
			res = append(res, list[idx])
		}
	}
	return res
}

// mean calculates the mean value over the given list,
// while excluding the item with the specified index.
func mean(tabletStatsList []*TabletStats, idxExclude int) (uint64, error) {
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

// TrivialStatsUpdate returns true iff the old and new TabletStats
// haven't changed enough to warrant re-calling FilterByReplicationLag.
func TrivialStatsUpdate(o, n *TabletStats) bool {
	// Skip replag filter when replag remains in the low rep lag range,
	// which should be the case majority of the time.
	lowRepLag := lowReplicationLag.Seconds()
	oldRepLag := float64(o.Stats.SecondsBehindMaster)
	newRepLag := float64(n.Stats.SecondsBehindMaster)
	if oldRepLag <= lowRepLag && newRepLag <= lowRepLag {
		return true
	}

	// Skip replag filter when replag remains in the high rep lag range,
	// and did not change beyond +/- 10%.
	// when there is a high rep lag, it takes a long time for it to reduce,
	// so it is not necessary to re-calculate every time.
	// In that case, we won't save the new record, so we still
	// remember the original replication lag.
	if oldRepLag > lowRepLag && newRepLag > lowRepLag && newRepLag < oldRepLag*1.1 && newRepLag > oldRepLag*0.9 {
		return true
	}

	return false
}
