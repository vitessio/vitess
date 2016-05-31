package discovery

import (
	"flag"
	"fmt"
	"math"
	"time"
)

var (
	// LowReplicationLag defines the duration that replication lag is low enough that the VTTablet is considered healthy.
	LowReplicationLag            = flag.Duration("discovery_low_replication_lag", 30*time.Second, "the replication lag that is considered low enough to be healthy")
	highReplicationLagMinServing = flag.Duration("discovery_high_replication_lag_minimum_serving", 2*time.Hour, "the replication lag that is considered too high when selecting miminum 2 vttablets for serving")
)

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
		if float64(ts.Stats.SecondsBehindMaster) > LowReplicationLag.Seconds() {
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
