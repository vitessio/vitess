package discovery

import (
	"flag"
	"fmt"
	"time"
)

var lowReplicationLag = flag.Duration("discovery_low_replication_lag", 30*time.Second, "the replication lag that is considered low enough to be healthy")

// FilterByReplicationLag filters the list of EndPointStats by EndPointStats.Stats.SecondsBehindMaster.
// The algorithm (EndPointStats that is non-serving or has error is ignored):
// - Return the list if there is 0 or 1 endpoint.
// - Return the list if all endpoints have <=30s lag.
// - Filter by replication lag: for each endpoint, if the mean value without it is more than 0.7 of the mean value across all endpoints, it is valid.
// For example, lags of (5s, 10s, 15s, 120s) return the first three;
// lags of (30m, 35m, 40m, 45m) return all.
func FilterByReplicationLag(epsList []*EndPointStats) []*EndPointStats {
	list := make([]*EndPointStats, 0, len(epsList))
	// filter non-serving endpoints
	for _, eps := range epsList {
		if !eps.Serving || eps.LastError != nil || eps.Stats == nil {
			continue
		}
		list = append(list, eps)
	}
	if len(list) <= 1 {
		return list
	}
	// if all have low replication lag (<=30s), return all endpoints.
	allLowLag := true
	for _, eps := range list {
		if float64(eps.Stats.SecondsBehindMaster) > lowReplicationLag.Seconds() {
			allLowLag = false
			break
		}
	}
	if allLowLag {
		return list
	}
	// filter those affecting "mean" lag significantly
	// calculate mean for all endpoints
	res := make([]*EndPointStats, 0, len(list))
	m, _ := mean(list, -1)
	for i, eps := range list {
		// calculate mean by excluding ith endpoint
		mi, _ := mean(list, i)
		if float64(mi) > float64(m)*0.7 {
			res = append(res, eps)
		}
	}
	return res
}

// mean calculates the mean value over the given list,
// while excluding the item with the specified index.
func mean(epsList []*EndPointStats, idxExclude int) (uint64, error) {
	var sum uint64
	var count uint64
	for i, eps := range epsList {
		if i == idxExclude {
			continue
		}
		sum = sum + uint64(eps.Stats.SecondsBehindMaster)
		count++
	}
	if count == 0 {
		return 0, fmt.Errorf("empty list")
	}
	return sum / count, nil
}
