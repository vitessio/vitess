package discovery

import (
	"flag"
	"fmt"
	"strconv"
	"time"
)

var lowReplicationLag = flag.Duration("discovery_low_replication_lag", 30*time.Second, "the replication lag that is considered low enough to be healthy")

// FilterByReplicationLag filters the list of EndPointStats by EndPointStats.Stats.SecondsBehindMaster.
func FilterByReplicationLag(epsList []*EndPointStats) []*EndPointStats {
	m := make(map[string]*EndPointStats)
	for i, eps := range epsList {
		m[strconv.Itoa(i)] = eps
	}
	filtered := FilterMapByReplicationLag(m)
	res := make([]*EndPointStats, 0, len(filtered))
	for _, eps := range filtered {
		res = append(res, eps)
	}
	return res
}

// FilterMapByReplicationLag filters the map of EndPointStats by EndPointStats.Stats.SecondsBehindMaster.
// The algorithm (EndPointStats that is non-serving or has error is ignored):
// - Return the list if there is 0 or 1 endpoint.
// - Return the list if all endpoints have <=30s lag (configurable).
// - Filter by replication lag: for each endpoint, if the mean value without it is more than 0.7 of the mean value across all endpoints, it is valid.
// For example, lags of (5s, 10s, 15s, 120s) return the first three;
// lags of (30m, 35m, 40m, 45m) return all.
func FilterMapByReplicationLag(epsMap map[string]*EndPointStats) map[string]*EndPointStats {
	epsServing := make(map[string]*EndPointStats)
	// filter non-serving endpoints
	for addr, eps := range epsMap {
		if !eps.Serving || eps.LastError != nil || eps.Stats == nil {
			continue
		}
		epsServing[addr] = eps
	}
	if len(epsServing) <= 1 {
		return epsServing
	}
	// if all have low replication lag (<=30s), return all endpoints.
	allLowLag := true
	for _, eps := range epsServing {
		if float64(eps.Stats.SecondsBehindMaster) > lowReplicationLag.Seconds() {
			allLowLag = false
			break
		}
	}
	if allLowLag {
		return epsServing
	}
	// filter those affecting "mean" lag significantly
	// calculate mean for all endpoints
	res := make(map[string]*EndPointStats)
	m, _ := mean(epsServing, "")
	for addr, eps := range epsServing {
		// calculate mean by excluding ith endpoint
		mi, _ := mean(epsServing, addr)
		if float64(mi) > float64(m)*0.7 {
			res[addr] = eps
		}
	}
	return res
}

// mean calculates the mean value over the given map,
// while excluding the item with the specified address.
func mean(epsMap map[string]*EndPointStats, addrExclude string) (uint64, error) {
	var sum uint64
	var count uint64
	for addr, eps := range epsMap {
		if addr == addrExclude {
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
