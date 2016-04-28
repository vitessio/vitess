package discovery

import (
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains helper filter methods to process the unfiltered list of
// tablets returned by HealthCheck.GetEndPointStatsFrom*.
// See also replicationlag.go for a more sophisicated filter used by vtgate.

// RemoveUnhealthyEndpoints filters all unhealthy tablets out.
// NOTE: Non-serving tablets are considered healthy.
func RemoveUnhealthyEndpoints(epsList []*EndPointStats) []*EndPointStats {
	result := make([]*EndPointStats, 0, len(epsList))
	for _, eps := range epsList {
		// Note we do not check the 'Serving' flag here.
		// This is mainly to avoid the case where we run a vtworker Diff between a
		// source and destination, and the source is not serving (disabled by
		// TabletControl). When we switch the tablet to 'worker', it will
		// go back to serving state.
		if eps.Stats == nil || eps.Stats.HealthError != "" || float64(eps.Stats.SecondsBehindMaster) > LowReplicationLag.Seconds() {
			continue
		}
		result = append(result, eps)
	}
	return result
}

// GetCurrentMaster returns the MASTER tablet with the highest
// TabletExternallyReparentedTimestamp value.
func GetCurrentMaster(epsList []*EndPointStats) []*EndPointStats {
	var master *EndPointStats
	// If there are multiple masters (e.g. during a reparent), pick the most
	// recent one (i.e. with the highest TabletExternallyReparentedTimestamp value).
	for _, eps := range epsList {
		if eps.Target.TabletType != topodatapb.TabletType_MASTER {
			continue
		}

		if master == nil || master.TabletExternallyReparentedTimestamp < eps.TabletExternallyReparentedTimestamp {
			master = eps
		}
	}
	if master == nil {
		return []*EndPointStats{}
	}

	return []*EndPointStats{master}
}
