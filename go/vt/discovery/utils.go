package discovery

import (
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains helper filter methods to process the unfiltered list of
// tablets returned by HealthCheck.GetTabletStatsFrom*.
// See also replicationlag.go for a more sophisicated filter used by vtgate.

// RemoveUnhealthyTablets filters all unhealthy tablets out.
// NOTE: Non-serving tablets are considered healthy.
func RemoveUnhealthyTablets(tabletStatsList []*TabletStats) []*TabletStats {
	result := make([]*TabletStats, 0, len(tabletStatsList))
	for _, ts := range tabletStatsList {
		// Note we do not check the 'Serving' flag here.
		// This is mainly to avoid the case where we run a vtworker Diff between a
		// source and destination, and the source is not serving (disabled by
		// TabletControl). When we switch the tablet to 'worker', it will
		// go back to serving state.
		if ts.Stats == nil || ts.Stats.HealthError != "" || float64(ts.Stats.SecondsBehindMaster) > LowReplicationLag.Seconds() {
			continue
		}
		result = append(result, ts)
	}
	return result
}

// GetCurrentMaster returns the MASTER tablet with the highest
// TabletExternallyReparentedTimestamp value.
func GetCurrentMaster(tabletStatsList []*TabletStats) []*TabletStats {
	var master *TabletStats
	// If there are multiple masters (e.g. during a reparent), pick the most
	// recent one (i.e. with the highest TabletExternallyReparentedTimestamp value).
	for _, ts := range tabletStatsList {
		if ts.Target.TabletType != topodatapb.TabletType_MASTER {
			continue
		}

		if master == nil || master.TabletExternallyReparentedTimestamp < ts.TabletExternallyReparentedTimestamp {
			master = ts
		}
	}
	if master == nil {
		return nil
	}

	return []*TabletStats{master}
}
