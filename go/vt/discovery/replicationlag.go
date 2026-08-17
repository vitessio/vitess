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
	"sort"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/viperutil"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	// lowReplicationLag defines the duration that replication lag is low enough that the VTTablet is considered healthy.
	lowReplicationLag = viperutil.Configure(
		"discovery_low_replication_lag",
		viperutil.Options[time.Duration]{
			FlagName: "discovery-low-replication-lag",
			Default:  30 * time.Second,
			Dynamic:  true,
		},
	)
	highReplicationLagMinServing = viperutil.Configure(
		"discovery_high_replication_lag",
		viperutil.Options[time.Duration]{
			FlagName: "discovery-high-replication-lag-minimum-serving",
			Default:  2 * time.Hour,
			Dynamic:  true,
		},
	)
	minNumTablets = viperutil.Configure(
		"discovery_min_number_serving_vttablets",
		viperutil.Options[int]{
			FlagName: "min-number-serving-vttablets",
			Default:  2,
			Dynamic:  true,
		},
	)
	legacyReplicationLagAlgorithm = viperutil.Configure(
		"discovery_legacy_replication_lag_algorithm",
		viperutil.Options[bool]{
			FlagName: "legacy-replication-lag-algorithm",
			Default:  false,
		},
	)
)

func init() {
	servenv.OnParseFor("vtgate", registerReplicationFlags)
}

func registerReplicationFlags(fs *pflag.FlagSet) {
	fs.Duration("discovery-low-replication-lag", lowReplicationLag.Default(), "Threshold below which replication lag is considered low enough to be healthy.")
	fs.Duration("discovery-high-replication-lag-minimum-serving", highReplicationLagMinServing.Default(), "Threshold above which replication lag is considered too high when applying the min_number_serving_vttablets flag.")
	fs.Int("min-number-serving-vttablets", minNumTablets.Default(), "The minimum number of vttablets for each replicating tablet_type (e.g. replica, rdonly) that will be continue to be used even with replication lag above discovery_low_replication_lag, but still below discovery_high_replication_lag_minimum_serving.")
	fs.Bool("legacy-replication-lag-algorithm", legacyReplicationLagAlgorithm.Default(), "(DEPRECATED) Use the legacy algorithm when selecting vttablets for serving.")
	_ = fs.MarkDeprecated("legacy-replication-lag-algorithm", "this flag is a no-op and will be removed in v26")

	viperutil.BindFlags(
		fs,
		lowReplicationLag,
		highReplicationLagMinServing,
		minNumTablets,
		legacyReplicationLagAlgorithm,
	)
}

// GetLowReplicationLag getter for use by debugenv
func GetLowReplicationLag() time.Duration {
	return lowReplicationLag.Get()
}

// SetLowReplicationLag setter for use by debugenv
func SetLowReplicationLag(lag time.Duration) {
	lowReplicationLag.Set(lag)
}

// GetHighReplicationLagMinServing getter for use by debugenv
func GetHighReplicationLagMinServing() time.Duration {
	return highReplicationLagMinServing.Get()
}

// SetHighReplicationLagMinServing setter for use by debugenv
func SetHighReplicationLagMinServing(lag time.Duration) {
	highReplicationLagMinServing.Set(lag)
}

// GetMinNumTablets getter for use by debugenv
func GetMinNumTablets() int {
	return minNumTablets.Get()
}

// SetMinNumTablets setter for use by debugenv
func SetMinNumTablets(numTablets int) {
	minNumTablets.Set(numTablets)
}

// FilterStatsByReplicationLag filters the list of TabletHealth by TabletHealth.Stats.ReplicationLagSeconds.
// Note that TabletHealth that is non-serving or has error is ignored.
//
// Logic:
// - Return tablets that have lag <= lowReplicationLag.
// - Make sure we return at least minNumTablets tablets, if there are enough one with lag <= highReplicationLagMinServing.
// For example, with the default of 30s / 2h / 2, this means:
// - lags of (5s, 10s, 15s, 120s) return the first three
// - lags of (30m, 35m, 40m, 45m) return the first two
// - lags of (2h, 3h, 4h, 5h) return the first one
//
// One thing to know about this code: vttablet also has a couple flags that impact the logic here:
//   - unhealthy-threshold: if replication lag is higher than this, a tablet will be reported as unhealthy.
//     The default for this is 2h, same as the discovery_high_replication_lag_minimum_serving here.
//   - degraded-threshold: this is only used by vttablet for display. It should match
//     discovery_low_replication_lag here, so the vttablet status display matches what vtgate will do of it.
func FilterStatsByReplicationLag(tabletHealthList []*TabletHealth) []*TabletHealth {
	// The legacy algorithm has been deprecated; the --legacy-replication-lag-algorithm
	// flag is now a no-op and will be removed in v26. See https://github.com/vitessio/vitess/issues/18914.
	return filterStatsByLag(tabletHealthList)
}

func filterStatsByLag(tabletHealthList []*TabletHealth) []*TabletHealth {
	// These thresholds are viper-backed flags whose Get() is comparatively
	// expensive, so read them once instead of per tablet: filtering runs over
	// every tablet in a shard while holding the healthcheck lock.
	lowLag := lowReplicationLag.Get().Seconds()
	highLag := highReplicationLagMinServing.Get().Seconds()

	list := make([]tabletLagSnapshot, 0, len(tabletHealthList))
	// Filter out non-serving tablets and those with very high replication lag.
	for _, ts := range tabletHealthList {
		if !ts.Serving || ts.LastError != nil || ts.Stats == nil || float64(ts.Stats.ReplicationLagSeconds) > highLag {
			continue
		}
		// Save the current replication lag for a stable sort later.
		list = append(list, tabletLagSnapshot{
			ts:     ts,
			replag: ts.Stats.ReplicationLagSeconds,
		})
	}

	// Sort by replication lag.
	sort.Sort(tabletLagSnapshotList(list))

	// Pick tablets with low replication lag, but at least minNumTablets tablets
	// regardless. minNumTablets.Get() is a viper read that allocates, so defer it:
	// the list is sorted by ascending lag, so once we reach a high-lag tablet every
	// later one is also high-lag. Read minNumTablets at most once, and only when a
	// high-lag tablet actually forces the question — zero times when all tablets
	// are healthy, which is the common case on the healthcheck hot path.
	res := make([]*TabletHealth, 0, len(list))
	var minTablets int
	minTabletsRead := false
	for i := 0; i < len(list); i++ {
		if float64(list[i].ts.Stats.ReplicationLagSeconds) <= lowLag {
			res = append(res, list[i].ts)
			continue
		}
		if !minTabletsRead {
			minTablets = minNumTablets.Get()
			minTabletsRead = true
		}
		if i < minTablets {
			res = append(res, list[i].ts)
		}
	}
	return res
}

type tabletLagSnapshot struct {
	ts     *TabletHealth
	replag uint32
}
type tabletLagSnapshotList []tabletLagSnapshot

func (a tabletLagSnapshotList) Len() int           { return len(a) }
func (a tabletLagSnapshotList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a tabletLagSnapshotList) Less(i, j int) bool { return a[i].replag < a[j].replag }
