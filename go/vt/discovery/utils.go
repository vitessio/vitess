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

// This file contains helper filter methods to process the unfiltered list of
// tablets returned by LegacyHealthCheck.GetTabletStatsFrom*.
// See also legacy_replicationlag.go for a more sophisicated filter used by vtgate.

// RemoveUnhealthyTablets filters all unhealthy tablets out.
// NOTE: Non-serving tablets are considered healthy.
func RemoveUnhealthyTablets(tabletStatsList []LegacyTabletStats) []LegacyTabletStats {
	result := make([]LegacyTabletStats, 0, len(tabletStatsList))
	for _, ts := range tabletStatsList {
		// Note we do not check the 'Serving' flag here.
		// This is mainly to avoid the case where we run a vtworker Diff between a
		// source and destination, and the source is not serving (disabled by
		// TabletControl). When we switch the tablet to 'worker', it will
		// go back to serving state.
		if ts.Stats == nil || ts.Stats.HealthError != "" || ts.LastError != nil || LegacyIsReplicationLagHigh(&ts) {
			continue
		}
		result = append(result, ts)
	}
	return result
}
