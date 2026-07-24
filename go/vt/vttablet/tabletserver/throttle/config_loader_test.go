/*
Copyright 2026 The Vitess Authors.

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

package throttle

import (
	"testing"

	"github.com/stretchr/testify/require"

	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

// TestConvertQueryThrottlerConfigToThrottlerConfig_PicksMinFloorWhenUnsorted
// verifies that an unsorted Thresholds slice still yields the true minimum
// threshold as the underlying throttler's floor. The function originally read
// thresholds[0].GetAbove() assuming sorted order; sanitizeQueryThrottlerConfig
// in grpcvtctldserver enforces that on the RPC write path but a direct topo
// write would bypass it. Sorting defensively on receipt here closes the gap.
func TestConvertQueryThrottlerConfigToThrottlerConfig_PicksMinFloorWhenUnsorted(t *testing.T) {
	// Thresholds intentionally OUT OF ORDER: a pre-fix read of thresholds[0]
	// would pick 50 (the first element). The true min is 10 — that's what the
	// underlying throttler must use as its floor.
	cfg := &querythrottlerpb.Config{
		Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		Enabled:  true,
		TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
			TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
				"PRIMARY": {
					StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
						"SELECT": {
							MetricRules: map[string]*querythrottlerpb.MetricRule{
								"lag": {
									Thresholds: []*querythrottlerpb.ThrottleThreshold{
										{Above: 50, Throttle: 100},
										{Above: 10, Throttle: 25},
										{Above: 25, Throttle: 50},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	tc := convertQueryThrottlerConfigToThrottlerConfig(cfg)
	require.NotNil(t, tc, "convert must return a non-nil ThrottlerConfig for TABLET_THROTTLER strategy")
	appName := throttlerapp.QueryThrottlerName.String()
	require.NotNil(t, tc.AppCheckedMetrics[appName], "AppCheckedMetrics for the query throttler app must be populated")
	require.Equal(t, []string{"lag"}, tc.AppCheckedMetrics[appName].GetNames())
	require.Equal(t, float64(10), tc.MetricThresholds["lag"],
		"underlying throttler floor must be the TRUE minimum threshold (10), not thresholds[0] (50) — defensive sort on receipt required")
}

// TestConvertQueryThrottlerConfigToThrottlerConfig_MinAcrossRules verifies the
// cross-rule MIN reduction: when the same metric appears in MULTIPLE rules
// (different tablet types, different statement types, or both), the underlying
// throttler's floor is the GLOBAL minimum across all of them, not whichever rule
// happens to be visited first by the map iteration. Independent metrics keep
// their own thresholds, and the AppCheckedMetrics name list deduplicates so the
// same metric is registered once even when it appears in many rules.
//
// This exercises the math.Min branch that the existing patch coverage missed.
func TestConvertQueryThrottlerConfigToThrottlerConfig_MinAcrossRules(t *testing.T) {
	// Same `lag` metric appears in TWO rules with different floors; an
	// independent `cpu` metric appears in a third rule.
	//   PRIMARY/SELECT/lag → floor 25
	//   PRIMARY/INSERT/cpu → floor 80 (different metric, independent)
	//   REPLICA/SELECT/lag → floor 5  ← global minimum across `lag` rules
	cfg := &querythrottlerpb.Config{
		Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		Enabled:  true,
		TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
			TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
				"PRIMARY": {
					StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
						"SELECT": {
							MetricRules: map[string]*querythrottlerpb.MetricRule{
								"lag": {Thresholds: []*querythrottlerpb.ThrottleThreshold{{Above: 25, Throttle: 50}}},
							},
						},
						"INSERT": {
							MetricRules: map[string]*querythrottlerpb.MetricRule{
								"cpu": {Thresholds: []*querythrottlerpb.ThrottleThreshold{{Above: 80, Throttle: 100}}},
							},
						},
					},
				},
				"REPLICA": {
					StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
						"SELECT": {
							MetricRules: map[string]*querythrottlerpb.MetricRule{
								"lag": {Thresholds: []*querythrottlerpb.ThrottleThreshold{{Above: 5, Throttle: 25}}},
							},
						},
					},
				},
			},
		},
	}

	tc := convertQueryThrottlerConfigToThrottlerConfig(cfg)
	require.NotNil(t, tc)

	require.Equal(t, float64(5), tc.MetricThresholds["lag"],
		"`lag` floor must be the GLOBAL minimum across all rules (5), not whichever rule was visited first (25)")
	require.Equal(t, float64(80), tc.MetricThresholds["cpu"],
		"independent metric `cpu` must keep its own threshold")

	appName := throttlerapp.QueryThrottlerName.String()
	require.NotNil(t, tc.AppCheckedMetrics[appName])
	names := tc.AppCheckedMetrics[appName].GetNames()
	require.Len(t, names, 2, "AppCheckedMetrics names must contain each unique metric exactly once (no duplicates from multiple rules)")
	require.ElementsMatch(t, []string{"lag", "cpu"}, names)
}

// TestConvertQueryThrottlerConfigToThrottlerConfig_ScopedMetricName verifies scope
// normalization for a scoped config key such as "shard/lag":
//   - AppCheckedMetrics keeps the SCOPED name so the underlying throttler checks the
//     correct scope.
//   - MetricThresholds is keyed by the BARE metric name ("lag"), because the throttler
//     applies thresholds per bare metric and convergeMetricThresholds only converges bare
//     names. Keying by the scoped name ("shard/lag") would be silently dropped by
//     convergeMetricThresholds, leaving the throttler on the inventory default.
func TestConvertQueryThrottlerConfigToThrottlerConfig_ScopedMetricName(t *testing.T) {
	cfg := &querythrottlerpb.Config{
		Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		Enabled:  true,
		TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
			TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
				"PRIMARY": {
					StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
						"SELECT": {
							MetricRules: map[string]*querythrottlerpb.MetricRule{
								"shard/lag": {
									Thresholds: []*querythrottlerpb.ThrottleThreshold{
										{Above: 15, Throttle: 100},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	tc := convertQueryThrottlerConfigToThrottlerConfig(cfg)
	require.NotNil(t, tc)

	appName := throttlerapp.QueryThrottlerName.String()
	require.NotNil(t, tc.AppCheckedMetrics[appName])
	require.Equal(t, []string{"shard/lag"}, tc.AppCheckedMetrics[appName].GetNames(),
		"AppCheckedMetrics must keep the scoped name so the throttler checks the shard scope")

	require.Equal(t, float64(15), tc.MetricThresholds["lag"],
		"threshold must be keyed by the bare metric name so convergeMetricThresholds honors it")
	_, scopedKeyPresent := tc.MetricThresholds["shard/lag"]
	require.False(t, scopedKeyPresent,
		"threshold must NOT be keyed by the scoped name — convergeMetricThresholds would drop it")
}
