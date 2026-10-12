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
	"google.golang.org/protobuf/proto"

	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

// An unsorted Thresholds slice must still yield the true minimum as the floor, with the
// input left unmutated. Only the RPC write path sorts, so a direct topo write can arrive
// unordered — and the config is the shared srvtopo proto, so we scan instead of sorting.
func TestConvertQueryThrottlerConfigToThrottlerConfig_PicksMinFloorWhenUnsorted(t *testing.T) {
	// Out of order on purpose: reading thresholds[0] picks 50, but the floor must be 10.
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

	// srvtopo shares this pointer with every other reader, so convert must not reorder it.
	before := cfg.CloneVT()

	tc := convertQueryThrottlerConfigToThrottlerConfig(cfg)
	require.NotNil(t, tc, "convert must return a non-nil ThrottlerConfig for TABLET_THROTTLER strategy")
	appName := throttlerapp.QueryThrottlerName.String()
	require.NotNil(t, tc.AppCheckedMetrics[appName], "AppCheckedMetrics for the query throttler app must be populated")
	require.Equal(t, []string{"lag"}, tc.AppCheckedMetrics[appName].GetNames())
	require.Equal(t, float64(10), tc.MetricThresholds["lag"],
		"floor must be the TRUE minimum threshold (10), not thresholds[0] (50)")

	require.True(t, proto.Equal(before, cfg),
		"convert must not mutate the caller's config; srvtopo shares it with other readers")
}

// When one metric appears in several rules, the floor must be the global minimum across
// all of them, not whichever rule the map iteration reached first. Independent metrics
// keep their own thresholds, and AppCheckedMetrics lists each metric only once.
func TestConvertQueryThrottlerConfigToThrottlerConfig_MinAcrossRules(t *testing.T) {
	//   PRIMARY/SELECT/lag → floor 25
	//   PRIMARY/INSERT/cpu → floor 80 (independent metric)
	//   REPLICA/SELECT/lag → floor 5  ← global minimum for `lag`
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

// A scoped config key like "shard/lag" splits two ways: AppCheckedMetrics keeps the
// scoped name so the throttler checks the right scope, while MetricThresholds is keyed by
// the bare name — convergeMetricThresholds only handles bare names and would drop the
// scoped one, leaving the throttler on its inventory default.
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
