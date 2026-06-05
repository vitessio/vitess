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
