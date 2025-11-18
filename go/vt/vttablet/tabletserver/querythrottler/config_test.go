package querythrottler

import (
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/proto/querythrottler"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"
)

// Test_ConfigFromProto tests conversion from protobuf to internal Config struct.
func Test_ConfigFromProto(t *testing.T) {
	tests := []struct {
		name           string
		protoConfig    *querythrottler.Config
		expectedConfig Config
	}{
		{
			name: "EnabledWithTabletStrategy",
			protoConfig: &querythrottler.Config{
				Enabled:  true,
				DryRun:   false,
				Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
				TabletStrategyConfig: &querythrottler.TabletStrategyConfig{
					TabletRules: make(map[string]*querythrottler.StatementRuleSet),
				},
			},
			expectedConfig: Config{
				Enabled:      true,
				DryRun:       false,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
			},
		},
		{
			name: "DisabledDryRun",
			protoConfig: &querythrottler.Config{
				Enabled:  false,
				DryRun:   true,
				Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
				TabletStrategyConfig: &querythrottler.TabletStrategyConfig{
					TabletRules: make(map[string]*querythrottler.StatementRuleSet),
				},
			},
			expectedConfig: Config{
				Enabled:      false,
				DryRun:       true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
			},
		},
		{
			name: "UnknownStrategy",
			protoConfig: &querythrottler.Config{
				Enabled:  true,
				Strategy: querythrottler.ThrottlingStrategy(-1), // Invalid strategy
				TabletStrategyConfig: &querythrottler.TabletStrategyConfig{
					TabletRules: make(map[string]*querythrottler.StatementRuleSet),
				},
			},
			expectedConfig: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyUnknown,
			},
		},
		{
			name: "NilTabletStrategyConfig",
			protoConfig: &querythrottler.Config{
				Enabled:              true,
				Strategy:             querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
				TabletStrategyConfig: nil,
			},
			expectedConfig: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
			},
		},
		{
			name: "WithMetricThresholds",
			protoConfig: &querythrottler.Config{
				Enabled:  true,
				Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
				TabletStrategyConfig: &querythrottler.TabletStrategyConfig{
					TabletRules: map[string]*querythrottler.StatementRuleSet{
						"PRIMARY": {
							StatementRules: map[string]*querythrottler.MetricRuleSet{
								"SELECT": {
									MetricRules: map[string]*querythrottler.MetricRule{
										"lag": {
											Thresholds: []*querythrottler.ThrottleThreshold{
												{Above: 100, Throttle: 50},
												{Above: 200, Throttle: 75},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedConfig: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
			},
		},
		{
			name: "MultipleTabletTypes",
			protoConfig: &querythrottler.Config{
				Enabled:  true,
				Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
				TabletStrategyConfig: &querythrottler.TabletStrategyConfig{
					TabletRules: map[string]*querythrottler.StatementRuleSet{
						"PRIMARY": {
							StatementRules: map[string]*querythrottler.MetricRuleSet{
								"SELECT": {
									MetricRules: map[string]*querythrottler.MetricRule{
										"lag": {
											Thresholds: []*querythrottler.ThrottleThreshold{
												{Above: 100, Throttle: 50},
												{Above: 200, Throttle: 75},
											},
										},
									},
								},
							},
						},
						"REPLICA": {
							StatementRules: map[string]*querythrottler.MetricRuleSet{
								"SELECT": {
									MetricRules: map[string]*querythrottler.MetricRule{
										"lag": {
											Thresholds: []*querythrottler.ThrottleThreshold{
												{Above: 100, Throttle: 50},
												{Above: 200, Throttle: 75},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedConfig: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := ConfigFromProto(tt.protoConfig)

			require.Equal(t, tt.expectedConfig, cfg, "Config should match expected value")
		})
	}
}
