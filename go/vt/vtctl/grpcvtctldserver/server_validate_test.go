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

package grpcvtctldserver

import (
	"testing"

	"github.com/stretchr/testify/require"

	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestValidateQueryThrottlerConfigRequest(t *testing.T) {
	validTabletThrottlerConfig := &querythrottlerpb.Config{
		Enabled:  true,
		Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
			TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
				"PRIMARY": {
					StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
						"SELECT": {
							MetricRules: map[string]*querythrottlerpb.MetricRule{
								"lag": {
									Thresholds: []*querythrottlerpb.ThrottleThreshold{
										{Above: 5.0, Throttle: 100},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	tests := []struct {
		name    string
		req     *vtctldatapb.UpdateQueryThrottlerConfigRequest
		wantErr string
	}{
		{
			name: "valid tablet throttler config",
			req: &vtctldatapb.UpdateQueryThrottlerConfigRequest{
				Keyspace:             "ks1",
				QueryThrottlerConfig: validTabletThrottlerConfig,
			},
		},
		{
			name: "disabled config with UNKNOWN strategy is valid",
			req: &vtctldatapb.UpdateQueryThrottlerConfigRequest{
				Keyspace:             "ks1",
				QueryThrottlerConfig: &querythrottlerpb.Config{Enabled: false},
			},
		},
		{
			name: "disabled config with zero-value proto is valid",
			req: &vtctldatapb.UpdateQueryThrottlerConfigRequest{
				Keyspace:             "ks1",
				QueryThrottlerConfig: &querythrottlerpb.Config{},
			},
		},
		{
			name: "empty keyspace",
			req: &vtctldatapb.UpdateQueryThrottlerConfigRequest{
				Keyspace:             "",
				QueryThrottlerConfig: validTabletThrottlerConfig,
			},
			wantErr: "keyspace is required",
		},
		{
			name: "nil config",
			req: &vtctldatapb.UpdateQueryThrottlerConfigRequest{
				Keyspace: "ks1",
			},
			wantErr: "query_throttler_config is required",
		},
		{
			name: "enabled with UNKNOWN strategy",
			req: &vtctldatapb.UpdateQueryThrottlerConfigRequest{
				Keyspace: "ks1",
				QueryThrottlerConfig: &querythrottlerpb.Config{
					Enabled:  true,
					Strategy: querythrottlerpb.ThrottlingStrategy_UNKNOWN,
				},
			},
			wantErr: "strategy must be TABLET_THROTTLER when enabled is true",
		},
		{
			name: "TABLET_THROTTLER without tablet_strategy_config",
			req: &vtctldatapb.UpdateQueryThrottlerConfigRequest{
				Keyspace: "ks1",
				QueryThrottlerConfig: &querythrottlerpb.Config{
					Enabled:  true,
					Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				},
			},
			wantErr: "tablet_strategy_config is required when strategy is TABLET_THROTTLER",
		},
		{
			name: "TABLET_THROTTLER with empty tablet_rules",
			req: &vtctldatapb.UpdateQueryThrottlerConfigRequest{
				Keyspace: "ks1",
				QueryThrottlerConfig: &querythrottlerpb.Config{
					Enabled:  true,
					Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
					TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
						TabletRules: map[string]*querythrottlerpb.StatementRuleSet{},
					},
				},
			},
			wantErr: "tablet_rules cannot be empty when strategy is TABLET_THROTTLER",
		},
		{
			name: "negative threshold above value",
			req: &vtctldatapb.UpdateQueryThrottlerConfigRequest{
				Keyspace: "ks1",
				QueryThrottlerConfig: &querythrottlerpb.Config{
					Enabled:  true,
					Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
					TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
						TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
							"PRIMARY": {
								StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
									"SELECT": {
										MetricRules: map[string]*querythrottlerpb.MetricRule{
											"lag": {
												Thresholds: []*querythrottlerpb.ThrottleThreshold{
													{Above: -1.0, Throttle: 100},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			wantErr: "threshold[0] 'above' must be >= 0, got -1 (tablet_type=PRIMARY, statement=SELECT, metric=lag)",
		},
		{
			name: "throttle percentage out of range (above 100)",
			req: &vtctldatapb.UpdateQueryThrottlerConfigRequest{
				Keyspace: "ks1",
				QueryThrottlerConfig: &querythrottlerpb.Config{
					Enabled:  true,
					Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
					TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
						TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
							"PRIMARY": {
								StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
									"SELECT": {
										MetricRules: map[string]*querythrottlerpb.MetricRule{
											"lag": {
												Thresholds: []*querythrottlerpb.ThrottleThreshold{
													{Above: 5.0, Throttle: 150},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			wantErr: "threshold[0] 'throttle' must be between 0 and 100, got 150 (tablet_type=PRIMARY, statement=SELECT, metric=lag)",
		},
		{
			name: "throttle percentage negative",
			req: &vtctldatapb.UpdateQueryThrottlerConfigRequest{
				Keyspace: "ks1",
				QueryThrottlerConfig: &querythrottlerpb.Config{
					Enabled:  true,
					Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
					TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
						TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
							"PRIMARY": {
								StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
									"SELECT": {
										MetricRules: map[string]*querythrottlerpb.MetricRule{
											"lag": {
												Thresholds: []*querythrottlerpb.ThrottleThreshold{
													{Above: 5.0, Throttle: -10},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			wantErr: "threshold[0] 'throttle' must be between 0 and 100, got -10 (tablet_type=PRIMARY, statement=SELECT, metric=lag)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateQueryThrottlerConfigRequest(tt.req)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.wantErr)
			}
		})
	}
}

func TestSanitizeQueryThrottlerConfig(t *testing.T) {
	tests := []struct {
		name           string
		cfg            *querythrottlerpb.Config
		wantThresholds []*querythrottlerpb.ThrottleThreshold
	}{
		{
			name: "sorts thresholds ascending by Above",
			cfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
					TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
						"PRIMARY": {
							StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
								"SELECT": {
									MetricRules: map[string]*querythrottlerpb.MetricRule{
										"lag": {
											Thresholds: []*querythrottlerpb.ThrottleThreshold{
												{Above: 50, Throttle: 100},
												{Above: 10, Throttle: 50},
												{Above: 30, Throttle: 75},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			wantThresholds: []*querythrottlerpb.ThrottleThreshold{
				{Above: 10, Throttle: 50},
				{Above: 30, Throttle: 75},
				{Above: 50, Throttle: 100},
			},
		},
		{
			name: "already sorted thresholds unchanged",
			cfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
					TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
						"PRIMARY": {
							StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
								"SELECT": {
									MetricRules: map[string]*querythrottlerpb.MetricRule{
										"lag": {
											Thresholds: []*querythrottlerpb.ThrottleThreshold{
												{Above: 5, Throttle: 10},
												{Above: 15, Throttle: 50},
												{Above: 25, Throttle: 100},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			wantThresholds: []*querythrottlerpb.ThrottleThreshold{
				{Above: 5, Throttle: 10},
				{Above: 15, Throttle: 50},
				{Above: 25, Throttle: 100},
			},
		},
		{
			name: "single threshold is a no-op",
			cfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
					TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
						"PRIMARY": {
							StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
								"SELECT": {
									MetricRules: map[string]*querythrottlerpb.MetricRule{
										"lag": {
											Thresholds: []*querythrottlerpb.ThrottleThreshold{
												{Above: 10, Throttle: 100},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			wantThresholds: []*querythrottlerpb.ThrottleThreshold{
				{Above: 10, Throttle: 100},
			},
		},
		{
			name:           "nil config is a no-op",
			cfg:            nil,
			wantThresholds: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				sanitizeQueryThrottlerConfig(tt.cfg)
			})

			if tt.wantThresholds == nil {
				return
			}

			thresholds := tt.cfg.TabletStrategyConfig.TabletRules["PRIMARY"].
				StatementRules["SELECT"].MetricRules["lag"].Thresholds
			require.Len(t, thresholds, len(tt.wantThresholds))
			for i, want := range tt.wantThresholds {
				require.Equal(t, want.Above, thresholds[i].Above)
				require.Equal(t, want.Throttle, thresholds[i].Throttle)
			}
		})
	}
}
