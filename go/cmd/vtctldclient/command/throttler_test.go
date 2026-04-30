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

package command

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/test/utils"

	"vitess.io/vitess/go/vt/proto/querythrottler"
)

func TestValidateUpdateQueryThrottlerConfigFlags(t *testing.T) {
	tests := []struct {
		name       string
		config     string
		configFile string
		wantErr    string
	}{
		{
			name:    "config flag only is valid",
			config:  `{"enabled":true}`,
			wantErr: "",
		},
		{
			name:       "config-file flag only is valid",
			configFile: "/tmp/some-file.json",
			wantErr:    "",
		},
		{
			name:    "neither flag set",
			wantErr: "must pass exactly one of --config or --config-file",
		},
		{
			name:       "both flags set",
			config:     `{"enabled":true}`,
			configFile: "/tmp/some-file.json",
			wantErr:    "must pass exactly one of --config or --config-file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save and restore the global options
			saved := updateQueryThrottlerConfigOptions
			defer func() { updateQueryThrottlerConfigOptions = saved }()

			updateQueryThrottlerConfigOptions.Config = tt.config
			updateQueryThrottlerConfigOptions.ConfigFile = tt.configFile

			err := validateUpdateQueryThrottlerConfig(UpdateQueryThrottlerConfig, []string{"test_keyspace"})
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.wantErr)
			}
		})
	}
}

func TestValidateQueryThrottlerConfigContent(t *testing.T) {
	tests := []struct {
		name    string
		config  *querythrottler.Config
		wantErr string
	}{
		{
			name: "valid enabled config",
			config: &querythrottler.Config{
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
												{Above: 5.0, Throttle: 100},
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
		{
			name:   "disabled config is always valid",
			config: &querythrottler.Config{Enabled: false},
		},
		{
			name: "enabled with UNKNOWN strategy",
			config: &querythrottler.Config{
				Enabled:  true,
				Strategy: querythrottler.ThrottlingStrategy_UNKNOWN,
			},
			wantErr: "strategy must be TABLET_THROTTLER when enabled is true",
		},
		{
			name: "enabled without tablet_strategy_config",
			config: &querythrottler.Config{
				Enabled:  true,
				Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
			},
			wantErr: "tablet_strategy_config is required when strategy is TABLET_THROTTLER",
		},
		{
			name: "enabled with empty tablet_rules",
			config: &querythrottler.Config{
				Enabled:  true,
				Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
				TabletStrategyConfig: &querythrottler.TabletStrategyConfig{
					TabletRules: map[string]*querythrottler.StatementRuleSet{},
				},
			},
			wantErr: "tablet_rules cannot be empty when strategy is TABLET_THROTTLER",
		},
		{
			name: "negative threshold above",
			config: &querythrottler.Config{
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
			wantErr: "threshold[0] 'above' must be >= 0, got -1 (tablet_type=PRIMARY, statement=SELECT, metric=lag)",
		},
		{
			name: "throttle percentage over 100",
			config: &querythrottler.Config{
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
			wantErr: "threshold[0] 'throttle' must be between 0 and 100, got 150 (tablet_type=PRIMARY, statement=SELECT, metric=lag)",
		},
		{
			name: "above zero is valid (boundary)",
			config: &querythrottler.Config{
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
												{Above: 0, Throttle: 50},
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
		{
			name: "throttle zero is valid (boundary)",
			config: &querythrottler.Config{
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
												{Above: 5.0, Throttle: 0},
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
		{
			name: "throttle one hundred is valid (boundary)",
			config: &querythrottler.Config{
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
												{Above: 5.0, Throttle: 100},
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
		{
			name: "second threshold fails (validates loop iterates past index 0)",
			config: &querythrottler.Config{
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
												{Above: 5.0, Throttle: 50},
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
			wantErr: "threshold[1] 'above' must be >= 0, got -1 (tablet_type=PRIMARY, statement=SELECT, metric=lag)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateQueryThrottlerConfigContent(tt.config)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestQueryThrottlerConfigJSONParsing(t *testing.T) {
	tests := []struct {
		name       string
		json       string
		wantConfig *querythrottler.Config
		wantErr    bool
	}{
		{
			name: "full config with tablet strategy",
			json: `{
				"enabled": true,
				"strategy": "TABLET_THROTTLER",
				"dry_run": false,
				"tablet_strategy_config": {
					"tablet_rules": {
						"PRIMARY": {
							"statement_rules": {
								"SELECT": {
									"metric_rules": {
										"lag": {
											"thresholds": [
												{"above": 5.0, "throttle": 100}
											]
										}
									}
								}
							}
						}
					}
				}
			}`,
			wantConfig: &querythrottler.Config{
				Enabled:  true,
				Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   false,
				TabletStrategyConfig: &querythrottler.TabletStrategyConfig{
					TabletRules: map[string]*querythrottler.StatementRuleSet{
						"PRIMARY": {
							StatementRules: map[string]*querythrottler.MetricRuleSet{
								"SELECT": {
									MetricRules: map[string]*querythrottler.MetricRule{
										"lag": {
											Thresholds: []*querythrottler.ThrottleThreshold{
												{Above: 5.0, Throttle: 100},
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
		{
			name: "disabled config",
			json: `{"enabled": false}`,
			wantConfig: &querythrottler.Config{
				Enabled: false,
			},
		},
		{
			name: "dry run config",
			json: `{"enabled": true, "strategy": "TABLET_THROTTLER", "dry_run": true, "tablet_strategy_config": {"tablet_rules": {"PRIMARY": {"statement_rules": {"SELECT": {"metric_rules": {"lag": {"thresholds": [{"above": 5.0, "throttle": 100}]}}}}}}}}`,
			wantConfig: &querythrottler.Config{
				Enabled:  true,
				Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   true,
				TabletStrategyConfig: &querythrottler.TabletStrategyConfig{
					TabletRules: map[string]*querythrottler.StatementRuleSet{
						"PRIMARY": {
							StatementRules: map[string]*querythrottler.MetricRuleSet{
								"SELECT": {
									MetricRules: map[string]*querythrottler.MetricRule{
										"lag": {
											Thresholds: []*querythrottler.ThrottleThreshold{
												{Above: 5.0, Throttle: 100},
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
		{
			name:    "invalid json",
			json:    `{not valid json}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &querythrottler.Config{}
			err := json2.UnmarshalPB([]byte(tt.json), config)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			utils.MustMatch(t, tt.wantConfig, config)
		})
	}
}

func TestQueryThrottlerConfigFromFile(t *testing.T) {
	configJSON := `{"enabled": true, "strategy": "TABLET_THROTTLER", "tablet_strategy_config": {"tablet_rules": {"PRIMARY": {"statement_rules": {"SELECT": {"metric_rules": {"lag": {"thresholds": [{"above": 5.0, "throttle": 100}]}}}}}}}}`

	tmpFile := filepath.Join(t.TempDir(), "config.json")
	err := os.WriteFile(tmpFile, []byte(configJSON), 0o644)
	require.NoError(t, err)

	data, err := os.ReadFile(tmpFile)
	require.NoError(t, err)

	config := &querythrottler.Config{}
	err = json2.UnmarshalPB(data, config)
	require.NoError(t, err)

	utils.MustMatch(t, &querythrottler.Config{
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
										{Above: 5.0, Throttle: 100},
									},
								},
							},
						},
					},
				},
			},
		},
	}, config)
}
