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
	"math"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

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
			t.Cleanup(func() { updateQueryThrottlerConfigOptions = saved })

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
		{
			name: "unknown metric name",
			config: &querythrottler.Config{
				Enabled:  true,
				Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
				TabletStrategyConfig: &querythrottler.TabletStrategyConfig{
					TabletRules: map[string]*querythrottler.StatementRuleSet{
						"PRIMARY": {
							StatementRules: map[string]*querythrottler.MetricRuleSet{
								"SELECT": {
									MetricRules: map[string]*querythrottler.MetricRule{
										"lags": {
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
			wantErr: `unknown metric name "lags" (tablet_type=PRIMARY, statement=SELECT)`,
		},
		{
			name: "unknown tablet type",
			config: &querythrottler.Config{
				Enabled:  true,
				Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
				TabletStrategyConfig: &querythrottler.TabletStrategyConfig{
					TabletRules: map[string]*querythrottler.StatementRuleSet{
						"PRIMAY": {
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
			wantErr: `unsupported tablet type "PRIMAY"; must be one of PRIMARY, REPLICA, RDONLY, EXPERIMENTAL`,
		},
		{
			name: "unknown statement type",
			config: &querythrottler.Config{
				Enabled:  true,
				Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
				TabletStrategyConfig: &querythrottler.TabletStrategyConfig{
					TabletRules: map[string]*querythrottler.StatementRuleSet{
						"PRIMARY": {
							StatementRules: map[string]*querythrottler.MetricRuleSet{
								"SELEC": {
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
			wantErr: `unsupported statement type "SELEC" (tablet_type=PRIMARY)`,
		},
		{
			name: "custom metric rejected",
			config: &querythrottler.Config{
				Enabled:  true,
				Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
				TabletStrategyConfig: &querythrottler.TabletStrategyConfig{
					TabletRules: map[string]*querythrottler.StatementRuleSet{
						"PRIMARY": {
							StatementRules: map[string]*querythrottler.MetricRuleSet{
								"SELECT": {
									MetricRules: map[string]*querythrottler.MetricRule{
										"custom": {
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
			wantErr: `custom metric is not supported by the query throttler (tablet_type=PRIMARY, statement=SELECT, metric=custom)`,
		},
		{
			name: "duplicate bare metric across scopes rejected",
			config: &querythrottler.Config{
				Enabled:  true,
				Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
				TabletStrategyConfig: &querythrottler.TabletStrategyConfig{
					TabletRules: map[string]*querythrottler.StatementRuleSet{
						"PRIMARY": {
							StatementRules: map[string]*querythrottler.MetricRuleSet{
								"SELECT": {
									MetricRules: map[string]*querythrottler.MetricRule{
										"self/lag": {
											Thresholds: []*querythrottler.ThrottleThreshold{
												{Above: 5.0, Throttle: 100},
											},
										},
									},
								},
								"INSERT": {
									MetricRules: map[string]*querythrottler.MetricRule{
										"shard/lag": {
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
			wantErr: `metric "lag" is configured under multiple scopes; only one scope of a metric may be configured across the query throttler config`,
		},
		{
			name:   "RDONLY tablet type and REPLACE statement type are accepted",
			config: singleRuleConfig("RDONLY", "REPLACE", &querythrottler.ThrottleThreshold{Above: 5, Throttle: 50}),
		},
		{
			name:   "EXPERIMENTAL tablet type is accepted",
			config: singleRuleConfig("EXPERIMENTAL", "SELECT", &querythrottler.ThrottleThreshold{Above: 5, Throttle: 50}),
		},
		{
			name:    "non-serving tablet type rejected",
			config:  singleRuleConfig("SPARE", "SELECT", &querythrottler.ThrottleThreshold{Above: 5, Throttle: 50}),
			wantErr: `unsupported tablet type "SPARE"; must be one of PRIMARY, REPLICA, RDONLY, EXPERIMENTAL`,
		},
		{
			name:    "BATCH alias rejected",
			config:  singleRuleConfig("BATCH", "SELECT", &querythrottler.ThrottleThreshold{Above: 5, Throttle: 50}),
			wantErr: `unsupported tablet type "BATCH"; must be one of PRIMARY, REPLICA, RDONLY, EXPERIMENTAL`,
		},
		{
			name:    "statement type that never reaches the query throttler rejected",
			config:  singleRuleConfig("PRIMARY", "BEGIN", &querythrottler.ThrottleThreshold{Above: 5, Throttle: 50}),
			wantErr: `unsupported statement type "BEGIN" (tablet_type=PRIMARY)`,
		},
		{
			name: "duplicate above rejected",
			config: singleRuleConfig("PRIMARY", "INSERT",
				&querythrottler.ThrottleThreshold{Above: 5, Throttle: 50},
				&querythrottler.ThrottleThreshold{Above: 10, Throttle: 80},
				&querythrottler.ThrottleThreshold{Above: 5, Throttle: 100},
			),
			wantErr: `threshold[0] and threshold[2] have the same 'above' 5 (tablet_type=PRIMARY, statement=INSERT, metric=lag)`,
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

// singleRuleConfig returns an enabled TABLET_THROTTLER config with one lag rule.
func singleRuleConfig(tabletType, stmtType string, thresholds ...*querythrottler.ThrottleThreshold) *querythrottler.Config {
	return &querythrottler.Config{
		Enabled:  true,
		Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
		TabletStrategyConfig: &querythrottler.TabletStrategyConfig{
			TabletRules: map[string]*querythrottler.StatementRuleSet{
				tabletType: {
					StatementRules: map[string]*querythrottler.MetricRuleSet{
						stmtType: {
							MetricRules: map[string]*querythrottler.MetricRule{
								"lag": {Thresholds: thresholds},
							},
						},
					},
				},
			},
		},
	}
}

// TestUpdateQueryThrottlerConfigExamples keeps the help examples working: every --config
// in UpdateQueryThrottlerConfig.Example must parse and pass validation.
func TestUpdateQueryThrottlerConfigExamples(t *testing.T) {
	matches := regexp.MustCompile(`--config '([^']*)'`).FindAllStringSubmatch(UpdateQueryThrottlerConfig.Example, -1)
	require.Len(t, matches, 2)
	for _, m := range matches {
		cfg, err := parseQueryThrottlerConfig([]byte(m[1]))
		require.NoError(t, err, m[1])
		require.NoError(t, validateQueryThrottlerConfigContent(cfg), m[1])
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
		{
			name: "camelCase field names",
			json: `{"enabled": true, "strategy": "TABLET_THROTTLER", "dryRun": true}`,
			wantConfig: &querythrottler.Config{
				Enabled:  true,
				Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   true,
			},
		},
		// A misspelled key must fail rather than be dropped: dropping "dry-run" would push the
		// config in enforcing mode.
		{
			name:    "misspelled dry_run",
			json:    `{"enabled": true, "strategy": "TABLET_THROTTLER", "dry-run": true}`,
			wantErr: true,
		},
		{
			name:    "unknown nested field",
			json:    `{"enabled": true, "strategy": "TABLET_THROTTLER", "tablet_strategy_config": {"tablet_rule": {}}}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := parseQueryThrottlerConfig([]byte(tt.json))
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

	config, err := parseQueryThrottlerConfig(data)
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

// TestValidateQueryThrottlerConfigContent_RejectsUnenforceableRules covers configs that would be
// accepted as enabled while containing a rule that can never throttle a query.
func TestValidateQueryThrottlerConfigContent_RejectsUnenforceableRules(t *testing.T) {
	// config builds an enabled TABLET_THROTTLER config with a single PRIMARY rule set.
	config := func(stmtRules map[string]*querythrottler.MetricRuleSet) *querythrottler.Config {
		return &querythrottler.Config{
			Enabled:  true,
			Strategy: querythrottler.ThrottlingStrategy_TABLET_THROTTLER,
			TabletStrategyConfig: &querythrottler.TabletStrategyConfig{
				TabletRules: map[string]*querythrottler.StatementRuleSet{
					"PRIMARY": {StatementRules: stmtRules},
				},
			},
		}
	}
	// rule builds a single-metric rule set for stmtType.
	rule := func(stmtType string, thresholds ...*querythrottler.ThrottleThreshold) map[string]*querythrottler.MetricRuleSet {
		return map[string]*querythrottler.MetricRuleSet{
			stmtType: {MetricRules: map[string]*querythrottler.MetricRule{
				"lag": {Thresholds: thresholds},
			}},
		}
	}

	tests := []struct {
		name    string
		config  *querythrottler.Config
		wantErr string
	}{
		{
			name:    "empty statement_rules",
			config:  config(nil),
			wantErr: "statement_rules cannot be empty (tablet_type=PRIMARY)",
		},
		{
			name:    "empty metric_rules",
			config:  config(map[string]*querythrottler.MetricRuleSet{"SELECT": {}}),
			wantErr: "metric_rules cannot be empty (tablet_type=PRIMARY, statement=SELECT)",
		},
		{
			name: "nil metric rule",
			config: config(map[string]*querythrottler.MetricRuleSet{
				"SELECT": {MetricRules: map[string]*querythrottler.MetricRule{"lag": nil}},
			}),
			wantErr: "thresholds cannot be empty (tablet_type=PRIMARY, statement=SELECT, metric=lag)",
		},
		{
			name:    "empty thresholds",
			config:  config(rule("SELECT")),
			wantErr: "thresholds cannot be empty (tablet_type=PRIMARY, statement=SELECT, metric=lag)",
		},
		{
			name:    "NaN above",
			config:  config(rule("SELECT", &querythrottler.ThrottleThreshold{Above: math.NaN(), Throttle: 100})),
			wantErr: "threshold[0] 'above' must be a finite number, got NaN",
		},
		{
			name:    "positive infinity above",
			config:  config(rule("SELECT", &querythrottler.ThrottleThreshold{Above: math.Inf(1), Throttle: 100})),
			wantErr: "threshold[0] 'above' must be a finite number, got +Inf",
		},
		{
			name:    "negative infinity above",
			config:  config(rule("SELECT", &querythrottler.ThrottleThreshold{Above: math.Inf(-1), Throttle: 100})),
			wantErr: "threshold[0] 'above' must be a finite number, got -Inf",
		},
	}
	// DDL and MIGRATION plans reach the throttler without a parsed query, and their rules
	// still apply, so they are valid statement types like the data-plane ones.
	for _, stmtType := range []string{"SELECT", "INSERT", "REPLACE", "UPDATE", "DELETE", "DDL", "MIGRATION"} {
		tests = append(tests, struct {
			name    string
			config  *querythrottler.Config
			wantErr string
		}{
			name:   stmtType + " statement type is supported",
			config: config(rule(stmtType, &querythrottler.ThrottleThreshold{Above: 5, Throttle: 100})),
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateQueryThrottlerConfigContent(tt.config)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
