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

package throttler

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	throttlerdatapb "vitess.io/vitess/go/vt/proto/throttlerdata"
)

// MaxReplicationLagModuleConfig stores all configuration parameters for
// MaxReplicationLagModule. Internally, the parameters are represented by a
// protobuf message. This message is also used to update the parameters.
type MaxReplicationLagModuleConfig struct {
	*throttlerdatapb.Configuration
}

func (cfg MaxReplicationLagModuleConfig) Clone() MaxReplicationLagModuleConfig {
	return MaxReplicationLagModuleConfig{
		proto.Clone(cfg.Configuration).(*throttlerdatapb.Configuration),
	}
}

// Most of the values are based on the assumption that vttablet is started
// with the flag --health_check_interval=20s.
const healthCheckInterval = 20

var defaultMaxReplicationLagModuleConfig = MaxReplicationLagModuleConfig{
	&throttlerdatapb.Configuration{
		TargetReplicationLagSec: 2,
		MaxReplicationLagSec:    ReplicationLagModuleDisabled,

		InitialRate: 100,
		// 1 means 100% i.e. double rates by default.
		MaxIncrease:       1,
		EmergencyDecrease: 0.5,

		// Wait for two health broadcast rounds. Otherwise, the "decrease" mode
		// has less than 2 lag records available to calculate the actual replication rate.
		MinDurationBetweenIncreasesSec: 2 * healthCheckInterval,
		// MaxDurationBetweenIncreasesSec defaults to 60+2 seconds because this
		// corresponds to 3 broadcasts.
		// The 2 extra seconds give us headroom to account for delay in the process.
		MaxDurationBetweenIncreasesSec: 3*healthCheckInterval + 2,
		MinDurationBetweenDecreasesSec: healthCheckInterval,
		SpreadBacklogAcrossSec:         healthCheckInterval,

		AgeBadRateAfterSec:       3 * 60,
		BadRateIncrease:          0.10,
		MaxRateApproachThreshold: 0.90,
	},
}

// DefaultMaxReplicationLagModuleConfig returns a copy of the default config object.
func DefaultMaxReplicationLagModuleConfig() MaxReplicationLagModuleConfig {
	return defaultMaxReplicationLagModuleConfig.Clone()
}

// NewMaxReplicationLagModuleConfig returns a default configuration where
// only "maxReplicationLag" is set.
func NewMaxReplicationLagModuleConfig(maxReplicationLag int64) MaxReplicationLagModuleConfig {
	config := defaultMaxReplicationLagModuleConfig.Clone()
	config.MaxReplicationLagSec = maxReplicationLag
	return config
}

// TODO(mberlin): Add method which updates the config using a (partially) filled
// in protobuf.

// Verify returns an error if the config is invalid.
func (cfg MaxReplicationLagModuleConfig) Verify() error {
	if cfg.TargetReplicationLagSec < 1 {
		return fmt.Errorf("target_replication_lag_sec must be >= 1")
	}
	if cfg.MaxReplicationLagSec < 2 {
		return fmt.Errorf("max_replication_lag_sec must be >= 2")
	}
	if cfg.TargetReplicationLagSec > cfg.MaxReplicationLagSec {
		return fmt.Errorf("target_replication_lag_sec must not be higher than max_replication_lag_sec: invalid: %v > %v",
			cfg.TargetReplicationLagSec, cfg.MaxReplicationLagSec)
	}
	if cfg.InitialRate < 1 {
		return fmt.Errorf("initial_rate must be >= 1")
	}
	if cfg.MaxIncrease <= 0 {
		return fmt.Errorf("max_increase must be > 0")
	}
	if cfg.EmergencyDecrease <= 0 {
		return fmt.Errorf("emergency_decrease must be > 0")
	}
	if cfg.MinDurationBetweenIncreasesSec < 1 {
		return fmt.Errorf("min_duration_between_increases_sec must be >= 1")
	}
	if cfg.MaxDurationBetweenIncreasesSec < 1 {
		return fmt.Errorf("max_duration_between_increases_sec must be >= 1")
	}
	if cfg.MinDurationBetweenDecreasesSec < 1 {
		return fmt.Errorf("min_duration_between_decreases_sec must be >= 1")
	}
	if cfg.SpreadBacklogAcrossSec < 1 {
		return fmt.Errorf("spread_backlog_across_sec must be >= 1")
	}
	if cfg.IgnoreNSlowestReplicas < 0 {
		return fmt.Errorf("ignore_n_slowest_replicas must be >= 0")
	}
	if cfg.IgnoreNSlowestRdonlys < 0 {
		return fmt.Errorf("ignore_n_slowest_rdonlys must be >= 0")
	}
	if cfg.AgeBadRateAfterSec < 1 {
		return fmt.Errorf("age_bad_rate_after_sec must be >= 1")
	}
	if cfg.MaxRateApproachThreshold < 0 {
		return fmt.Errorf("max_rate_approach_threshold must be >=0")
	}
	if cfg.MaxRateApproachThreshold > 1 {
		return fmt.Errorf("max_rate_approach_threshold must be <=1")
	}
	return nil
}

// MinDurationBetweenIncreases is a helper function which returns the respective
// protobuf field as native Go type.
func (cfg MaxReplicationLagModuleConfig) MinDurationBetweenIncreases() time.Duration {
	return time.Duration(cfg.MinDurationBetweenIncreasesSec) * time.Second
}

// MaxDurationBetweenIncreases is a helper function which returns the respective
// protobuf field as native Go type.
func (cfg MaxReplicationLagModuleConfig) MaxDurationBetweenIncreases() time.Duration {
	return time.Duration(cfg.MaxDurationBetweenIncreasesSec) * time.Second
}

// MinDurationBetweenDecreases is a helper function which returns the respective
// protobuf field as native Go type.
func (cfg MaxReplicationLagModuleConfig) MinDurationBetweenDecreases() time.Duration {
	return time.Duration(cfg.MinDurationBetweenDecreasesSec) * time.Second
}

// SpreadBacklogAcross is a helper function which returns the respective
// protobuf field as native Go type.
func (cfg MaxReplicationLagModuleConfig) SpreadBacklogAcross() time.Duration {
	return time.Duration(cfg.SpreadBacklogAcrossSec) * time.Second
}

// AgeBadRateAfter is a helper function which returns the respective
// protobuf field as native Go type.
func (cfg MaxReplicationLagModuleConfig) AgeBadRateAfter() time.Duration {
	return time.Duration(cfg.AgeBadRateAfterSec) * time.Second
}
