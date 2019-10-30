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

	throttlerdatapb "vitess.io/vitess/go/vt/proto/throttlerdata"
)

// MaxReplicationLagModuleConfig stores all configuration parameters for
// MaxReplicationLagModule. Internally, the parameters are represented by a
// protobuf message. This message is also used to update the parameters.
type MaxReplicationLagModuleConfig struct {
	throttlerdatapb.Configuration
}

// Most of the values are based on the assumption that vttablet is started
// with the flag --health_check_interval=20s.
const healthCheckInterval = 20

var defaultMaxReplicationLagModuleConfig = MaxReplicationLagModuleConfig{
	throttlerdatapb.Configuration{
		TargetReplicationLagSec: 2,
		MaxReplicationLagSec:    ReplicationLagModuleDisabled,

		InitialRate: 100,
		// 1 means 100% i.e. double rates by default.
		MaxIncrease:       1,
		EmergencyDecrease: 0.5,

		// Wait for two health broadcast rounds. Otherwise, the "decrease" mode
		// has less than 2 lag records available to calculate the actual slave rate.
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
	return defaultMaxReplicationLagModuleConfig
}

// NewMaxReplicationLagModuleConfig returns a default configuration where
// only "maxReplicationLag" is set.
func NewMaxReplicationLagModuleConfig(maxReplicationLag int64) MaxReplicationLagModuleConfig {
	config := defaultMaxReplicationLagModuleConfig
	config.MaxReplicationLagSec = maxReplicationLag
	return config
}

// TODO(mberlin): Add method which updates the config using a (partially) filled
// in protobuf.

// Verify returns an error if the config is invalid.
func (c MaxReplicationLagModuleConfig) Verify() error {
	if c.TargetReplicationLagSec < 1 {
		return fmt.Errorf("target_replication_lag_sec must be >= 1")
	}
	if c.MaxReplicationLagSec < 2 {
		return fmt.Errorf("max_replication_lag_sec must be >= 2")
	}
	if c.TargetReplicationLagSec > c.MaxReplicationLagSec {
		return fmt.Errorf("target_replication_lag_sec must not be higher than max_replication_lag_sec: invalid: %v > %v",
			c.TargetReplicationLagSec, c.MaxReplicationLagSec)
	}
	if c.InitialRate < 1 {
		return fmt.Errorf("initial_rate must be >= 1")
	}
	if c.MaxIncrease <= 0 {
		return fmt.Errorf("max_increase must be > 0")
	}
	if c.EmergencyDecrease <= 0 {
		return fmt.Errorf("emergency_decrease must be > 0")
	}
	if c.MinDurationBetweenIncreasesSec < 1 {
		return fmt.Errorf("min_duration_between_increases_sec must be >= 1")
	}
	if c.MaxDurationBetweenIncreasesSec < 1 {
		return fmt.Errorf("max_duration_between_increases_sec must be >= 1")
	}
	if c.MinDurationBetweenDecreasesSec < 1 {
		return fmt.Errorf("min_duration_between_decreases_sec must be >= 1")
	}
	if c.SpreadBacklogAcrossSec < 1 {
		return fmt.Errorf("spread_backlog_across_sec must be >= 1")
	}
	if c.IgnoreNSlowestReplicas < 0 {
		return fmt.Errorf("ignore_n_slowest_replicas must be >= 0")
	}
	if c.IgnoreNSlowestRdonlys < 0 {
		return fmt.Errorf("ignore_n_slowest_rdonlys must be >= 0")
	}
	if c.AgeBadRateAfterSec < 1 {
		return fmt.Errorf("age_bad_rate_after_sec must be >= 1")
	}
	if c.MaxRateApproachThreshold < 0 {
		return fmt.Errorf("max_rate_approach_threshold must be >=0")
	}
	if c.MaxRateApproachThreshold > 1 {
		return fmt.Errorf("max_rate_approach_threshold must be <=1")
	}
	return nil
}

// MinDurationBetweenIncreases is a helper function which returns the respective
// protobuf field as native Go type.
func (c MaxReplicationLagModuleConfig) MinDurationBetweenIncreases() time.Duration {
	return time.Duration(c.MinDurationBetweenIncreasesSec) * time.Second
}

// MaxDurationBetweenIncreases is a helper function which returns the respective
// protobuf field as native Go type.
func (c MaxReplicationLagModuleConfig) MaxDurationBetweenIncreases() time.Duration {
	return time.Duration(c.MaxDurationBetweenIncreasesSec) * time.Second
}

// MinDurationBetweenDecreases is a helper function which returns the respective
// protobuf field as native Go type.
func (c MaxReplicationLagModuleConfig) MinDurationBetweenDecreases() time.Duration {
	return time.Duration(c.MinDurationBetweenDecreasesSec) * time.Second
}

// SpreadBacklogAcross is a helper function which returns the respective
// protobuf field as native Go type.
func (c MaxReplicationLagModuleConfig) SpreadBacklogAcross() time.Duration {
	return time.Duration(c.SpreadBacklogAcrossSec) * time.Second
}

// AgeBadRateAfter is a helper function which returns the respective
// protobuf field as native Go type.
func (c MaxReplicationLagModuleConfig) AgeBadRateAfter() time.Duration {
	return time.Duration(c.AgeBadRateAfterSec) * time.Second
}
