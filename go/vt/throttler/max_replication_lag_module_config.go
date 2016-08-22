package throttler

import (
	"fmt"
	"time"

	"github.com/youtube/vitess/go/vt/proto/throttlerdata"
)

// MaxReplicationLagModuleConfig stores all configuration parameters for
// MaxReplicationLagModule. Internally, the parameters are represented by a
// protobuf message. This message is also used to update the parameters.
type MaxReplicationLagModuleConfig struct {
	throttlerdata.Configuration
}

var defaultMaxReplicationLagModuleConfig = MaxReplicationLagModuleConfig{
	throttlerdata.Configuration{
		TargetReplicationLagSec: 2,
		MaxReplicationLagSec:    ReplicationLagModuleDisabled,

		InitialRate: 100,
		// 1 means 100% i.e. double rates by default.
		MaxIncrease:       1,
		EmergencyDecrease: 0.5,

		MinDurationBetweenChangesSec: 10,
		// MaxDurationBetweenIncreasesSec defaults to 60+2 seconds because this
		// corresponds to three 3 broadcasts (assuming --health_check_interval=20s).
		// The 2 extra seconds give us headroom to account for delay in the process.
		MaxDurationBetweenIncreasesSec: 60 + 2,
	},
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
	if c.MinDurationBetweenChangesSec < 1 {
		return fmt.Errorf("min_duration_between_changes_sec must be >= 1")
	}
	if c.MaxDurationBetweenIncreasesSec < 1 {
		return fmt.Errorf("max_duration_between_increases_sec must be >= 1")
	}
	if c.IgnoreNSlowestReplicas < 0 {
		return fmt.Errorf("ignore_n_slowest_replicas must be >= 0")
	}
	return nil
}

// MinDurationBetweenChanges is a helper function which returns the respective
// protobuf field as native Go type.
func (c MaxReplicationLagModuleConfig) MinDurationBetweenChanges() time.Duration {
	return time.Duration(c.MinDurationBetweenChangesSec) * time.Second
}

// MaxDurationBetweenIncreases is a helper function which returns the respective
// protobuf field as native Go type.
func (c MaxReplicationLagModuleConfig) MaxDurationBetweenIncreases() time.Duration {
	return time.Duration(c.MaxDurationBetweenIncreasesSec) * time.Second
}
