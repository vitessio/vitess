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
	throttlerdata.MaxReplicationLagModuleConfig
}

var defaultMaxReplicationLagModuleConfig = MaxReplicationLagModuleConfig{
	throttlerdata.MaxReplicationLagModuleConfig{
		TargetReplicationLagSec: 2,
		MaxReplicationLagSec:    ReplicationLagModuleDisabled,

		InitialRate: 100,
		// 1 means 100% i.e. double rates by default.
		MaxIncrease:       1,
		EmergencyDecrease: 0.5,

		MinDurationBetweenChangesSec:   10,
		MaxDurationBetweenIncreasesSec: 61,
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
	if c.TargetReplicationLagSec > c.MaxReplicationLagSec {
		return fmt.Errorf("target replication lag must not be higher than the configured max replication lag: invalid: %v > %v",
			c.TargetReplicationLagSec, c.MaxReplicationLagSec)
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
