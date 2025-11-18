package querythrottler

import (
	"vitess.io/vitess/go/vt/proto/querythrottler"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"
)

// createTestSrvKeyspace creates a SrvKeyspace with query throttler config for testing
func createTestSrvKeyspace(enabled bool, strategy registry.ThrottlingStrategy, dryRun bool) *topodatapb.SrvKeyspace {
	var protoStrategy querythrottler.ThrottlingStrategy
	switch strategy {
	case registry.ThrottlingStrategyTabletThrottler:
		protoStrategy = querythrottler.ThrottlingStrategy_TABLET_THROTTLER
	default:
		protoStrategy = querythrottler.ThrottlingStrategy_UNKNOWN
	}

	return &topodatapb.SrvKeyspace{
		IncomingQueryThrottlerConfig: &querythrottler.Config{
			Enabled:  enabled,
			Strategy: protoStrategy,
			DryRun:   dryRun,
		},
	}
}
