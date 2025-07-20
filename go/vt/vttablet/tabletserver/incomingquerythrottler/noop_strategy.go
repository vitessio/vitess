package incomingquerythrottler

import (
	"context"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var _ ThrottlingStrategyHandler = (*NoOpStrategy)(nil)

// NoOpStrategy is a fallback strategy that performs no throttling.
type NoOpStrategy struct{}

func (s *NoOpStrategy) ThrottleIfNeeded(ctx context.Context, targetTabletType topodatapb.TabletType, sql string, transactionID int64, options *querypb.ExecuteOptions) error {
	return nil
}

// Start is a no-op for the NoOpStrategy since it requires no initialization.
func (s *NoOpStrategy) Start() {
	// No-op: NoOpStrategy requires no initialization or background processes
}

// Stop is a no-op for the NoOpStrategy since it has no resources to clean up.
func (s *NoOpStrategy) Stop() {
	// No-op: NoOpStrategy has no resources to clean up
}
