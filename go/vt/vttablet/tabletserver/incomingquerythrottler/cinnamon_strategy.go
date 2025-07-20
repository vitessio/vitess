package incomingquerythrottler

import (
	"context"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var _ ThrottlingStrategyHandler = (*CinnamonStrategy)(nil)

// CinnamonStrategy is a placeholder for the Cinnamon throttling strategy.
type CinnamonStrategy struct{}

func (s *CinnamonStrategy) ThrottleIfNeeded(ctx context.Context, targetTabletType topodatapb.TabletType, sql string, transactionID int64, options *querypb.ExecuteOptions) error {
	// No-op for now
	return nil
}

// Start is a placeholder for initializing the Cinnamon throttling strategy.
// TODO: Implement actual Cinnamon strategy initialization when the strategy is fully developed.
func (s *CinnamonStrategy) Start() {
	// TODO: Initialize Cinnamon throttling strategy resources
}

// Stop is a placeholder for cleaning up the Cinnamon throttling strategy.
// TODO: Implement actual Cinnamon strategy cleanup when the strategy is fully developed.
func (s *CinnamonStrategy) Stop() {
	// TODO: Clean up Cinnamon throttling strategy resources
}
