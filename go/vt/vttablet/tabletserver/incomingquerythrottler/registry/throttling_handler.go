package registry

import (
	"context"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// ThrottlingStrategyHandler defines the interface for throttling strategies
// used by the IncomingQueryThrottler. Each strategy encapsulates its own logic
// to determine whether throttling should be applied for an incoming query.
type ThrottlingStrategyHandler interface {
	// Evaluate determines whether a query should be throttled and returns detailed information about the decision.
	// This method separates the decision-making logic from the enforcement action, enabling features like dry-run mode.
	// It returns a ThrottleDecision struct containing all relevant information about the throttling decision.
	Evaluate(ctx context.Context, targetTabletType topodatapb.TabletType, sql string, transactionID int64, options *querypb.ExecuteOptions) ThrottleDecision

	// Start initializes and starts the throttling strategy.
	// This method should be called when the strategy becomes active.
	// Implementations may start background processes, caching, or other resources.
	Start()

	// Stop gracefully shuts down the throttling strategy and releases any resources.
	// This method should be called when the strategy is no longer needed.
	// Implementations should clean up background processes, caches, or other resources.
	Stop()
}
