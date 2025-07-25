package incomingquerythrottler

import (
	"context"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// ThrottlingStrategyHandler defines the interface for throttling strategies
// used by the IncomingQueryThrottler. Each strategy encapsulates its own logic
// to determine whether throttling should be applied for an incoming query.
type ThrottlingStrategyHandler interface {
	// ThrottleIfNeeded method is invoked before a query is executed and evaluates whether the incoming query should be throttled.
	// It returns an error if the system is overloaded or throttling conditions are met.
	// A nil error means the query can proceed normally.
	ThrottleIfNeeded(ctx context.Context, targetTabletType topodatapb.TabletType, sql string, transactionID int64, options *querypb.ExecuteOptions) error

	// Start initializes and starts the throttling strategy.
	// This method should be called when the strategy becomes active.
	// Implementations may start background processes, caching, or other resources.
	Start()

	// Stop gracefully shuts down the throttling strategy and releases any resources.
	// This method should be called when the strategy is no longer needed.
	// Implementations should clean up background processes, caches, or other resources.
	Stop()
}
