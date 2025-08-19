package registry

import (
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
)

// ThrottleDecision represents the result of evaluating whether a query should be throttled.
// It separates the decision-making logic from the enforcement action.
type ThrottleDecision struct {
	// Throttle indicates whether the query should be throttled.
	Throttle bool

	// Message contains a human-readable description of the throttling decision.
	// This message can be used for logging, error responses, or metrics.
	Message string

	// MetricName identifies which metric triggered the throttling decision (if any).
	MetricName string

	// MetricValue contains the current value of the metric that triggered throttling.
	MetricValue float64

	// Threshold contains the threshold value that was breached.
	Threshold float64

	// ThrottlePercentage contains the percentage chance this query was throttled (0.0-1.0).
	ThrottlePercentage float64
}

// StrategyConfig defines the configuration interface that strategy implementations
// must satisfy. This avoids circular imports by using a generic interface.
type StrategyConfig interface {
	GetStrategy() ThrottlingStrategy
	GetTabletStrategyConfig() interface{} // Using interface{} to avoid circular dependency
}

// Deps holds the dependencies required by strategy factories.
type Deps struct {
	ThrottleClient *throttle.Client
	TabletConfig   *tabletenv.TabletConfig
}

// StrategyFactory creates a new strategy instance with the given dependencies and configuration.
type StrategyFactory interface {
	New(deps Deps, cfg StrategyConfig) (ThrottlingStrategyHandler, error)
}
