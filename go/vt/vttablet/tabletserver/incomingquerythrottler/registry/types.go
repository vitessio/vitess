package registry

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
