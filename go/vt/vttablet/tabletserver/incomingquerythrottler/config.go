package incomingquerythrottler

// ThrottlingStrategy represents the strategy used to apply throttling
// to incoming queries based on system load or external signals.
type ThrottlingStrategy string

// Predefined throttling strategies for the IncomingQueryThrottler.
const (
	// ThrottlingStrategyTabletThrottler uses Vitess Tablet Throttler to shed load
	// from incoming queries when the tablet is under pressure.
	// Reference: https://vitess.io/docs/21.0/reference/features/tablet-throttler/
	ThrottlingStrategyTabletThrottler ThrottlingStrategy = "TabletThrottler"

	// ThrottlingStrategyUnknown is used when the strategy is not known.
	ThrottlingStrategyUnknown ThrottlingStrategy = "Unknown"
)

// Config defines the runtime configuration for the IncomingQueryThrottler.
// It specifies whether throttling is enabled and which strategy to use.
type Config struct {
	// Enabled indicates whether the throttler should actively apply throttling logic.
	Enabled bool `json:"enabled"`

	// Strategy selects which throttling strategy should be used.
	Strategy ThrottlingStrategy `json:"strategy"`
}
