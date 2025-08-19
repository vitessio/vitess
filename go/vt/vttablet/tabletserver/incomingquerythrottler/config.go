package incomingquerythrottler

import "vitess.io/vitess/go/vt/vttablet/tabletserver/incomingquerythrottler/registry"

// Compile-time interface compliance check
var _ registry.StrategyConfig = (*Config)(nil)

// Config defines the runtime configuration for the IncomingQueryThrottler.
// It specifies whether throttling is enabled and which strategy to use.
type Config struct {
	// Enabled indicates whether the throttler should actively apply throttling logic.
	Enabled bool `json:"enabled"`

	// Strategy selects which throttling strategy should be used.
	Strategy registry.ThrottlingStrategy `json:"strategy"`
}

// GetStrategy implements registry.StrategyConfig interface
func (c Config) GetStrategy() registry.ThrottlingStrategy {
	return c.Strategy
}
