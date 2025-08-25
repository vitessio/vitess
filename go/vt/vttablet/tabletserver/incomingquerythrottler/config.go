package incomingquerythrottler

import (
	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/incomingquerythrottler/registry"
)

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

// Viper-based configuration values for dynamic updates
var (
	// enabled controls whether the incoming query throttler is active
	enabled = viperutil.Configure("incoming_query_throttler_enabled", viperutil.Options[bool]{
		Default:  false,
		FlagName: "incoming-query-throttler-enabled",
	})

	// strategy selects which throttling strategy to use
	strategy = viperutil.Configure("incoming_query_throttler_strategy", viperutil.Options[registry.ThrottlingStrategy]{
		Default:  registry.ThrottlingStrategyUnknown,
		FlagName: "incoming-query-throttler-strategy",
		GetFunc: func(v *viper.Viper) func(key string) registry.ThrottlingStrategy {
			return func(key string) registry.ThrottlingStrategy {
				val := v.GetString(key)
				if val == "" {
					return registry.ThrottlingStrategyUnknown
				}
				return registry.ThrottlingStrategy(val)
			}
		},
	})
)

// GetCurrentConfig returns the current configuration from Viper
// This provides a point-in-time snapshot for consistent reads
func GetCurrentConfig() Config {
	return Config{
		Enabled:  enabled.Get(),
		Strategy: strategy.Get(),
	}
}
