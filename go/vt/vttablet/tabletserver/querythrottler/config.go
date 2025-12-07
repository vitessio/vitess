/*
Copyright 2025 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package querythrottler

import (
	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"
)

// Compile-time interface compliance check
var _ registry.StrategyConfig = (*Config)(nil)

// Config defines the runtime configuration for the QueryThrottler.
// It specifies whether throttling is enabled and which strategy to use.
type Config struct {
	// Enabled indicates whether the throttler should actively apply throttling logic.
	Enabled bool `json:"enabled"`

	// DryRun indicates whether throttling decisions should be logged but not enforced.
	// When true, queries that would be throttled are allowed to proceed, but the
	// throttling decision is logged for observability.
	DryRun bool `json:"dry_run"`

	// StrategyName name of the strategy to use for throttling.
	StrategyName registry.ThrottlingStrategy `json:"strategy"`
}

// GetStrategyName implements registry.StrategyConfig interface
func (c Config) GetStrategyName() registry.ThrottlingStrategy {
	return c.StrategyName
}

// ConfigFromProto converts a protobuf QueryThrottler configuration into its internal Config representation.
// It processes the incoming configuration and creates a complete Config struct with all necessary mappings for tablet rules, statement rules, and metric rules.
func ConfigFromProto(queryThrottlerConfig *querythrottlerpb.Config) Config {
	return Config{
		Enabled:      queryThrottlerConfig.GetEnabled(),
		DryRun:       queryThrottlerConfig.GetDryRun(),
		StrategyName: ThrottlingStrategyFromProto(queryThrottlerConfig.GetStrategy()),
	}
}

func ThrottlingStrategyFromProto(strategy querythrottlerpb.ThrottlingStrategy) registry.ThrottlingStrategy {
	switch strategy {
	case querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER:
		return registry.ThrottlingStrategyTabletThrottler
	default:
		return registry.ThrottlingStrategyUnknown
	}
}
