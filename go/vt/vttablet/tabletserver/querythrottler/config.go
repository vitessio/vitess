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

import "vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"

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

	// Strategy selects which throttling strategy should be used.
	Strategy registry.ThrottlingStrategy `json:"strategy"`
}

// GetStrategy implements registry.StrategyConfig interface
func (c Config) GetStrategy() registry.ThrottlingStrategy {
	return c.Strategy
}
