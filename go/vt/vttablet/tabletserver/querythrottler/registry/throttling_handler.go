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

package registry

import (
	"context"

	"vitess.io/vitess/go/vt/sqlparser"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Predefined throttling strategies for the QueryThrottler.
const (
	// ThrottlingStrategyTabletThrottler uses Vitess Tablet Throttler to shed load
	// from incoming queries when the tablet is under pressure.
	// Reference: https://vitess.io/docs/21.0/reference/features/tablet-throttler/
	ThrottlingStrategyTabletThrottler ThrottlingStrategy = "TabletThrottler"

	// ThrottlingStrategyUnknown is used when the strategy is not known.
	ThrottlingStrategyUnknown ThrottlingStrategy = "Unknown"
)

// ThrottlingStrategy represents the strategy used to apply throttling
// to incoming queries based on system load or external signals.
type ThrottlingStrategy string

// ThrottlingStrategyHandler defines the interface for throttling strategies
// used by the QueryThrottler. Each strategy encapsulates its own logic
// to determine whether throttling should be applied for an incoming query.
type ThrottlingStrategyHandler interface {
	// Evaluate determines whether a query should be throttled and returns detailed information about the decision.
	// This method separates the decision-making logic from the enforcement action, enabling features like dry-run mode.
	// QueryAttributes contains pre-computed workload and priority information to avoid re computation.
	// It returns a ThrottleDecision struct containing all relevant information about the throttling decision.
	Evaluate(ctx context.Context, targetTabletType topodatapb.TabletType, parsedQuery *sqlparser.ParsedQuery, transactionID int64, attrs QueryAttributes) ThrottleDecision

	// Start initializes and starts the throttling strategy.
	// This method should be called when the strategy becomes active.
	// Implementations may start background processes, caching, or other resources.
	Start()

	// Stop gracefully shuts down the throttling strategy and releases any resources.
	// This method should be called when the strategy is no longer needed.
	// Implementations should clean up background processes, caches, or other resources.
	Stop()
}
