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

	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// ThrottlingStrategyHandler defines the interface for throttling strategies
// used by the QueryThrottler. Each strategy encapsulates its own logic
// to determine whether throttling should be applied for an incoming query.
type ThrottlingStrategyHandler interface {
	// Evaluate determines whether a query should be throttled and returns detailed information about the decision.
	// This method separates the decision-making logic from the enforcement action, enabling features like dry-run mode.
	// statementType is the statement type resolved from the parsed AST (sqlparser.ASTToStatementType) so that
	// rule matching is correct for CTE queries, which a textual scan (sqlparser.Preview) misclassifies.
	// QueryAttributes contains pre-computed workload and priority information to avoid re computation.
	// It returns a ThrottleDecision struct containing all relevant information about the throttling decision.
	Evaluate(ctx context.Context, targetTabletType topodatapb.TabletType, parsedQuery *sqlparser.ParsedQuery, statementType sqlparser.StatementType, transactionID int64, attrs QueryAttributes) ThrottleDecision

	// Start initializes and starts the throttling strategy.
	// This method should be called when the strategy becomes active.
	// Implementations may start background processes, caching, or other resources.
	Start()

	// Stop gracefully shuts down the throttling strategy and releases any resources.
	// This method should be called when the strategy is no longer needed.
	// Implementations should clean up background processes, caches, or other resources.
	// Stop is terminal: implementations are single-use and must not be restarted after Stop returns.
	// The QueryThrottler enforces this by always constructing a fresh strategy instance on strategy change.
	Stop()

	// UpdateConfig applies a new querythrottler config to a live strategy. It is the
	// single mechanism by which an installed strategy receives nested-config updates,
	// invoked synchronously by QueryThrottler.HandleConfigUpdate before the snapshot
	// swap so the (top-level, nested) config pair is published atomically — eliminating
	// the race that existed when each strategy maintained its own SrvKeyspace watch.
	// Implementations must extract their relevant sub-config and apply it in a
	// goroutine-safe way; they must not start watches or do unbounded work here.
	UpdateConfig(cfg *querythrottlerpb.Config)

	// GetStrategyName returns the name of the strategy.
	GetStrategyName() string
}
