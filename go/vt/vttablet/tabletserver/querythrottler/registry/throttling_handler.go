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
	// Evaluate decides whether a query should be throttled, returning the decision rather
	// than acting on it so callers can implement dry-run mode. cfg is the config to decide
	// by; the caller passes the same config it reads dry_run from, so a config update can't
	// pair new rules with an old dry_run. statementType comes from the parsed AST, which —
	// unlike a textual scan — classifies CTE queries correctly. transactionID is always 0
	// for now, since QueryThrottler.Throttle doesn't evaluate statements inside a
	// transaction. attrs carries pre-computed workload and priority so strategies need not
	// recompute them.
	Evaluate(ctx context.Context, cfg *querythrottlerpb.Config, targetTabletType topodatapb.TabletType, parsedQuery *sqlparser.ParsedQuery, statementType sqlparser.StatementType, transactionID int64, attrs QueryAttributes) ThrottleDecision

	// Start initializes and starts the throttling strategy.
	// This method should be called when the strategy becomes active.
	// Implementations may start background processes, caching, or other resources.
	Start()

	// Stop shuts the strategy down and releases its background processes and caches.
	// Stop is terminal: implementations are single-use and must not be restarted.
	// QueryThrottler enforces this by building a fresh instance on every strategy change.
	Stop()

	// GetStrategyName returns the name of the strategy.
	GetStrategyName() string
}
