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
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
)

// QueryAttributes contains query-level metadata used by throttling strategies.
// This centralizes extraction of workload and priority information.
type QueryAttributes struct {
	// WorkloadName contains the name of the workload for the query.
	WorkloadName string

	// Priority contains the priority of the query (0-100, where 0 is highest priority).
	Priority int
}

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
