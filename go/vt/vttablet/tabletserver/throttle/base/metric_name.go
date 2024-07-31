/*
Copyright 2023 The Vitess Authors.

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

package base

import (
	"fmt"
	"slices"
	"strings"
)

// MetricName is a formalized name for a metric, such as "lag" or "threads_running". A metric name
// may include a scope, such as "self/lag" or "shard/threads_running". It is possible to add a
// scope to a name, or to parse the scope out of a name, and there is also always a default scope
// associated with a metric name.
type MetricName string

// MetricNames is a formalized list of metric names
type MetricNames []MetricName

func (names MetricNames) Contains(name MetricName) bool {
	return slices.Contains(names, name)
}

func (names MetricNames) String() string {
	s := make([]string, len(names))
	for i, name := range names {
		s[i] = name.String()
	}
	return strings.Join(s, ",")
}

// Unique returns a subset of unique metric names, in same order as the original names
func (names MetricNames) Unique() MetricNames {
	if names == nil {
		return nil
	}
	uniqueMetricNamesMap := map[MetricName]bool{}
	uniqueMetricNames := MetricNames{}
	for _, metricName := range names {
		if _, ok := uniqueMetricNamesMap[metricName]; !ok {
			uniqueMetricNames = append(uniqueMetricNames, metricName)
			uniqueMetricNamesMap[metricName] = true
		}
	}
	return uniqueMetricNames
}

const (
	DefaultMetricName        MetricName = "default"
	LagMetricName            MetricName = "lag"
	ThreadsRunningMetricName MetricName = "threads_running"
	CustomMetricName         MetricName = "custom"
	LoadAvgMetricName        MetricName = "loadavg"
)

func (metric MetricName) DefaultScope() Scope {
	if selfMetric := RegisteredSelfMetrics[metric]; selfMetric != nil {
		return selfMetric.DefaultScope()
	}
	return SelfScope
}

func (metric MetricName) String() string {
	return string(metric)
}

// AggregatedName returns the string representation of this metric in the given scope, e.g.:
// - "self/loadavg"
// - "shard/lag"
func (metric MetricName) AggregatedName(scope Scope) string {
	if metric == DefaultMetricName {
		// backwards (v20) compatibility
		return scope.String()
	}
	if scope == UndefinedScope {
		scope = metric.DefaultScope()
	}
	return fmt.Sprintf("%s/%s", scope.String(), metric.String())
}

// Disaggregated returns a breakdown of this metric into scope + name.
func (metric MetricName) Disaggregated() (scope Scope, metricName MetricName, err error) {
	return DisaggregateMetricName(metric.String())
}

type AggregatedMetricName struct {
	Scope  Scope
	Metric MetricName
}

var (
	KnownMetricNames = make(MetricNames, 0)
	// aggregatedMetricNames precomputes the aggregated metric names for all known metric names,
	// mapped to their breakdowns. e.g. "self/loadavg" -> {SelfScope, LoadAvgMetricName}
	// This means:
	// - no textual parsing is needed in the critical path
	// - we can easily check if a metric name is valid
	aggregatedMetricNames = make(map[string]AggregatedMetricName)
)

// DisaggregateMetricName splits a metric name into its scope name and metric name
// aggregated metric name could be in the form:
// - loadavg
// - self
// - self/threads_running
// - shard
// - shard/lag
func DisaggregateMetricName(aggregatedMetricName string) (scope Scope, metricName MetricName, err error) {
	breakdown, ok := aggregatedMetricNames[aggregatedMetricName]
	if !ok {
		return UndefinedScope, DefaultMetricName, ErrNoSuchMetric
	}
	return breakdown.Scope, breakdown.Metric, nil
}
