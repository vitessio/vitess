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
	"errors"
	"fmt"
	"strings"
)

// Scope defines the tablet range from which a metric is collected. This can be the local tablet
// ("self") or the entire shard ("shard")
type Scope string

const (
	UndefinedScope Scope = ""
	ShardScope     Scope = "shard"
	SelfScope      Scope = "self"
)

func (s Scope) String() string {
	return string(s)
}

func ScopeFromString(s string) (Scope, error) {
	switch scope := Scope(s); scope {
	case UndefinedScope, ShardScope, SelfScope:
		return scope, nil
	default:
		return "", fmt.Errorf("unknown scope: %s", s)
	}
}

// MetricName is a formalized name for a metric, such as "lag" or "threads_running". A metric name
// may include a scope, such as "self/lag" or "shard/threads_running". It is possible to add a
// scope to a name, or to parse the scope out of a name, and there is also always a default scope
// associated with a metric name.
type MetricName string

// MetricNames is a formalized list of metric names
type MetricNames []MetricName

func (names MetricNames) Contains(name MetricName) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
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
	switch metric {
	case LagMetricName:
		return ShardScope
	default:
		return SelfScope
	}
}

func (metric MetricName) String() string {
	return string(metric)
}

// AggregatedName returns the string representation of this metric in the given scope, e.g.:
// - "self/loadavg"
// - "shard/lag"
func (metric MetricName) AggregatedName(scope Scope) string {
	if metric == DefaultMetricName {
		// backwards (v19) compatibility
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

var KnownMetricNames = MetricNames{
	DefaultMetricName,
	LagMetricName,
	ThreadsRunningMetricName,
	CustomMetricName,
	LoadAvgMetricName,
}

type AggregatedMetricName struct {
	Scope  Scope
	Metric MetricName
}

var (
	// aggregatedMetricNames precomputes the aggregated metric names for all known metric names,
	// mapped to their breakdowns. e.g. "self/loadavg" -> {SelfScope, LoadAvgMetricName}
	// This means:
	// - no textual parsing is needed in the critical path
	// - we can easily check if a metric name is valid
	aggregatedMetricNames map[string]AggregatedMetricName
)

func init() {
	aggregatedMetricNames = make(map[string]AggregatedMetricName)
	for _, metricName := range KnownMetricNames {
		aggregatedMetricNames[metricName.String()] = AggregatedMetricName{
			Scope:  metricName.DefaultScope(),
			Metric: metricName,
		}
		for _, scope := range []Scope{ShardScope, SelfScope} {
			aggregatedName := metricName.AggregatedName(scope)
			aggregatedMetricNames[aggregatedName] = AggregatedMetricName{
				Scope:  scope,
				Metric: metricName,
			}
		}
	}
}

// splitMetricTokens splits a metric name into its scope name and metric name
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

// MetricResult is what we expect our probes to return. This can be a numeric result, or
// a special type of result indicating more meta-information
type MetricResult interface {
	Get() (float64, error)
}

// MetricResultFunc is a function that returns a metric result
type MetricResultFunc func() (metricResult MetricResult, threshold float64)

type MetricResultMap map[MetricName]MetricResult

func NewMetricResultMap() MetricResultMap {
	result := make(MetricResultMap)
	for _, metricName := range KnownMetricNames {
		result[metricName] = nil
	}
	return result
}

// ErrThresholdExceeded is the common error one may get checking on metric result
var ErrThresholdExceeded = errors.New("threshold exceeded")
var ErrNoResultYet = errors.New("metric not collected yet")

// ErrNoSuchMetric is for when a user requests a metric by an unknown metric name
var ErrNoSuchMetric = errors.New("no such metric")

// ErrInvalidCheckType is an internal error indicating an unknown check type
var ErrInvalidCheckType = errors.New("unknown throttler check type")

// IsDialTCPError sees if the given error indicates a TCP issue
func IsDialTCPError(e error) bool {
	if e == nil {
		return false
	}
	return strings.HasPrefix(e.Error(), "dial tcp")
}

type noHostsMetricResult struct{}

// Get implements MetricResult
func (metricResult *noHostsMetricResult) Get() (float64, error) {
	return 0, nil
}

// NoHostsMetricResult is a result indicating "no hosts"
var NoHostsMetricResult = &noHostsMetricResult{}

type noMetricResultYet struct{}

// Get implements MetricResult
func (metricResult *noMetricResultYet) Get() (float64, error) {
	return 0, ErrNoResultYet
}

// NoMetricResultYet is a result indicating "no data"
var NoMetricResultYet = &noMetricResultYet{}

type noSuchMetric struct{}

// Get implements MetricResult
func (metricResult *noSuchMetric) Get() (float64, error) {
	return 0, ErrNoSuchMetric
}

// NoSuchMetric is a metric results for an unknown metric name
var NoSuchMetric = &noSuchMetric{}

// simpleMetricResult is a result with float value
type simpleMetricResult struct {
	Value float64
}

// NewSimpleMetricResult creates a simpleMetricResult
func NewSimpleMetricResult(value float64) MetricResult {
	return &simpleMetricResult{Value: value}
}

// Get implements MetricResult
func (metricResult *simpleMetricResult) Get() (float64, error) {
	return metricResult.Value, nil
}
