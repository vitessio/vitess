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
	"strings"
)

type Store string

const (
	UndefinedStore Store = ""
	ShardStore     Store = "shard"
	SelfStore      Store = "self"
)

func (s Store) String() string {
	return string(s)
}

type MetricName string

type MetricNames []MetricName

func (names MetricNames) Contains(name MetricName) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

const (
	DefaultMetricName        MetricName = "default"
	LagMetricName            MetricName = "lag"
	ThreadsRunningMetricName MetricName = "threads_running"
	CustomMetricName         MetricName = "custom"
	LoadAvgMetricName        MetricName = "loadavg"
)

func (metric MetricName) String() string {
	return string(metric)
}

var KnownMetricNames = MetricNames{
	DefaultMetricName,
	LagMetricName,
	ThreadsRunningMetricName,
	CustomMetricName,
	LoadAvgMetricName,
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
