/*
Copyright 2024 The Vitess Authors.

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

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// MetricResult is what we expect our probes to return. This can be a numeric result, or
// a special type of result indicating more meta-information
type MetricResult interface {
	Get() (float64, error)
}

// MetricResultFunc is a function that returns a metric result
type MetricResultFunc func() (metricResult MetricResult, threshold float64)

type MetricResultMap map[MetricName]MetricResult

func NewMetricResultMap() MetricResultMap {
	result := make(MetricResultMap, len(KnownMetricNames))
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

// ErrAppDenied is seen when an app is denied access
var ErrAppDenied = errors.New("app denied")

// ErrInvalidCheckType is an internal error indicating an unknown check type
var ErrInvalidCheckType = errors.New("unknown throttler check type")

// IsTabletRPCError sees if the given error indicates an issue performing an RPC call
// to the tabletmanager service of a tablet. This is used to parse errors returned by
// the CheckThrottler RPC of grpctmclient.
func IsTabletRPCError(err error) bool {
	if err == nil {
		return false
	}

	// The tmclient returns vterrors-style errors. Any
	// error code other than "OK" indicates a problem.
	return vterrors.Code(err) != vtrpcpb.Code_OK
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

type appDeniedMetric struct{}

// Get implements MetricResult
func (metricResult *appDeniedMetric) Get() (float64, error) {
	return 0, ErrAppDenied
}

// AppDeniedMetric is a special metric indicating a "denied" situation
var AppDeniedMetric = &appDeniedMetric{}
