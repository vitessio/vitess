/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package base

import (
	"errors"
	"strings"
)

// MetricResult is what we expect our probes to return. This can be a numeric result, or
// a special type of result indicating more meta-information
type MetricResult interface {
	Get() (float64, error)
}

// MetricResultFunc is a function that returns a metric result
type MetricResultFunc func() (metricResult MetricResult, threshold float64)

// ErrThresholdExceeded is the common error one may get checking on metric result
var ErrThresholdExceeded = errors.New("Threshold exceeded")
var errNoResultYet = errors.New("Metric not collected yet")

// ErrNoSuchMetric is for when a user requests a metric by an unknown metric name
var ErrNoSuchMetric = errors.New("No such metric")

// ErrInvalidCheckType is an internal error indicating an unknown check type
var ErrInvalidCheckType = errors.New("Unknown throttler check type")

// IsDialTCPError sees if th egiven error indicates a TCP issue
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
	return 0, errNoResultYet
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
