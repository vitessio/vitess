/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package throttle

import (
	"net/http"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
)

// CheckResult is the result for an app inquiring on a metric. It also exports as JSON via the API
type CheckResult struct {
	StatusCode int     `json:"StatusCode"`
	Value      float64 `json:"Value"`
	Threshold  float64 `json:"Threshold"`
	Error      error   `json:"-"`
	Message    string  `json:"Message"`
}

// NewCheckResult returns a CheckResult
func NewCheckResult(statusCode int, value float64, threshold float64, err error) *CheckResult {
	result := &CheckResult{
		StatusCode: statusCode,
		Value:      value,
		Threshold:  threshold,
		Error:      err,
	}
	if err != nil {
		result.Message = err.Error()
	}
	return result
}

// NewErrorCheckResult returns a check result that indicates an error
func NewErrorCheckResult(statusCode int, err error) *CheckResult {
	return NewCheckResult(statusCode, 0, 0, err)
}

// NoSuchMetricCheckResult is a result returns when a metric is unknown
var NoSuchMetricCheckResult = NewErrorCheckResult(http.StatusNotFound, base.ErrNoSuchMetric)

var okMetricCheckResult = NewCheckResult(http.StatusOK, 0, 0, nil)

var invalidCheckTypeCheckResult = NewErrorCheckResult(http.StatusInternalServerError, base.ErrInvalidCheckType)
