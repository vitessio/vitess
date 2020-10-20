/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package base

import (
	"errors"
)

// ErrAppDenied is seen when an app is denied access
var ErrAppDenied = errors.New("App denied")

type appDeniedMetric struct{}

// Get implements MetricResult
func (metricResult *appDeniedMetric) Get() (float64, error) {
	return 0, ErrAppDenied
}

// AppDeniedMetric is a special metric indicating a "denied" situation
var AppDeniedMetric = &appDeniedMetric{}
