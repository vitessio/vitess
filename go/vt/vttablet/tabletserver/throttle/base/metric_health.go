/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package base

import (
	"time"
)

// MetricHealth is a health status for a metric, and more specifically,
// when it was last checked to be "OK"
type MetricHealth struct {
	LastHealthyAt           time.Time
	SecondsSinceLastHealthy int64
}

// NewMetricHealth returns a MetricHealth
func NewMetricHealth(lastHealthyAt time.Time) *MetricHealth {
	result := &MetricHealth{
		LastHealthyAt:           lastHealthyAt,
		SecondsSinceLastHealthy: int64(time.Since(lastHealthyAt).Seconds()),
	}
	return result
}

// MetricHealthMap maps metric names to metric healths
type MetricHealthMap map[string](*MetricHealth)

// Aggregate another map into this map, take the worst metric of the two
func (m MetricHealthMap) Aggregate(other MetricHealthMap) MetricHealthMap {
	for metricName, otherHealth := range other {
		if currentHealth, ok := m[metricName]; ok {
			if currentHealth.SecondsSinceLastHealthy < otherHealth.SecondsSinceLastHealthy {
				m[metricName] = otherHealth
			}
		} else {
			m[metricName] = otherHealth
		}
	}
	return m
}
