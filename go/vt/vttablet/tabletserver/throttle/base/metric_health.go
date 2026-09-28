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

// This codebase originates from https://github.com/github/freno, See https://github.com/github/freno/blob/master/LICENSE
/*
	MIT License

	Copyright (c) 2017 GitHub

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
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
