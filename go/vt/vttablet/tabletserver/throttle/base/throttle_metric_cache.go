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
	"context"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"

	"vitess.io/vitess/go/stats"
)

// MetricsQueryType indicates the type of metrics query on MySQL backend. See following.
type MetricsQueryType int

const (
	// MetricsQueryTypeDefault indicates the default, internal implementation. Specifically, our throttler runs a replication lag query
	MetricsQueryTypeDefault MetricsQueryType = iota
	// MetricsQueryTypeShowGlobal indicates SHOW GLOBAL (STATUS|VARIABLES) query
	MetricsQueryTypeShowGlobal
	// MetricsQueryTypeSelect indicates a custom SELECT query
	MetricsQueryTypeSelect
	// MetricsQueryTypeUnknown is an unknown query type, which we cannot run. This is an error
	MetricsQueryTypeUnknown
)

var metricCache = cache.New(cache.NoExpiration, 10*time.Second)

func getMetricCacheKey(probe *Probe) string {
	return probe.Alias
}

func cacheThrottleMetric(probe *Probe, throttleMetrics ThrottleMetrics) ThrottleMetrics {
	for _, metric := range throttleMetrics {
		if metric.Err != nil {
			return throttleMetrics
		}
	}
	if probe.CacheMillis > 0 {
		metricCache.Set(getMetricCacheKey(probe), throttleMetrics, time.Duration(probe.CacheMillis)*time.Millisecond)
	}
	return throttleMetrics
}

func getCachedThrottleMetrics(probe *Probe) ThrottleMetrics {
	if probe.CacheMillis == 0 {
		return nil
	}
	if metrics, found := metricCache.Get(getMetricCacheKey(probe)); found {
		throttleMetrics, _ := metrics.(ThrottleMetrics)
		return throttleMetrics
	}
	return nil
}

// GetMetricsQueryType analyzes the type of a metrics query
func GetMetricsQueryType(query string) MetricsQueryType {
	if query == "" {
		return MetricsQueryTypeDefault
	}
	if strings.HasPrefix(strings.ToLower(query), "select") {
		return MetricsQueryTypeSelect
	}
	if strings.HasPrefix(strings.ToLower(query), "show global") {
		return MetricsQueryTypeShowGlobal
	}
	return MetricsQueryTypeUnknown
}

// ThrottleMetric has the probed metric for a tablet
type ThrottleMetric struct { // nolint:revive
	Name  MetricName
	Scope Scope
	Alias string
	Value float64
	Err   error
}

type ThrottleMetrics map[MetricName]*ThrottleMetric // nolint:revive

// NewThrottleMetric creates a new ThrottleMetric
func NewThrottleMetric() *ThrottleMetric {
	return &ThrottleMetric{Value: 0}
}

// GetClusterTablet returns the ClusterTablet part of the metric
func (metric *ThrottleMetric) GetTabletAlias() string {
	return metric.Alias
}

// Get implements MetricResult
func (metric *ThrottleMetric) Get() (float64, error) {
	return metric.Value, metric.Err
}

// WithError returns this metric with given error
func (metric *ThrottleMetric) WithError(err error) *ThrottleMetric {
	metric.Err = err
	return metric
}

// ReadThrottleMetrics returns a metric for the given probe. Either by explicit query
// or via SHOW REPLICA STATUS
func ReadThrottleMetrics(ctx context.Context, probe *Probe, metricsFunc func(context.Context) ThrottleMetrics) ThrottleMetrics {
	if metrics := getCachedThrottleMetrics(probe); metrics != nil {
		return metrics
	}

	started := time.Now()
	throttleMetrics := metricsFunc(ctx)

	go func(metrics ThrottleMetrics, started time.Time) {
		stats.GetOrNewGauge("ThrottlerProbesLatency", "probes latency").Set(time.Since(started).Nanoseconds())
		stats.GetOrNewCounter("ThrottlerProbesTotal", "total probes").Add(1)
		for _, metric := range metrics {
			if metric.Err != nil {
				stats.GetOrNewCounter("ThrottlerProbesError", "total probes errors").Add(1)
				break
			}
		}
	}(throttleMetrics, started)

	return cacheThrottleMetric(probe, throttleMetrics)
}
