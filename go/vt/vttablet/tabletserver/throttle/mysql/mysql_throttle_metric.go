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

package mysql

import (
	"context"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
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

var mysqlMetricCache = cache.New(cache.NoExpiration, 10*time.Second)

func getMySQLMetricCacheKey(probe *Probe) string {
	return probe.Alias
}

func cacheMySQLThrottleMetric(probe *Probe, mySQLThrottleMetrics MySQLThrottleMetrics) MySQLThrottleMetrics {
	for _, metric := range mySQLThrottleMetrics {
		if metric.Err != nil {
			return mySQLThrottleMetrics
		}
	}
	if probe.CacheMillis > 0 {
		mysqlMetricCache.Set(getMySQLMetricCacheKey(probe), mySQLThrottleMetrics, time.Duration(probe.CacheMillis)*time.Millisecond)
	}
	return mySQLThrottleMetrics
}

func getCachedMySQLThrottleMetrics(probe *Probe) MySQLThrottleMetrics {
	if probe.CacheMillis == 0 {
		return nil
	}
	if metrics, found := mysqlMetricCache.Get(getMySQLMetricCacheKey(probe)); found {
		mySQLThrottleMetrics, _ := metrics.(MySQLThrottleMetrics)
		return mySQLThrottleMetrics
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

// MySQLThrottleMetric has the probed metric for a tablet
type MySQLThrottleMetric struct { // nolint:revive
	Name      base.MetricName
	StoreName string
	Alias     string
	Value     float64
	Err       error
}

type MySQLThrottleMetrics map[base.MetricName]*MySQLThrottleMetric // nolint:revive

// NewMySQLThrottleMetric creates a new MySQLThrottleMetric
func NewMySQLThrottleMetric() *MySQLThrottleMetric {
	return &MySQLThrottleMetric{Value: 0}
}

// GetClusterTablet returns the ClusterTablet part of the metric
func (metric *MySQLThrottleMetric) GetTabletAlias() string {
	return metric.Alias
}

// Get implements MetricResult
func (metric *MySQLThrottleMetric) Get() (float64, error) {
	return metric.Value, metric.Err
}

// WithError returns this metric with given error
func (metric *MySQLThrottleMetric) WithError(err error) *MySQLThrottleMetric {
	metric.Err = err
	return metric
}

// ReadThrottleMetrics returns a metric for the given probe. Either by explicit query
// or via SHOW REPLICA STATUS
func ReadThrottleMetrics(ctx context.Context, probe *Probe, clusterName string, metricsFunc func(context.Context) MySQLThrottleMetrics) MySQLThrottleMetrics {
	if metrics := getCachedMySQLThrottleMetrics(probe); metrics != nil {
		return metrics
	}

	started := time.Now()
	mySQLThrottleMetrics := metricsFunc(ctx)

	go func(metrics MySQLThrottleMetrics, started time.Time) {
		stats.GetOrNewGauge("ThrottlerProbesLatency", "probes latency").Set(time.Since(started).Nanoseconds())
		stats.GetOrNewCounter("ThrottlerProbesTotal", "total probes").Add(1)
		// if metric.Err != nil {
		// 	stats.GetOrNewCounter("ThrottlerProbesError", "total probes errors").Add(1)
		// }
	}(mySQLThrottleMetrics, started)

	return cacheMySQLThrottleMetric(probe, mySQLThrottleMetrics)
}
