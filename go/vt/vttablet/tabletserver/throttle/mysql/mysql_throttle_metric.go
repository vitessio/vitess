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
	"fmt"
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

var mysqlMetricCache = cache.New(cache.NoExpiration, 10*time.Second)

func getMySQLMetricCacheKey(probe *Probe) string {
	return fmt.Sprintf("%s:%s", probe.Alias, probe.MetricQuery)
}

func cacheMySQLThrottleMetric(probe *Probe, mySQLThrottleMetric *MySQLThrottleMetric) *MySQLThrottleMetric {
	if mySQLThrottleMetric.Err != nil {
		return mySQLThrottleMetric
	}
	if probe.CacheMillis > 0 {
		mysqlMetricCache.Set(getMySQLMetricCacheKey(probe), mySQLThrottleMetric, time.Duration(probe.CacheMillis)*time.Millisecond)
	}
	return mySQLThrottleMetric
}

func getCachedMySQLThrottleMetric(probe *Probe) *MySQLThrottleMetric {
	if probe.CacheMillis == 0 {
		return nil
	}
	if metric, found := mysqlMetricCache.Get(getMySQLMetricCacheKey(probe)); found {
		mySQLThrottleMetric, _ := metric.(*MySQLThrottleMetric)
		return mySQLThrottleMetric
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
	ClusterName string
	Alias       string
	Value       float64
	Err         error
}

// NewMySQLThrottleMetric creates a new MySQLThrottleMetric
func NewMySQLThrottleMetric() *MySQLThrottleMetric {
	return &MySQLThrottleMetric{Value: 0}
}

// GetClusterTablet returns the ClusterTablet part of the metric
func (metric *MySQLThrottleMetric) GetClusterTablet() ClusterTablet {
	return GetClusterTablet(metric.ClusterName, metric.Alias)
}

// Get implements MetricResult
func (metric *MySQLThrottleMetric) Get() (float64, error) {
	return metric.Value, metric.Err
}

// ReadThrottleMetric returns a metric for the given probe. Either by explicit query
// or via SHOW REPLICA STATUS
func ReadThrottleMetric(probe *Probe, clusterName string, overrideGetMetricFunc func() *MySQLThrottleMetric) (mySQLThrottleMetric *MySQLThrottleMetric) {
	if mySQLThrottleMetric := getCachedMySQLThrottleMetric(probe); mySQLThrottleMetric != nil {
		return mySQLThrottleMetric
		// On cached results we avoid taking latency metrics
	}

	started := time.Now()
	mySQLThrottleMetric = NewMySQLThrottleMetric()
	mySQLThrottleMetric.ClusterName = clusterName
	mySQLThrottleMetric.Alias = probe.Alias

	defer func(metric *MySQLThrottleMetric, started time.Time) {
		go func() {
			stats.GetOrNewGauge("ThrottlerProbesLatency", "probes latency").Set(time.Since(started).Nanoseconds())
			stats.GetOrNewCounter("ThrottlerProbesTotal", "total probes").Add(1)
			if metric.Err != nil {
				stats.GetOrNewCounter("ThrottlerProbesError", "total probes errors").Add(1)
			}
		}()
	}(mySQLThrottleMetric, started)

	mySQLThrottleMetric = overrideGetMetricFunc()
	return cacheMySQLThrottleMetric(probe, mySQLThrottleMetric)
}
