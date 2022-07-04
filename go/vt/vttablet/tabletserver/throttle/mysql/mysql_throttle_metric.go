/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
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
	// MetricsQueryTypeDefault indictes the default, internal implementation. Specifically, our throttler runs a replication lag query
	MetricsQueryTypeDefault MetricsQueryType = iota
	// MetricsQueryTypeShowGlobal indicatesa SHOW GLOBAL (STATUS|VARIABLES) query
	MetricsQueryTypeShowGlobal
	// MetricsQueryTypeSelect indicates a custom SELECT query
	MetricsQueryTypeSelect
	// MetricsQueryTypeUnknown is an unknown query type, which we cannot run. This is an error
	MetricsQueryTypeUnknown
)

var mysqlMetricCache = cache.New(cache.NoExpiration, 10*time.Second)

func getMySQLMetricCacheKey(probe *Probe) string {
	return fmt.Sprintf("%s:%s", probe.Key, probe.MetricQuery)
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

// MySQLThrottleMetric has the probed metric for a mysql instance
type MySQLThrottleMetric struct { // nolint:revive
	ClusterName string
	Key         InstanceKey
	Value       float64
	Err         error
}

// NewMySQLThrottleMetric creates a new MySQLThrottleMetric
func NewMySQLThrottleMetric() *MySQLThrottleMetric {
	return &MySQLThrottleMetric{Value: 0}
}

// GetClusterInstanceKey returns the ClusterInstanceKey part of the metric
func (metric *MySQLThrottleMetric) GetClusterInstanceKey() ClusterInstanceKey {
	return GetClusterInstanceKey(metric.ClusterName, &metric.Key)
}

// Get implements MetricResult
func (metric *MySQLThrottleMetric) Get() (float64, error) {
	return metric.Value, metric.Err
}

// ReadThrottleMetric returns a metric for the given probe. Either by explicit query
// or via SHOW SLAVE STATUS
func ReadThrottleMetric(probe *Probe, clusterName string, overrideGetMetricFunc func() *MySQLThrottleMetric) (mySQLThrottleMetric *MySQLThrottleMetric) {
	if mySQLThrottleMetric := getCachedMySQLThrottleMetric(probe); mySQLThrottleMetric != nil {
		return mySQLThrottleMetric
		// On cached results we avoid taking latency metrics
	}

	started := time.Now()
	mySQLThrottleMetric = NewMySQLThrottleMetric()
	mySQLThrottleMetric.ClusterName = clusterName
	mySQLThrottleMetric.Key = probe.Key

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
