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
	"context"
	"fmt"
	"strconv"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
)

type SelfMetric interface {
	Name() MetricName
	DefaultScope() Scope
	DefaultThreshold() float64
	RequiresConn() bool
	Read(ctx context.Context, throttler ThrottlerMetricsPublisher, conn *connpool.Conn) *ThrottleMetric
}

var (
	RegisteredSelfMetrics = make(map[MetricName]SelfMetric)
)

func registerSelfMetric(selfMetric SelfMetric) SelfMetric {
	RegisteredSelfMetrics[selfMetric.Name()] = selfMetric
	KnownMetricNames = append(KnownMetricNames, selfMetric.Name())
	aggregatedMetricNames[selfMetric.Name().String()] = AggregatedMetricName{
		Scope:  selfMetric.DefaultScope(),
		Metric: selfMetric.Name(),
	}
	for _, scope := range []Scope{ShardScope, SelfScope} {
		aggregatedName := selfMetric.Name().AggregatedName(scope)
		aggregatedMetricNames[aggregatedName] = AggregatedMetricName{
			Scope:  scope,
			Metric: selfMetric.Name(),
		}
	}
	return selfMetric
}

// ReadSelfMySQLThrottleMetric reads a metric using a given MySQL connection and a query.
func ReadSelfMySQLThrottleMetric(ctx context.Context, conn *connpool.Conn, query string) *ThrottleMetric {
	metric := &ThrottleMetric{
		Scope: SelfScope,
	}
	if query == "" {
		return metric
	}
	if conn == nil {
		return metric.WithError(fmt.Errorf("conn is nil"))
	}

	tm, err := conn.Exec(ctx, query, 1, true)
	if err != nil {
		return metric.WithError(err)
	}
	if len(tm.Rows) == 0 {
		return metric.WithError(fmt.Errorf("no results in ReadSelfMySQLThrottleMetric for query %s", query))
	}
	if len(tm.Rows) > 1 {
		return metric.WithError(fmt.Errorf("expecting single row in ReadSelfMySQLThrottleMetric for query %s", query))
	}

	metricsQueryType := GetMetricsQueryType(query)
	switch metricsQueryType {
	case MetricsQueryTypeSelect:
		metric.Value, metric.Err = tm.Rows[0][0].ToFloat64()
	case MetricsQueryTypeShowGlobal:
		// Columns are [Variable_name, Value]
		metric.Value, metric.Err = strconv.ParseFloat(tm.Rows[0][1].ToString(), 64)
	default:
		metric.Err = fmt.Errorf("unsupported metrics query type for query: %s", query)
	}

	return metric
}
