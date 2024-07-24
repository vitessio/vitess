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

type Metric interface {
	Name() MetricName
	DefaultScope() Scope
	DefaultThreshold() float32
	RequiresConn() bool
	Read(ctx context.Context, conn *connpool.Conn) *ThrottleMetric
}

// ReadSelfMySQLThrottleMetric reads a metric using a given MySQL connection and a query.
func ReadSelfMySQLThrottleMetric(ctx context.Context, conn *connpool.Conn, query string) *ThrottleMetric {
	metric := &ThrottleMetric{
		Scope: SelfScope,
	}
	if query == "" {
		return metric
	}

	tm, err := conn.Exec(ctx, query, 1, true)
	if err != nil {
		return metric.WithError(err)
	}
	row := tm.Named().Row()
	if row == nil {
		return metric.WithError(fmt.Errorf("no results for readSelfThrottleMetric"))
	}

	metricsQueryType := GetMetricsQueryType(query)
	switch metricsQueryType {
	case MetricsQueryTypeSelect:
		// We expect a single row, single column result.
		// The "for" iteration below is just a way to get first result without knowing column name
		for k := range row {
			metric.Value, metric.Err = row.ToFloat64(k)
		}
	case MetricsQueryTypeShowGlobal:
		metric.Value, metric.Err = strconv.ParseFloat(row["Value"].ToString(), 64)
	default:
		metric.Err = fmt.Errorf("unsupported metrics query type for query: %s", query)
	}

	return metric
}
