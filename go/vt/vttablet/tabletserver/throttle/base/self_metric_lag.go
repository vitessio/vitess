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
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
)

var (
	lagSelfMetricQueryBase  = "select unix_timestamp(now(6))-max(ts/1000000000) as replication_lag from %s.heartbeat"
	lagSelfDefaultThreshold = 5 * time.Second
)

var _ SelfMetric = registerSelfMetric(&LagSelfMetric{})

type LagSelfMetric struct {
	lagSelfMetricQuery atomic.Value
}

// SetQuery is only used by unit tests to override the query.
func (m *LagSelfMetric) SetQuery(query string) {
	m.lagSelfMetricQuery.Store(query)
}

func (m *LagSelfMetric) GetQuery() string {
	if query := m.lagSelfMetricQuery.Load(); query == nil {
		m.lagSelfMetricQuery.Store(sqlparser.BuildParsedQuery(lagSelfMetricQueryBase, sidecar.GetIdentifier()).Query)
	}
	return m.lagSelfMetricQuery.Load().(string)
}

func (m *LagSelfMetric) Name() MetricName {
	return LagMetricName
}

func (m *LagSelfMetric) DefaultScope() Scope {
	return ShardScope
}

func (m *LagSelfMetric) DefaultThreshold() float64 {
	return lagSelfDefaultThreshold.Seconds()
}

func (m *LagSelfMetric) RequiresConn() bool {
	return true
}

func (m *LagSelfMetric) Read(ctx context.Context, throttler ThrottlerMetricsPublisher, conn *connpool.Conn) *ThrottleMetric {
	return ReadSelfMySQLThrottleMetric(ctx, conn, m.GetQuery())
}
