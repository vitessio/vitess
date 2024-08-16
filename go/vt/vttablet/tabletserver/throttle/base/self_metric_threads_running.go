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

	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
)

var (
	threadsRunningMetricQuery = "show global status like 'threads_running'"
)

var _ SelfMetric = registerSelfMetric(&ThreadsRunningSelfMetric{})

type ThreadsRunningSelfMetric struct {
}

func (m *ThreadsRunningSelfMetric) Name() MetricName {
	return ThreadsRunningMetricName
}

func (m *ThreadsRunningSelfMetric) DefaultScope() Scope {
	return SelfScope
}

func (m *ThreadsRunningSelfMetric) DefaultThreshold() float64 {
	return 100
}

func (m *ThreadsRunningSelfMetric) RequiresConn() bool {
	return true
}

func (m *ThreadsRunningSelfMetric) Read(ctx context.Context, throttler ThrottlerMetricsPublisher, conn *connpool.Conn) *ThrottleMetric {
	return ReadSelfMySQLThrottleMetric(ctx, conn, threadsRunningMetricQuery)
}
