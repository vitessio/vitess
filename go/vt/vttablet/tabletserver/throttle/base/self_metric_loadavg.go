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
	"runtime"

	"vitess.io/vitess/go/osutil"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
)

var _ SelfMetric = registerSelfMetric(&LoadAvgSelfMetric{})

type LoadAvgSelfMetric struct {
}

func (m *LoadAvgSelfMetric) Name() MetricName {
	return LoadAvgMetricName
}

func (m *LoadAvgSelfMetric) DefaultScope() Scope {
	return SelfScope
}

func (m *LoadAvgSelfMetric) DefaultThreshold() float64 {
	return 1.0
}

func (m *LoadAvgSelfMetric) RequiresConn() bool {
	return false
}

func (m *LoadAvgSelfMetric) Read(ctx context.Context, throttler ThrottlerMetricsPublisher, conn *connpool.Conn) *ThrottleMetric {
	metric := &ThrottleMetric{
		Scope: SelfScope,
	}
	val, err := osutil.LoadAvg()
	if err != nil {
		return metric.WithError(err)
	}
	metric.Value = val / float64(runtime.NumCPU())
	return metric
}
