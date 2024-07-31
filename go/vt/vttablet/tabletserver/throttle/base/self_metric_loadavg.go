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
	"os"
	"runtime"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
)

var (
	loadavgOnlyAvailableOnLinuxMetric = &ThrottleMetric{
		Scope: SelfScope,
		Err:   fmt.Errorf("loadavg metric is only available on Linux"),
	}
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
	if runtime.GOOS != "linux" {
		return loadavgOnlyAvailableOnLinuxMetric
	}
	metric := &ThrottleMetric{
		Scope: SelfScope,
	}
	{
		content, err := os.ReadFile("/proc/loadavg")
		if err != nil {
			return metric.WithError(err)
		}
		fields := strings.Fields(string(content))
		if len(fields) == 0 {
			return metric.WithError(fmt.Errorf("unexpected /proc/loadavg content"))
		}
		loadAvg, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			return metric.WithError(err)
		}
		metric.Value = loadAvg / float64(runtime.NumCPU())
	}
	return metric
}
