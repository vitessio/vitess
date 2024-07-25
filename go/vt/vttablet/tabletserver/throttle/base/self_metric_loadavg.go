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
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
)

var _ SelfMetric = registerSelfMetric(&LoadAvgSelfMetric{})

type LoadAvgSelfMetric struct {
	hostCpuCoreCount atomic.Int32
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

	coreCount := m.hostCpuCoreCount.Load()
	if coreCount == 0 {
		// Count cores. This number is not going to change in the lifetime of this tablet,
		// hence it makes sense to read it once then cache it.

		// We choose to read /proc/cpuinfo over executing "nproc" or similar commands.
		var coreCount int32
		f, err := os.Open("/proc/cpuinfo")
		if err != nil {
			return metric.WithError(err)
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			if strings.HasPrefix(scanner.Text(), "processor") {
				coreCount++
			}
		}

		if err := scanner.Err(); err != nil {
			return metric.WithError(err)
		}
		m.hostCpuCoreCount.Store(coreCount)
	}
	if coreCount == 0 {
		return metric.WithError(fmt.Errorf("could not determine number of cores"))
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
		metric.Value = loadAvg / float64(m.hostCpuCoreCount.Load())
	}
	return metric
}
