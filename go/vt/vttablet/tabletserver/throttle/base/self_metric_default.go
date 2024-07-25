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

	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
)

var _ SelfMetric = registerSelfMetric(&DefaultSelfMetric{})

type DefaultSelfMetric struct {
}

func (m *DefaultSelfMetric) Name() MetricName {
	return DefaultMetricName
}

func (m *DefaultSelfMetric) DefaultScope() Scope {
	return SelfScope
}

func (m *DefaultSelfMetric) DefaultThreshold() float64 {
	return 0
}

func (m *DefaultSelfMetric) RequiresConn() bool {
	return false
}

func (m *DefaultSelfMetric) Read(ctx context.Context, throttler ThrottlerMetricsPublisher, conn *connpool.Conn) *ThrottleMetric {
	return &ThrottleMetric{
		Err: fmt.Errorf("unexpected direct call to DefaultSelfMetric.Read"),
	}
}
