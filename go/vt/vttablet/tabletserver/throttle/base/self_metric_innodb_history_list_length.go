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
	"math"
	"sync/atomic"
	"time"
)

var (
	historyListLengthQuery = "select count as history_len from information_schema.INNODB_METRICS where name = 'trx_rseg_history_len'"

	cachedHistoryListLengthMetric     atomic.Pointer[ThrottleMetric]
	historyListLengthCacheDuration    = 5 * time.Second
	historyListLengthDefaultThreshold = math.Pow10(9)
)

var _ SelfMetric = registerSelfMetric(&HistoryListLengthSelfMetric{})

type HistoryListLengthSelfMetric struct{}

func (m *HistoryListLengthSelfMetric) Name() MetricName {
	return HistoryListLengthMetricName
}

func (m *HistoryListLengthSelfMetric) DefaultScope() Scope {
	return SelfScope
}

func (m *HistoryListLengthSelfMetric) DefaultThreshold() float64 {
	return historyListLengthDefaultThreshold
}

func (m *HistoryListLengthSelfMetric) RequiresConn() bool {
	return true
}

func (m *HistoryListLengthSelfMetric) Read(ctx context.Context, params *SelfMetricReadParams) *ThrottleMetric {
	// This function will be called sequentially, and therefore does not need strong mutex protection. Still, we use atomics
	// to ensure correctness in case an external goroutine tries to read the metric concurrently.
	metric := cachedHistoryListLengthMetric.Load()
	if metric != nil {
		return metric
	}
	metric = ReadSelfMySQLThrottleMetric(ctx, params.Conn, historyListLengthQuery)
	cachedHistoryListLengthMetric.Store(metric)
	time.AfterFunc(historyListLengthCacheDuration, func() {
		cachedHistoryListLengthMetric.Store(nil)
	})
	return metric
}
