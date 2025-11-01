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
	"errors"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/timer"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

var (
	mysqlHostMetricsRpcTimeout   = 5 * time.Second
	mysqlHostMetricsRateLimit    = 10 * time.Second
	mysqlHostMetricsRateLimiter  atomic.Pointer[timer.RateLimiter]
	lastMySQLHostMetricsResponse atomic.Pointer[tabletmanagerdatapb.MysqlHostMetricsResponse]
)

// getMysqlMetricsRateLimiter returns a rate limiter that is active until the given context is cancelled.
// This function will be called sequentially, but nonetheless it offers _some_ concurrent safety. Namely,
// that a created rate limiter is guaranteed to be cleaned up
func getMysqlMetricsRateLimiter(ctx context.Context, rateLimit time.Duration) *timer.RateLimiter {
	rateLimiter := mysqlHostMetricsRateLimiter.Load()
	if rateLimiter == nil {
		rateLimiter = timer.NewRateLimiter(rateLimit)
		go func() {
			defer mysqlHostMetricsRateLimiter.Store(nil)
			defer rateLimiter.Stop()
			<-ctx.Done()
		}()
		mysqlHostMetricsRateLimiter.Store(rateLimiter)
	}
	return rateLimiter
}

// readMysqlHostMetrics reads MySQL host metrics sporadically from the tablet manager (which in turn reads
// them from mysql deamon). The metrics are then cached, whether successful or not.
// This idea is that is is very wasteful to read these metrics for every single query. E.g. right now the throttler
// can issue 4 reads per second, which is wasteful to go through two RPCs to get the disk space usage for example. Even the load
// average on the MySQL server is not that susceptible to change.
func readMysqlHostMetrics(ctx context.Context, params *SelfMetricReadParams) error {
	if params.TmClient == nil {
		return errors.New("tmClient is nil")
	}
	if params.TabletInfo == nil {
		return errors.New("tabletInfo is nil")
	}
	rateLimiter := getMysqlMetricsRateLimiter(ctx, mysqlHostMetricsRateLimit)
	err := rateLimiter.Do(func() error {
		ctx, cancel := context.WithTimeout(ctx, mysqlHostMetricsRpcTimeout)
		defer cancel()

		resp, err := params.TmClient.MysqlHostMetrics(ctx, params.TabletInfo.Tablet, &tabletmanagerdatapb.MysqlHostMetricsRequest{})
		if err != nil {
			return err
		}
		lastMySQLHostMetricsResponse.Store(resp)
		return nil
	})
	return err
}

// getMysqlHostMetric gets a metric from the last read MySQL host metrics. The metric will either be directly read from
// tablet manager (which then reads it from the mysql deamon), or from the cache.
func getMysqlHostMetric(ctx context.Context, params *SelfMetricReadParams, mysqlHostMetricName string) *ThrottleMetric {
	metric := &ThrottleMetric{
		Scope: SelfScope,
	}
	if err := readMysqlHostMetrics(ctx, params); err != nil {
		return metric.WithError(err)
	}
	resp := lastMySQLHostMetricsResponse.Load()
	if resp == nil {
		return metric.WithError(ErrNoResultYet)
	}
	mysqlMetric := resp.HostMetrics.Metrics[mysqlHostMetricName]
	if mysqlMetric == nil {
		return metric.WithError(ErrNoSuchMetric)
	}
	metric.Value = mysqlMetric.Value
	if mysqlMetric.Error != nil {
		metric.Err = errors.New(mysqlMetric.Error.Message)
	}
	return metric
}

var _ SelfMetric = registerSelfMetric(&MysqldLoadAvgSelfMetric{})
var _ SelfMetric = registerSelfMetric(&MysqldDatadirUsedRatioSelfMetric{})

// MysqldLoadAvgSelfMetric stands for the load average per cpu, on the MySQL host.
type MysqldLoadAvgSelfMetric struct {
}

func (m *MysqldLoadAvgSelfMetric) Name() MetricName {
	return MysqldLoadAvgMetricName
}

func (m *MysqldLoadAvgSelfMetric) DefaultScope() Scope {
	return SelfScope
}

func (m *MysqldLoadAvgSelfMetric) DefaultThreshold() float64 {
	return 1.0
}

func (m *MysqldLoadAvgSelfMetric) RequiresConn() bool {
	return false
}

func (m *MysqldLoadAvgSelfMetric) Read(ctx context.Context, params *SelfMetricReadParams) *ThrottleMetric {
	return getMysqlHostMetric(ctx, params, "loadavg")
}

// MysqldDatadirUsedRatioSelfMetric stands for the disk space usage of the mount where MySQL's datadir is located.
// Range: 0.0 (empty) - 1.0 (full)
type MysqldDatadirUsedRatioSelfMetric struct {
}

func (m *MysqldDatadirUsedRatioSelfMetric) Name() MetricName {
	return MysqldDatadirUsedRatioMetricName
}

func (m *MysqldDatadirUsedRatioSelfMetric) DefaultScope() Scope {
	return SelfScope
}

func (m *MysqldDatadirUsedRatioSelfMetric) DefaultThreshold() float64 {
	return 0.98
}

func (m *MysqldDatadirUsedRatioSelfMetric) RequiresConn() bool {
	return false
}

func (m *MysqldDatadirUsedRatioSelfMetric) Read(ctx context.Context, params *SelfMetricReadParams) *ThrottleMetric {
	return getMysqlHostMetric(ctx, params, "datadir-used-ratio")
}
