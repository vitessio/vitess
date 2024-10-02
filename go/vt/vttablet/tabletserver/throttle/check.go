/*
Copyright 2023 The Vitess Authors.

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

// This codebase originates from https://github.com/github/freno, See https://github.com/github/freno/blob/master/LICENSE
/*
	MIT License

	Copyright (c) 2017 GitHub

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/

package throttle

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

const (
	selfCheckInterval = 250 * time.Millisecond
)

var (
	statsThrottlerCheckAnyTotal = stats.NewCounter("ThrottlerCheckAnyTotal", "total number of checks")
	statsThrottlerCheckAnyError = stats.GetOrNewCounter("ThrottlerCheckAnyError", "total number of failed checks")
)

// CheckFlags provide hints for a check
type CheckFlags struct {
	Scope                 base.Scope
	ReadCheck             bool
	OverrideThreshold     float64
	OKIfNotExists         bool
	SkipRequestHeartbeats bool
	MultiMetricsEnabled   bool
}

// selfCheckFlags have no special hints
var selfCheckFlags = &CheckFlags{
	MultiMetricsEnabled: true,
}

// ThrottlerCheck provides methods for an app checking on metrics
type ThrottlerCheck struct {
	throttler *Throttler
}

// NewThrottlerCheck creates a ThrottlerCheck
func NewThrottlerCheck(throttler *Throttler) *ThrottlerCheck {
	return &ThrottlerCheck{
		throttler: throttler,
	}
}

// checkAppMetricResult allows an app to check on a metric
func (check *ThrottlerCheck) checkAppMetricResult(ctx context.Context, appName string, metricResultFunc base.MetricResultFunc, flags *CheckFlags) (checkResult *CheckResult) {
	// Handle deprioritized app logic
	denyApp := false
	//
	metricResult, threshold, matchedApp := check.throttler.AppRequestMetricResult(ctx, appName, metricResultFunc, denyApp)
	if flags.OverrideThreshold > 0 {
		threshold = flags.OverrideThreshold
	}
	value, err := metricResult.Get()
	if appName == "" {
		return NewCheckResult(tabletmanagerdatapb.CheckThrottlerResponseCode_APP_DENIED, http.StatusExpectationFailed, value, threshold, "", fmt.Errorf("no app indicated"))
	}

	var statusCode int
	var responseCode tabletmanagerdatapb.CheckThrottlerResponseCode

	switch {
	case err == base.ErrAppDenied:
		// app specifically not allowed to get metrics
		statusCode = http.StatusExpectationFailed // 417
		responseCode = tabletmanagerdatapb.CheckThrottlerResponseCode_APP_DENIED
	case err == base.ErrNoSuchMetric:
		// not collected yet, or metric does not exist
		statusCode = http.StatusNotFound // 404
		responseCode = tabletmanagerdatapb.CheckThrottlerResponseCode_UNKNOWN_METRIC
	case err != nil:
		// any error
		statusCode = http.StatusInternalServerError // 500
		responseCode = tabletmanagerdatapb.CheckThrottlerResponseCode_INTERNAL_ERROR
	case value > threshold:
		// casual throttling
		statusCode = http.StatusTooManyRequests // 429
		responseCode = tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED
		err = base.ErrThresholdExceeded
	default:
		// all good!
		statusCode = http.StatusOK // 200
		responseCode = tabletmanagerdatapb.CheckThrottlerResponseCode_OK
	}
	return NewCheckResult(responseCode, statusCode, value, threshold, matchedApp, err)
}

// Check is the core function that runs when a user wants to check a metric
func (check *ThrottlerCheck) Check(ctx context.Context, appName string, scope base.Scope, metricNames base.MetricNames, flags *CheckFlags) (checkResult *CheckResult) {
	checkResult = &CheckResult{
		StatusCode:   http.StatusOK,
		ResponseCode: tabletmanagerdatapb.CheckThrottlerResponseCode_OK,
		Metrics:      make(map[string]*MetricResult),
	}
	if len(metricNames) == 0 {
		metricNames = base.MetricNames{check.throttler.metricNameUsedAsDefault()}
	}
	metricNames = metricNames.Unique()
	applyMetricToCheckResult := func(metricName base.MetricName, metric *MetricResult) {
		checkResult.StatusCode = metric.StatusCode
		checkResult.ResponseCode = metric.ResponseCode
		checkResult.Value = metric.Value
		checkResult.Threshold = metric.Threshold
		checkResult.Error = metric.Error
		checkResult.Message = metric.Message
		checkResult.AppName = metric.AppName
		checkResult.Scope = metric.Scope
		checkResult.MetricName = metricName.String()
	}
	for _, metricName := range metricNames {
		// Make sure not to modify the given scope. We create a new scope variable to work with.
		metricScope := scope
		// It's possible that the metric name looks like "shard/loadavg". This means the the check is meant to
		// check the "loadavg" metric for the "shard" scope (while normally "loadavg" is a "self" scope metric).
		// So we first need to find out what the underlying metric name is ("loadavg" in this case), and then
		// see whether we need to change the scope.
		// It's also possible that the metric name is just "loadavg", in which case we extract the default
		// scope for this metric.
		// If given scope is defined, then it overrides any metric scope.
		// Noteworthy that self checks will always have a defined scope, because those are based on aggregated metrics.
		if disaggregatedScope, disaggregatedName, err := metricName.Disaggregated(); err == nil {
			if metricScope == base.UndefinedScope {
				// Client has not indicated any specific scope, so we use the disaggregated scope
				metricScope = disaggregatedScope
			}
			metricName = disaggregatedName
		}

		metricResultFunc := func() (metricResult base.MetricResult, threshold float64) {
			return check.throttler.getScopedMetric(metricScope, metricName)
		}

		metricCheckResult := check.checkAppMetricResult(ctx, appName, metricResultFunc, flags)
		if !throttlerapp.VitessName.Equals(appName) {
			go func(metricCheckResult *CheckResult) {
				if metricScope == base.UndefinedScope {
					// While we should never get here, the following code will panic if we do
					// because it will attempt to recreate ThrottlerCheckAnyTotal.
					// Out of abundance of caution, we will protect against such a scenario.
					return
				}
				stats.GetOrNewCounter(fmt.Sprintf("ThrottlerCheck%s%sTotal", textutil.SingleWordCamel(metricScope.String()), textutil.SingleWordCamel(metricName.String())), "").Add(1)
				if !metricCheckResult.IsOK() {
					stats.GetOrNewCounter(fmt.Sprintf("ThrottlerCheck%s%sError", textutil.SingleWordCamel(metricScope.String()), textutil.SingleWordCamel(metricName.String())), "").Add(1)
				}
			}(metricCheckResult)
		}
		if metricCheckResult.RecentlyChecked {
			checkResult.RecentlyChecked = true
		}
		metric := &MetricResult{
			StatusCode:   metricCheckResult.StatusCode,
			ResponseCode: metricCheckResult.ResponseCode,
			Value:        metricCheckResult.Value,
			Threshold:    metricCheckResult.Threshold,
			Error:        metricCheckResult.Error,
			Message:      metricCheckResult.Message,
			AppName:      metricCheckResult.AppName,
			Scope:        metricScope.String(), // This reports back the actual scope used for the check
		}
		checkResult.Metrics[metricName.String()] = metric
		if flags.MultiMetricsEnabled && !metricCheckResult.IsOK() && metricName != base.DefaultMetricName {
			// If we're checking multiple metrics, and one of them fails, we should return any of the failing metric.
			// For backwards compatibility, if flags.MultiMetricsEnabled is not set, we do not report back failing
			// metrics, because a v20 primary would not know how to deal with it, and is not expecting any of those
			// metrics.
			// The only metric we ever report back is the default metric, see below.
			applyMetricToCheckResult(metricName, metric)
		}
	}
	metricNameUsedAsDefault := check.throttler.metricNameUsedAsDefault()
	if metric, ok := checkResult.Metrics[metricNameUsedAsDefault.String()]; ok && checkResult.IsOK() {
		applyMetricToCheckResult(metricNameUsedAsDefault, metric)
	}
	if metric, ok := checkResult.Metrics[base.DefaultMetricName.String()]; ok && checkResult.IsOK() {
		// v20 compatibility: if this v21 server is a replica, reporting to a v20 primary,
		// then we must supply the v20-flavor check result.
		// If checkResult is not OK, then we will have populated these fields already by the failing metric.
		applyMetricToCheckResult(base.DefaultMetricName, metric)
	}
	go func(checkResult *CheckResult) {
		statsThrottlerCheckAnyTotal.Add(1)
		if !checkResult.IsOK() {
			statsThrottlerCheckAnyError.Add(1)
		}
	}(checkResult)
	go check.throttler.markRecentApp(appName, checkResult.StatusCode, checkResult.ResponseCode)
	return checkResult
}

// localCheck
func (check *ThrottlerCheck) localCheck(ctx context.Context, aggregatedMetricName string) (checkResult *CheckResult) {
	scope, metricName, err := base.DisaggregateMetricName(aggregatedMetricName)
	if err != nil {
		return NoSuchMetricCheckResult
	}
	checkResult = check.Check(ctx, throttlerapp.VitessName.String(), scope, base.MetricNames{metricName}, selfCheckFlags)

	if checkResult.IsOK() {
		check.throttler.markMetricHealthy(aggregatedMetricName)
	}
	if timeSinceHealthy, found := check.throttler.timeSinceMetricHealthy(aggregatedMetricName); found {
		go stats.GetOrNewGauge(fmt.Sprintf("ThrottlerCheck%sSecondsSinceHealthy", textutil.SingleWordCamel(scope.String())), fmt.Sprintf("seconds since last healthy check for %v", scope)).Set(int64(timeSinceHealthy.Seconds()))
	}

	return checkResult
}

func (check *ThrottlerCheck) reportAggregated(aggregatedMetricName string, metricResult base.MetricResult) {
	scope, metricName, err := base.DisaggregateMetricName(aggregatedMetricName)
	if err != nil {
		return
	}
	if value, err := metricResult.Get(); err == nil {
		stats.GetOrNewGaugeFloat64(fmt.Sprintf("ThrottlerAggregated%s%s", textutil.SingleWordCamel(scope.String()), textutil.SingleWordCamel(metricName.String())), fmt.Sprintf("aggregated value for %v", scope)).Set(value)
	}
}

// AggregatedMetrics is a convenience access method into throttler's `aggregatedMetricsSnapshot`
func (check *ThrottlerCheck) AggregatedMetrics(ctx context.Context) map[string]base.MetricResult {
	return check.throttler.aggregatedMetricsSnapshot()
}

// SelfChecks runs checks on all known metrics as if we were an app.
// This runs asynchronously, continuously, and independently of any user interaction
func (check *ThrottlerCheck) SelfChecks(ctx context.Context) {
	selfCheckTicker := time.NewTicker(selfCheckInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-selfCheckTicker.C:
				for metricName, metricResult := range check.AggregatedMetrics(ctx) {
					aggregatedMetricName := metricName
					metricResult := metricResult

					go check.localCheck(ctx, aggregatedMetricName)
					go check.reportAggregated(aggregatedMetricName, metricResult)
				}
			}
		}
	}()
}
