/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package throttle

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
)

const (
	// DefaultAppName is the app name used by vitess when app doesn't indicate its name
	DefaultAppName = "default"
	vitessAppName  = "vitess"

	selfCheckInterval = 250 * time.Millisecond
)

// CheckFlags provide hints for a check
type CheckFlags struct {
	ReadCheck         bool
	OverrideThreshold float64
	LowPriority       bool
	OKIfNotExists     bool
}

// StandardCheckFlags have no special hints
var StandardCheckFlags = &CheckFlags{}

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
func (check *ThrottlerCheck) checkAppMetricResult(ctx context.Context, appName string, storeType string, storeName string, metricResultFunc base.MetricResultFunc, flags *CheckFlags) (checkResult *CheckResult) {
	// Handle deprioritized app logic
	denyApp := false
	metricName := fmt.Sprintf("%s/%s", storeType, storeName)
	if flags.LowPriority {
		if _, exists := check.throttler.nonLowPriorityAppRequestsThrottled.Get(metricName); exists {
			// a non-deprioritized app, ie a "normal" app, has recently been throttled.
			// This is now a deprioritized app. Deny access to this request.
			denyApp = true
		}
	}
	//
	metricResult, threshold := check.throttler.AppRequestMetricResult(ctx, appName, metricResultFunc, denyApp)
	if flags.OverrideThreshold > 0 {
		threshold = flags.OverrideThreshold
	}
	value, err := metricResult.Get()
	if appName == "" {
		return NewCheckResult(http.StatusExpectationFailed, value, threshold, fmt.Errorf("no app indicated"))
	}

	var statusCode int

	if err == base.ErrAppDenied {
		// app specifically not allowed to get metrics
		statusCode = http.StatusExpectationFailed // 417
	} else if err == base.ErrNoSuchMetric {
		// not collected yet, or metric does not exist
		statusCode = http.StatusNotFound // 404
	} else if err != nil {
		// any error
		statusCode = http.StatusInternalServerError // 500
	} else if value > threshold {
		// casual throttling
		statusCode = http.StatusTooManyRequests // 429
		err = base.ErrThresholdExceeded

		if !flags.LowPriority && !flags.ReadCheck && appName != vitessAppName {
			// low priority requests will henceforth be denied
			go check.throttler.nonLowPriorityAppRequestsThrottled.SetDefault(metricName, true)
		}
	} else {
		// all good!
		statusCode = http.StatusOK // 200
	}
	return NewCheckResult(statusCode, value, threshold, err)
}

// Check is the core function that runs when a user wants to check a metric
func (check *ThrottlerCheck) Check(ctx context.Context, appName string, storeType string, storeName string, remoteAddr string, flags *CheckFlags) (checkResult *CheckResult) {
	var metricResultFunc base.MetricResultFunc
	switch storeType {
	case "mysql":
		{
			metricResultFunc = func() (metricResult base.MetricResult, threshold float64) {
				return check.throttler.getMySQLClusterMetrics(ctx, storeName)
			}
		}
	}
	if metricResultFunc == nil {
		return NoSuchMetricCheckResult
	}

	checkResult = check.checkAppMetricResult(ctx, appName, storeType, storeName, metricResultFunc, flags)
	atomic.StoreInt64(&check.throttler.lastCheckTimeNano, time.Now().UnixNano())

	go func(statusCode int) {
		stats.GetOrNewCounter("ThrottlerCheckAnyTotal", "total number of checks").Add(1)
		stats.GetOrNewCounter(fmt.Sprintf("ThrottlerCheckAny%s%sTotal", textutil.SingleWordCamel(storeType), textutil.SingleWordCamel(storeName)), "").Add(1)

		if statusCode != http.StatusOK {
			stats.GetOrNewCounter("ThrottlerCheckAnyError", "total number of failed checks").Add(1)
			stats.GetOrNewCounter(fmt.Sprintf("ThrottlerCheckAny%s%sError", textutil.SingleWordCamel(storeType), textutil.SingleWordCamel(storeName)), "").Add(1)
		}

		check.throttler.markRecentApp(appName, remoteAddr)
	}(checkResult.StatusCode)

	return checkResult
}

func (check *ThrottlerCheck) splitMetricTokens(metricName string) (storeType string, storeName string, err error) {
	metricTokens := strings.Split(metricName, "/")
	if len(metricTokens) != 2 {
		return storeType, storeName, base.ErrNoSuchMetric
	}
	storeType = metricTokens[0]
	storeName = metricTokens[1]

	return storeType, storeName, nil
}

// localCheck
func (check *ThrottlerCheck) localCheck(ctx context.Context, metricName string) (checkResult *CheckResult) {
	storeType, storeName, err := check.splitMetricTokens(metricName)
	if err != nil {
		return NoSuchMetricCheckResult
	}
	checkResult = check.Check(ctx, vitessAppName, storeType, storeName, "local", StandardCheckFlags)

	if checkResult.StatusCode == http.StatusOK {
		check.throttler.markMetricHealthy(metricName)
	}
	if timeSinceHealthy, found := check.throttler.timeSinceMetricHealthy(metricName); found {
		stats.GetOrNewGauge(fmt.Sprintf("ThrottlerCheck%s%sSecondsSinceHealthy", textutil.SingleWordCamel(storeType), textutil.SingleWordCamel(storeName)), fmt.Sprintf("seconds since last healthy cehck for %s.%s", storeType, storeName)).Set(int64(timeSinceHealthy.Seconds()))
	}

	return checkResult
}

func (check *ThrottlerCheck) reportAggregated(metricName string, metricResult base.MetricResult) {
	storeType, storeName, err := check.splitMetricTokens(metricName)
	if err != nil {
		return
	}
	if value, err := metricResult.Get(); err == nil {
		stats.GetOrNewGaugeFloat64(fmt.Sprintf("ThrottlerAggregated%s%s", textutil.SingleWordCamel(storeType), textutil.SingleWordCamel(storeName)), fmt.Sprintf("aggregated value for %s.%s", storeType, storeName)).Set(value)
	}
}

// AggregatedMetrics is a convenience access method into throttler's `aggregatedMetricsSnapshot`
func (check *ThrottlerCheck) AggregatedMetrics(ctx context.Context) map[string]base.MetricResult {
	return check.throttler.aggregatedMetricsSnapshot()
}

// MetricsHealth is a convenience access method into throttler's `metricsHealthSnapshot`
func (check *ThrottlerCheck) MetricsHealth() map[string](*base.MetricHealth) {
	return check.throttler.metricsHealthSnapshot()
}

// SelfChecks runs checks on all known metrics as if we were an app.
// This runs asynchronously, continuously, and independently of any user interaction
func (check *ThrottlerCheck) SelfChecks(ctx context.Context) {
	selfCheckTicker := time.NewTicker(selfCheckInterval)
	go func() {
		for range selfCheckTicker.C {
			for metricName, metricResult := range check.AggregatedMetrics(ctx) {
				metricName := metricName
				metricResult := metricResult
				go check.localCheck(ctx, metricName)
				go check.reportAggregated(metricName, metricResult)
			}
		}
	}()
}
