// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"time"

	"github.com/youtube/vitess/go/stats"
)

// QueryServiceStats contains stats that used in queryservice level.
type QueryServiceStats struct {
	// MySQLStats shows the time histogram for operations spent on mysql side.
	MySQLStats *stats.Timings
	// QueryStats shows the time histogram for each type of queries.
	QueryStats *stats.Timings
	// WaitStats shows the time histogram for wait operations
	WaitStats *stats.Timings
	// KillStats shows number of connections being killed.
	KillStats *stats.Counters
	// InfoErrors shows number of various non critical errors happened.
	InfoErrors *stats.Counters
	// ErrorStats shows number of critial erros happened.
	ErrorStats *stats.Counters
	// InternalErros shows number of errors from internal components.
	InternalErrors *stats.Counters
	// QPSRates shows the qps.
	QPSRates *stats.Rates
	// ResultStats shows the histogram of number of rows returned.
	ResultStats *stats.Histogram
	// SpotCheckCount shows the number of spot check events happened.
	SpotCheckCount *stats.Int
}

// NewQueryServiceStats returns a new QueryServiceStats instance.
func NewQueryServiceStats(statsPrefix string, enablePublishStats bool) *QueryServiceStats {
	mysqlStatsName := ""
	queryStatsName := ""
	qpsRateName := ""
	waitStatsName := ""
	killStatsName := ""
	infoErrorsName := ""
	errorStatsName := ""
	internalErrorsName := ""
	resultStatsName := ""
	spotCheckCountName := ""
	if enablePublishStats {
		mysqlStatsName = statsPrefix + "Mysql"
		queryStatsName = statsPrefix + "Queries"
		qpsRateName = statsPrefix + "QPS"
		waitStatsName = statsPrefix + "Waits"
		killStatsName = statsPrefix + "Kills"
		infoErrorsName = statsPrefix + "InfoErrors"
		errorStatsName = statsPrefix + "Errors"
		internalErrorsName = statsPrefix + "InternalErrors"
		resultStatsName = statsPrefix + "Results"
		spotCheckCountName = statsPrefix + "RowcacheSpotCheckCount"
	}
	resultBuckets := []int64{0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000}
	queryStats := stats.NewTimings(queryStatsName)
	return &QueryServiceStats{
		MySQLStats:     stats.NewTimings(mysqlStatsName),
		QueryStats:     queryStats,
		WaitStats:      stats.NewTimings(waitStatsName),
		KillStats:      stats.NewCounters(killStatsName),
		InfoErrors:     stats.NewCounters(infoErrorsName),
		ErrorStats:     stats.NewCounters(errorStatsName),
		InternalErrors: stats.NewCounters(internalErrorsName),
		QPSRates:       stats.NewRates(qpsRateName, queryStats, 15, 60*time.Second),
		ResultStats:    stats.NewHistogram(resultStatsName, resultBuckets),
		SpotCheckCount: stats.NewInt(spotCheckCountName),
	}
}
