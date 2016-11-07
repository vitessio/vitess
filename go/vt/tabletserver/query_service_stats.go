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
	// UserTableQueryCount shows number of queries received for each CallerID/table combination.
	UserTableQueryCount *stats.MultiCounters
	// UserTableQueryTimesNs shows total latency for each CallerID/table combination.
	UserTableQueryTimesNs *stats.MultiCounters
	// UserTransactionCount shows number of transactions received for each CallerID.
	UserTransactionCount *stats.MultiCounters
	// UserTransactionTimesNs shows total transaction latency for each CallerID.
	UserTransactionTimesNs *stats.MultiCounters
	// QPSRates shows the qps.
	QPSRates *stats.Rates
	// ResultStats shows the histogram of number of rows returned.
	ResultStats *stats.Histogram
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
	userTableQueryCountName := ""
	userTableQueryTimesNsName := ""
	userTransactionCountName := ""
	userTransactionTimesNsName := ""
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
		userTableQueryCountName = statsPrefix + "UserTableQueryCount"
		userTableQueryTimesNsName = statsPrefix + "UserTableQueryTimesNs"
		userTransactionCountName = statsPrefix + "UserTransactionCount"
		userTransactionTimesNsName = statsPrefix + "UserTransactionTimesNs"
	}
	resultBuckets := []int64{0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000}
	queryStats := stats.NewTimings(queryStatsName)
	return &QueryServiceStats{
		MySQLStats: stats.NewTimings(mysqlStatsName),
		QueryStats: queryStats,
		WaitStats:  stats.NewTimings(waitStatsName),
		KillStats:  stats.NewCounters(killStatsName, "Transactions", "Queries"),
		InfoErrors: stats.NewCounters(infoErrorsName, "Retry", "Fatal", "DupKey"),
		ErrorStats: stats.NewCounters(errorStatsName, "Fail", "TxPoolFull", "NotInTx", "Deadlock"),
		InternalErrors: stats.NewCounters(internalErrorsName, "Task",
			"StrayTransactions", "Panic", "HungQuery", "Schema", "TwopcCommit", "TwopcResurrection"),
		UserTableQueryCount: stats.NewMultiCounters(
			userTableQueryCountName, []string{"TableName", "CallerID", "Type"}),
		UserTableQueryTimesNs: stats.NewMultiCounters(
			userTableQueryTimesNsName, []string{"TableName", "CallerID", "Type"}),
		UserTransactionCount: stats.NewMultiCounters(
			userTransactionCountName, []string{"CallerID", "Conclusion"}),
		UserTransactionTimesNs: stats.NewMultiCounters(
			userTransactionTimesNsName, []string{"CallerID", "Conclusion"}),
		// Sample every 5 seconds and keep samples for up to 15 minutes.
		QPSRates:    stats.NewRates(qpsRateName, queryStats, 15*60/5, 5*time.Second),
		ResultStats: stats.NewHistogram(resultStatsName, resultBuckets),
	}
}
