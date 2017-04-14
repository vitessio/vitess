// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package tabletenv maintains environment variables and types that
// are common for all packages of tabletserver.
package tabletenv

import (
	"context"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/callerid"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

var (
	// MySQLStats shows the time histogram for operations spent on mysql side.
	MySQLStats = stats.NewTimings("Mysql")
	// QueryStats shows the time histogram for each type of queries.
	QueryStats = stats.NewTimings("Queries")
	// QPSRates shows the qps of QueryStats. Sample every 5 seconds and keep samples for up to 15 mins.
	QPSRates = stats.NewRates("QPS", QueryStats, 15*60/5, 5*time.Second)
	// WaitStats shows the time histogram for wait operations
	WaitStats = stats.NewTimings("Waits")
	// KillStats shows number of connections being killed.
	KillStats = stats.NewCounters("Kills", "Transactions", "Queries")
	// ErrorStats shows number of critial erros happened.
	ErrorStats = stats.NewCounters(
		"Errors",
		vtrpcpb.Code_OK.String(),
		vtrpcpb.Code_CANCELED.String(),
		vtrpcpb.Code_UNKNOWN.String(),
		vtrpcpb.Code_INVALID_ARGUMENT.String(),
		vtrpcpb.Code_DEADLINE_EXCEEDED.String(),
		vtrpcpb.Code_NOT_FOUND.String(),
		vtrpcpb.Code_ALREADY_EXISTS.String(),
		vtrpcpb.Code_PERMISSION_DENIED.String(),
		vtrpcpb.Code_UNAUTHENTICATED.String(),
		vtrpcpb.Code_RESOURCE_EXHAUSTED.String(),
		vtrpcpb.Code_FAILED_PRECONDITION.String(),
		vtrpcpb.Code_ABORTED.String(),
		vtrpcpb.Code_OUT_OF_RANGE.String(),
		vtrpcpb.Code_UNIMPLEMENTED.String(),
		vtrpcpb.Code_INTERNAL.String(),
		vtrpcpb.Code_UNAVAILABLE.String(),
		vtrpcpb.Code_DATA_LOSS.String(),
	)
	// InternalErrors shows number of errors from internal components.
	InternalErrors = stats.NewCounters("InternalErrors", "Task", "StrayTransactions", "Panic", "HungQuery", "Schema", "TwopcCommit", "TwopcResurrection", "WatchdogFail")
	// Unresolved tracks unresolved items. For now it's just Prepares.
	Unresolved = stats.NewCounters("Unresolved", "Prepares")
	// UserTableQueryCount shows number of queries received for each CallerID/table combination.
	UserTableQueryCount = stats.NewMultiCounters("UserTableQueryCount", []string{"TableName", "CallerID", "Type"})
	// UserTableQueryTimesNs shows total latency for each CallerID/table combination.
	UserTableQueryTimesNs = stats.NewMultiCounters("UserTableQueryTimesNs", []string{"TableName", "CallerID", "Type"})
	// UserTransactionCount shows number of transactions received for each CallerID.
	UserTransactionCount = stats.NewMultiCounters("UserTransactionCount", []string{"CallerID", "Conclusion"})
	// UserTransactionTimesNs shows total transaction latency for each CallerID.
	UserTransactionTimesNs = stats.NewMultiCounters("UserTransactionTimesNs", []string{"CallerID", "Conclusion"})
	// ResultStats shows the histogram of number of rows returned.
	ResultStats = stats.NewHistogram("Results", []int64{0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000})
	// TableaclAllowed tracks the number allows.
	TableaclAllowed = stats.NewMultiCounters("TableACLAllowed", []string{"TableName", "TableGroup", "PlanID", "Username"})
	// TableaclDenied tracks the number of denials.
	TableaclDenied = stats.NewMultiCounters("TableACLDenied", []string{"TableName", "TableGroup", "PlanID", "Username"})
	// TableaclPseudoDenied tracks the number of pseudo denies.
	TableaclPseudoDenied = stats.NewMultiCounters("TableACLPseudoDenied", []string{"TableName", "TableGroup", "PlanID", "Username"})
)

// RecordUserQuery records the query data against the user.
func RecordUserQuery(ctx context.Context, tableName sqlparser.TableIdent, queryType string, duration int64) {
	username := callerid.GetPrincipal(callerid.EffectiveCallerIDFromContext(ctx))
	if username == "" {
		username = callerid.GetUsername(callerid.ImmediateCallerIDFromContext(ctx))
	}
	UserTableQueryCount.Add([]string{tableName.String(), username, queryType}, 1)
	UserTableQueryTimesNs.Add([]string{tableName.String(), username, queryType}, int64(duration))
}

// LogError logs panics and increments InternalErrors.
func LogError() {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		InternalErrors.Add("Panic", 1)
	}
}
