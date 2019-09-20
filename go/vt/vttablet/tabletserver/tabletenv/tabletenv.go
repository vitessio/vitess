/*
Copyright 2019 The Vitess Authors.

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

// Package tabletenv maintains environment variables and types that
// are common for all packages of tabletserver.
package tabletenv

import (
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	// MySQLStats shows the time histogram for operations spent on mysql side.
	MySQLStats = stats.NewTimings("Mysql", "MySQl query time", "operation")
	// QueryStats shows the time histogram for each type of queries.
	QueryStats = stats.NewTimings("Queries", "MySQL query timings", "plan_type")
	// QPSRates shows the qps of QueryStats. Sample every 5 seconds and keep samples for up to 15 mins.
	QPSRates = stats.NewRates("QPS", QueryStats, 15*60/5, 5*time.Second)
	// WaitStats shows the time histogram for wait operations
	WaitStats = stats.NewTimings("Waits", "Wait operations", "type")
	// KillStats shows number of connections being killed.
	KillStats = stats.NewCountersWithSingleLabel("Kills", "Number of connections being killed", "query_type", "Transactions", "Queries")
	// ErrorStats shows number of critial errors happened.
	ErrorStats = stats.NewCountersWithSingleLabel(
		"Errors",
		"Critical errors",
		"error_code",
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
	InternalErrors = stats.NewCountersWithSingleLabel("InternalErrors", "Internal component errors", "type", "Task", "StrayTransactions", "Panic", "HungQuery", "Schema", "TwopcCommit", "TwopcResurrection", "WatchdogFail", "Messages")
	// Warnings shows number of warnings
	Warnings = stats.NewCountersWithSingleLabel("Warnings", "Warnings", "type", "ResultsExceeded")
	// Unresolved tracks unresolved items. For now it's just Prepares.
	Unresolved = stats.NewGaugesWithSingleLabel("Unresolved", "Unresolved items", "item_type", "Prepares")
	// UserTableQueryCount shows number of queries received for each CallerID/table combination.
	UserTableQueryCount = stats.NewCountersWithMultiLabels(
		"UserTableQueryCount",
		"Queries received for each CallerID/table combination",
		[]string{"TableName", "CallerID", "Type"})
	// UserTableQueryTimesNs shows total latency for each CallerID/table combination.
	UserTableQueryTimesNs = stats.NewCountersWithMultiLabels(
		"UserTableQueryTimesNs",
		"Total latency for each CallerID/table combination",
		[]string{"TableName", "CallerID", "Type"})
	// UserTransactionCount shows number of transactions received for each CallerID.
	UserTransactionCount = stats.NewCountersWithMultiLabels(
		"UserTransactionCount",
		"transactions received for each CallerID",
		[]string{"CallerID", "Conclusion"})
	// UserTransactionTimesNs shows total transaction latency for each CallerID.
	UserTransactionTimesNs = stats.NewCountersWithMultiLabels(
		"UserTransactionTimesNs",
		"Total transaction latency for each CallerID",
		[]string{"CallerID", "Conclusion"})
	// ResultStats shows the histogram of number of rows returned.
	ResultStats = stats.NewHistogram("Results",
		"Distribution of rows returned",
		[]int64{0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000})
	// TableaclAllowed tracks the number allows.
	TableaclAllowed = stats.NewCountersWithMultiLabels(
		"TableACLAllowed",
		"ACL acceptances",
		[]string{"TableName", "TableGroup", "PlanID", "Username"})
	// TableaclDenied tracks the number of denials.
	TableaclDenied = stats.NewCountersWithMultiLabels(
		"TableACLDenied",
		"ACL denials",
		[]string{"TableName", "TableGroup", "PlanID", "Username"})
	// TableaclPseudoDenied tracks the number of pseudo denies.
	TableaclPseudoDenied = stats.NewCountersWithMultiLabels(
		"TableACLPseudoDenied",
		"ACL pseudodenials",
		[]string{"TableName", "TableGroup", "PlanID", "Username"})
	// Infof can be overridden during tests
	Infof = log.Infof
	// Warningf can be overridden during tests
	Warningf = log.Warningf
	// Errorf can be overridden during tests
	Errorf = log.Errorf
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
