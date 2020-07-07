/*
Copyright 2020 The Vitess Authors.

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

package tabletenv

import (
	"time"

	"vitess.io/vitess/go/stats"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
)

// Stats contains tracked by various parts of TabletServer.
type Stats struct {
	MySQLTimings           *servenv.TimingsWrapper        // Time spent executing MySQL commands
	QueryTimings           *servenv.TimingsWrapper        // Query timings
	QPSRates               *stats.Rates                   // Human readable QPS rates
	WaitTimings            *servenv.TimingsWrapper        // waits like Consolidations etc
	KillCounters           *stats.CountersWithSingleLabel // Connection and transaction kills
	ErrorCounters          *stats.CountersWithSingleLabel
	InternalErrors         *stats.CountersWithSingleLabel
	Warnings               *stats.CountersWithSingleLabel
	Unresolved             *stats.GaugesWithSingleLabel   // For now, only Prepares are tracked
	UserTableQueryCount    *stats.CountersWithMultiLabels // Per CallerID/table counts
	UserTableQueryTimesNs  *stats.CountersWithMultiLabels // Per CallerID/table latencies
	UserTransactionCount   *stats.CountersWithMultiLabels // Per CallerID transaction counts
	UserTransactionTimesNs *stats.CountersWithMultiLabels // Per CallerID transaction latencies
	ResultHistogram        *stats.Histogram               // Row count histograms
	TableaclAllowed        *stats.CountersWithMultiLabels // Number of allows
	TableaclDenied         *stats.CountersWithMultiLabels // Number of denials
	TableaclPseudoDenied   *stats.CountersWithMultiLabels // Number of pseudo denials

	UserActiveReservedCount *stats.CountersWithSingleLabel // Per CallerID active reserved connection counts
	UserReservedCount       *stats.CountersWithSingleLabel // Per CallerID reserved connection counts
	UserReservedTimesNs     *stats.CountersWithSingleLabel // Per CallerID reserved connection duration
}

// NewStats instantiates a new set of stats scoped by exporter.
func NewStats(exporter *servenv.Exporter) *Stats {
	stats := &Stats{
		MySQLTimings: exporter.NewTimings("Mysql", "MySQl query time", "operation"),
		QueryTimings: exporter.NewTimings("Queries", "MySQL query timings", "plan_type"),
		WaitTimings:  exporter.NewTimings("Waits", "Wait operations", "type"),
		KillCounters: exporter.NewCountersWithSingleLabel("Kills", "Number of connections being killed", "query_type", "Transactions", "Queries", "ReservedConnection"),
		ErrorCounters: exporter.NewCountersWithSingleLabel(
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
		),
		InternalErrors:         exporter.NewCountersWithSingleLabel("InternalErrors", "Internal component errors", "type", "Task", "StrayTransactions", "Panic", "HungQuery", "Schema", "TwopcCommit", "TwopcResurrection", "WatchdogFail", "Messages"),
		Warnings:               exporter.NewCountersWithSingleLabel("Warnings", "Warnings", "type", "ResultsExceeded"),
		Unresolved:             exporter.NewGaugesWithSingleLabel("Unresolved", "Unresolved items", "item_type", "Prepares"),
		UserTableQueryCount:    exporter.NewCountersWithMultiLabels("UserTableQueryCount", "Queries received for each CallerID/table combination", []string{"TableName", "CallerID", "Type"}),
		UserTableQueryTimesNs:  exporter.NewCountersWithMultiLabels("UserTableQueryTimesNs", "Total latency for each CallerID/table combination", []string{"TableName", "CallerID", "Type"}),
		UserTransactionCount:   exporter.NewCountersWithMultiLabels("UserTransactionCount", "transactions received for each CallerID", []string{"CallerID", "Conclusion"}),
		UserTransactionTimesNs: exporter.NewCountersWithMultiLabels("UserTransactionTimesNs", "Total transaction latency for each CallerID", []string{"CallerID", "Conclusion"}),
		ResultHistogram:        exporter.NewHistogram("Results", "Distribution of rows returned", []int64{0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000}),
		TableaclAllowed:        exporter.NewCountersWithMultiLabels("TableACLAllowed", "ACL acceptances", []string{"TableName", "TableGroup", "PlanID", "Username"}),
		TableaclDenied:         exporter.NewCountersWithMultiLabels("TableACLDenied", "ACL denials", []string{"TableName", "TableGroup", "PlanID", "Username"}),
		TableaclPseudoDenied:   exporter.NewCountersWithMultiLabels("TableACLPseudoDenied", "ACL pseudodenials", []string{"TableName", "TableGroup", "PlanID", "Username"}),

		UserActiveReservedCount: exporter.NewCountersWithSingleLabel("UserActiveReservedCount", "active reserved connection for each CallerID", "CallerID"),
		UserReservedCount:       exporter.NewCountersWithSingleLabel("UserReservedCount", "reserved connection received for each CallerID", "CallerID"),
		UserReservedTimesNs:     exporter.NewCountersWithSingleLabel("UserReservedTimesNs", "Total reserved connection latency for each CallerID", "CallerID"),
	}
	stats.QPSRates = exporter.NewRates("QPS", stats.QueryTimings, 15*60/5, 5*time.Second)
	return stats
}
