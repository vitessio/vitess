// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package queryservice contains the interface for the service definition
// of the Query Service.
package queryservice

import (
	"github.com/youtube/vitess/go/sqltypes"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
)

// QueryService is the interface implemented by the tablet's query service.
type QueryService interface {
	// Transaction management

	// Begin returns the transaction id to use for further operations
	Begin(ctx context.Context, target *querypb.Target) (int64, error)

	// Commit commits the current transaction
	Commit(ctx context.Context, target *querypb.Target, transactionID int64) error

	// Rollback aborts the current transaction
	Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error

	// Query execution
	Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, transactionID int64) (*sqltypes.Result, error)
	StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, sendReply func(*sqltypes.Result) error) error
	ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error)

	// Combo methods: if err != nil, the transactionID may still
	// be non-zero, and needs to be propagated back.
	BeginExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}) (*sqltypes.Result, int64, error)
	BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool) ([]sqltypes.Result, int64, error)

	// SplitQuery is a map reduce helper function
	// TODO(erez): Remove this and rename the following func to SplitQuery
	// once we migrate to SplitQuery V2.
	SplitQuery(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64) ([]querytypes.QuerySplit, error)

	// SplitQueryV2 is a MapReduce helper function.
	// This is version of SplitQuery supports multiple algorithms and multiple split columns.
	// See the documentation of SplitQueryRequest in 'proto/vtgate.proto' for more information.
	SplitQueryV2(
		ctx context.Context,
		target *querypb.Target,
		sql string,
		bindVariables map[string]interface{},
		splitColumns []string,
		splitCount int64,
		numRowsPerQueryPart int64,
		algorithm querypb.SplitQueryRequest_Algorithm,
	) ([]querytypes.QuerySplit, error)

	// StreamHealthRegister registers a listener for StreamHealth
	StreamHealthRegister(chan<- *querypb.StreamHealthResponse) (int, error)

	// StreamHealthUnregister unregisters a listener for StreamHealth
	StreamHealthUnregister(int) error

	// Helper for RPC panic handling: call this in a defer statement
	// at the beginning of each RPC handling method.
	HandlePanic(*error)
}

// CallCorrectSplitQuery calls the correct SplitQuery.
// This trivial logic is encapsulated in a function here so it can be easily tested.
// TODO(erez): Remove once the migration to SplitQueryV2 is done.
func CallCorrectSplitQuery(
	queryService QueryService,
	useSplitQueryV2 bool,
	ctx context.Context,
	target *querypb.Target,
	sql string,
	bindVariables map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm,
) ([]querytypes.QuerySplit, error) {

	if useSplitQueryV2 {
		return queryService.SplitQueryV2(
			ctx,
			target,
			sql,
			bindVariables,
			splitColumns,
			splitCount,
			numRowsPerQueryPart,
			algorithm)
	}
	return queryService.SplitQuery(
		ctx,
		target,
		sql,
		bindVariables,
		splitColumnsToSplitColumn(splitColumns),
		splitCount)
}

// SplitColumnsToSplitColumn returns the first SplitColumn in the given slice or an empty
// string if the slice is empty.
//
// This method is used to get the traditional behavior when accessing the SplitColumn field in an
// older SplitQuery-V1 querypb.SplitQueryRequest represented in the newer SplitQuery-V2
// querypb.SplitQueryRequest message. In the new V2 message the SplitColumn field has been converted
// into a repeated string field.
// TODO(erez): Remove this function when migration to SplitQueryV2 is done.
func splitColumnsToSplitColumn(splitColumns []string) string {
	if len(splitColumns) == 0 {
		return ""
	}
	return splitColumns[0]
}

// Command to generate a mock for this interface with mockgen.
//go:generate mockgen -source $GOFILE -destination queryservice_testing/mock_queryservice.go -package queryservice_testing
