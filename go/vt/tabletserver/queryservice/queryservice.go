// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package queryservice contains the interface for the service definition
// of the Query Service.
package queryservice

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
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

	// Prepare prepares the specified transaction.
	Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error)

	// CommitPrepared commits the prepared transaction.
	CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error)

	// RollbackPrepared rolls back the prepared transaction.
	RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error)

	// CreateTransaction creates the metadata for a 2PC transaction.
	CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error)

	// StartCommit atomically commits the transaction along with the
	// decision to commit the associated 2pc transaction.
	StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error)

	// SetRollback transitions the 2pc transaction to the Rollback state.
	// If a transaction id is provided, that transaction is also rolled back.
	SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error)

	// ConcludeTransaction deletes the 2pc transaction metadata
	// essentially resolving it.
	ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error)

	// ReadTransaction returns the metadata for the sepcified dtid.
	ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error)

	// Query execution
	Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error)
	StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, options *querypb.ExecuteOptions, sendReply func(*sqltypes.Result) error) error
	ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error)

	// Combo methods, they also return the transactionID from the
	// Begin part. If err != nil, the transactionID may still be
	// non-zero, and needs to be propagated back (like for a DB
	// Integrity Error)
	BeginExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, error)
	BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, int64, error)

	// Messaging methods.
	MessageStream(ctx context.Context, target *querypb.Target, name string, sendReply func(*querypb.MessageStreamResponse) error) error
	MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error)

	// SplitQuery is a MapReduce helper function
	// This version of SplitQuery supports multiple algorithms and multiple split columns.
	// See the documentation of SplitQueryRequest in 'proto/vtgate.proto' for more information.
	SplitQuery(
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

	// UpdateStream streams updates from the provided position or timestamp.
	UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64, sendReply func(*querypb.StreamEvent) error) error

	// Helper for RPC panic handling: call this in a defer statement
	// at the beginning of each RPC handling method.
	HandlePanic(*error)
}

// Command to generate a mock for this interface with mockgen.
//go:generate mockgen -source $GOFILE -destination queryservice_testing/mock_queryservice.go -package queryservice_testing
