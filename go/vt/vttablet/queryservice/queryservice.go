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

// Package queryservice contains the interface for the service definition
// of the Query Service.
package queryservice

import (
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"context"

	"vitess.io/vitess/go/sqltypes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// QueryService is the interface implemented by the tablet's query service.
// All streaming methods accept a callback function that will be called for
// each response. If the callback returns an error, that error is returned
// back by the function, except in the case of io.EOF in which case the stream
// will be terminated with no error. Streams can also be terminated by canceling
// the context.
// This API is common for both server and client implementations. All functions
// must be safe to be called concurrently.
type QueryService interface {
	// Transaction management

	// Begin returns the transaction id to use for further operations
	Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (TransactionState, error)

	// Commit commits the current transaction
	Commit(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error)

	// Rollback aborts the current transaction
	Rollback(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error)

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

	// ReadTransaction returns the metadata for the specified dtid.
	ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error)

	// Execute for query execution
	Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error)
	// StreamExecute for query execution with streaming
	StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error

	// Combo methods, they also return the transactionID from the
	// Begin part. If err != nil, the transactionID may still be
	// non-zero, and needs to be propagated back (like for a DB
	// Integrity Error)
	BeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions) (TransactionState, *sqltypes.Result, error)
	BeginStreamExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (TransactionState, error)

	// Messaging methods.
	MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error
	MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error)

	// VStream streams VReplication events based on the specified filter.
	VStream(ctx context.Context, request *binlogdatapb.VStreamRequest, send func([]*binlogdatapb.VEvent) error) error

	// VStreamRows streams rows of a table from the specified starting point.
	VStreamRows(ctx context.Context, request *binlogdatapb.VStreamRowsRequest, send func(*binlogdatapb.VStreamRowsResponse) error) error

	// VStreamResults streams results along with the gtid of the snapshot.
	VStreamResults(ctx context.Context, target *querypb.Target, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error

	// StreamHealth streams health status.
	StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error

	// HandlePanic will be called if any of the functions panic.
	HandlePanic(err *error)

	ReserveBeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (ReservedTransactionState, *sqltypes.Result, error)

	ReserveBeginStreamExecute(ctx context.Context, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (ReservedTransactionState, error)

	ReserveExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (ReservedState, *sqltypes.Result, error)

	ReserveStreamExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (ReservedState, error)

	Release(ctx context.Context, target *querypb.Target, transactionID, reservedID int64) error

	// GetSchema returns the table definition for the specified tables.
	GetSchema(ctx context.Context, target *querypb.Target, tableType querypb.SchemaTableType, tableNames []string, callback func(schemaRes *querypb.GetSchemaResponse) error) error

	// Close must be called for releasing resources.
	Close(ctx context.Context) error
}

type TransactionState struct {
	TransactionID       int64
	TabletAlias         *topodatapb.TabletAlias
	SessionStateChanges string
}

type ReservedState struct {
	ReservedID  int64
	TabletAlias *topodatapb.TabletAlias
}

type ReservedTransactionState struct {
	ReservedID          int64
	TransactionID       int64
	TabletAlias         *topodatapb.TabletAlias
	SessionStateChanges string
}
