/*
Copyright 2017 Google Inc.

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
	"io"

	"golang.org/x/net/context"

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
	Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (int64, error)

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
	Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error)
	StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error
	ExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error)

	// Combo methods, they also return the transactionID from the
	// Begin part. If err != nil, the transactionID may still be
	// non-zero, and needs to be propagated back (like for a DB
	// Integrity Error)
	BeginExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, error)
	BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, int64, error)

	// Messaging methods.
	MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error
	MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error)

	// SplitQuery is a MapReduce helper function
	// This version of SplitQuery supports multiple algorithms and multiple split columns.
	// See the documentation of SplitQueryRequest in 'proto/vtgate.proto' for more information.
	SplitQuery(ctx context.Context, target *querypb.Target, query *querypb.BoundQuery, splitColumns []string, splitCount int64, numRowsPerQueryPart int64, algorithm querypb.SplitQueryRequest_Algorithm) ([]*querypb.QuerySplit, error)

	// UpdateStream streams updates from the provided position or timestamp.
	UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64, callback func(*querypb.StreamEvent) error) error

	// VStream streams VReplication events based on the specified filter.
	VStream(ctx context.Context, target *querypb.Target, startPos string, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error

	// VStreamRows streams rows of a table from the specified starting point.
	VStreamRows(ctx context.Context, target *querypb.Target, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error

	// StreamHealth streams health status.
	StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error

	// HandlePanic will be called if any of the functions panic.
	HandlePanic(err *error)

	// Close must be called for releasing resources.
	Close(ctx context.Context) error
}

type resultStreamer struct {
	done chan struct{}
	ch   chan *sqltypes.Result
	err  error
}

func (rs *resultStreamer) Recv() (*sqltypes.Result, error) {
	select {
	case <-rs.done:
		return nil, rs.err
	case qr := <-rs.ch:
		return qr, nil
	}
}

// ExecuteWithStreamer performs a StreamExecute, but returns a *sqltypes.ResultStream to iterate on.
// This function should only be used for legacy code. New usage should directly use StreamExecute.
func ExecuteWithStreamer(ctx context.Context, conn QueryService, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) sqltypes.ResultStream {
	rs := &resultStreamer{
		done: make(chan struct{}),
		ch:   make(chan *sqltypes.Result),
	}
	go func() {
		defer close(rs.done)
		rs.err = conn.StreamExecute(ctx, target, sql, bindVariables, 0, options, func(qr *sqltypes.Result) error {
			select {
			case <-ctx.Done():
				return io.EOF
			case rs.ch <- qr:
			}
			return nil
		})
		if rs.err == nil {
			rs.err = io.EOF
		}
	}()
	return rs
}

// ExecuteWithTransactionalStreamer does the same thing as ExecuteWithStreamer, but inside a transaction
func ExecuteWithTransactionalStreamer(ctx context.Context, conn QueryService, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) sqltypes.ResultStream {
	rs := &resultStreamer{
		done: make(chan struct{}),
		ch:   make(chan *sqltypes.Result),
	}
	go func() {
		defer close(rs.done)
		rs.err = conn.StreamExecute(ctx, target, sql, bindVariables, transactionID, options, func(qr *sqltypes.Result) error {
			select {
			case <-ctx.Done():
				return io.EOF
			case rs.ch <- qr:
			}
			return nil
		})
		if rs.err == nil {
			rs.err = io.EOF
		}
	}()
	return rs
}
