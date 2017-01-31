// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package queryservice

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var _ QueryService = &wrappedService{}

// WrapperFunc defines the signature for the wrapper function used by Wrap.
// Parameter ordering is as follows: original parameters, connection, method name, additional parameters and inner func.
type WrapperFunc func(ctx context.Context, target *querypb.Target, conn QueryService, name string, inTransaction, isStreaming bool, inner func(context.Context, *querypb.Target, QueryService) error) error

// Wrap returns a wrapped version of the original QueryService implementation.
// For every method invocation, the wrapper function is called, which can
// in turn call the provided inner function that will use the input parameters
// to call the implementation. In order to load balance across multiple
// implementations, you can set impl to be nil and provide the connection
// as input to the action function. In the case of StreamHealth or Close,
// there is no target and it will be nil. If necessary, the wrapper
// can validate the nil against the method name. The wrapper is also
// responsible for calling HandlePanic where necessary.
func Wrap(impl QueryService, wrapper WrapperFunc) QueryService {
	return &wrappedService{
		impl:    impl,
		wrapper: wrapper,
	}
}

// wrappedService wraps an existing QueryService with
// a decorator function.
type wrappedService struct {
	impl    QueryService
	wrapper WrapperFunc
}

func (ws *wrappedService) Begin(ctx context.Context, target *querypb.Target) (transactionID int64, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "Begin", false, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		var innerErr error
		transactionID, innerErr = conn.Begin(ctx, target)
		return innerErr
	})
	return transactionID, err
}

func (ws *wrappedService) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return ws.wrapper(ctx, target, ws.impl, "Commit", true, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		return conn.Commit(ctx, target, transactionID)
	})
}

func (ws *wrappedService) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return ws.wrapper(ctx, target, ws.impl, "Rollback", true, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		return conn.Rollback(ctx, target, transactionID)
	})
}

func (ws *wrappedService) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) error {
	return ws.wrapper(ctx, target, ws.impl, "Prepare", true, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		return conn.Prepare(ctx, target, transactionID, dtid)
	})
}

func (ws *wrappedService) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "CommitPrepared", true, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		return conn.CommitPrepared(ctx, target, dtid)
	})
}

func (ws *wrappedService) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "RollbackPrepared", true, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		return conn.RollbackPrepared(ctx, target, dtid, originalID)
	})
}

func (ws *wrappedService) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "CreateTransaction", true, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		return conn.CreateTransaction(ctx, target, dtid, participants)
	})
}

func (ws *wrappedService) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "StartCommit", true, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		return conn.StartCommit(ctx, target, transactionID, dtid)
	})
}

func (ws *wrappedService) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "SetRollback", true, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		return conn.SetRollback(ctx, target, dtid, transactionID)
	})
}

func (ws *wrappedService) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "ConcludeTransaction", true, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		return conn.ConcludeTransaction(ctx, target, dtid)
	})
}

func (ws *wrappedService) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "ReadTransaction", false, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		var innerErr error
		metadata, innerErr = conn.ReadTransaction(ctx, target, dtid)
		return innerErr
	})
	return metadata, err
}

func (ws *wrappedService) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, transactionID int64, options *querypb.ExecuteOptions) (qr *sqltypes.Result, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "Execute", transactionID != 0, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		var innerErr error
		qr, innerErr = conn.Execute(ctx, target, query, bindVars, transactionID, options)
		return innerErr
	})
	return qr, err
}

func (ws *wrappedService) StreamExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return ws.wrapper(ctx, target, ws.impl, "StreamExecute", false, true, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		return conn.StreamExecute(ctx, target, query, bindVars, options, callback)
	})
}

func (ws *wrappedService) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) (qrs []sqltypes.Result, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "ExecuteBatch", transactionID != 0, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		var innerErr error
		qrs, innerErr = conn.ExecuteBatch(ctx, target, queries, asTransaction, transactionID, options)
		return innerErr
	})
	return qrs, err
}

func (ws *wrappedService) BeginExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, options *querypb.ExecuteOptions) (qr *sqltypes.Result, transactionID int64, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "BeginExecute", false, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		var innerErr error
		qr, transactionID, innerErr = conn.BeginExecute(ctx, target, query, bindVars, options)
		return innerErr
	})
	return qr, transactionID, err
}

func (ws *wrappedService) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) (qrs []sqltypes.Result, transactionID int64, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "BeginExecuteBatch", false, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		var innerErr error
		qrs, transactionID, innerErr = conn.BeginExecuteBatch(ctx, target, queries, asTransaction, options)
		return innerErr
	})
	return qrs, transactionID, err
}

func (ws *wrappedService) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error {
	return ws.wrapper(ctx, target, ws.impl, "MessageStream", false, true, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		return conn.MessageStream(ctx, target, name, callback)
	})
}

func (ws *wrappedService) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "MessageAck", false, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		var innerErr error
		count, innerErr = conn.MessageAck(ctx, target, name, ids)
		return innerErr
	})
	return count, err
}

func (ws *wrappedService) SplitQuery(ctx context.Context, target *querypb.Target, query querytypes.BoundQuery, splitColumns []string, splitCount int64, numRowsPerQueryPart int64, algorithm querypb.SplitQueryRequest_Algorithm) (queries []querytypes.QuerySplit, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "SplitQuery", false, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		var innerErr error
		queries, innerErr = conn.SplitQuery(ctx, target, query, splitColumns, splitCount, numRowsPerQueryPart, algorithm)
		return innerErr
	})
	return queries, err
}

func (ws *wrappedService) UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64, callback func(*querypb.StreamEvent) error) error {
	return ws.wrapper(ctx, target, ws.impl, "UpdateStream", false, true, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		return conn.UpdateStream(ctx, target, position, timestamp, callback)
	})
}

func (ws *wrappedService) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	return ws.wrapper(ctx, nil, ws.impl, "StreamHealth", false, true, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		return conn.StreamHealth(ctx, callback)
	})
}

func (ws *wrappedService) HandlePanic(err *error) {
	// No-op. Wrappers must call HandlePanic.
}

func (ws *wrappedService) Close(ctx context.Context) error {
	return ws.wrapper(ctx, nil, ws.impl, "Close", false, false, func(ctx context.Context, target *querypb.Target, conn QueryService) error {
		return conn.Close(ctx)
	})
}
