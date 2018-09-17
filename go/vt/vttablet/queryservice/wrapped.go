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

package queryservice

import (
	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var _ QueryService = &wrappedService{}

// WrapperFunc defines the signature for the wrapper function used by Wrap.
// Parameter ordering is as follows: original parameters, connection, method name, additional parameters and inner func.
// The inner function returns err and canRetry.
// If canRetry is true, the error is specific to the current vttablet and can be retried elsewhere.
// The flag will be false if there was no error.
type WrapperFunc func(ctx context.Context, target *querypb.Target, conn QueryService, name string, inTransaction bool, inner func(context.Context, *querypb.Target, QueryService) (err error, canRetry bool)) error

// Wrap returns a wrapped version of the original QueryService implementation.
// This lets you avoid repeating boiler-plate code by consolidating it in the
// wrapper function.
// A good example of this is go/vt/vtgate/gateway/discoverygateway.go.
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

// canRetry returns true if the error is retryable on a different vttablet.
// Nil error or a canceled context make it return
// false. Otherwise, the error code determines the outcome.
func canRetry(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}

	select {
	case <-ctx.Done():
		return false
	default:
	}

	switch vterrors.Code(err) {
	case vtrpcpb.Code_UNAVAILABLE, vtrpcpb.Code_FAILED_PRECONDITION:
		return true
	}
	return false
}

// wrappedService wraps an existing QueryService with
// a decorator function.
type wrappedService struct {
	impl    QueryService
	wrapper WrapperFunc
}

func (ws *wrappedService) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (transactionID int64, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "Begin", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		var innerErr error
		transactionID, innerErr = conn.Begin(ctx, target, options)
		return innerErr, canRetry(ctx, innerErr)
	})
	return transactionID, err
}

func (ws *wrappedService) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return ws.wrapper(ctx, target, ws.impl, "Commit", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		innerErr := conn.Commit(ctx, target, transactionID)
		return innerErr, canRetry(ctx, innerErr)
	})
}

func (ws *wrappedService) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return ws.wrapper(ctx, target, ws.impl, "Rollback", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		innerErr := conn.Rollback(ctx, target, transactionID)
		return innerErr, canRetry(ctx, innerErr)
	})
}

func (ws *wrappedService) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) error {
	return ws.wrapper(ctx, target, ws.impl, "Prepare", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		innerErr := conn.Prepare(ctx, target, transactionID, dtid)
		return innerErr, canRetry(ctx, innerErr)
	})
}

func (ws *wrappedService) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "CommitPrepared", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		innerErr := conn.CommitPrepared(ctx, target, dtid)
		return innerErr, canRetry(ctx, innerErr)
	})
}

func (ws *wrappedService) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "RollbackPrepared", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		innerErr := conn.RollbackPrepared(ctx, target, dtid, originalID)
		return innerErr, canRetry(ctx, innerErr)
	})
}

func (ws *wrappedService) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "CreateTransaction", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		innerErr := conn.CreateTransaction(ctx, target, dtid, participants)
		return innerErr, canRetry(ctx, innerErr)
	})
}

func (ws *wrappedService) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "StartCommit", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		innerErr := conn.StartCommit(ctx, target, transactionID, dtid)
		return innerErr, canRetry(ctx, innerErr)
	})
}

func (ws *wrappedService) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "SetRollback", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		innerErr := conn.SetRollback(ctx, target, dtid, transactionID)
		return innerErr, canRetry(ctx, innerErr)
	})
}

func (ws *wrappedService) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "ConcludeTransaction", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		innerErr := conn.ConcludeTransaction(ctx, target, dtid)
		return innerErr, canRetry(ctx, innerErr)
	})
}

func (ws *wrappedService) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "ReadTransaction", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		var innerErr error
		metadata, innerErr = conn.ReadTransaction(ctx, target, dtid)
		return innerErr, canRetry(ctx, innerErr)
	})
	return metadata, err
}

func (ws *wrappedService) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (qr *sqltypes.Result, err error) {
	inTransaction := (transactionID != 0)
	err = ws.wrapper(ctx, target, ws.impl, "Execute", inTransaction, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		var innerErr error
		qr, innerErr = conn.Execute(ctx, target, query, bindVars, transactionID, options)
		// You cannot retry if you're in a transaction.
		retryable := canRetry(ctx, innerErr) && (!inTransaction)
		return innerErr, retryable
	})
	return qr, err
}

func (ws *wrappedService) StreamExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return ws.wrapper(ctx, target, ws.impl, "StreamExecute", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		streamingStarted := false
		innerErr := conn.StreamExecute(ctx, target, query, bindVars, transactionID, options, func(qr *sqltypes.Result) error {
			streamingStarted = true
			return callback(qr)
		})
		// You cannot restart a stream once it's sent results.
		retryable := canRetry(ctx, innerErr) && (!streamingStarted)
		return innerErr, retryable
	})
}

func (ws *wrappedService) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) (qrs []sqltypes.Result, err error) {
	inTransaction := (transactionID != 0)
	err = ws.wrapper(ctx, target, ws.impl, "ExecuteBatch", inTransaction, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		var innerErr error
		qrs, innerErr = conn.ExecuteBatch(ctx, target, queries, asTransaction, transactionID, options)
		// You cannot retry if you're in a transaction.
		retryable := canRetry(ctx, innerErr) && (!inTransaction)
		return innerErr, retryable
	})
	return qrs, err
}

func (ws *wrappedService) BeginExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (qr *sqltypes.Result, transactionID int64, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "BeginExecute", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		var innerErr error
		qr, transactionID, innerErr = conn.BeginExecute(ctx, target, query, bindVars, options)
		return innerErr, canRetry(ctx, innerErr)
	})
	return qr, transactionID, err
}

func (ws *wrappedService) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) (qrs []sqltypes.Result, transactionID int64, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "BeginExecuteBatch", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		var innerErr error
		qrs, transactionID, innerErr = conn.BeginExecuteBatch(ctx, target, queries, asTransaction, options)
		return innerErr, canRetry(ctx, innerErr)
	})
	return qrs, transactionID, err
}

func (ws *wrappedService) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error {
	return ws.wrapper(ctx, target, ws.impl, "MessageStream", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		innerErr := conn.MessageStream(ctx, target, name, callback)
		return innerErr, canRetry(ctx, innerErr)
	})
}

func (ws *wrappedService) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "MessageAck", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		var innerErr error
		count, innerErr = conn.MessageAck(ctx, target, name, ids)
		return innerErr, canRetry(ctx, innerErr)
	})
	return count, err
}

func (ws *wrappedService) SplitQuery(ctx context.Context, target *querypb.Target, query *querypb.BoundQuery, splitColumns []string, splitCount int64, numRowsPerQueryPart int64, algorithm querypb.SplitQueryRequest_Algorithm) (queries []*querypb.QuerySplit, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "SplitQuery", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		var innerErr error
		queries, innerErr = conn.SplitQuery(ctx, target, query, splitColumns, splitCount, numRowsPerQueryPart, algorithm)
		return innerErr, canRetry(ctx, innerErr)
	})
	return queries, err
}

func (ws *wrappedService) UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64, callback func(*querypb.StreamEvent) error) error {
	return ws.wrapper(ctx, target, ws.impl, "UpdateStream", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		innerErr := conn.UpdateStream(ctx, target, position, timestamp, callback)
		return innerErr, canRetry(ctx, innerErr)
	})
}

func (ws *wrappedService) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	return ws.wrapper(ctx, nil, ws.impl, "StreamHealth", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		innerErr := conn.StreamHealth(ctx, callback)
		return innerErr, canRetry(ctx, innerErr)
	})
}

func (ws *wrappedService) HandlePanic(err *error) {
	// No-op. Wrappers must call HandlePanic.
}

func (ws *wrappedService) Close(ctx context.Context) error {
	return ws.wrapper(ctx, nil, ws.impl, "Close", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (error, bool) {
		// No point retrying Close.
		return conn.Close(ctx), false
	})
}
