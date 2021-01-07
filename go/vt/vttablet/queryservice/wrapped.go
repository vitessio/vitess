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

package queryservice

import (
	"context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var _ QueryService = &wrappedService{}

// WrapperFunc defines the signature for the wrapper function used by Wrap.
// Parameter ordering is as follows: original parameters, connection, method name, additional parameters and inner func.
// The inner function returns err and canRetry.
// If canRetry is true, the error is specific to the current vttablet and can be retried elsewhere.
// The flag will be false if there was no error.
type WrapperFunc func(ctx context.Context, target *querypb.Target, conn QueryService, name string, inTransaction bool, inner func(context.Context, *querypb.Target, QueryService) (canRetry bool, err error)) error

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

func (ws *wrappedService) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (transactionID int64, alias *topodatapb.TabletAlias, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "Begin", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		var innerErr error
		transactionID, alias, innerErr = conn.Begin(ctx, target, options)
		return canRetry(ctx, innerErr), innerErr
	})
	return transactionID, alias, err
}

func (ws *wrappedService) Commit(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	var rID int64
	err := ws.wrapper(ctx, target, ws.impl, "Commit", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		var innerErr error
		rID, innerErr = conn.Commit(ctx, target, transactionID)
		return canRetry(ctx, innerErr), innerErr
	})
	if err != nil {
		return 0, err
	}
	return rID, nil
}

func (ws *wrappedService) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	var rID int64
	err := ws.wrapper(ctx, target, ws.impl, "Rollback", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		var innerErr error
		rID, innerErr = conn.Rollback(ctx, target, transactionID)
		return canRetry(ctx, innerErr), innerErr
	})
	if err != nil {
		return 0, err
	}
	return rID, nil
}

func (ws *wrappedService) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) error {
	return ws.wrapper(ctx, target, ws.impl, "Prepare", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		innerErr := conn.Prepare(ctx, target, transactionID, dtid)
		return canRetry(ctx, innerErr), innerErr
	})
}

func (ws *wrappedService) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "CommitPrepared", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		innerErr := conn.CommitPrepared(ctx, target, dtid)
		return canRetry(ctx, innerErr), innerErr
	})
}

func (ws *wrappedService) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "RollbackPrepared", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		innerErr := conn.RollbackPrepared(ctx, target, dtid, originalID)
		return canRetry(ctx, innerErr), innerErr
	})
}

func (ws *wrappedService) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "CreateTransaction", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		innerErr := conn.CreateTransaction(ctx, target, dtid, participants)
		return canRetry(ctx, innerErr), innerErr
	})
}

func (ws *wrappedService) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "StartCommit", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		innerErr := conn.StartCommit(ctx, target, transactionID, dtid)
		return canRetry(ctx, innerErr), innerErr
	})
}

func (ws *wrappedService) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "SetRollback", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		innerErr := conn.SetRollback(ctx, target, dtid, transactionID)
		return canRetry(ctx, innerErr), innerErr
	})
}

func (ws *wrappedService) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return ws.wrapper(ctx, target, ws.impl, "ConcludeTransaction", true, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		innerErr := conn.ConcludeTransaction(ctx, target, dtid)
		return canRetry(ctx, innerErr), innerErr
	})
}

func (ws *wrappedService) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "ReadTransaction", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		var innerErr error
		metadata, innerErr = conn.ReadTransaction(ctx, target, dtid)
		return canRetry(ctx, innerErr), innerErr
	})
	return metadata, err
}

func (ws *wrappedService) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (qr *sqltypes.Result, err error) {
	inDedicatedConn := transactionID != 0 || reservedID != 0
	err = ws.wrapper(ctx, target, ws.impl, "Execute", inDedicatedConn, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		var innerErr error
		qr, innerErr = conn.Execute(ctx, target, query, bindVars, transactionID, reservedID, options)
		// You cannot retry if you're in a transaction.
		retryable := canRetry(ctx, innerErr) && (!inDedicatedConn)
		return retryable, innerErr
	})
	return qr, err
}

func (ws *wrappedService) StreamExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return ws.wrapper(ctx, target, ws.impl, "StreamExecute", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		streamingStarted := false
		innerErr := conn.StreamExecute(ctx, target, query, bindVars, transactionID, options, func(qr *sqltypes.Result) error {
			streamingStarted = true
			return callback(qr)
		})
		// You cannot restart a stream once it's sent results.
		retryable := canRetry(ctx, innerErr) && (!streamingStarted)
		return retryable, innerErr
	})
}

func (ws *wrappedService) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) (qrs []sqltypes.Result, err error) {
	inTransaction := transactionID != 0
	err = ws.wrapper(ctx, target, ws.impl, "ExecuteBatch", inTransaction, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		var innerErr error
		qrs, innerErr = conn.ExecuteBatch(ctx, target, queries, asTransaction, transactionID, options)
		// You cannot retry if you're in a transaction.
		retryable := canRetry(ctx, innerErr) && !inTransaction
		return retryable, innerErr
	})
	return qrs, err
}

func (ws *wrappedService) BeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, query string, bindVars map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions) (qr *sqltypes.Result, transactionID int64, alias *topodatapb.TabletAlias, err error) {
	inDedicatedConn := reservedID != 0
	err = ws.wrapper(ctx, target, ws.impl, "BeginExecute", inDedicatedConn, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		var innerErr error
		qr, transactionID, alias, innerErr = conn.BeginExecute(ctx, target, preQueries, query, bindVars, reservedID, options)
		return canRetry(ctx, innerErr) && !inDedicatedConn, innerErr
	})
	return qr, transactionID, alias, err
}

func (ws *wrappedService) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) (qrs []sqltypes.Result, transactionID int64, alias *topodatapb.TabletAlias, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "BeginExecuteBatch", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		var innerErr error
		qrs, transactionID, alias, innerErr = conn.BeginExecuteBatch(ctx, target, queries, asTransaction, options)
		return canRetry(ctx, innerErr), innerErr
	})
	return qrs, transactionID, alias, err
}

func (ws *wrappedService) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error {
	return ws.wrapper(ctx, target, ws.impl, "MessageStream", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		innerErr := conn.MessageStream(ctx, target, name, callback)
		return canRetry(ctx, innerErr), innerErr
	})
}

func (ws *wrappedService) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error) {
	err = ws.wrapper(ctx, target, ws.impl, "MessageAck", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		var innerErr error
		count, innerErr = conn.MessageAck(ctx, target, name, ids)
		return canRetry(ctx, innerErr), innerErr
	})
	return count, err
}

func (ws *wrappedService) VStream(ctx context.Context, target *querypb.Target, startPos string, tableLastPKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	return ws.wrapper(ctx, target, ws.impl, "VStream", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		innerErr := conn.VStream(ctx, target, startPos, tableLastPKs, filter, send)
		return false, innerErr
	})
}

func (ws *wrappedService) VStreamRows(ctx context.Context, target *querypb.Target, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	return ws.wrapper(ctx, target, ws.impl, "VStreamRows", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		innerErr := conn.VStreamRows(ctx, target, query, lastpk, send)
		return false, innerErr
	})
}

func (ws *wrappedService) VStreamResults(ctx context.Context, target *querypb.Target, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error {
	return ws.wrapper(ctx, target, ws.impl, "VStreamResults", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		innerErr := conn.VStreamResults(ctx, target, query, send)
		return false, innerErr
	})
}

func (ws *wrappedService) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	return ws.wrapper(ctx, nil, ws.impl, "StreamHealth", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		innerErr := conn.StreamHealth(ctx, callback)
		return canRetry(ctx, innerErr), innerErr
	})
}

func (ws *wrappedService) HandlePanic(err *error) {
	// No-op. Wrappers must call HandlePanic.
}

func (ws *wrappedService) ReserveBeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, int64, *topodatapb.TabletAlias, error) {
	var res *sqltypes.Result
	var transactionID, reservedID int64
	var alias *topodatapb.TabletAlias
	err := ws.wrapper(ctx, target, ws.impl, "ReserveBeginExecute", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		var err error
		res, transactionID, reservedID, alias, err = conn.ReserveBeginExecute(ctx, target, preQueries, sql, bindVariables, options)
		return canRetry(ctx, err), err
	})

	return res, transactionID, reservedID, alias, err
}

func (ws *wrappedService) ReserveExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	inDedicatedConn := transactionID != 0
	var res *sqltypes.Result
	var reservedID int64
	var alias *topodatapb.TabletAlias
	err := ws.wrapper(ctx, target, ws.impl, "ReserveExecute", inDedicatedConn, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		var err error
		res, reservedID, alias, err = conn.ReserveExecute(ctx, target, preQueries, sql, bindVariables, transactionID, options)
		return canRetry(ctx, err) && !inDedicatedConn, err
	})

	return res, reservedID, alias, err
}

func (ws *wrappedService) Release(ctx context.Context, target *querypb.Target, transactionID, reservedID int64) error {
	inDedicatedConn := transactionID != 0 || reservedID != 0
	return ws.wrapper(ctx, target, ws.impl, "Release", inDedicatedConn, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		// No point retrying Release.
		return false, conn.Release(ctx, target, transactionID, reservedID)
	})
}

func (ws *wrappedService) Close(ctx context.Context) error {
	return ws.wrapper(ctx, nil, ws.impl, "Close", false, func(ctx context.Context, target *querypb.Target, conn QueryService) (bool, error) {
		// No point retrying Close.
		return false, conn.Close(ctx)
	})
}
