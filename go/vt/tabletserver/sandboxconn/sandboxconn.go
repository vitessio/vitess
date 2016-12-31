// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sandboxconn provides a fake TabletConn implementation for tests.
// It can return real results, and simulate error cases.
package sandboxconn

import (
	"fmt"
	"io"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// SandboxConn satisfies the TabletConn interface
type SandboxConn struct {
	tablet *topodatapb.Tablet

	// These errors work for all functions.
	MustFailRetry            int
	MustFailFatal            int
	MustFailServer           int
	MustFailConn             int
	MustFailTxPool           int
	MustFailNotTx            int
	MustFailCanceled         int
	MustFailUnknownError     int
	MustFailDeadlineExceeded int
	MustFailIntegrityError   int
	MustFailPermissionDenied int
	MustFailTransientError   int
	MustFailUnauthenticated  int

	// These errors are triggered only for specific functions.
	// For now these are just for the 2PC functions.
	MustFailPrepare             int
	MustFailCommitPrepared      int
	MustFailRollbackPrepared    int
	MustFailCreateTransaction   int
	MustFailStartCommit         int
	MustFailSetRollback         int
	MustFailConcludeTransaction int

	// These Count vars report how often the corresponding
	// functions were called.
	ExecCount                sync2.AtomicInt64
	BeginCount               sync2.AtomicInt64
	CommitCount              sync2.AtomicInt64
	RollbackCount            sync2.AtomicInt64
	AsTransactionCount       sync2.AtomicInt64
	PrepareCount             sync2.AtomicInt64
	CommitPreparedCount      sync2.AtomicInt64
	RollbackPreparedCount    sync2.AtomicInt64
	CreateTransactionCount   sync2.AtomicInt64
	StartCommitCount         sync2.AtomicInt64
	SetRollbackCount         sync2.AtomicInt64
	ConcludeTransactionCount sync2.AtomicInt64
	ReadTransactionCount     sync2.AtomicInt64

	// Queries stores the non-batch requests received.
	Queries []querytypes.BoundQuery

	// BatchQueries stores the batch requests received
	// Each batch request is inlined as a slice of Queries.
	BatchQueries [][]querytypes.BoundQuery

	// Options stores the options received by all calls.
	Options []*querypb.ExecuteOptions

	// results specifies the results to be returned.
	// They're consumed as results are returned. If there are
	// no results left, SingleRowResult is returned.
	results []*sqltypes.Result

	// ReadTransactionResults is used for returning results for ReadTransaction.
	ReadTransactionResults []*querypb.TransactionMetadata

	// transaction id generator
	TransactionID sync2.AtomicInt64
}

var _ tabletconn.TabletConn = (*SandboxConn)(nil) // compile-time interface check

// NewSandboxConn returns a new SandboxConn targeted to the provided tablet.
func NewSandboxConn(t *topodatapb.Tablet) *SandboxConn {
	return &SandboxConn{
		tablet: t,
	}
}

func (sbc *SandboxConn) getError() error {
	if sbc.MustFailRetry > 0 {
		sbc.MustFailRetry--
		return &tabletconn.ServerError{
			Err:        "retry: err",
			ServerCode: vtrpcpb.ErrorCode_QUERY_NOT_SERVED,
		}
	}
	if sbc.MustFailFatal > 0 {
		sbc.MustFailFatal--
		return &tabletconn.ServerError{
			Err:        "fatal: err",
			ServerCode: vtrpcpb.ErrorCode_INTERNAL_ERROR,
		}
	}
	if sbc.MustFailServer > 0 {
		sbc.MustFailServer--
		return &tabletconn.ServerError{
			Err:        "error: err",
			ServerCode: vtrpcpb.ErrorCode_BAD_INPUT,
		}
	}
	if sbc.MustFailConn > 0 {
		sbc.MustFailConn--
		return tabletconn.OperationalError(fmt.Sprintf("error: conn"))
	}
	if sbc.MustFailTxPool > 0 {
		sbc.MustFailTxPool--
		return &tabletconn.ServerError{
			Err:        "tx_pool_full: err",
			ServerCode: vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED,
		}
	}
	if sbc.MustFailNotTx > 0 {
		sbc.MustFailNotTx--
		return &tabletconn.ServerError{
			Err:        "not_in_tx: err",
			ServerCode: vtrpcpb.ErrorCode_NOT_IN_TX,
		}
	}
	if sbc.MustFailCanceled > 0 {
		sbc.MustFailCanceled--
		return &tabletconn.ServerError{
			Err:        "canceled: err",
			ServerCode: vtrpcpb.ErrorCode_CANCELLED,
		}
	}
	if sbc.MustFailUnknownError > 0 {
		sbc.MustFailUnknownError--
		return &tabletconn.ServerError{
			Err:        "unknown error: err",
			ServerCode: vtrpcpb.ErrorCode_UNKNOWN_ERROR,
		}
	}
	if sbc.MustFailDeadlineExceeded > 0 {
		sbc.MustFailDeadlineExceeded--
		return &tabletconn.ServerError{
			Err:        "deadline exceeded: err",
			ServerCode: vtrpcpb.ErrorCode_DEADLINE_EXCEEDED,
		}
	}
	if sbc.MustFailIntegrityError > 0 {
		sbc.MustFailIntegrityError--
		return &tabletconn.ServerError{
			Err:        "integrity error: err",
			ServerCode: vtrpcpb.ErrorCode_INTEGRITY_ERROR,
		}
	}
	if sbc.MustFailPermissionDenied > 0 {
		sbc.MustFailPermissionDenied--
		return &tabletconn.ServerError{
			Err:        "permission denied: err",
			ServerCode: vtrpcpb.ErrorCode_PERMISSION_DENIED,
		}
	}
	if sbc.MustFailTransientError > 0 {
		sbc.MustFailTransientError--
		return &tabletconn.ServerError{
			Err:        "transient error: err",
			ServerCode: vtrpcpb.ErrorCode_TRANSIENT_ERROR,
		}
	}
	if sbc.MustFailUnauthenticated > 0 {
		sbc.MustFailUnauthenticated--
		return &tabletconn.ServerError{
			Err:        "unauthenticated: err",
			ServerCode: vtrpcpb.ErrorCode_UNAUTHENTICATED,
		}
	}

	return nil
}

// SetResults sets what this con should return next time.
func (sbc *SandboxConn) SetResults(r []*sqltypes.Result) {
	sbc.results = r
}

// Execute is part of the TabletConn interface.
func (sbc *SandboxConn) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	sbc.ExecCount.Add(1)
	bv := make(map[string]interface{})
	for k, v := range bindVars {
		bv[k] = v
	}
	sbc.Queries = append(sbc.Queries, querytypes.BoundQuery{
		Sql:           query,
		BindVariables: bv,
	})
	sbc.Options = append(sbc.Options, options)
	if err := sbc.getError(); err != nil {
		return nil, err
	}
	return sbc.getNextResult(), nil
}

// ExecuteBatch is part of the TabletConn interface.
func (sbc *SandboxConn) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	sbc.ExecCount.Add(1)
	if asTransaction {
		sbc.AsTransactionCount.Add(1)
	}
	if err := sbc.getError(); err != nil {
		return nil, err
	}
	sbc.BatchQueries = append(sbc.BatchQueries, queries)
	sbc.Options = append(sbc.Options, options)
	result := make([]sqltypes.Result, 0, len(queries))
	for range queries {
		result = append(result, *(sbc.getNextResult()))
	}
	return result, nil
}

type streamExecuteAdapter struct {
	result *sqltypes.Result
	done   bool
}

func (a *streamExecuteAdapter) Recv() (*sqltypes.Result, error) {
	if a.done {
		return nil, io.EOF
	}
	a.done = true
	return a.result, nil
}

// StreamExecute is part of the TabletConn interface.
func (sbc *SandboxConn) StreamExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	sbc.ExecCount.Add(1)
	bv := make(map[string]interface{})
	for k, v := range bindVars {
		bv[k] = v
	}
	sbc.Queries = append(sbc.Queries, querytypes.BoundQuery{
		Sql:           query,
		BindVariables: bv,
	})
	sbc.Options = append(sbc.Options, options)
	err := sbc.getError()
	if err != nil {
		return nil, err
	}
	r := sbc.getNextResult()
	return &streamExecuteAdapter{result: r}, nil
}

// Begin is part of the TabletConn interface.
func (sbc *SandboxConn) Begin(ctx context.Context, target *querypb.Target) (int64, error) {
	sbc.BeginCount.Add(1)
	err := sbc.getError()
	if err != nil {
		return 0, err
	}
	return sbc.TransactionID.Add(1), nil
}

// Commit is part of the TabletConn interface.
func (sbc *SandboxConn) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	sbc.CommitCount.Add(1)
	return sbc.getError()
}

// Rollback is part of the TabletConn interface.
func (sbc *SandboxConn) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	sbc.RollbackCount.Add(1)
	return sbc.getError()
}

// Prepare prepares the specified transaction.
func (sbc *SandboxConn) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	sbc.PrepareCount.Add(1)
	if sbc.MustFailPrepare > 0 {
		sbc.MustFailPrepare--
		return &tabletconn.ServerError{
			Err:        "error: err",
			ServerCode: vtrpcpb.ErrorCode_QUERY_NOT_SERVED,
		}
	}
	return sbc.getError()
}

// CommitPrepared commits the prepared transaction.
func (sbc *SandboxConn) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	sbc.CommitPreparedCount.Add(1)
	if sbc.MustFailCommitPrepared > 0 {
		sbc.MustFailCommitPrepared--
		return &tabletconn.ServerError{
			Err:        "error: err",
			ServerCode: vtrpcpb.ErrorCode_QUERY_NOT_SERVED,
		}
	}
	return sbc.getError()
}

// RollbackPrepared rolls back the prepared transaction.
func (sbc *SandboxConn) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	sbc.RollbackPreparedCount.Add(1)
	if sbc.MustFailRollbackPrepared > 0 {
		sbc.MustFailRollbackPrepared--
		return &tabletconn.ServerError{
			Err:        "error: err",
			ServerCode: vtrpcpb.ErrorCode_QUERY_NOT_SERVED,
		}
	}
	return sbc.getError()
}

// CreateTransaction creates the metadata for a 2PC transaction.
func (sbc *SandboxConn) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	sbc.CreateTransactionCount.Add(1)
	if sbc.MustFailCreateTransaction > 0 {
		sbc.MustFailCreateTransaction--
		return &tabletconn.ServerError{
			Err:        "error: err",
			ServerCode: vtrpcpb.ErrorCode_QUERY_NOT_SERVED,
		}
	}
	return sbc.getError()
}

// StartCommit atomically commits the transaction along with the
// decision to commit the associated 2pc transaction.
func (sbc *SandboxConn) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	sbc.StartCommitCount.Add(1)
	if sbc.MustFailStartCommit > 0 {
		sbc.MustFailStartCommit--
		return &tabletconn.ServerError{
			Err:        "error: err",
			ServerCode: vtrpcpb.ErrorCode_QUERY_NOT_SERVED,
		}
	}
	return sbc.getError()
}

// SetRollback transitions the 2pc transaction to the Rollback state.
// If a transaction id is provided, that transaction is also rolled back.
func (sbc *SandboxConn) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	sbc.SetRollbackCount.Add(1)
	if sbc.MustFailSetRollback > 0 {
		sbc.MustFailSetRollback--
		return &tabletconn.ServerError{
			Err:        "error: err",
			ServerCode: vtrpcpb.ErrorCode_QUERY_NOT_SERVED,
		}
	}
	return sbc.getError()
}

// ConcludeTransaction deletes the 2pc transaction metadata
// essentially resolving it.
func (sbc *SandboxConn) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	sbc.ConcludeTransactionCount.Add(1)
	if sbc.MustFailConcludeTransaction > 0 {
		sbc.MustFailConcludeTransaction--
		return &tabletconn.ServerError{
			Err:        "error: err",
			ServerCode: vtrpcpb.ErrorCode_QUERY_NOT_SERVED,
		}
	}
	return sbc.getError()
}

// ReadTransaction returns the metadata for the sepcified dtid.
func (sbc *SandboxConn) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	sbc.ReadTransactionCount.Add(1)
	if err := sbc.getError(); err != nil {
		return nil, err
	}
	if len(sbc.ReadTransactionResults) >= 1 {
		res := sbc.ReadTransactionResults[0]
		sbc.ReadTransactionResults = sbc.ReadTransactionResults[1:]
		return res, nil
	}
	return nil, nil
}

// BeginExecute is part of the TabletConn interface.
func (sbc *SandboxConn) BeginExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, error) {
	transactionID, err := sbc.Begin(ctx, target)
	if err != nil {
		return nil, 0, err
	}
	result, err := sbc.Execute(ctx, target, query, bindVars, transactionID, options)
	return result, transactionID, err
}

// BeginExecuteBatch is part of the TabletConn interface.
func (sbc *SandboxConn) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, int64, error) {
	transactionID, err := sbc.Begin(ctx, target)
	if err != nil {
		return nil, 0, err
	}
	results, err := sbc.ExecuteBatch(ctx, target, queries, asTransaction, transactionID, options)
	return results, transactionID, err
}

// SandboxSQRowCount is the default number of fake splits returned.
var SandboxSQRowCount = int64(10)

// SplitQuery returns a single QuerySplit whose 'sql' field describes the received arguments.
func (sbc *SandboxConn) SplitQuery(
	ctx context.Context,
	target *querypb.Target,
	query querytypes.BoundQuery,
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) ([]querytypes.QuerySplit, error) {
	err := sbc.getError()
	if err != nil {
		return nil, err
	}
	splits := []querytypes.QuerySplit{
		{
			Sql: fmt.Sprintf(
				"query:%v, splitColumns:%v, splitCount:%v,"+
					" numRowsPerQueryPart:%v, algorithm:%v, shard:%v",
				query, splitColumns, splitCount, numRowsPerQueryPart, algorithm, sbc.tablet.Shard),
		},
	}
	return splits, nil
}

// StreamHealth is not implemented.
func (sbc *SandboxConn) StreamHealth(ctx context.Context) (tabletconn.StreamHealthReader, error) {
	return nil, fmt.Errorf("Not implemented in test")
}

// UpdateStream is part of the TabletConn interface.
func (sbc *SandboxConn) UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64) (tabletconn.StreamEventReader, error) {
	// FIXME(alainjobart) implement, use in vtgate tests.
	return nil, fmt.Errorf("Not implemented in test")
}

// Close does not change ExecCount
func (sbc *SandboxConn) Close(ctx context.Context) error {
	return nil
}

// Tablet is part of the TabletConn interface.
func (sbc *SandboxConn) Tablet() *topodatapb.Tablet {
	return sbc.tablet
}

func (sbc *SandboxConn) getNextResult() *sqltypes.Result {
	if len(sbc.results) != 0 {
		r := sbc.results[0]
		sbc.results = sbc.results[1:]
		return r
	}
	return SingleRowResult
}

// SingleRowResult is returned when there is no pre-stored result.
var SingleRowResult = &sqltypes.Result{
	Fields: []*querypb.Field{
		{Name:"id", Type:sqltypes.Int32},
		{Name:"value", Type:sqltypes.VarChar},
	},
	RowsAffected: 1,
	InsertID:     0,
	Rows: [][]sqltypes.Value{{
		sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo")),
	}},
}
