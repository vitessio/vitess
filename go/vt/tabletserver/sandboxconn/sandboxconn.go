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

	MustFailRetry  int
	MustFailFatal  int
	MustFailServer int
	MustFailConn   int
	MustFailTxPool int
	MustFailNotTx  int

	// These Count vars report how often the corresponding
	// functions were called.
	ExecCount               sync2.AtomicInt64
	BeginCount              sync2.AtomicInt64
	CommitCount             sync2.AtomicInt64
	RollbackCount           sync2.AtomicInt64
	AsTransactionCount      sync2.AtomicInt64
	PrepareCount            sync2.AtomicInt64
	CommitPreparedCount     sync2.AtomicInt64
	RollbackPreparedCount   sync2.AtomicInt64
	CreateTransactionCount  sync2.AtomicInt64
	StartCommitCount        sync2.AtomicInt64
	SetRollbackCount        sync2.AtomicInt64
	ResolveTransactionCount sync2.AtomicInt64
	ReadTransactionCount    sync2.AtomicInt64

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
	return sbc.getError()
}

// CommitPrepared commits the prepared transaction.
func (sbc *SandboxConn) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	sbc.CommitPreparedCount.Add(1)
	return sbc.getError()
}

// RollbackPrepared rolls back the prepared transaction.
func (sbc *SandboxConn) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	sbc.RollbackPreparedCount.Add(1)
	return sbc.getError()
}

// CreateTransaction creates the metadata for a 2PC transaction.
func (sbc *SandboxConn) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	sbc.CreateTransactionCount.Add(1)
	return sbc.getError()
}

// StartCommit atomically commits the transaction along with the
// decision to commit the associated 2pc transaction.
func (sbc *SandboxConn) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	sbc.StartCommitCount.Add(1)
	return sbc.getError()
}

// SetRollback transitions the 2pc transaction to the Rollback state.
// If a transaction id is provided, that transaction is also rolled back.
func (sbc *SandboxConn) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	sbc.SetRollbackCount.Add(1)
	return sbc.getError()
}

// ResolveTransaction deletes the 2pc transaction metadata
// essentially resolving it.
func (sbc *SandboxConn) ResolveTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	sbc.ResolveTransactionCount.Add(1)
	return sbc.getError()
}

// ReadTransaction returns the metadata for the sepcified dtid.
func (sbc *SandboxConn) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	sbc.ReadTransactionCount.Add(1)
	return nil, sbc.getError()
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

// SplitQuery creates splits from the original query by appending the
// split index as a comment to the SQL. RowCount is always SandboxSQRowCount
func (sbc *SandboxConn) SplitQuery(ctx context.Context, target *querypb.Target, query querytypes.BoundQuery, splitColumn string, splitCount int64) ([]querytypes.QuerySplit, error) {
	splits := []querytypes.QuerySplit{}
	for i := 0; i < int(splitCount); i++ {
		split := querytypes.QuerySplit{
			Sql:           fmt.Sprintf("%s /*split %v */", query.Sql, i),
			BindVariables: query.BindVariables,
			RowCount:      SandboxSQRowCount,
		}
		splits = append(splits, split)
	}
	return splits, nil
}

// SplitQueryV2 returns a single QuerySplit whose 'sql' field describes the received arguments.
// TODO(erez): Rename to SplitQuery after the migration to SplitQuery V2 is done.
func (sbc *SandboxConn) SplitQueryV2(
	ctx context.Context,
	target *querypb.Target,
	query querytypes.BoundQuery,
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) ([]querytypes.QuerySplit, error) {
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
func (sbc *SandboxConn) Close() {
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
		{"id", sqltypes.Int32},
		{"value", sqltypes.VarChar},
	},
	RowsAffected: 1,
	InsertID:     0,
	Rows: [][]sqltypes.Value{{
		sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo")),
	}},
}
