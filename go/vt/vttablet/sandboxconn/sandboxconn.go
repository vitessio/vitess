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

// Package sandboxconn provides a fake QueryService implementation for tests.
// It can return real results, and simulate error cases.
package sandboxconn

import (
	"fmt"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// SandboxConn satisfies the QueryService interface
type SandboxConn struct {
	tablet *topodatapb.Tablet

	// These errors work for all functions.
	MustFailCodes map[vtrpcpb.Code]int

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
	Queries []*querypb.BoundQuery

	// BatchQueries stores the batch requests received
	// Each batch request is inlined as a slice of Queries.
	BatchQueries [][]*querypb.BoundQuery

	// Options stores the options received by all calls.
	Options []*querypb.ExecuteOptions

	// results specifies the results to be returned.
	// They're consumed as results are returned. If there are
	// no results left, SingleRowResult is returned.
	results []*sqltypes.Result

	// ReadTransactionResults is used for returning results for ReadTransaction.
	ReadTransactionResults []*querypb.TransactionMetadata

	MessageIDs []*querypb.Value

	// transaction id generator
	TransactionID sync2.AtomicInt64
}

var _ queryservice.QueryService = (*SandboxConn)(nil) // compile-time interface check

// NewSandboxConn returns a new SandboxConn targeted to the provided tablet.
func NewSandboxConn(t *topodatapb.Tablet) *SandboxConn {
	return &SandboxConn{
		tablet:        t,
		MustFailCodes: make(map[vtrpcpb.Code]int),
	}
}

func (sbc *SandboxConn) getError() error {
	for code, count := range sbc.MustFailCodes {
		if count == 0 {
			continue
		}
		sbc.MustFailCodes[code] = count - 1
		return vterrors.New(code, fmt.Sprintf("%v error", code))
	}
	return nil
}

// SetResults sets what this con should return next time.
func (sbc *SandboxConn) SetResults(r []*sqltypes.Result) {
	sbc.results = r
}

// Execute is part of the QueryService interface.
func (sbc *SandboxConn) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	sbc.ExecCount.Add(1)
	bv := make(map[string]*querypb.BindVariable)
	for k, v := range bindVars {
		bv[k] = v
	}
	sbc.Queries = append(sbc.Queries, &querypb.BoundQuery{
		Sql:           query,
		BindVariables: bv,
	})
	sbc.Options = append(sbc.Options, options)
	if err := sbc.getError(); err != nil {
		return nil, err
	}
	return sbc.getNextResult(), nil
}

// ExecuteBatch is part of the QueryService interface.
func (sbc *SandboxConn) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
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

// StreamExecute is part of the QueryService interface.
func (sbc *SandboxConn) StreamExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	sbc.ExecCount.Add(1)
	bv := make(map[string]*querypb.BindVariable)
	for k, v := range bindVars {
		bv[k] = v
	}
	sbc.Queries = append(sbc.Queries, &querypb.BoundQuery{
		Sql:           query,
		BindVariables: bv,
	})
	sbc.Options = append(sbc.Options, options)
	err := sbc.getError()
	if err != nil {
		return err
	}
	return callback(sbc.getNextResult())
}

// Begin is part of the QueryService interface.
func (sbc *SandboxConn) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (int64, error) {
	sbc.BeginCount.Add(1)
	err := sbc.getError()
	if err != nil {
		return 0, err
	}
	return sbc.TransactionID.Add(1), nil
}

// Commit is part of the QueryService interface.
func (sbc *SandboxConn) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	sbc.CommitCount.Add(1)
	return sbc.getError()
}

// Rollback is part of the QueryService interface.
func (sbc *SandboxConn) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	sbc.RollbackCount.Add(1)
	return sbc.getError()
}

// Prepare prepares the specified transaction.
func (sbc *SandboxConn) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	sbc.PrepareCount.Add(1)
	if sbc.MustFailPrepare > 0 {
		sbc.MustFailPrepare--
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "error: err")
	}
	return sbc.getError()
}

// CommitPrepared commits the prepared transaction.
func (sbc *SandboxConn) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	sbc.CommitPreparedCount.Add(1)
	if sbc.MustFailCommitPrepared > 0 {
		sbc.MustFailCommitPrepared--
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "error: err")
	}
	return sbc.getError()
}

// RollbackPrepared rolls back the prepared transaction.
func (sbc *SandboxConn) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	sbc.RollbackPreparedCount.Add(1)
	if sbc.MustFailRollbackPrepared > 0 {
		sbc.MustFailRollbackPrepared--
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "error: err")
	}
	return sbc.getError()
}

// CreateTransaction creates the metadata for a 2PC transaction.
func (sbc *SandboxConn) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	sbc.CreateTransactionCount.Add(1)
	if sbc.MustFailCreateTransaction > 0 {
		sbc.MustFailCreateTransaction--
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "error: err")
	}
	return sbc.getError()
}

// StartCommit atomically commits the transaction along with the
// decision to commit the associated 2pc transaction.
func (sbc *SandboxConn) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	sbc.StartCommitCount.Add(1)
	if sbc.MustFailStartCommit > 0 {
		sbc.MustFailStartCommit--
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "error: err")
	}
	return sbc.getError()
}

// SetRollback transitions the 2pc transaction to the Rollback state.
// If a transaction id is provided, that transaction is also rolled back.
func (sbc *SandboxConn) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	sbc.SetRollbackCount.Add(1)
	if sbc.MustFailSetRollback > 0 {
		sbc.MustFailSetRollback--
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "error: err")
	}
	return sbc.getError()
}

// ConcludeTransaction deletes the 2pc transaction metadata
// essentially resolving it.
func (sbc *SandboxConn) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	sbc.ConcludeTransactionCount.Add(1)
	if sbc.MustFailConcludeTransaction > 0 {
		sbc.MustFailConcludeTransaction--
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "error: err")
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

// BeginExecute is part of the QueryService interface.
func (sbc *SandboxConn) BeginExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, error) {
	transactionID, err := sbc.Begin(ctx, target, options)
	if err != nil {
		return nil, 0, err
	}
	result, err := sbc.Execute(ctx, target, query, bindVars, transactionID, options)
	return result, transactionID, err
}

// BeginExecuteBatch is part of the QueryService interface.
func (sbc *SandboxConn) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, int64, error) {
	transactionID, err := sbc.Begin(ctx, target, options)
	if err != nil {
		return nil, 0, err
	}
	results, err := sbc.ExecuteBatch(ctx, target, queries, asTransaction, transactionID, options)
	return results, transactionID, err
}

// MessageStream is part of the QueryService interface.
func (sbc *SandboxConn) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) (err error) {
	if err := sbc.getError(); err != nil {
		return err
	}
	r := sbc.getNextResult()
	if r == nil {
		return nil
	}
	callback(r)
	return nil
}

// MessageAck is part of the QueryService interface.
func (sbc *SandboxConn) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error) {
	sbc.MessageIDs = ids
	return int64(len(ids)), nil
}

// SandboxSQRowCount is the default number of fake splits returned.
var SandboxSQRowCount = int64(10)

// SplitQuery returns a single QuerySplit whose 'sql' field describes the received arguments.
func (sbc *SandboxConn) SplitQuery(
	ctx context.Context,
	target *querypb.Target,
	query *querypb.BoundQuery,
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) ([]*querypb.QuerySplit, error) {
	err := sbc.getError()
	if err != nil {
		return nil, err
	}
	splits := []*querypb.QuerySplit{
		{
			Query: &querypb.BoundQuery{
				Sql: fmt.Sprintf(
					"query:%v, splitColumns:%v, splitCount:%v,"+
						" numRowsPerQueryPart:%v, algorithm:%v, shard:%v",
					query, splitColumns, splitCount, numRowsPerQueryPart, algorithm, sbc.tablet.Shard),
			},
		},
	}
	return splits, nil
}

// StreamHealth is not implemented.
func (sbc *SandboxConn) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	return fmt.Errorf("not implemented in test")
}

// UpdateStream is part of the QueryService interface.
func (sbc *SandboxConn) UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64, callback func(*querypb.StreamEvent) error) error {
	return fmt.Errorf("not implemented in test")
}

// VStream is part of the QueryService interface.
func (sbc *SandboxConn) VStream(ctx context.Context, target *querypb.Target, startPos string, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	return fmt.Errorf("not implemented in test")
}

// VStreamRows is part of the QueryService interface.
func (sbc *SandboxConn) VStreamRows(ctx context.Context, target *querypb.Target, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	return fmt.Errorf("not implemented in test")
}

// HandlePanic is part of the QueryService interface.
func (sbc *SandboxConn) HandlePanic(err *error) {
}

// Close does not change ExecCount
func (sbc *SandboxConn) Close(ctx context.Context) error {
	return nil
}

// Tablet is part of the QueryService interface.
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
		{Name: "id", Type: sqltypes.Int32},
		{Name: "value", Type: sqltypes.VarChar},
	},
	RowsAffected: 1,
	InsertID:     0,
	Rows: [][]sqltypes.Value{{
		sqltypes.NewInt32(1),
		sqltypes.NewVarChar("foo"),
	}},
}

// StreamRowResult is SingleRowResult with RowsAffected set to 0.
var StreamRowResult = &sqltypes.Result{
	Fields: []*querypb.Field{
		{Name: "id", Type: sqltypes.Int32},
		{Name: "value", Type: sqltypes.VarChar},
	},
	RowsAffected: 0,
	InsertID:     0,
	Rows: [][]sqltypes.Value{{
		sqltypes.NewInt32(1),
		sqltypes.NewVarChar("foo"),
	}},
}
