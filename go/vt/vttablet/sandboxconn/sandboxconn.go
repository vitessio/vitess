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

// Package sandboxconn provides a fake QueryService implementation for tests.
// It can return real results, and simulate error cases.
package sandboxconn

import (
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"

	"context"

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
	ReserveCount             sync2.AtomicInt64
	ReleaseCount             sync2.AtomicInt64

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

	// vstream expectations.
	StartPos      string
	VStreamEvents [][]*binlogdatapb.VEvent
	VStreamErrors []error
	VStreamCh     chan *binlogdatapb.VEvent

	// transaction id generator
	TransactionID sync2.AtomicInt64

	// reserve id generator
	ReserveID sync2.AtomicInt64

	mapMu     sync.Mutex //protects the map txIDToRID
	txIDToRID map[int64]int64

	sExecMu sync.Mutex
	execMu  sync.Mutex

	// this error will only happen once
	EphemeralShardErr error
}

var _ queryservice.QueryService = (*SandboxConn)(nil) // compile-time interface check

// NewSandboxConn returns a new SandboxConn targeted to the provided tablet.
func NewSandboxConn(t *topodatapb.Tablet) *SandboxConn {
	return &SandboxConn{
		tablet:        t,
		MustFailCodes: make(map[vtrpcpb.Code]int),
		txIDToRID:     make(map[int64]int64),
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
	if sbc.EphemeralShardErr != nil {
		err := sbc.EphemeralShardErr
		sbc.EphemeralShardErr = nil
		return err
	}
	return nil
}

// SetResults sets what this con should return next time.
func (sbc *SandboxConn) SetResults(r []*sqltypes.Result) {
	sbc.results = r
}

// Execute is part of the QueryService interface.
func (sbc *SandboxConn) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	sbc.execMu.Lock()
	defer sbc.execMu.Unlock()
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

	stmt, _ := sqlparser.Parse(query) // knowingly ignoring the error
	return sbc.getNextResult(stmt), nil
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
		result = append(result, *(sbc.getNextResult(nil)))
	}
	return result, nil
}

// StreamExecute is part of the QueryService interface.
func (sbc *SandboxConn) StreamExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	sbc.sExecMu.Lock()
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
		sbc.sExecMu.Unlock()
		return err
	}
	parse, _ := sqlparser.Parse(query)
	nextRs := sbc.getNextResult(parse)
	sbc.sExecMu.Unlock()

	return callback(nextRs)
}

// Begin is part of the QueryService interface.
func (sbc *SandboxConn) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (int64, *topodatapb.TabletAlias, error) {
	return sbc.begin(ctx, target, nil, 0, options)
}

func (sbc *SandboxConn) begin(ctx context.Context, target *querypb.Target, preQueries []string, reservedID int64, options *querypb.ExecuteOptions) (int64, *topodatapb.TabletAlias, error) {
	sbc.BeginCount.Add(1)
	err := sbc.getError()
	if err != nil {
		return 0, nil, err
	}

	transactionID := reservedID
	if transactionID == 0 {
		transactionID = sbc.TransactionID.Add(1)
	}
	for _, preQuery := range preQueries {
		_, err := sbc.Execute(ctx, target, preQuery, nil, transactionID, reservedID, options)
		if err != nil {
			return 0, nil, err
		}
	}
	return transactionID, sbc.tablet.Alias, nil
}

// Commit is part of the QueryService interface.
func (sbc *SandboxConn) Commit(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	sbc.CommitCount.Add(1)
	reservedID := sbc.getTxReservedID(transactionID)
	if reservedID != 0 {
		reservedID = sbc.ReserveID.Add(1)
	}
	return reservedID, sbc.getError()
}

// Rollback is part of the QueryService interface.
func (sbc *SandboxConn) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	sbc.RollbackCount.Add(1)
	reservedID := sbc.getTxReservedID(transactionID)
	if reservedID != 0 {
		reservedID = sbc.ReserveID.Add(1)
	}
	return reservedID, sbc.getError()
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
func (sbc *SandboxConn) BeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, query string, bindVars map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	transactionID, alias, err := sbc.begin(ctx, target, preQueries, reservedID, options)
	if transactionID != 0 {
		sbc.setTxReservedID(transactionID, reservedID)
	}
	if err != nil {
		return nil, 0, nil, err
	}
	result, err := sbc.Execute(ctx, target, query, bindVars, transactionID, reservedID, options)
	return result, transactionID, alias, err
}

// BeginExecuteBatch is part of the QueryService interface.
func (sbc *SandboxConn) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	transactionID, alias, err := sbc.Begin(ctx, target, options)
	if err != nil {
		return nil, 0, nil, err
	}
	results, err := sbc.ExecuteBatch(ctx, target, queries, asTransaction, transactionID, options)
	return results, transactionID, alias, err
}

// MessageStream is part of the QueryService interface.
func (sbc *SandboxConn) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) (err error) {
	if err := sbc.getError(); err != nil {
		return err
	}
	r := sbc.getNextResult(nil)
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

// StreamHealth is not implemented.
func (sbc *SandboxConn) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	return fmt.Errorf("not implemented in test")
}

// ExpectVStreamStartPos makes the conn verify that that the next vstream request has the right startPos.
func (sbc *SandboxConn) ExpectVStreamStartPos(startPos string) {
	sbc.StartPos = startPos
}

// AddVStreamEvents adds a set of VStream events to be returned.
func (sbc *SandboxConn) AddVStreamEvents(events []*binlogdatapb.VEvent, err error) {
	sbc.VStreamEvents = append(sbc.VStreamEvents, events)
	sbc.VStreamErrors = append(sbc.VStreamErrors, err)
}

// VStream is part of the QueryService interface.
func (sbc *SandboxConn) VStream(ctx context.Context, target *querypb.Target, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	if sbc.StartPos != "" && sbc.StartPos != startPos {
		log.Errorf("startPos(%v): %v, want %v", target, startPos, sbc.StartPos)
		return fmt.Errorf("startPos(%v): %v, want %v", target, startPos, sbc.StartPos)
	}
	done := false
	// for testing the minimize stream skew feature (TestStreamSkew) we need the ability to send events in specific sequences from
	// multiple streams. We introduce a channel in the sandbox that we listen on and vstream those events
	// as we receive them. We also need to simulate vstreamer heartbeats since the skew detection logic depends on it
	// in case of shards where there are no real events within a second
	if sbc.VStreamCh != nil {
		lastTimestamp := int64(0)
		for !done {
			timer := time.NewTimer(1 * time.Second)
			select {
			case <-timer.C:
				events := []*binlogdatapb.VEvent{{
					Type:        binlogdatapb.VEventType_HEARTBEAT,
					Timestamp:   lastTimestamp,
					CurrentTime: lastTimestamp,
				}, {
					Type:        binlogdatapb.VEventType_COMMIT,
					Timestamp:   lastTimestamp,
					CurrentTime: lastTimestamp,
				}}

				if err := send(events); err != nil {
					log.Infof("error sending event in test sandbox %s", err.Error())
					return err
				}
				lastTimestamp++

			case ev := <-sbc.VStreamCh:
				if ev == nil {
					done = true
				}
				if err := send([]*binlogdatapb.VEvent{ev}); err != nil {
					log.Infof("error sending event in test sandbox %s", err.Error())
					return err
				}
				lastTimestamp = ev.Timestamp
			}
		}
	} else {
		// this path is followed for all vstream tests other than the skew tests
		for len(sbc.VStreamEvents) != 0 {
			ev := sbc.VStreamEvents[0]
			err := sbc.VStreamErrors[0]
			sbc.VStreamEvents = sbc.VStreamEvents[1:]
			sbc.VStreamErrors = sbc.VStreamErrors[1:]
			if ev == nil {
				return err
			}
			if err := send(ev); err != nil {
				return err
			}
		}
	}
	// Don't return till context is canceled.
	<-ctx.Done()
	return ctx.Err()
}

// VStreamRows is part of the QueryService interface.
func (sbc *SandboxConn) VStreamRows(ctx context.Context, target *querypb.Target, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	return fmt.Errorf("not implemented in test")
}

// VStreamResults is part of the QueryService interface.
func (sbc *SandboxConn) VStreamResults(ctx context.Context, target *querypb.Target, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error {
	return fmt.Errorf("not implemented in test")
}

// QueryServiceByAlias is part of the Gateway interface.
func (sbc *SandboxConn) QueryServiceByAlias(_ *topodatapb.TabletAlias) (queryservice.QueryService, error) {
	return sbc, nil
}

// HandlePanic is part of the QueryService interface.
func (sbc *SandboxConn) HandlePanic(err *error) {
}

//ReserveBeginExecute implements the QueryService interface
func (sbc *SandboxConn) ReserveBeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, int64, *topodatapb.TabletAlias, error) {
	reservedID := sbc.reserve(ctx, target, preQueries, bindVariables, 0, options)
	result, transactionID, alias, err := sbc.BeginExecute(ctx, target, nil, sql, bindVariables, reservedID, options)
	if transactionID != 0 {
		sbc.setTxReservedID(transactionID, reservedID)
	}
	if err != nil {
		return nil, transactionID, reservedID, alias, err
	}
	return result, transactionID, reservedID, alias, nil
}

//ReserveExecute implements the QueryService interface
func (sbc *SandboxConn) ReserveExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	reservedID := sbc.reserve(ctx, target, preQueries, bindVariables, transactionID, options)
	result, err := sbc.Execute(ctx, target, sql, bindVariables, transactionID, reservedID, options)
	if transactionID != 0 {
		sbc.setTxReservedID(transactionID, reservedID)
	}
	if err != nil {
		return nil, 0, nil, err
	}
	return result, reservedID, sbc.tablet.Alias, nil
}

func (sbc *SandboxConn) reserve(ctx context.Context, target *querypb.Target, preQueries []string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) int64 {
	sbc.ReserveCount.Add(1)
	for _, query := range preQueries {
		sbc.Execute(ctx, target, query, bindVariables, transactionID, 0, options)
	}
	if transactionID != 0 {
		return transactionID
	}
	return sbc.ReserveID.Add(1)
}

//Release implements the QueryService interface
func (sbc *SandboxConn) Release(ctx context.Context, target *querypb.Target, transactionID, reservedID int64) error {
	sbc.ReleaseCount.Add(1)
	return sbc.getError()
}

// Close does not change ExecCount
func (sbc *SandboxConn) Close(ctx context.Context) error {
	return nil
}

// Tablet is part of the QueryService interface.
func (sbc *SandboxConn) Tablet() *topodatapb.Tablet {
	return sbc.tablet
}

func (sbc *SandboxConn) getNextResult(stmt sqlparser.Statement) *sqltypes.Result {
	if len(sbc.results) != 0 {
		r := sbc.results[0]
		sbc.results = sbc.results[1:]
		return r
	}
	if stmt == nil {
		// if we didn't get a valid query, we'll assume we need a SELECT
		return getSingleRowResult()
	}
	switch stmt.(type) {
	case *sqlparser.Select,
		*sqlparser.Union,
		*sqlparser.Show,
		sqlparser.Explain,
		*sqlparser.OtherRead:
		return getSingleRowResult()
	case *sqlparser.Set,
		sqlparser.DDLStatement,
		*sqlparser.AlterVschema,
		*sqlparser.Use,
		*sqlparser.OtherAdmin,
		*sqlparser.SetTransaction,
		*sqlparser.Savepoint,
		*sqlparser.SRollback,
		*sqlparser.Release:
		return &sqltypes.Result{}
	}

	// for everything else we fake a single row being affected
	return &sqltypes.Result{RowsAffected: 1}
}

func (sbc *SandboxConn) setTxReservedID(transactionID int64, reservedID int64) {
	sbc.mapMu.Lock()
	defer sbc.mapMu.Unlock()
	sbc.txIDToRID[transactionID] = reservedID
}

func (sbc *SandboxConn) getTxReservedID(txID int64) int64 {
	sbc.mapMu.Lock()
	defer sbc.mapMu.Unlock()
	return sbc.txIDToRID[txID]
}

//StringQueries returns the queries executed as a slice of strings
func (sbc *SandboxConn) StringQueries() []string {
	result := make([]string, len(sbc.Queries))
	for i, query := range sbc.Queries {
		result[i] = query.Sql
	}
	return result
}

// getSingleRowResult is used to get a SingleRowResult but it creates separate fields because some tests change the fields
// If these fields are not created separately then the constants value also changes which leads to some other tests failing later
func getSingleRowResult() *sqltypes.Result {
	singleRowResult := &sqltypes.Result{
		InsertID:    SingleRowResult.InsertID,
		StatusFlags: SingleRowResult.StatusFlags,
		Rows:        SingleRowResult.Rows,
	}

	for _, field := range SingleRowResult.Fields {
		singleRowResult.Fields = append(singleRowResult.Fields, &querypb.Field{
			Name: field.Name,
			Type: field.Type,
		})
	}

	return singleRowResult
}

// SingleRowResult is returned when there is no pre-stored result.
var SingleRowResult = &sqltypes.Result{
	Fields: []*querypb.Field{
		{Name: "id", Type: sqltypes.Int32},
		{Name: "value", Type: sqltypes.VarChar},
	},
	InsertID: 0,
	Rows: [][]sqltypes.Value{{
		sqltypes.NewInt32(1),
		sqltypes.NewVarChar("foo"),
	}},
	StatusFlags: sqltypes.ServerStatusAutocommit,
}

// StreamRowResult is SingleRowResult with RowsAffected set to 0.
var StreamRowResult = &sqltypes.Result{
	Fields: []*querypb.Field{
		{Name: "id", Type: sqltypes.Int32},
		{Name: "value", Type: sqltypes.VarChar},
	},
	Rows: [][]sqltypes.Value{{
		sqltypes.NewInt32(1),
		sqltypes.NewVarChar("foo"),
	}},
}
